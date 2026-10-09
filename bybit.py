"""Исполнение сигналов на Bybit — настоящий paper trading, а не имитация.

Это единственное, чего нельзя добиться внутри бота: ордер получает
биржа, деньги фальшивые, но всё остальное настоящее — подпись запросов,
плечо, стоп-лосс и тейк-профит стоят на бирже и срабатывают без
участия процесса. Поэтому позиция переживает даже полную потерю базы:
она читается с биржи, а не из SQLite.

По умолчанию — тестнет (BYBIT_TESTNET=1). Боевой режим включается
явно, переменной BYBIT_TESTNET=0 вместе с боевыми ключами: код от
этого не меняется, это и есть цель — после 2–3 недель paper trading
переключиться на реальные деньги сменой двух переменных.

Анализ рынка остаётся в market.py: данные тестнета неполные, а входить
надо по тем же ценам, по которым считался сигнал.

Ключи живут только в переменных окружения и в git не попадают.
"""

import hashlib
import hmac
import json
import math
import os
import time
from urllib.parse import urlencode

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=10)
RECV_WINDOW = "5000"

TESTNET = os.getenv("BYBIT_TESTNET", "1") != "0"
# .strip(): значение, скопированное в дашборд с пробелом или переводом строки,
# ломает подпись молча — а 401 без объяснений ищется долго.
KEY = os.getenv("BYBIT_KEY", "").strip()
SECRET = os.getenv("BYBIT_SECRET", "").strip()
BASE = os.getenv("BYBIT_API") or (
    "https://api-testnet.bybit.com" if TESTNET else "https://api.bybit.com"
)
CATEGORY = "linear"      # USDT-перпетуал, как и расчёты бота с плечом
SYMBOL = "BTCUSDT"       # bot.SYMBOL = "BTC/USDT"

KEY_NAMES = "BYBIT_KEY / BYBIT_SECRET"
KEY_HINT = "тестнет: testnet.bybit.com → API"


class BybitError(RuntimeError):
    """Биржа ответила ошибкой или ключей нет."""


def mode() -> str:
    """«testnet» или «mainnet» — для сообщений и статуса."""
    return "testnet" if TESTNET else "mainnet"


def venue() -> str:
    """Название биржи для сообщений: бот не должен врать, куда встал ордер."""
    return "Bybit"


def enabled() -> bool:
    """Выставлять ли ордера вообще: нужны оба ключа."""
    return bool(KEY and SECRET)


def sign(payload: str, secret: str = None) -> str:
    """HMAC-SHA256 по правилу Bybit: timestamp + api_key + recv_window + payload.

    secret подаётся параметром, чтобы тесты могли проверить вектор
    на известном ключе, не трогая переменные окружения.
    """
    key = SECRET if secret is None else secret
    return hmac.new(key.encode(), payload.encode(), hashlib.sha256).hexdigest()


def round_to(value: float, step: float) -> float:
    """Округление до шага биржи (тиковый размер, шаг лота)."""
    if step <= 0:
        return value
    return round(round(value / step) * step, 10)


def round_down(value: float, step: float) -> float:
    """Округление вниз — для стопа: он не должен подобраться к входу."""
    if step <= 0:
        return value
    return round(math.floor(value / step) * step, 10)


def round_up(value: float, step: float) -> float:
    if step <= 0:
        return value
    return round(math.ceil(value / step) * step, 10)


def num(value) -> str:
    """Число в формате биржи: без хвостовых нулей, точка как разделитель."""
    return f"{float(value):.12f}".rstrip("0").rstrip(".") or "0"


def build_qty(size: float, price: float, info: dict) -> tuple:
    """Количество монет в шаг лота вниз + проверки минимумов биржи.

    Возвращает (qty, None) если можно ставить, и (None, причина) если нет:
    биржа отвергла бы такой ордер, а падать на этом не нужно.
    """
    step = info["step"]
    qty = round_down(size, step)
    if qty <= 0:
        return None, f"расчётное количество {size:.6f} меньше шага лота {step}"
    if qty < info["min_qty"]:
        return None, f"количество {qty} меньше минимума биржи {info['min_qty']}"
    notional = qty * price
    if notional < info["min_notional"]:
        return None, (
            f"объём {notional:.2f} USDT меньше минимума биржи "
            f"{info['min_notional']} USDT"
        )
    return qty, None


async def _request(method: str, path: str, *, params=None, body=None, signed=False):
    """Один запрос к Bybit v5. Ошибка биржи поднимается как BybitError."""
    if signed and not enabled():
        raise BybitError("ключей Bybit нет — задай BYBIT_KEY и BYBIT_SECRET")

    query = urlencode(params) if params else ""
    url = f"{BASE}{path}" + (f"?{query}" if query else "")
    payload = query if method == "GET" else json.dumps(body or {}, ensure_ascii=False)

    headers = {"User-Agent": "crypto-bot/1.0"}
    if signed:
        stamp = int(time.time() * 1000)
        headers.update(
            {
                "X-BAPI-API-KEY": KEY,
                "X-BAPI-TIMESTAMP": str(stamp),
                "X-BAPI-RECV-WINDOW": RECV_WINDOW,
                "X-BAPI-SIGN": sign(f"{stamp}{KEY}{RECV_WINDOW}{payload}"),
            }
        )
    if method == "POST":
        headers["Content-Type"] = "application/json"

    async with aiohttp.ClientSession(headers=headers) as session:
        kwargs = {"data": payload} if method == "POST" else {}
        async with session.request(method, url, timeout=TIMEOUT, **kwargs) as response:
            status = response.status
            raw = await response.text(errors="replace")

    # Ответ читаем текстом и парсим сами: иначе пустое тело от 401
    # превращается в невнятное «Expecting property name enclosed...».
    try:
        data = json.loads(raw) if raw.strip() else {}
    except ValueError as exc:
        raise BybitError(
            f"биржа вернула не JSON (HTTP {status}), начало ответа: {raw[:200]!r}"
        ) from exc

    if status >= 400:
        detail = data.get("retMsg") or (raw[:200] if raw.strip() else "пустое тело")
        hint = ""
        if status in (401, 403):
            hint = (
                " — ключ отклонён: проверь, что создан на том же хосте "
                "(testnet.bybit.com ↔ api-testnet.bybit.com), что в него не "
                "попали пробелы и что выданы права на торговлю"
            )
        raise BybitError(f"HTTP {status} (retCode={data.get('retCode')}): {detail}{hint}")

    if data.get("retCode", -1) != 0:
        raise BybitError(
            f"{data.get('retMsg', 'ошибка биржи')} (retCode={data.get('retCode')}, {path})"
        )
    return data.get("result") or {}


# --- Публичные данные (без ключей) ---------------------------------------

async def ticker_price() -> float:
    """Текущая цена — для проверки дрейфа входа и для статуса."""
    result = await _request(
        "GET", "/v5/market/tickers", params={"category": CATEGORY, "symbol": SYMBOL}
    )
    rows = result.get("list") or []
    if not rows:
        raise BybitError("биржа не вернула цену")
    return float(rows[0]["lastPrice"])


async def instrument_info() -> dict:
    """Шаг лота, минимум объёма и тиковый размер — у каждой биржи свои."""
    result = await _request(
        "GET",
        "/v5/market/instruments-info",
        params={"category": CATEGORY, "symbol": SYMBOL},
    )
    rows = result.get("list") or []
    if not rows:
        raise BybitError(f"инструмент {SYMBOL} не найден")
    item = rows[0]
    lot = item.get("lotSizeFilter") or {}
    price_filter = item.get("priceFilter") or {}
    return {
        "min_qty": float(lot.get("minOrderQty") or 0),
        "step": float(lot.get("qtyStep") or 0),
        "min_notional": float(lot.get("minNotionalValue") or 0),
        "tick": float(price_filter.get("tickSize") or 0),
    }


# --- Торговые операции (нужны ключи) -------------------------------------

async def set_leverage(leverage: float) -> None:
    await _request(
        "POST",
        "/v5/position/set-leverage",
        body={
            "category": CATEGORY,
            "symbol": SYMBOL,
            "buyLeverage": num(leverage),
            "sellLeverage": num(leverage),
        },
        signed=True,
    )


async def open_position(side: str, qty, entry, sl, tp, tick=0.1) -> str:
    """Маркет-вход с одновременным стопом и тейком.

    Стоп округляется в сторону, удалённую от входа: округление к входу
    сузило бы риск, а риск считает код, а не округление биржи.
    Возвращает orderId.
    """
    side = side.upper()
    if side == "LONG":
        stop = round_down(sl, tick)
        target = round_to(tp, tick)
    else:
        stop = round_up(sl, tick)
        target = round_to(tp, tick)

    result = await _request(
        "POST",
        "/v5/order/create",
        body={
            "category": CATEGORY,
            "symbol": SYMBOL,
            "side": "Buy" if side == "LONG" else "Sell",
            "orderType": "Market",
            "qty": num(qty),
            "timeInForce": "GTC",
            "stopLoss": num(stop),
            "takeProfit": num(target),
        },
        signed=True,
    )
    return result.get("orderId", "")


async def positions() -> list:
    """Открытые позиции с биржи — источник правды, а не база бота."""
    result = await _request(
        "GET",
        "/v5/position/list",
        params={"category": CATEGORY, "symbol": SYMBOL},
        signed=True,
    )
    out = []
    for item in result.get("list") or []:
        size = float(item.get("size") or 0)
        if size <= 0:
            continue
        out.append(
            {
                "side": item.get("side"),          # Buy | Sell
                "size": size,
                "entry": float(item.get("entryPrice") or 0),
                "mark": float(item.get("markPrice") or 0),
                "leverage": float(item.get("leverage") or 0),
                "value": float(item.get("positionValue") or 0),
                "pnl": float(item.get("unrealisedPnl") or 0),
                "liq": float(item.get("liqPrice") or 0),
                "sl": float(item.get("stopLoss") or 0),
                "tp": float(item.get("takeProfit") or 0),
            }
        )
    return out


async def cancel_all() -> None:
    """Снимает ожидающие ордера (на случай, если стоп/тейк висят отдельно)."""
    await _request(
        "POST",
        "/v5/order/cancel-all",
        body={"category": CATEGORY, "symbol": SYMBOL},
        signed=True,
    )


async def close_position() -> dict:
    """Закрывает позицию целиком по рынку."""
    return await _request(
        "POST",
        "/v5/position/close-reduce-only",
        body={"category": CATEGORY, "symbol": SYMBOL},
        signed=True,
    )


async def closed_pnl(limit: int = 20) -> list:
    """Закрытые сделки с биржи: здесь берётся настоящий результат."""
    result = await _request(
        "GET",
        "/v5/position/closed-pnl",
        params={"category": CATEGORY, "symbol": SYMBOL, "limit": str(limit)},
        signed=True,
    )
    out = []
    for item in result.get("list") or []:
        out.append(
            {
                "pnl": float(item.get("closedPnl") or 0),
                "side": item.get("side"),
                "qty": float(item.get("qty") or 0),
                "entry": float(item.get("avgEntryPrice") or 0),
                "exit": float(item.get("avgExitPrice") or 0),
                "fee": float(item.get("fee") or 0),
                "closed_at": int(item.get("updatedTime") or 0),
                "reason": item.get("orderType") or "",
            }
        )
    return out


async def wallet_balance() -> dict:
    """Баланс тестнета/боевого счёта. Тип аккаунта у всех разный."""
    last_error = None
    for account_type in ("UNIFIED", "CONTRACT"):
        try:
            result = await _request(
                "GET",
                "/v5/account/wallet-balance",
                params={"accountType": account_type},
                signed=True,
            )
        except BybitError as exc:
            last_error = exc
            continue
        rows = result.get("list") or []
        if not rows:
            continue
        row = rows[0]
        coins = row.get("coin") or []
        usdt = next((c for c in coins if c.get("coin") == "USDT"), None)
        return {
            "equity": float(row.get("totalEquity") or 0),
            "wallet": float(row.get("totalWalletBalance") or 0),
            "available": float(usdt.get("availableToWithdraw") or 0) if usdt else 0.0,
            "account_type": account_type,
        }
    raise BybitError(f"не удалось получить баланс: {last_error}")
