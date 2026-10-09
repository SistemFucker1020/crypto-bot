"""Исполнение сигналов на OKX Demo — настоящий paper trading, как на Bybit.

Зачем вторая биржа: тестнет Bybit даёт только спот, а фьючерсы с плечом
есть на боевом ключе — чужие деньги брать в работу нельзя. OKX Demo
закрывает эту дыру честно: живые котировки, перпетуал, плечо, стоп и
тейк стоят на бирже и срабатывают без участия процесса, деньги фальшивые.
Ключи Demo Trading API живут в переменных OKX_*; подпись обычная
HMAC-SHA256, но в формате OKX — Base64, а не hex, как у Bybit.

Демо-режим включается заголовком x-simulated-trading: 1. Один флаг
OKX_DEMO=0 снимает его — это и есть путь к реальным деньгам после
2–3 недель paper trading, код от этого не меняется.

Анализ рынка остаётся в market.py: сигнал считается по данным Bybit
(публичный API, без ключей), поэтому вход защищён проверкой дрейфа
MAX_ENTRY_DRIFT — расхождение двух бирж меньше лимита.

Ключи живут только в переменных окружения и в git не попадают.
"""

import base64
import hashlib
import hmac
import json
import math
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=10)

DEMO = os.getenv("OKX_DEMO", "1") != "0"
# .strip(): значение, скопированное в дашборд с пробелом или переводом строки,
# ломает подпись молча — а 401 без объяснений ищется долго.
KEY = os.getenv("OKX_KEY", "").strip()
SECRET = os.getenv("OKX_SECRET", "").strip()
PASSPHRASE = os.getenv("OKX_PASSPHRASE", "").strip()
BASE = os.getenv("OKX_API") or "https://www.okx.com"
INST = os.getenv("OKX_INST_ID", "BTC-USDT-SWAP")   # bot.SYMBOL = "BTC/USDT"
MGN_MODE = "cross"     # расчёт риска в коде бота, поэтому кросс-режим

KEY_NAMES = "OKX_KEY / OKX_SECRET / OKX_PASSPHRASE"
KEY_HINT = "демо: OKX → Avatar → Demo Trading API → Create"

CT_VAL = 0.0    # размер контракта в монетах; заполняется instrument_info()
POS_MODE = None  # «net» или «hedge» — узнаётся один раз у account/config


class OkxError(RuntimeError):
    """Биржа ответила ошибкой или ключей нет."""


def mode() -> str:
    """«demo» или «mainnet» — для сообщений и статуса."""
    return "demo" if DEMO else "mainnet"


def venue() -> str:
    """Название биржи для сообщений: бот не должен врать, куда встал ордер."""
    return "OKX"


def enabled() -> bool:
    """Выставлять ли ордера вообще: нужны все три части ключа."""
    return bool(KEY and SECRET and PASSPHRASE)


def sign(payload: str, secret: str = None) -> str:
    """Base64(HMAC-SHA256) по правилу OKX: timestamp+method+path+body.

    secret подаётся параметром, чтобы тесты могли проверить вектор
    на известном ключе, не трогая переменные окружения.
    """
    key = SECRET if secret is None else secret
    digest = hmac.new(key.encode(), payload.encode(), hashlib.sha256).digest()
    return base64.b64encode(digest).decode()


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

    OKX считает размер в контрактах (lotSz × ctVal), а бот считает
    позицию в монетах, поэтому шаг переведён в монеты, а контракты
    считаются заново в open_position — в чат и журнал попадают монеты.
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
    """Один запрос к OKX v5. Ошибка биржи поднимается как OkxError.

    Возвращает data — список полезной нагрузки; код «0» означает успех.
    """
    if signed and not enabled():
        raise OkxError("ключей OKX нет — задай " + KEY_NAMES)

    query = urlencode(params) if params else ""
    full_path = path + (f"?{query}" if query else "")
    url = f"{BASE}{full_path}"
    payload = "" if method == "GET" else json.dumps(body or {}, ensure_ascii=False)

    headers = {"User-Agent": "crypto-bot/1.0"}
    if DEMO:
        # Без него демо-ключ уходит на боевую торговлю и получает отказ.
        headers["x-simulated-trading"] = "1"
    if signed:
        now = time.time()
        stamp = (
            datetime.fromtimestamp(now, timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.")
            + f"{int(now * 1000) % 1000:03d}Z"
        )
        headers.update(
            {
                "OK-ACCESS-KEY": KEY,
                "OK-ACCESS-TIMESTAMP": stamp,
                "OK-ACCESS-PASSPHRASE": PASSPHRASE,
                "OK-ACCESS-SIGN": sign(f"{stamp}{method}{full_path}{payload}"),
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
        raise OkxError(
            f"биржа вернула не JSON (HTTP {status}), начало ответа: {raw[:200]!r}"
        ) from exc

    code = str(data.get("code", ""))
    if status >= 400 or code != "0":
        detail = data.get("msg") or (raw[:200] if raw.strip() else "пустое тело")
        hint = ""
        if status in (401, 403):
            hint = (
                " — ключ отклонён: проверь, что ключ создан в Demo Trading API, "
                "что passphrase совпадает с созданным при ключе и что в ключе "
                "не осталось пробелов"
            )
        elif code == "50102":
            hint = " — время запроса вне окна 30 секунд, проверь часы"
        elif code == "50111":
            hint = " — неверный ключ или passphrase демо-счёта"
        raise OkxError(
            f"HTTP {status} (code={code or '-'}, {path}): {detail}{hint}"
        )
    return data.get("data") or []


# --- Публичные данные (без ключей) ---------------------------------------

async def ticker_price() -> float:
    """Текущая цена — для проверки дрейфа входа и для статуса."""
    data = await _request("GET", "/api/v5/market/ticker", params={"instId": INST})
    if not data:
        raise OkxError("биржа не вернула цену")
    return float(data[0]["last"])


async def instrument_info() -> dict:
    """Шаг лота, минимум объёма и тиковый размер — у каждой биржи свои."""
    global CT_VAL
    data = await _request(
        "GET",
        "/api/v5/public/instruments",
        params={"instType": "SWAP", "instId": INST},
    )
    if not data:
        raise OkxError(f"инструмент {INST} не найден")
    item = data[0]
    ct_val = float(item.get("ctVal") or 0)
    lot = float(item.get("lotSz") or 0)
    if ct_val <= 0 or lot <= 0:
        raise OkxError(
            f"биржа не вернула размер контракта {INST}: "
            f"ctVal={item.get('ctVal')!r}, lotSz={item.get('lotSz')!r}"
        )
    CT_VAL = ct_val
    return {
        "min_qty": float(item.get("minSz") or 0) * ct_val,  # минимум в монетах
        "step": lot * ct_val,                               # шаг лота в монетах
        "min_notional": 0.0,   # у OKX нет минимума объёма на перпетуале
        "tick": float(item.get("tickSz") or 0),
        "ct_val": ct_val,
    }


async def _ct_val() -> float:
    """Размер контракта в монетах: instrument_info мог ещё не вызываться."""
    if CT_VAL <= 0:
        await instrument_info()
    return CT_VAL


async def _contracts(qty: float) -> str:
    """Монеты в контракты — единица, в которой OKX принимает sz."""
    ct_val = await _ct_val()
    contracts = round(float(qty) / ct_val, 9)
    if contracts <= 0:
        raise OkxError(f"количество {qty} монет меньше одного контракта ({ct_val})")
    return num(contracts)


async def _pos_mode() -> str:
    """«net» или «hedge»: от этого зависит, нужен ли posSide в ордере."""
    global POS_MODE
    if POS_MODE is None:
        data = await _request("GET", "/api/v5/account/config", signed=True)
        pos_mode = (data[0] if data else {}).get("posMode") or "net_mode"
        POS_MODE = "hedge" if pos_mode == "long_short_mode" else "net"
    return POS_MODE


# --- Торговые операции (нужны ключи) -------------------------------------

async def set_leverage(leverage: float) -> None:
    await _request(
        "POST",
        "/api/v5/account/set-leverage",
        body={"instId": INST, "lever": num(leverage), "mgnMode": MGN_MODE},
        signed=True,
    )


async def open_position(side: str, qty, entry, sl, tp, tick=0.1) -> str:
    """Маркет-вход с приложенным стопом и тейком.

    Стоп округляется в сторону, удалённую от входа: округление к входу
    сузило бы риск, а риск считает код, а не округление биржи.
    Возвращает ordId (по аналогии с orderId у Bybit).
    """
    side = side.upper()
    is_long = side == "LONG"
    if is_long:
        stop = round_down(sl, tick)
        target = round_to(tp, tick)
    else:
        stop = round_up(sl, tick)
        target = round_to(tp, tick)

    body = {
        "instId": INST,
        "tdMode": MGN_MODE,
        "side": "buy" if is_long else "sell",
        "ordType": "market",
        "sz": await _contracts(qty),
        "attachAlgoOrds": [
            {"slTriggerPx": num(stop), "slOrdPx": "-1",
             "tpTriggerPx": num(target), "tpOrdPx": "-1"}
        ],
    }
    if await _pos_mode() == "hedge":
        body["posSide"] = "long" if is_long else "short"

    data = await _request("POST", "/api/v5/trade/order", body=body, signed=True)
    row = data[0] if data else {}
    if str(row.get("sCode", "0")) != "0":
        raise OkxError(
            f"{row.get('sMsg') or 'ордер отклонён'} "
            f"(sCode={row.get('sCode')}, {INST})"
        )
    return row.get("ordId", "")


async def _pending_stops() -> tuple:
    """Стоп и тейк из ожидающих алго-ордеров: OKX держит их отдельно."""
    try:
        data = await _request(
            "GET",
            "/api/v5/trade/orders-algo-pending",
            params={"ordType": "conditional,oco", "instId": INST},
            signed=True,
        )
    except OkxError:
        return 0.0, 0.0   # показать позицию важнее, чем показать стоп
    sl = tp = 0.0
    for item in data:
        sl = sl or float(item.get("slTriggerPx") or 0)
        tp = tp or float(item.get("tpTriggerPx") or 0)
    return sl, tp


async def positions() -> list:
    """Открытые позиции с биржи — источник правды, а не база бота."""
    data = await _request(
        "GET", "/api/v5/account/positions", params={"instId": INST}, signed=True
    )
    ct_val = await _ct_val()
    out = []
    for item in data:
        pos = float(item.get("pos") or 0)
        if pos == 0:
            continue
        pos_side = item.get("posSide")
        if pos_side in ("long", "short"):      # хедж-режим: знак не несёт смысла
            side = "Buy" if pos_side == "long" else "Sell"
        else:                                   # односторонний: знак = направление
            side = "Buy" if pos > 0 else "Sell"
        out.append(
            {
                "side": side,
                "size": round(abs(pos) * ct_val, 12),   # контракты → монеты
                "entry": float(item.get("avgPx") or 0),
                "mark": float(item.get("markPx") or item.get("last") or 0),
                "leverage": float(item.get("lever") or 0),
                "value": float(item.get("notionalUsd") or 0),
                "pnl": float(item.get("upl") or 0),
                "liq": float(item.get("liqPx") or 0),
                "sl": 0.0,
                "tp": 0.0,
            }
        )
    if out:
        sl, tp = await _pending_stops()
        out[0]["sl"], out[0]["tp"] = sl, tp
    return out


async def cancel_all() -> None:
    """Снимает ожидающие ордера и алго-стопы (стоп/тейк висят отдельно)."""
    pending = await _request(
        "GET", "/api/v5/trade/orders-pending", params={"instId": INST}, signed=True
    )
    for item in pending:
        if item.get("ordId"):
            await _request(
                "POST",
                "/api/v5/trade/cancel-order",
                body={"instId": INST, "ordId": item["ordId"]},
                signed=True,
            )
    algos = await _request(
        "GET",
        "/api/v5/trade/orders-algo-pending",
        params={"ordType": "conditional,oco", "instId": INST},
        signed=True,
    )
    pairs = [{"algoId": a["algoId"], "instId": INST} for a in algos if a.get("algoId")]
    if pairs:
        await _request("POST", "/api/v5/trade/cancel-algos", body=pairs, signed=True)


async def close_position() -> list:
    """Закрывает позицию целиком по рынку."""
    body = {"instId": INST, "mgnMode": MGN_MODE}
    if await _pos_mode() == "hedge":
        data = await _request(
            "GET", "/api/v5/account/positions", params={"instId": INST}, signed=True
        )
        if data:
            body["posSide"] = data[0].get("posSide") or "net"
    return await _request(
        "POST", "/api/v5/trade/close-position", body=body, signed=True
    )


async def closed_pnl(limit: int = 20) -> list:
    """Закрытые сделки с биржи: здесь берётся настоящий результат."""
    data = await _request(
        "GET",
        "/api/v5/account/positions-history",
        params={"instType": "SWAP", "instId": INST, "limit": str(limit)},
        signed=True,
    )
    ct_val = await _ct_val()
    # Тип закрытия по OKX: 1 — частичное, 2 — всё, 3/4 — ликвидация, 5/6 — ADL.
    reasons = {
        "1": "PartialClose", "2": "Close", "3": "Liquidation",
        "4": "PartialLiquidation", "5": "ADL", "6": "ADL",
    }
    out = []
    for item in data:
        direction = item.get("direction") or item.get("posSide") or "long"
        out.append(
            {
                # realizedPnl уже включает комиссию и фондирование,
                # как и closedPnl у Bybit: суммы сравнимы между биржами.
                "pnl": float(item.get("realizedPnl") or 0),
                "side": "Buy" if direction == "long" else "Sell",
                "qty": round(float(item.get("closeTotalPos") or 0) * ct_val, 12),
                "entry": float(item.get("openAvgPx") or 0),
                "exit": float(item.get("closeAvgPx") or 0),
                "fee": float(item.get("fee") or 0),
                "closed_at": int(item.get("uTime") or item.get("cTime") or 0),
                "reason": reasons.get(item.get("type", ""), item.get("type") or ""),
            }
        )
    return out


async def wallet_balance() -> dict:
    """Баланс счёта. Тип аккаунта: демо или боевой — зависит от флага."""
    data = await _request(
        "GET", "/api/v5/account/balance", params={"ccy": "USDT"}, signed=True
    )
    if not data:
        raise OkxError("биржа не вернула баланс")
    row = data[0]
    usdt = next(
        (d for d in (row.get("details") or []) if d.get("ccy") == "USDT"), None
    )
    if usdt is None:
        raise OkxError("на счёте нет строки USDT — пополни демо-счёт")
    return {
        "equity": float(usdt.get("eq") or 0),
        "wallet": float(usdt.get("cashBal") or 0),
        "available": float(usdt.get("availBal") or 0),
        "account_type": "demo" if DEMO else "trading",
    }
