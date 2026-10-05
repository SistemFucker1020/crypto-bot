"""Сбор рыночных данных для AI-аналитика.

Бесплатные источники, ключи не нужны:
  * стакан и свечи    — Binance, запасной вариант Bybit
  * OI и funding      — Binance Futures, запасной Bybit
  * лонг/шорт ратио   — Binance Futures, запасной OKX
  * новости           — RSS CoinDesk + Cointelegraph

Binance блокирует часть регионов (в том числе IP некоторых хостингов),
поэтому у каждого рыночного блока есть запасная биржа: первый источник,
который ответил, и становится источником блока.

Недоступный источник попадает в контекст вместе с текстом ошибки и не
роняет весь анализ. Базовые адреса можно переопределить переменными
BINANCE_SPOT / BINANCE_FUT / BYBIT / OKX — так удобно проверять падение.
"""

import asyncio
import os
import xml.etree.ElementTree as ET

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=8)

BINANCE_SPOT = os.getenv("BINANCE_SPOT", "https://api.binance.com")
BINANCE_FUT = os.getenv("BINANCE_FUT", "https://fapi.binance.com")
BYBIT = os.getenv("BYBIT", "https://api.bybit.com")
OKX = os.getenv("OKX", "https://www.okx.com")

NEWS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
)
HEADERS = {"User-Agent": "crypto-bot/1.0"}

KLINE_LIMIT = 300          # больше 200 — нужен EMA200
BYBIT_INTERVAL = {"15m": "15", "1h": "60"}


async def _get_json(session, url, params=None):
    async with session.get(url, params=params, timeout=TIMEOUT) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def _with_fallback(providers):
    """Пробует источники по очереди. Побеждает первый, кто ответил.

    Каждый provider — корутина, возвращающая словарь. При неудаче
    ошибка запоминается, чтобы её можно было показать в логах.
    """
    errors = {}
    for name, factory in providers:
        try:
            result = await factory()
            if not result:
                raise ValueError("пустой ответ")
            result["source"] = name
            return result
        except Exception as exc:  # noqa: BLE001 - источник не должен ронять анализ
            errors[name] = f"{type(exc).__name__}: {exc}"

    return {"error": "; ".join(f"{name}: {err}" for name, err in errors.items())}


def _check_bybit(data):
    if data.get("retCode") != 0:
        raise ValueError(f"retCode {data.get('retCode')}: {data.get('retMsg')}")
    return data.get("result") or {}


def _check_okx(data):
    if str(data.get("code")) != "0":
        raise ValueError(f"code {data.get('code')}: {data.get('msg')}")
    return data.get("data") or []


# --- Индикаторы (считаются локально, без внешних сервисов) ---------------

def _ema(values, period):
    if len(values) < period:
        return None
    k = 2 / (period + 1)
    result = sum(values[:period]) / period
    for value in values[period:]:
        result = value * k + result * (1 - k)
    return result


def _rsi(closes, period=14):
    if len(closes) < period + 1:
        return None
    gains = [max(closes[i] - closes[i - 1], 0) for i in range(1, len(closes))]
    losses = [max(closes[i - 1] - closes[i], 0) for i in range(1, len(closes))]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)


def _atr(highs, lows, closes, period=14):
    if len(closes) < period + 1:
        return None
    true_ranges = [
        max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i - 1]),
            abs(lows[i] - closes[i - 1]),
        )
        for i in range(1, len(closes))
    ]
    result = sum(true_ranges[:period]) / period
    for value in true_ranges[period:]:
        result = (result * (period - 1) + value) / period
    return result


# --- Стакан ---------------------------------------------------------------

async def _binance_depth(session, symbol, limit):
    data = await _get_json(
        session, f"{BINANCE_SPOT}/api/v3/depth", {"symbol": symbol, "limit": limit}
    )
    return {
        "bids": [(float(p), float(q)) for p, q in data.get("bids", [])],
        "asks": [(float(p), float(q)) for p, q in data.get("asks", [])],
    }


async def _bybit_depth(session, symbol, limit):
    data = await _get_json(
        session,
        f"{BYBIT}/v5/market/orderbook",
        {"category": "spot", "symbol": symbol, "limit": limit},
    )
    result = _check_bybit(data)
    return {
        "bids": [(float(p), float(q)) for p, q in result.get("b", [])],
        "asks": [(float(p), float(q)) for p, q in result.get("a", [])],
    }


async def get_orderbook(session, symbol, limit=50, top=20, wall_usd=1_000_000):
    """Стакан: суммарный объём сторон, дисбаланс, крупные стенки."""
    raw = await _with_fallback(
        [
            ("binance", lambda: _binance_depth(session, symbol, limit)),
            ("bybit", lambda: _bybit_depth(session, symbol, limit)),
        ]
    )
    if "error" in raw:
        return raw

    bids = raw["bids"][:top]
    asks = raw["asks"][:top]
    if not bids and not asks:
        return {"error": f"{raw['source']}: пустой стакан"}

    bid_usd = sum(p * q for p, q in bids)
    ask_usd = sum(p * q for p, q in asks)
    total = bid_usd + ask_usd

    walls = [{"side": "BUY", "price": p, "usd": p * q} for p, q in bids if p * q >= wall_usd]
    walls += [{"side": "SELL", "price": p, "usd": p * q} for p, q in asks if p * q >= wall_usd]
    walls.sort(key=lambda w: -w["usd"])

    return {
        "source": raw["source"],
        "bid_usd": round(bid_usd),
        "ask_usd": round(ask_usd),
        "imbalance_pct": round(bid_usd / total * 100, 1) if total else 0.0,
        "walls": walls[:5],
    }


# --- Деривативы -----------------------------------------------------------

async def _binance_derivatives(session, symbol):
    open_interest = await _get_json(
        session, f"{BINANCE_FUT}/fapi/v1/openInterest", {"symbol": symbol}
    )
    premium = await _get_json(
        session, f"{BINANCE_FUT}/fapi/v1/premiumIndex", {"symbol": symbol}
    )
    oi_btc = float(open_interest["openInterest"])
    mark_price = float(premium["markPrice"])
    return {
        "oi_btc": round(oi_btc, 2),
        "oi_usd": round(oi_btc * mark_price),
        "funding_pct": round(float(premium["lastFundingRate"]) * 100, 5),
        "mark_price": mark_price,
    }


async def _bybit_derivatives(session, symbol):
    data = await _get_json(
        session, f"{BYBIT}/v5/market/tickers", {"category": "linear", "symbol": symbol}
    )
    rows = _check_bybit(data).get("list") or []
    if not rows:
        raise ValueError("Bybit не вернул тикер")
    row = rows[0]
    mark_price = float(row.get("markPrice") or row["lastPrice"])
    return {
        "oi_btc": round(float(row["openInterest"]), 2),
        "oi_usd": round(float(row.get("openInterestValue") or 0)),
        "funding_pct": round(float(row["fundingRate"]) * 100, 5),
        "mark_price": mark_price,
    }


async def _binance_ls(session, symbol):
    ratio = await _get_json(
        session,
        f"{BINANCE_FUT}/futures/data/globalLongShortAccountRatio",
        {"symbol": symbol, "period": "5m", "limit": 1},
    )
    if not ratio:
        raise ValueError("Binance не вернул лонг/шорт")
    return {
        "ls_ratio": round(float(ratio[0]["longShortRatio"]), 3),
        "long_pct": round(float(ratio[0]["longAccount"]) * 100, 1),
        "short_pct": round(float(ratio[0]["shortAccount"]) * 100, 1),
    }


async def _okx_ls():
    """OKX даёт только общее соотношение, без разбивки на проценты."""
    async with aiohttp.ClientSession(headers=HEADERS) as session:
        data = await _get_json(
            session,
            f"{OKX}/api/v5/rubik/stat/contracts/long-short-account-ratio",
            {"ccy": "BTC", "period": "5m"},
        )
    rows = _check_okx(data)
    if not rows:
        raise ValueError("OKX не вернул лонг/шорт")
    return {"ls_ratio": round(float(rows[0][1]), 3)}


async def get_derivatives(session, symbol):
    """Деривативы: открытый интерес, ставка финансирования, лонг/шорт.

    Лонг/шорт ратио — необязательный бонус: если не дали ни Binance,
    ни OKX, остальные данные блока всё равно считаются валидными.
    """
    out = await _with_fallback(
        [
            ("binance", lambda: _binance_derivatives(session, symbol)),
            ("bybit", lambda: _bybit_derivatives(session, symbol)),
        ]
    )
    if "error" in out:
        return out

    ls = await _with_fallback([("binance", lambda: _binance_ls(session, symbol)), ("okx", _okx_ls)])
    if "error" in ls:
        out["ls_error"] = ls["error"]
    else:
        ls.pop("source", None)
        out.update(ls)

    return out


# --- Свечи и индикаторы ---------------------------------------------------

async def _binance_klines(session, symbol, interval, limit):
    rows = await _get_json(
        session,
        f"{BINANCE_SPOT}/api/v3/klines",
        {"symbol": symbol, "interval": interval, "limit": limit},
    )
    return {"candles": [(float(r[2]), float(r[3]), float(r[4])) for r in rows]}


async def _bybit_klines(session, symbol, interval, limit):
    data = await _get_json(
        session,
        f"{BYBIT}/v5/market/kline",
        {
            "category": "spot",
            "symbol": symbol,
            "interval": BYBIT_INTERVAL[interval],
            "limit": limit,
        },
    )
    rows = _check_bybit(data).get("list") or []
    if not rows:
        raise ValueError("Bybit не вернул свечи")
    # Bybit отдаёт новые свечи первыми — разворачиваем в хронологический порядок.
    rows = list(reversed(rows))
    return {"candles": [(float(r[2]), float(r[3]), float(r[4])) for r in rows]}


async def get_indicators(session, symbol, limit=KLINE_LIMIT):
    """EMA 20/50/200, RSI(14), ATR(14) на 15m и 1h. Считается локально."""
    out, failures = {}, {}

    for interval in ("15m", "1h"):
        data = await _with_fallback(
            [
                ("binance", lambda: _binance_klines(session, symbol, interval, limit)),
                ("bybit", lambda: _bybit_klines(session, symbol, interval, limit)),
            ]
        )
        if "error" in data:
            failures[interval] = data["error"]
            continue

        highs = [c[0] for c in data["candles"]]
        lows = [c[1] for c in data["candles"]]
        closes = [c[2] for c in data["candles"]]
        if len(closes) < 200:
            failures[interval] = f"{data['source']}: свечей всего {len(closes)}, нужно 200"
            continue

        atr = _atr(highs, lows, closes, 14)
        out[interval] = {
            "source": data["source"],
            "close": closes[-1],
            "ema20": _ema(closes, 20),
            "ema50": _ema(closes, 50),
            "ema200": _ema(closes, 200),
            "rsi14": _rsi(closes, 14),
            "atr14": atr,
            "atr_pct": round(atr / closes[-1] * 100, 3) if atr else None,
        }

    if not out:
        return {"error": "; ".join(f"{k}: {v}" for k, v in failures.items())}
    if failures:
        out["partial"] = failures
    return out


# --- Новости --------------------------------------------------------------

def _titles_from_rss(payload, limit):
    root = ET.fromstring(payload)
    titles = [item.findtext("title") for item in root.iter("item")]
    atom = "{http://www.w3.org/2005/Atom}"
    titles += [entry.findtext(f"{atom}title") for entry in root.iter(f"{atom}entry")]
    return [t.strip() for t in titles if t and t.strip()][:limit]


async def get_news(session, limit=6):
    """Топ заголовков из RSS. Без ключей и токенов."""

    async def fetch(url):
        async with session.get(url, timeout=TIMEOUT) as response:
            response.raise_for_status()
            return await response.read()

    results = await asyncio.gather(
        *(fetch(url) for url in NEWS_FEEDS), return_exceptions=True
    )

    headlines, seen = [], set()
    for result in results:
        if isinstance(result, Exception):
            continue
        try:
            for title in _titles_from_rss(result, limit):
                if title not in seen:
                    seen.add(title)
                    headlines.append(title)
        except ET.ParseError:
            continue

    if not headlines:
        return {"error": "RSS не вернул ни одного заголовка"}
    return {"headlines": headlines[:limit]}


# --- Сборка контекста -----------------------------------------------------

def _fmt(value, digits=2):
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def _block_status(block):
    """(доступен ли блок, текст причины отказа)."""
    if isinstance(block, Exception):  # noqa: BLE001
        return False, f"{type(block).__name__}: {block}"
    if not block:
        return False, "пустой ответ"
    if "error" in block:
        return False, str(block["error"])
    return True, ""


async def build_market_context(symbol="BTC/USDT"):
    """Собирает полный рыночный контекст.

    Возвращает словарь: текст для модели, список доступных факторов,
    ATR в процентах и словарь ошибок по недоступным источникам.
    """
    exchange_symbol = symbol.replace("/", "").upper()

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        orderbook, derivatives, indicators, news = await asyncio.gather(
            get_orderbook(session, exchange_symbol),
            get_derivatives(session, exchange_symbol),
            get_indicators(session, exchange_symbol),
            get_news(session),
            return_exceptions=True,
        )

    blocks = {
        "orderbook": orderbook,
        "derivatives": derivatives,
        "technicals": indicators,
        "news": news,
    }

    status = {name: _block_status(block) for name, block in blocks.items()}
    available = [name for name, (ok, _) in status.items() if ok]
    errors = {name: reason for name, (ok, reason) in status.items() if not ok}

    lines = [f"Пара: {symbol}", ""]

    lines.append("[СТАКАН — топ-20 с каждой стороны]")
    if "orderbook" in available:
        block = blocks["orderbook"]
        lines.append(f"Источник: {block['source'].title()}")
        lines.append(f"Объём заявок на покупку: ${block['bid_usd']:,}")
        lines.append(f"Объём заявок на продажу: ${block['ask_usd']:,}")
        lines.append(f"Дисбаланс (доля быков): {block['imbalance_pct']}%")
        if block["walls"]:
            lines.append("Крупные стенки (>$1M):")
            for wall in block["walls"]:
                lines.append(f"  {wall['side']} {wall['price']:,.2f} на ${wall['usd']:,}")
        else:
            lines.append("Крупных стенок свыше $1M нет")
    else:
        lines.append(f"unavailable: {errors.get('orderbook', '')}")
    lines.append("")

    lines.append("[ДЕРИВАТИВЫ]")
    if "derivatives" in available:
        block = blocks["derivatives"]
        lines.append(f"Источник: {block['source'].title()}")
        lines.append(f"Открытый интерес: {block['oi_btc']:,} BTC (~${block['oi_usd']:,})")
        lines.append(
            f"Funding rate: {block['funding_pct']:+.4f}% "
            f"({'перегрев лонгов' if block['funding_pct'] > 0 else 'перегрев шортов'})"
        )
        if "ls_ratio" in block:
            line = f"Лонг/шорт: {block['ls_ratio']}"
            if "long_pct" in block:
                line += f" (лонги {block['long_pct']}% / шорты {block['short_pct']}%)"
            lines.append(line)
        else:
            lines.append("Лонг/шорт: недоступен (необязательно)")
    else:
        lines.append(f"unavailable: {errors.get('derivatives', '')}")
    lines.append("")

    lines.append("[ТЕХНИЧЕСКИЙ АНАЛИЗ, посчитан локально]")
    if "technicals" in available:
        for interval, label in (("15m", "15 минут"), ("1h", "1 час")):
            block = blocks["technicals"].get(interval, {})
            if "error" in block or "close" not in block:
                reason = block.get("error") or blocks["technicals"].get("partial", {})
                lines.append(f"{label}: unavailable ({reason})")
                continue
            lines.append(
                f"{label} [{block['source']}]: close {_fmt(block['close'])} | "
                f"EMA20 {_fmt(block['ema20'])} | EMA50 {_fmt(block['ema50'])} | "
                f"EMA200 {_fmt(block['ema200'])} | "
                f"RSI {_fmt(block['rsi14'], 1)} | "
                f"ATR {_fmt(block['atr14'])} ({_fmt(block['atr_pct'], 3)}%)"
            )
    else:
        lines.append(f"unavailable: {errors.get('technicals', '')}")
    lines.append("")

    lines.append("[НОВОСТИ]")
    if "news" in available:
        for index, title in enumerate(blocks["news"]["headlines"], 1):
            lines.append(f"{index}. {title}")
    else:
        lines.append(f"unavailable: {errors.get('news', '')}")

    lines.append("")
    lines.append(f"Доступные факторы: {', '.join(available) or 'нет данных'}")

    atr_pct = None
    if "technicals" in available:
        technicals = blocks["technicals"]
        atr_pct = technicals.get("15m", {}).get("atr_pct")
        if atr_pct is None:
            atr_pct = technicals.get("1h", {}).get("atr_pct")

    return {
        "text": "\n".join(lines),
        "available": available,
        "atr_pct": atr_pct,
        "errors": errors,
    }
