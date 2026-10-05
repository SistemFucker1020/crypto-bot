"""Сбор рыночных данных для AI-аналитика.

Бесплатные источники, ключи не нужны:
  * стакан и свечи    — Binance REST
  * OI, funding, L/S  — Binance Futures REST
  * новости           — RSS CoinDesk + Cointelegraph

Каждый источник опрашивается независимо и с таймаутом: недоступный
попадает в контекст как unavailable, но не роняет весь анализ.
"""

import asyncio
import xml.etree.ElementTree as ET

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=8)
SPOT = "https://api.binance.com"
FUT = "https://fapi.binance.com"
NEWS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
)
HEADERS = {"User-Agent": "crypto-bot/1.0"}

FACTORS = ("orderbook", "derivatives", "technicals", "news")


async def _get_json(session, url, params=None):
    async with session.get(url, params=params, timeout=TIMEOUT) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


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


async def get_orderbook(session, symbol, limit=50, top=20, wall_usd=1_000_000):
    """Стакан: суммарный объём сторон, дисбаланс, крупные стенки."""
    try:
        data = await _get_json(
            session, f"{SPOT}/api/v3/depth", {"symbol": symbol, "limit": limit}
        )
        bids = [(float(p), float(q)) for p, q in data.get("bids", [])[:top]]
        asks = [(float(p), float(q)) for p, q in data.get("asks", [])[:top]]

        bid_usd = sum(p * q for p, q in bids)
        ask_usd = sum(p * q for p, q in asks)
        total = bid_usd + ask_usd

        walls = [
            {"side": "BUY", "price": p, "usd": p * q}
            for p, q in bids
            if p * q >= wall_usd
        ]
        walls += [
            {"side": "SELL", "price": p, "usd": p * q}
            for p, q in asks
            if p * q >= wall_usd
        ]
        walls.sort(key=lambda w: -w["usd"])

        return {
            "bid_usd": round(bid_usd),
            "ask_usd": round(ask_usd),
            "imbalance_pct": round(bid_usd / total * 100, 1) if total else 0.0,
            "walls": walls[:5],
        }
    except Exception as exc:  # noqa: BLE001 - источник не должен ронять анализ
        return {"error": str(exc)}


async def get_derivatives(session, symbol):
    """Деривативы: открытый интерес, ставка финансирования, лонг/шорт."""
    out = {}
    try:
        open_interest = await _get_json(
            session, f"{FUT}/fapi/v1/openInterest", {"symbol": symbol}
        )
        premium = await _get_json(
            session, f"{FUT}/fapi/v1/premiumIndex", {"symbol": symbol}
        )
        oi_btc = float(open_interest["openInterest"])
        mark_price = float(premium["markPrice"])
        out["oi_btc"] = round(oi_btc, 2)
        out["oi_usd"] = round(oi_btc * mark_price)
        out["funding_pct"] = round(float(premium["lastFundingRate"]) * 100, 5)
        out["mark_price"] = mark_price
    except Exception as exc:  # noqa: BLE001
        out["error"] = str(exc)

    try:
        ratio = await _get_json(
            session,
            f"{FUT}/futures/data/globalLongShortAccountRatio",
            {"symbol": symbol, "period": "5m", "limit": 1},
        )
        if ratio:
            out["ls_ratio"] = round(float(ratio[0]["longShortRatio"]), 3)
            out["long_pct"] = round(float(ratio[0]["longAccount"]) * 100, 1)
            out["short_pct"] = round(float(ratio[0]["shortAccount"]) * 100, 1)
    except Exception as exc:  # noqa: BLE001
        out["ls_error"] = str(exc)

    return out


async def get_indicators(session, symbol):
    """EMA 20/50/200, RSI(14), ATR(14) на 15m и 1h. Считается локально."""
    out = {}
    for interval in ("15m", "1h"):
        try:
            rows = await _get_json(
                session,
                f"{SPOT}/api/v3/klines",
                {"symbol": symbol, "interval": interval, "limit": 300},
            )
            closes = [float(row[4]) for row in rows]
            highs = [float(row[2]) for row in rows]
            lows = [float(row[3]) for row in rows]
            atr = _atr(highs, lows, closes, 14)
            out[interval] = {
                "close": closes[-1],
                "ema20": _ema(closes, 20),
                "ema50": _ema(closes, 50),
                "ema200": _ema(closes, 200),
                "rsi14": _rsi(closes, 14),
                "atr14": atr,
                "atr_pct": round(atr / closes[-1] * 100, 3) if atr else None,
            }
        except Exception as exc:  # noqa: BLE001
            out[interval] = {"error": str(exc)}
    return out


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


def _fmt(value, digits=2):
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


async def build_market_context(symbol="BTC/USDT"):
    """Собирает полный рыночный контекст. Возвращает (текст, доступные факторы)."""
    binance_symbol = symbol.replace("/", "").upper()

    async with aiohttp.ClientSession(headers=HEADERS) as session:
        orderbook, derivatives, indicators, news = await asyncio.gather(
            get_orderbook(session, binance_symbol),
            get_derivatives(session, binance_symbol),
            get_indicators(session, binance_symbol),
            get_news(session),
            return_exceptions=True,
        )

    blocks = {
        "orderbook": orderbook,
        "derivatives": derivatives,
        "technicals": indicators,
        "news": news,
    }

    available = [
        name
        for name, block in blocks.items()
        if not isinstance(block, Exception) and block and "error" not in block
    ]

    lines = [f"Пара: {symbol}", ""]

    lines.append("[СТАКАН BINANCE — топ-20 с каждой стороны]")
    if "orderbook" in available:
        lines.append(f"Объём заявок на покупку: ${blocks['orderbook']['bid_usd']:,}")
        lines.append(f"Объём заявок на продажу: ${blocks['orderbook']['ask_usd']:,}")
        lines.append(f"Дисбаланс (доля быков): {blocks['orderbook']['imbalance_pct']}%")
        if blocks["orderbook"]["walls"]:
            lines.append("Крупные стенки (>$1M):")
            for wall in blocks["orderbook"]["walls"]:
                lines.append(
                    f"  {wall['side']} {wall['price']:,.2f} на ${wall['usd']:,}"
                )
        else:
            lines.append("Крупных стенок свыше $1M нет")
    else:
        lines.append("unavailable")
    lines.append("")

    lines.append("[ДЕРИВАТИВЫ BINANCE FUTURES]")
    if "derivatives" in available:
        block = blocks["derivatives"]
        lines.append(f"Открытый интерес: {block['oi_btc']:,} BTC (~${block['oi_usd']:,})")
        lines.append(
            f"Funding rate: {block['funding_pct']:+.4f}% "
            f"({'перегрев лонгов' if block['funding_pct'] > 0 else 'перегрев шортов'})"
        )
        if "ls_ratio" in block:
            lines.append(
                f"Лонг/шорт: {block['ls_ratio']} "
                f"(лонги {block['long_pct']}% / шорты {block['short_pct']}%)"
            )
    else:
        lines.append("unavailable")
    lines.append("")

    lines.append("[ТЕХНИЧЕСКИЙ АНАЛИЗ, посчитан локально]")
    if "technicals" in available:
        for interval, label in (("15m", "15 минут"), ("1h", "1 час")):
            block = blocks["technicals"].get(interval, {})
            if "error" in block:
                lines.append(f"{label}: unavailable")
                continue
            lines.append(
                f"{label}: close {_fmt(block['close'])} | "
                f"EMA20 {_fmt(block['ema20'])} | EMA50 {_fmt(block['ema50'])} | "
                f"EMA200 {_fmt(block['ema200'])} | "
                f"RSI {_fmt(block['rsi14'], 1)} | "
                f"ATR {_fmt(block['atr14'])} ({_fmt(block['atr_pct'], 3)}%)"
            )
    else:
        lines.append("unavailable")
    lines.append("")

    lines.append("[НОВОСТИ]")
    if "news" in available:
        for index, title in enumerate(blocks["news"]["headlines"], 1):
            lines.append(f"{index}. {title}")
    else:
        lines.append("unavailable")

    lines.append("")
    lines.append(f"Доступные факторы: {', '.join(available) or 'нет данных'}")

    atr_pct = None
    if "technicals" in available:
        atr_pct = blocks["technicals"].get("15m", {}).get("atr_pct")

    return {"text": "\n".join(lines), "available": available, "atr_pct": atr_pct}
