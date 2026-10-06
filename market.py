"""Сбор рыночных данных для AI-аналитика.

Бесплатные источники, ключи не нужны. У каждого блока своя цепочка
из нескольких бирж — побеждает первая, кто ответил:

  стакан и свечи   Binance → Bybit → MEXC → OKX
  OI и funding     Binance → Bybit → OKX
  лонг/шорт        Binance → OKX
  новости          RSS CoinDesk + Cointelegraph

Зачем столько: Binance отдаёт 451 (геоблок) с IP США, а Bybit с США
тоже не работает — на хостинге это выглядело как «доступны только
новости». MEXC и OKX в США отвечают. Победивший источник попадает в
контекст, проигравшие — в словарь ошибок с текстом причины.

Ни один упавший источник не роняет весь анализ. Базовые адреса
переопределяются переменными BINANCE_SPOT / BINANCE_FUT / BYBIT /
MEXC_SPOT / OKX — так удобно эмулировать блокировку.
"""

import asyncio
import os
import xml.etree.ElementTree as ET

import aiohttp

TIMEOUT = aiohttp.ClientTimeout(total=8)

BINANCE_SPOT = os.getenv("BINANCE_SPOT", "https://api.binance.com")
BINANCE_FUT = os.getenv("BINANCE_FUT", "https://fapi.binance.com")
BYBIT = os.getenv("BYBIT", "https://api.bybit.com")
MEXC_SPOT = os.getenv("MEXC_SPOT", "https://api.mexc.com")
OKX = os.getenv("OKX", "https://www.okx.com")

NEWS_FEEDS = (
    "https://www.coindesk.com/arc/outboundfeeds/rss/",
    "https://cointelegraph.com/rss",
)
HEADERS = {"User-Agent": "crypto-bot/1.0"}

KLINE_LIMIT = 300          # нужно 200+ — иначе не посчитается EMA200

# Формат интервалов у каждой биржи свой: MEXC пишет "60m", OKX — "1H".
INTERVALS = {
    "binance": {"15m": "15m", "1h": "1h"},
    "bybit": {"15m": "15", "1h": "60"},
    "mexc": {"15m": "15m", "1h": "60m"},
    "okx": {"15m": "15m", "1h": "1H"},
}


async def _get_json(session, url, params=None):
    async with session.get(url, params=params, timeout=TIMEOUT) as response:
        response.raise_for_status()
        return await response.json(content_type=None)


async def _with_fallback(providers):
    """Пробует источники по очереди. Побеждает первый, кто ответил.

    Каждый provider — корутина, возвращающая словарь. Проигравшие
    источники складываются в error с разделителем " | ", чтобы текст
    можно было разбить обратно на строки при показе причины.
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

    return {"error": " | ".join(f"{name}: {err}" for name, err in errors.items())}


def _check_bybit(data):
    if data.get("retCode") != 0:
        raise ValueError(f"retCode {data.get('retCode')}: {data.get('retMsg')}")
    return data.get("result") or {}


def _check_okx(data):
    if str(data.get("code")) != "0":
        raise ValueError(f"code {data.get('code')}: {data.get('msg')}")
    return data.get("data") or []


def _okx_inst(symbol):
    """BTCUSDT → BTC-USDT (формат инструмента OKX)."""
    if symbol.endswith("USDT"):
        return f"{symbol[:-4]}-USDT"
    return symbol


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

def _spot_depth(base):
    """Binance и MEXC отдают стакан в одинаковом формате /api/v3/depth."""

    async def fetch(session, symbol, limit):
        data = await _get_json(
            session, f"{base}/api/v3/depth", {"symbol": symbol, "limit": limit}
        )
        return {
            "bids": [(float(p), float(q)) for p, q in data.get("bids", [])],
            "asks": [(float(p), float(q)) for p, q in data.get("asks", [])],
        }

    return fetch


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


async def _okx_books(session, symbol, limit):
    data = await _get_json(
        session,
        f"{OKX}/api/v5/market/books",
        {"instId": _okx_inst(symbol), "sz": min(limit, 400)},
    )
    rows = _check_okx(data)
    if not rows:
        raise ValueError("OKX не вернул стакан")
    book = rows[0]
    # У OKX в заявке 4 поля: цена, объём, ликвидации, число ордеров.
    return {
        "bids": [(float(row[0]), float(row[1])) for row in book.get("bids", [])],
        "asks": [(float(row[0]), float(row[1])) for row in book.get("asks", [])],
    }


BINANCE_DEPTH = _spot_depth(BINANCE_SPOT)
MEXC_DEPTH = _spot_depth(MEXC_SPOT)


async def get_orderbook(
    session, symbol, limit=500, top=20, wall_usd=1_000_000, band=0.002
):
    """Стакан: объём в полосе, дисбаланс по полосе, крупные стенки.

    Дисбаланс считается по полосе ±band вокруг середины книги, а не по
    первым 20 уровням. Замер показал, что топ-20 на BTC даёт $60-1300 на
    уровне и метается с 99% до 4% доли быков за 24 секунды (амплитуда
    77,5 п.п.), а глубина в полосе ±0.2% меняется максимум на 8,5 п.п.
    Старое измерение и переворачивало вердикт.
    """
    raw = await _with_fallback(
        [
            ("binance", lambda: BINANCE_DEPTH(session, symbol, limit)),
            ("bybit", lambda: _bybit_depth(session, symbol, limit)),
            # MEXC принимает максимум 50 уровней — больше отдаст ошибку.
            ("mexc", lambda: MEXC_DEPTH(session, symbol, min(limit, 50))),
            ("okx", lambda: _okx_books(session, symbol, limit)),
        ]
    )
    if "error" in raw:
        return raw

    bids, asks = raw["bids"], raw["asks"]
    if not bids and not asks:
        return {"error": f"{raw['source']}: пустой стакан"}

    # Середина книги — по лучшим ценам, чтобы не зависеть от порядка
    # уровней в ответе: OKX и Binance отдают их по-разному.
    best_bid = max(price for price, _ in bids)
    best_ask = min(price for price, _ in asks)
    mid = (best_bid + best_ask) / 2

    low, high = mid * (1 - band), mid * (1 + band)
    near_bids = [(p, q) for p, q in bids if p >= low]
    near_asks = [(p, q) for p, q in asks if p <= high]

    bid_usd = sum(p * q for p, q in near_bids)
    ask_usd = sum(p * q for p, q in near_asks)
    total = bid_usd + ask_usd

    # Стенки берём с первых ближайших к середине уровней.
    nearest_bids = sorted(bids, key=lambda row: -row[0])[:top]
    nearest_asks = sorted(asks, key=lambda row: row[0])[:top]
    walls = [
        {"side": "BUY", "price": p, "usd": p * q} for p, q in nearest_bids if p * q >= wall_usd
    ]
    walls += [
        {"side": "SELL", "price": p, "usd": p * q} for p, q in nearest_asks if p * q >= wall_usd
    ]
    walls.sort(key=lambda w: -w["usd"])

    return {
        "source": raw["source"],
        "bid_usd": round(bid_usd),
        "ask_usd": round(ask_usd),
        "band_pct": round(band * 100, 2),
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
    return {
        "oi_btc": round(float(row["openInterest"]), 2),
        "oi_usd": round(float(row.get("openInterestValue") or 0)),
        "funding_pct": round(float(row["fundingRate"]) * 100, 5),
        "mark_price": float(row.get("markPrice") or row["lastPrice"]),
    }


async def _okx_derivatives(session, symbol):
    inst_id = f"{_okx_inst(symbol)}-SWAP"
    funding = await _get_json(
        session, f"{OKX}/api/v5/public/funding-rate", {"instId": inst_id}
    )
    interest = await _get_json(
        session, f"{OKX}/api/v5/public/open-interest", {"instId": inst_id}
    )
    funding_rows, oi_rows = _check_okx(funding), _check_okx(interest)
    if not funding_rows or not oi_rows:
        raise ValueError("OKX не вернул funding/open interest")

    rate, oi = funding_rows[0], oi_rows[0]
    oi_btc = float(oi["oiCcy"])
    oi_usd = float(oi.get("oiUsd") or 0)
    mark_price = float(rate.get("markPrice") or 0) or (oi_usd / oi_btc if oi_btc else 0.0)
    if not oi_usd and mark_price:
        oi_usd = oi_btc * mark_price
    return {
        "oi_btc": round(oi_btc, 2),
        "oi_usd": round(oi_usd),
        "funding_pct": round(float(rate["fundingRate"]) * 100, 5),
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
            ("okx", lambda: _okx_derivatives(session, symbol)),
        ]
    )
    if "error" in out:
        return out

    ls = await _with_fallback(
        [("binance", lambda: _binance_ls(session, symbol)), ("okx", _okx_ls)]
    )
    if "error" in ls:
        out["ls_error"] = ls["error"]
    else:
        ls.pop("source", None)
        out.update(ls)

    return out


# --- Свечи и индикаторы ---------------------------------------------------

def _spot_klines(base, intervals):
    """Binance и MEXC отдают свечи одинаково, различается только интервал."""

    async def fetch(session, symbol, interval, limit):
        data = await _get_json(
            session,
            f"{base}/api/v3/klines",
            {"symbol": symbol, "interval": intervals[interval], "limit": limit},
        )
        return {"candles": [(float(r[2]), float(r[3]), float(r[4])) for r in data]}

    return fetch


async def _bybit_klines(session, symbol, interval, limit):
    data = await _get_json(
        session,
        f"{BYBIT}/v5/market/kline",
        {
            "category": "spot",
            "symbol": symbol,
            "interval": INTERVALS["bybit"][interval],
            "limit": limit,
        },
    )
    rows = _check_bybit(data).get("list") or []
    if not rows:
        raise ValueError("Bybit не вернул свечи")
    # Bybit отдаёт новые свечи первыми — разворачиваем в хронологический порядок.
    rows = list(reversed(rows))
    return {"candles": [(float(r[2]), float(r[3]), float(r[4])) for r in rows]}


async def _okx_klines(session, symbol, interval, limit):
    data = await _get_json(
        session,
        f"{OKX}/api/v5/market/candles",
        {
            "instId": _okx_inst(symbol),
            "bar": INTERVALS["okx"][interval],
            "limit": min(limit, 300),
        },
    )
    rows = _check_okx(data)
    if not rows:
        raise ValueError("OKX не вернул свечи")
    # OKX тоже отдаёт новые свечи первыми.
    rows = list(reversed(rows))
    return {"candles": [(float(r[2]), float(r[3]), float(r[4])) for r in rows]}


BINANCE_KLINES = _spot_klines(BINANCE_SPOT, INTERVALS["binance"])
MEXC_KLINES = _spot_klines(MEXC_SPOT, INTERVALS["mexc"])


async def get_indicators(session, symbol, limit=KLINE_LIMIT):
    """EMA 20/50/200, RSI(14), ATR(14) на 15m и 1h. Считается локально."""
    out, failures = {}, {}

    for interval in ("15m", "1h"):
        data = await _with_fallback(
            [
                ("binance", lambda: BINANCE_KLINES(session, symbol, interval, limit)),
                ("bybit", lambda: _bybit_klines(session, symbol, interval, limit)),
                ("mexc", lambda: MEXC_KLINES(session, symbol, interval, limit)),
                ("okx", lambda: _okx_klines(session, symbol, interval, limit)),
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
        return {"error": " | ".join(f"{k}: {v}" for k, v in failures.items())}
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

# --- Голоса факторов ------------------------------------------------------
# Направление считает код, а не модель: при одном и том же рынке нейросеть
# за минуту успевала ответить LONG, потом WAIT и снова LONG — три полных
# расчёта с одинаковым риском в разные стороны. Числа не мнение меняют.

def _orderbook_vote(block: dict) -> int:
    """Доля быков в полосе ±0.2% книги. Мёртвая зона ±6% вокруг равновесия.

    Замер на живом рынке: полоса меняется на 8,5 п.п. за полминуты, топ-20 —
    на 77,5 п.п., поэтому пороги взяты с запасом от шума измерения.
    """
    share = block.get("imbalance_pct")
    if share is None:
        return 0
    if share >= 56.0:
        return 1
    if share <= 44.0:
        return -1
    return 0


def _derivatives_vote(block: dict) -> int:
    """Funding и лонг/шорт — контр-трендовые: перегрев лонгов против лонга."""
    votes = []

    funding = block.get("funding_pct")
    if funding is not None:
        if funding >= 0.003:
            votes.append(-1)   # лонги перегреты и платят за удержание
        elif funding <= -0.003:
            votes.append(1)    # перегреты шорты — против толпы вниз

    long_pct, short_pct = block.get("long_pct"), block.get("short_pct")
    if long_pct is not None and short_pct is not None:
        crowd = long_pct - short_pct
        if crowd >= 15:
            votes.append(-1)
        elif crowd <= -15:
            votes.append(1)
    elif block.get("ls_ratio") is not None:
        ratio = block["ls_ratio"]
        if ratio >= 1.15:
            votes.append(-1)
        elif ratio <= 0.85:
            votes.append(1)

    if not votes:
        return 0
    total = sum(votes)
    return 1 if total > 0 else -1 if total < 0 else 0


def _technicals_vote(block: dict) -> int:
    """Порядок EMA 20/50/200 и зона RSI на 15m и 1h."""
    votes = []
    for frame in ("15m", "1h"):
        data = block.get(frame) or {}
        if "close" not in data:
            continue
        close = data["close"]
        levels = (data.get("ema20"), data.get("ema50"), data.get("ema200"))
        if all(level is not None for level in levels):
            ema20, ema50, ema200 = levels
            if close > ema20 > ema50 > ema200:
                votes.append(1)
            elif close < ema20 < ema50 < ema200:
                votes.append(-1)
            else:
                votes.append(0)
        rsi = data.get("rsi14")
        if rsi is not None:
            votes.append(1 if rsi >= 55 else -1 if rsi <= 45 else 0)

    if not votes:
        return 0
    total = sum(votes)
    # Без явного перевеса тренд считаем нейтральным: полумеры как раз и
    # давали ложную уверенность, из-за которой вердикт мотало.
    if total >= 2:
        return 1
    if total <= -2:
        return -1
    return 0


def score_factors(blocks: dict, available: list) -> dict:
    """Голос каждого фактора: +1 вверх, -1 вниз, 0 нейтрально.

    Новости не голосуют: их смысл не сводится к числу, а субъективная
    оценка заголовков моделью как раз переворачивала направление.
    """
    scorers = {
        "orderbook": _orderbook_vote,
        "derivatives": _derivatives_vote,
        "technicals": _technicals_vote,
    }
    scores = {}
    for name in available:
        block = blocks.get(name)
        if name not in scorers or not isinstance(block, dict):
            scores[name] = 0
            continue
        scores[name] = scorers[name](block)
    return scores


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
        lines.append(
            f"Объём заявок в полосе ±{block.get('band_pct', 0.2):g}% "
            f"(покупка ${block['bid_usd']:,} / продажа ${block['ask_usd']:,})"
        )
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
            if "close" not in block:
                reason = blocks["technicals"].get("partial", {}).get(interval, "")
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
    scores = score_factors(blocks, available)
    if scores:
        lines.append(
            "Голоса системы (+1 вверх / -1 вниз / 0 нейтрально): "
            + ", ".join(f"{name} {scores[name]:+d}" for name in scores)
        )

    atr_pct = None
    price = None
    if "technicals" in available:
        technicals = blocks["technicals"]
        atr_pct = technicals.get("15m", {}).get("atr_pct")
        if atr_pct is None:
            atr_pct = technicals.get("1h", {}).get("atr_pct")
        price = technicals.get("15m", {}).get("close")
        if price is None:
            price = technicals.get("1h", {}).get("close")

    return {
        "text": "\n".join(lines),
        "available": available,
        "atr_pct": atr_pct,
        "price": price,
        "scores": scores,
        "errors": errors,
    }
