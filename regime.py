"""Режим рынка: TREND / RANGE / HIGH_VOLATILITY / LOW_VOLATILITY / NEWS_EVENT.

Детерминированный Python на данных market-контекста, без LLM. Пороги —
в переменных окружения. Режимы НЕ взаимоисключающими по своей сути:
поэтому возвращается основной режим плюс отдельные признаки (флаги
волатильности и новостного риска) — они сохраняются в журнале всегда.

Режим сам по себе НИЧЕГО не блокирует. Фильтрация — только через явное
правило REGIME_BLOCKS (список режимов, при которых сигнал отменяется).
Пустое правило (по умолчанию) = бот торгует в любом режиме. Это защита
от «не торгуй в боковике просто потому что трендового режима нет».

UNKNOWN — данных недостаточно или они противоречивы (например, ADX
выше трендового порога, но EMA неупорядочены). UNKNOWN не блокирует.

ВАЖНО: «Score ≠ вероятность». Режим — тоже не вероятность: он просто
метка состояния рынка по правилам.
"""

import os

# ADX (15m) >= порога — рынок трендовый; <= нижнего порога — боковик.
# Между ними — переходная зона → режим UNKNOWN по структуре.
ADX_TREND = float(os.getenv("REGIME_ADX_TREND", "25"))
ADX_RANGE = float(os.getenv("REGIME_ADX_RANGE", "18"))

# ATR% (15m) относительно цены — всплеск или затишье волатильности.
ATR_HIGH_VOL = float(os.getenv("REGIME_ATR_HIGH_VOL", "0.5"))
ATR_LOW_VOL = float(os.getenv("REGIME_ATR_LOW_VOL", "0.1"))

# Явное правило блокировки: "HIGH_VOLATILITY,NEWS_EVENT" — и сигналы в
# этих режимах отменяются. По умолчанию пусто — ничего не блокируется.
BLOCKED = frozenset(
    s.strip().upper()
    for s in os.getenv("REGIME_BLOCKS", "").split(",")
    if s.strip()
)

# Достоверные маркеры новостного события в заголовках. Это не вероятность
# и не парсер новостей: точное «новостное событие» здесь недоступно,
# поэтому совпадение по этим словам даёт только флаг NEWS_EVENT, а не
# блокировку (если пользователь явно не включит её в REGIME_BLOCKS).
_NEWS_MARKERS = (
    "hack", "exploitation", "exploit", "stolen", "bankruptcy",
    "chapter 11", "sec approves", "sec rejects", "etf approval",
    "etf approved", "flash crash", "liquidation cascade", "halted",
    "suspend", "lawsuit filed", "criminal charges", "rug pull",
    "bridge exploit", "insolven",
)

MIN_NEWS_MATCHES = int(os.getenv("REGIME_NEWS_MIN_MATCHES", "1"))

MODES = ("TREND", "RANGE", "HIGH_VOLATILITY", "LOW_VOLATILITY", "NEWS_EVENT", "UNKNOWN")


def _num(value):
    """Число из данных биржи или None. Мусор (строка, None) — не исключение."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def detect(context: dict) -> dict:
    """Определяет режим рынка по market-контексту. Возвращает dict:

      mode       — основной режим из MODES
      flags      — {"high_volatility", "low_volatility", "news_event"} (bool)
      adx, atr_pct, detail — для журнала и объяснения

    Не бросает исключений: любые отсутствующие данные → UNKNOWN.
    """
    technicals = (context.get("blocks") or {}).get("technicals") or {}
    frame = technicals.get("15m") or {}
    adx = _num(frame.get("adx"))
    atr_pct = _num(frame.get("atr_pct"))
    # EMA тоже приводим к числам: биржа может прислать что угодно.
    close, ema20, ema50, ema200 = (
        _num(frame.get(k)) for k in ("close", "ema20", "ema50", "ema200")
    )
    ema_ok = all(v is not None for v in (close, ema20, ema50, ema200))

    flags = {
        "high_volatility": atr_pct is not None and atr_pct >= ATR_HIGH_VOL,
        "low_volatility": atr_pct is not None and atr_pct <= ATR_LOW_VOL,
        "news_event": _news_event(context),
    }
    detail = []

    # Приоритет: новостное событие > волатильность > тренд/боковик.
    # Иначе кризисный новостной день с низким ATR был бы «LOW_VOLATILITY».
    if flags["news_event"]:
        mode = "NEWS_EVENT"
        detail.append("маркеры новостного события в заголовках")
    elif flags["high_volatility"]:
        mode = "HIGH_VOLATILITY"
        detail.append(f"ATR% {atr_pct} >= {ATR_HIGH_VOL:g}")
    elif adx is None or (not ema_ok and adx < ADX_TREND):
        mode = "UNKNOWN"
        detail.append("нет ADX или EMA-данных — режим не определить")
    else:
        if ema_ok and close > ema20 > ema50 > ema200:
            structure = "up"
        elif ema_ok and close < ema20 < ema50 < ema200:
            structure = "down"
        else:
            structure = "none"
        detail.append(f"ADX {adx}, структура EMA {structure}")
        if adx >= ADX_TREND and structure != "none":
            mode = "TREND"
        elif adx <= ADX_RANGE:
            mode = "RANGE"
            if flags["low_volatility"]:
                detail.append(f"плюс низкая волатильность ATR% {atr_pct} <= {ATR_LOW_VOL:g}")
        else:
            mode = "UNKNOWN"
            detail.append(
                f"ADX {adx} в переходной зоне {ADX_RANGE:g}–{ADX_TREND:g} — "
                "тренд не подтверждён, боковик не доказан"
            )

    return {
        "mode": mode,
        "flags": flags,
        "adx": adx,
        "atr_pct": atr_pct,
        "detail": "; ".join(detail),
        "blocked": mode in BLOCKED,
    }


def _news_event(context: dict) -> bool:
    """True, если в свежих заголовках есть маркеры события.

    Это осознанно консервативный эвристический флаг: точного детектора
    «новостного события» бесплатные фиды не дают. Поэтому он влияет
    только на флаг/режим и никогда не блокирует сам по себе.
    """
    news = (context.get("blocks") or {}).get("news") or {}
    headlines = news.get("headlines") or []
    if not headlines:
        return False
    text = " ".join(str(h).lower() for h in headlines)
    return sum(1 for marker in _NEWS_MARKERS if marker in text) >= MIN_NEWS_MATCHES
