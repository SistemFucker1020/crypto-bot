import os
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta

from aiohttp import web
from aiogram import Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from groq import AsyncGroq

from market import build_market_context
import bybit
import signal_log

# --- Настройки окружения ----------------------------------------------
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

missing = [
    name
    for name, value in (
        ("TELEGRAM_TOKEN", TELEGRAM_TOKEN),
        ("GROQ_API_KEY", GROQ_API_KEY),
    )
    if not value
]
if missing:
    raise SystemExit(
        "Не заданы переменные окружения: " + ", ".join(missing)
        + ". Задай их в настройках Render (Environment → Environment Variables)."
    )

# --- Параметры риска ----------------------------------------------------
# Баланс можно править переменной BALANCE на Render (по умолчанию $100).
BALANCE = float(os.getenv("BALANCE", "100"))

# Риск на сделку зависит от баланса: до $200 — 5%, до $300 — 3%, дальше — 1%.
RISK_TIERS = ((200.0, 5.0), (300.0, 3.0), (float("inf"), 1.0))
MIN_RR = 3.0                      # жёсткий полигон: ниже — сетап отсекается
PREFERRED_RR = 5.0                # целевая цель: к ней модель должна стремиться
MAX_LEVERAGE = 10.0               # потолок плеча (правило: никогда 20x+)
LEVERAGE_LADDER = (1.0, 2.0, 3.0, 5.0, 10.0)
MIN_ATR_PCT = 0.05                # ниже — волатильности нет, сидим на руках
REQUIRED_FACTORS = int(           # конфлюэнция: голосов одного направления
    os.getenv("REQUIRED_FACTORS", "3")
)

# Автопроверка: раз в CHECK_INTERVAL_MIN минут, через CHECK_DELAY_SEC после
# закрытия свечи соответствующего таймфрейма. 0 — автопроверка выключена.
CHECK_INTERVAL_MIN = int(os.getenv("CHECK_INTERVAL_MIN", "15"))
CHECK_DELAY_SEC = int(os.getenv("CHECK_DELAY_SEC", "45"))
REPEAT_SIGNAL_SEC = int(os.getenv("REPEAT_SIGNAL_SEC", "3600"))
AUTO_FAILURE_ALERT = int(os.getenv("AUTO_FAILURE_ALERT", "4"))
TZ_OFFSET_MIN = int(os.getenv("TZ_OFFSET_MIN", "180"))   # показывать время: Москва = +180
# Сигнал уходит в чат, если это направление встретилось CONFIRM_REQUIRED раз
# за CONFIRM_WINDOW_SEC секунд — подряд не обязательно: WAIT из окна не выбрасываем,
# но и не подтверждаем им. Лог показал: три WAIT делят два LONG за 45 минут.
CONFIRM_REQUIRED = int(os.getenv("CONFIRM_REQUIRED", "2"))
CONFIRM_WINDOW_SEC = int(os.getenv("CONFIRM_WINDOW_SEC", "3600"))
# Вход модели не далее 1.5% от текущей цены — иначе уровни не от рынка.
MAX_ENTRY_DRIFT = float(os.getenv("MAX_ENTRY_DRIFT", "0.015"))

# Пинг из GitHub Actions ходит на /health: если автопроверка не бегла столько
# минут — ответ 503, job падает, GitHub присылает письмо. Пустое молчание
# бота иначе снова осталось бы незамеченным до утра.
HEALTH_STALE_MIN = int(os.getenv("HEALTH_STALE_MIN", "45"))
# Чаты для автосообщений, которые переживают рестарт: бот пишет готовую строку
# в ответе на /signals, её нужно один раз внести в переменную на Render.
# Список через запятую или точку с запятой: "123456,-100789". Старое имя
# TELEGRAM_CHAT_ID читаем тоже, чтобы вчерашняя настройка не потерялась.
TELEGRAM_CHAT_IDS = os.getenv("TELEGRAM_CHAT_IDS") or os.getenv("TELEGRAM_CHAT_ID") or ""


def env_chat_ids() -> list:
    """Чаты из переменной окружения; чужой мусор молча пропускаем."""
    ids = []
    for chunk in TELEGRAM_CHAT_IDS.replace(";", ",").split(","):
        chunk = chunk.strip()
        if chunk.lstrip("-").isdigit():
            ids.append(int(chunk))
    return ids


# Результат проверки чатов из переменной: chat_id -> "ok" | BAD_CHAT | "не проверен".
# Заполняется при старте restore_subscriptions(), читает /signals и подсказка.
_env_status: dict = {}
BAD_CHAT = "не найден"
# Пауза перед повторным запуском long polling после сбоя сети.
POLLING_RETRY_BASE_SEC = float(os.getenv("POLLING_RETRY_BASE_SEC", "5"))

GROQ_MODEL = "openai/gpt-oss-120b"
SYMBOL = "BTC/USDT"

FACTOR_NAMES = {
    "orderbook": "стакан",
    "derivatives": "деривативы (OI/funding)",
    "technicals": "индикаторы",
    "news": "новости",
}
FACTOR_ALIASES = {
    "orderbook": "orderbook", "order_book": "orderbook", "order book": "orderbook",
    "depth": "orderbook", "стакан": "orderbook", "liquidity": "orderbook",
    "derivatives": "derivatives", "futures": "derivatives", "funding": "derivatives",
    "oi": "derivatives", "open interest": "derivatives", "деривативы": "derivatives",
    "technicals": "technicals", "technical": "technicals", "ta": "technicals",
    "indicators": "technicals", "индикаторы": "technicals", "технический": "technicals",
    "news": "news", "sentiment": "news", "новости": "news", "сентимент": "news",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("crypto-bot")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# Фиктивный веб-сервер, чтобы хостинг не закрывал сервис по таймауту портов.
# Ссылки держим на уровне модуля: локальные переменные GC мог бы выбросить,
# и сервер перестал бы отвечать, пока бот работает.
_runner = None
_site = None

# HTTP отвечает 200, даже если умерла фоновая автопроверка, — это разные вещи.
# Поэтому отдельно помним, когда рынок смотрели в последний раз.
_health = {
    "started_at": time.time(),
    "last_check_at": time.time(),
    "last_check_kind": "startup",
    "checks": 0,
}


async def handle_health_check(request):
    return web.Response(text="Bot is running!")


async def handle_status(request):
    """200 — автопроверка жива. 503 — процесс есть, а рынок никто не смотрит."""
    age = time.time() - _health["last_check_at"]
    stale = CHECK_INTERVAL_MIN > 0 and age > HEALTH_STALE_MIN * 60
    return web.json_response(
        {
            "status": "stale" if stale else "ok",
            "uptime_sec": int(time.time() - _health["started_at"]),
            "last_check_age_sec": int(age),
            "last_check_kind": _health["last_check_kind"],
            "checks": _health["checks"],
            "interval_min": CHECK_INTERVAL_MIN,
            "stale_after_min": HEALTH_STALE_MIN if CHECK_INTERVAL_MIN > 0 else None,
        },
        status=503 if stale else 200,
    )


async def start_web_server():
    global _runner, _site
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    app.router.add_get("/health", handle_status)
    _runner = web.AppRunner(app)
    await _runner.setup()
    port = int(os.getenv("PORT", 10000))
    _site = web.TCPSite(_runner, "0.0.0.0", port)
    await _site.start()


# --- Риск-менеджмент (считает только Python, нейросеть не участвует) ----
def risk_percent(balance: float) -> float:
    for limit, percent in RISK_TIERS:
        if balance < limit:
            return percent
    return RISK_TIERS[-1][1]


def min_stop_pct(balance: float) -> float:
    """Минимальная ширина стопа, при которой весь бюджет риска влезает
    в потолок плеча.

    Позиция ограничена балансом × MAX_LEVERAGE, а риск = позиция × стоп.
    Значит полный бюджет достигается только при стопе не уже
    риск% / MAX_LEVERAGE. Теснее — позиция упирается в потолок и риск
    (а вместе с ним и профит) оказываются ниже цели.
    """
    return risk_percent(balance) / MAX_LEVERAGE


def choose_leverage(position_usd: float, balance: float) -> float:
    """Минимальное плечо, при котором маржа укладывается в баланс."""
    for level in LEVERAGE_LADDER:
        if position_usd / level <= balance:
            return level
    return MAX_LEVERAGE


def usd(value: float) -> str:
    return f"${value:,.2f}"


# --- Разбор ответа модели ----------------------------------------------
def parse_setup(raw: str) -> dict:
    """Достаёт JSON из ответа модели (переживает markdown-обёртку)."""
    start, end = raw.find("{"), raw.rfind("}")
    if start == -1 or end <= start:
        raise ValueError("в ответе модели нет JSON")
    data = json.loads(raw[start : end + 1])

    direction = str(data.get("direction", "")).strip().upper()
    if direction not in {"LONG", "SHORT", "WAIT"}:
        raise ValueError(f"неизвестное направление: {direction!r}")

    def number(key: str) -> float:
        try:
            return float(data.get(key))
        except (TypeError, ValueError):
            raise ValueError(f"поле {key} не число: {data.get(key)!r}")

    raw_factors = data.get("confirmed_factors") or []
    if isinstance(raw_factors, str):
        raw_factors = [raw_factors]
    factors = set()
    for item in raw_factors:
        key = FACTOR_ALIASES.get(str(item).strip().lower())
        if key:
            factors.add(key)

    return {
        "direction": direction,
        "entry": number("entry"),
        "sl": number("sl"),
        "tp": number("tp"),
        "reason": str(data.get("reason", "")).strip(),
        "factors": factors,
    }


def build_trade(setup: dict, available: list, balance: float, price=None) -> dict:
    """Считает риск, позицию, плечо и профит кодом — модель ими не управляет."""
    reason = setup["reason"]

    if setup["direction"] == "WAIT":
        return {"direction": "WAIT", "reason": reason or "Подходящего сетапа нет."}

    # Конфлюэнция: голосов должно хватать. Требование приходит из decide_direction,
    # чтобы при недоступном источнике не требовать голоса, которого не существует.
    required = setup.get("required") or min(REQUIRED_FACTORS, len(available))
    confirmed = setup["factors"] & set(available)
    if required and len(confirmed) < required:
        return {
            "direction": "WAIT",
            "reason": (
                f"Подтверждено факторов: {len(confirmed)} из требуемых {required}. "
                f"По правилу конфлюэнции сетап отброшен.\n{reason}"
            ),
        }

    entry, sl, tp = setup["entry"], setup["sl"], setup["tp"]
    if entry <= 0 or sl <= 0 or tp <= 0:
        return {
            "direction": "WAIT",
            "reason": "Модель не дала уровни (нули) — сетапа для входа нет.",
        }

    # Уровни обязаны лежать по ту сторону входа: иначе это сетап другого
    # направления, а направление теперь решает код, не модель.
    direction = setup["direction"]
    if direction == "LONG" and not (sl < entry < tp):
        return {
            "direction": "WAIT",
            "reason": (
                f"Уровни не согласованы с {direction}: вход {usd(entry)}, "
                f"стоп {usd(sl)}, тейк {usd(tp)} — нужен стоп ниже входа, тейк выше."
            ),
        }
    if direction == "SHORT" and not (tp < entry < sl):
        return {
            "direction": "WAIT",
            "reason": (
                f"Уровни не согласованы с {direction}: вход {usd(entry)}, "
                f"стоп {usd(sl)}, тейк {usd(tp)} — нужен стоп выше входа, тейк ниже."
            ),
        }

    if price and abs(entry - price) / price > MAX_ENTRY_DRIFT:
        return {
            "direction": "WAIT",
            "reason": (
                f"Вход {usd(entry)} вне {MAX_ENTRY_DRIFT * 100:g}% от текущей "
                f"цены {usd(price)} — уровень взят не от рынка."
            ),
        }

    atr_pct = setup.get("atr_pct")
    spread = abs(entry - sl)
    if atr_pct is not None and atr_pct < MIN_ATR_PCT:
        return {
            "direction": "WAIT",
            "reason": (
                f"ATR {atr_pct:.3f}% ниже порога {MIN_ATR_PCT}% — "
                "волатильности нет, рынок стоит."
            ),
        }

    rr = abs(tp - entry) / spread
    if rr < MIN_RR:
        return {
            "direction": "WAIT",
            "reason": f"R/R {rr:.2f} ниже требуемых 1:{MIN_RR:g} — сетап отброшен.\n{reason}",
        }

    stop_fraction = spread / entry
    percent = risk_percent(balance)
    target_risk = balance * percent / 100
    target_position = target_risk / stop_fraction

    # Позиция ограничена потолком плеча — тогда фактический риск оказывается ниже цели.
    # Допуск 0.1% нужен из-за плавающей точки: при стопе ровно на границе
    # позиция формально упирается в потолок, но риск остаётся полным.
    capped = target_position > balance * MAX_LEVERAGE * 1.001
    position = min(target_position, balance * MAX_LEVERAGE)

    leverage = choose_leverage(position, balance)
    actual_risk = position * stop_fraction
    profit = position * abs(tp - entry) / entry
    size = position / entry

    return {
        "direction": setup["direction"],
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "rr": rr,
        "size": size,
        "position": position,
        "leverage": leverage,
        "margin": position / leverage,
        "risk": actual_risk,
        "risk_pct": actual_risk / balance * 100,
        "target_pct": percent,
        "profit": profit,
        "sl_pct": stop_fraction * 100,
        "capped": capped,
        "balance": balance,
        "confirmed": sorted(confirmed, key=lambda k: list(FACTOR_NAMES).index(k)),
        "reason": reason,
    }


def format_trade(trade: dict) -> str:
    asset = SYMBOL.split("/")[0]

    if trade["direction"] == "WAIT":
        return "\n".join(
            [
                "⏸ WAIT — сигнала нет",
                "",
                trade["reason"],
            ]
        )

    factors = ", ".join(FACTOR_NAMES.get(key, key) for key in trade["confirmed"])
    lines = [
        f"🚨 AI SIGNAL: {SYMBOL} ({trade['direction']})",
        f"🔹 Вход: {usd(trade['entry'])}",
        f"🛑 Стоп-лосс: {usd(trade['sl'])} ({trade['sl_pct']:.2f}% от входа)",
        f"🎯 Тейк-профит: {usd(trade['tp'])}",
        "",
        f"📊 Математика сделки (баланс {usd(trade['balance'])}, "
        f"базовый риск {trade['target_pct']:g}%):",
        f"🔴 Возможный убыток (Риск): -{usd(trade['risk'])} "
        f"({trade['risk_pct']:.2f}% баланса)",
        f"🟢 Заработок (Профит): +{usd(trade['profit'])} "
        f"({trade['profit'] / trade['balance'] * 100:.1f}% баланса)",
        f"⚖️ Risk/Reward: 1 : {trade['rr']:.2f}",
        f"📦 Размер позиции: {trade['size']:.6f} {asset} ({usd(trade['position'])})",
        f"🔑 Плечо: {trade['leverage']:g}x (маржа {usd(trade['margin'])})",
    ]
    if trade["capped"]:
        lines.append(
            "⚠️ Позиция упёрлась в потолок плеча "
            f"{MAX_LEVERAGE:g}x — риск меньше целевого."
        )
        lines.append(
            f"   Риск вышел {trade['risk_pct']:.2f}% вместо "
            f"{trade['target_pct']:g}%, профит сократился. Для полного "
            f"бюджета нужен стоп не уже "
            f"{min_stop_pct(trade['balance']):.2f}% от цены входа."
        )
    if trade["rr"] >= PREFERRED_RR:
        lines.append(
            f"📈 Цель 1:{trade['rr']:.1f} — не ниже приоритетной "
            f"1:{PREFERRED_RR:g}, прибыль на максимуме."
        )
    lines += [
        f"🧮 Направление по голосам: {trade['votes']}",
        f"✅ Факторы: {factors}",
        f"💡 Причина: {trade['reason']}",
    ]
    return "\n".join(lines)


def decide_direction(context: dict) -> dict:
    """Направление и объяснение считает код: конфлюэнция голосов, не мнение.

    Возвращает {"direction", "reason", "confirmed"}. Новости в счёт не идут:
    они не голосуют числом, а доверять их оценке модели — ровно то, из-за чего
    вердикт прыгал с LONG на WAIT и обратно.
    """
    scores = context.get("scores") or {}
    available = set(context.get("available") or [])
    numeric = {name: score for name, score in scores.items() if name != "news"}

    bulls = sorted(name for name, score in numeric.items() if score > 0 and name in available)
    bears = sorted(name for name, score in numeric.items() if score < 0 and name in available)
    calm = sorted(name for name, score in numeric.items() if score == 0 and name in available)

    required = min(REQUIRED_FACTORS, len(numeric))
    summary = (
        f"Быки: {', '.join(bulls) or 'нет'}. "
        f"Медведи: {', '.join(bears) or 'нет'}. "
        f"Нейтрально: {', '.join(calm) or 'нет'}."
    )

    if required and len(bulls) >= required and not bears:
        return {
            "direction": "LONG",
            "reason": summary,
            "confirmed": set(bulls),
            "required": required,
        }
    if required and len(bears) >= required and not bulls:
        return {
            "direction": "SHORT",
            "reason": summary,
            "confirmed": set(bears),
            "required": required,
        }

    return {
        "direction": "WAIT",
        "reason": (
            f"Направление не определено: {summary} "
            f"Нужно {required} голоса одного направления из "
            f"{len(numeric)} считающихся факторов."
        ),
        "confirmed": set(),
        "required": required,
    }


def build_system_prompt(direction: str, available: list, balance: float, price=None) -> str:
    """Модель подбирает уровни под направление, которое уже решил код."""
    percent = risk_percent(balance)
    price_line = (
        f"Текущая цена: {usd(price)}. "
        "Вход не далее 1.5% от неё.\n"
        if price
        else ""
    )
    return (
        "Ты — трейдер, подбирающий уровни под решение системы.\n"
        f"- Направление: {direction}. Его определил код по голосам факторов "
        "и оно НЕ обсуждается: не меняй его на WAIT и не переворачивай.\n"
        "- Если считаешь, что рынок идёт в другую сторону — всё равно дай "
        "уровни для указанного направления или поставь все нули.\n\n"
        f"- Баланс: {usd(balance)}.\n"
        f"- Риск на сделку: {percent:g}% ({usd(balance * percent / 100)}).\n"
        f"- Risk/Reward: жёсткий минимум 1:{MIN_RR:g}, приоритет — максимум "
        f"прибыли. Целись в 1:{PREFERRED_RR:g} и дальше (1:8–1:10), пока уровень "
        "реально существует на графике. Не срезай TP ради вероятности попадания.\n"
        "- Размер позиции, плечо и риск считает система — ты их не указываешь.\n\n"
        + price_line +
        "Доступные источники: " + ", ".join(available) + ".\n\n"
        "Правила для уровней:\n"
        "- entry — ближайший реальный уровень из данных (стенки, EMA, "
        "экстремумы свечей), не выдумывай.\n"
        f"- стоп за ближайшим сильным уровнем, но не ближе "
        f"{min_stop_pct(balance):.2f}% от цены входа: при более тесном стопе "
        f"позиция упрётся в потолок плеча {MAX_LEVERAGE:g}x, риск будет меньше "
        "бюджета, а профит ниже возможного.\n"
        "- tp — следующий реальный уровень в направлении сделки.\n"
        "- Не придумывай уровни, которых нет в данных.\n"
        "- Пиши поле reason строго на русском языке и упомяни, что решение о "
        "направлении принято голосами факторов.\n\n"
        "Ответь СТРОГО одним JSON-объектом, без markdown и пояснений вокруг:\n"
        f'{{"direction": "{direction}", "entry": <число>, "sl": <число>, '
        '"tp": <число>, "confirmed_factors": ["orderbook","derivatives",'
        '"technicals","news"], "reason": "<2-3 предложения>"}\n'
        "Если сетапа нет — направление ОСТАВЬ тем же, а entry/sl/tp = 0."
    )


def data_report(context: dict) -> str:
    """Честный ответ, почему сигнала нет, вместо запроса к модели."""
    working = [FACTOR_NAMES[name] for name in context["available"]]
    broken = [FACTOR_NAMES[name] for name in context["errors"]]
    detail_lines = []
    for name, reason in context["errors"].items():
        detail_lines.append(f"{FACTOR_NAMES[name]}:")
        for part in str(reason).split(" | "):
            detail_lines.append(f"  • {part[:170]}")
    detail = "\n".join(detail_lines)
    return (
        "❌ Недостаточно данных для сигнала.\n\n"
        f"Работают: {', '.join(working) or 'ничего'}\n"
        f"Не отвечают: {', '.join(broken) or 'ничего'}\n\n"
        f"Для сигнала нужно минимум {REQUIRED_FACTORS} источника "
        f"из {len(FACTOR_NAMES)}.\n"
        + (f"\nПричины:\n{detail}" if detail else "")
    )


# --- Лог сигналов --------------------------------------------------------

def log_signal(trade: dict, context: dict, raw: str = "") -> None:
    """Пишет исход /predict в SQLite. Ошибка логирования не должна ронять бота."""
    try:
        signal_log.record(
            created_at=signal_log.now_iso(),
            symbol=SYMBOL,
            direction=trade.get("direction", "?"),
            entry=trade.get("entry"),
            sl=trade.get("sl"),
            tp=trade.get("tp"),
            rr=trade.get("rr"),
            stop_pct=trade.get("sl_pct"),
            risk_usd=trade.get("risk"),
            risk_pct=trade.get("risk_pct"),
            profit_usd=trade.get("profit"),
            position_usd=trade.get("position"),
            leverage=trade.get("leverage"),
            margin_usd=trade.get("margin"),
            capped=trade.get("capped"),
            factors=",".join(trade.get("confirmed") or []),
            sources=",".join(context.get("available") or []),
            balance=trade.get("balance", BALANCE),
            reason=trade.get("reason", ""),
            raw=raw[:4000],
        )
    except Exception:  # noqa: BLE001
        log.exception("не удалось записать сигнал в лог")


def no_data_trade(context: dict) -> dict:
    """Мини-запись для случая, когда источников данных меньше трёх."""
    return {
        "direction": "NODATA",
        "reason": "Нет источников: " + (", ".join(context["errors"]) or "неизвестно"),
    }


def _plural(number: int, forms: tuple) -> str:
    """Склонение существительного: (запись, записи, записей)."""
    if number % 10 == 1 and number % 100 != 11:
        return forms[0]
    if 2 <= number % 10 <= 4 and not 12 <= number % 100 <= 14:
        return forms[1]
    return forms[2]


def _stamp(iso: str) -> str:
    """Время записи из базы в нашем поясе: сервер пишет свой, мы показываем свой."""
    if not iso:
        return ""
    try:
        moment = datetime.fromisoformat(iso)
    except ValueError:
        # Мусор в базе не бывает: пишет now_iso(). Старые записи без T — да.
        return iso[5:16].replace("T", " ") if iso[:4].isdigit() else ""
    if moment.tzinfo is None:
        # Старые записи без пояса считаем UTC — серверы Render в нём живут.
        return (moment + timedelta(minutes=TZ_OFFSET_MIN)).strftime("%m-%d %H:%M")
    stored = moment.utcoffset() or timedelta(0)
    local = moment - stored + timedelta(minutes=TZ_OFFSET_MIN)
    return local.strftime("%m-%d %H:%M")


def format_history(rows: list) -> str:
    """Человекочитаемый список записей из базы."""
    lines = [
        f"📜 Последние {len(rows)} {_plural(len(rows), ('запись', 'записи', 'записей'))} "
        "(новые сверху):",
        "",
    ]
    for row in rows:
        stamp = _stamp(row["created_at"])
        direction = row["direction"]

        if direction in ("LONG", "SHORT") and row["profit_usd"] is not None:
            balance = row["balance"] or 0
            header = f"{stamp}  🚨 {direction}  профит ${row['profit_usd']:.2f}"
            if balance:
                header += f" ({row['profit_usd'] / balance * 100:.1f}%)"
            if row["rr"]:
                header += f"  R:R {row['rr']:.2f}"
            lines.append(header)

            details = []
            if row["stop_pct"] is not None:
                details.append(f"стоп {row['stop_pct']:.2f}%")
            if row["risk_usd"] is not None:
                details.append(f"риск ${row['risk_usd']:.2f}")
            if row["leverage"] is not None:
                details.append(f"плечо {row['leverage']:g}x")
            if row["capped"]:
                details.append("упёрлась в потолок")
            if details:
                lines.append("        " + ", ".join(details))

        elif direction == "WAIT":
            lines.append(f"{stamp}  ⏸ WAIT")
            reason = (row["reason"] or "").splitlines()
            if reason:
                lines.append(f"        {reason[0][:90]}")

        elif direction == "NODATA":
            lines.append(f"{stamp}  ❌ нет данных  ({row['sources'] or 'ничего'})")

        else:
            lines.append(f"{stamp}  ⚠️ {direction}")

    return "\n".join(lines)


# --- Команды -----------------------------------------------------------
async def safe_edit(message: Message, text: str) -> None:
    """edit_text, который не падает, если текст не изменился."""
    try:
        await message.edit_text(text)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


@dp.message(Command("start"))
async def start_cmd(message: Message):
    remember(message)
    percent = risk_percent(BALANCE)
    await message.answer(
        "👋 Привет! Я крипто-аналитик.\n"
        "Отправь /predict — соберу стакан, деривативы, индикаторы и новости, "
        "и дам торговый сетап.\n"
        "📜 /history — прошлые сигналы и что из них вышло.\n"
        "📡 /signals — подписка на автосигналы: вкл/выкл/статус.\n"
        "📒 /paper — бумажная торговля на Bybit: настоящие ордера, "
        "фальшивые деньги.\n"
        "📗 /positions — открытые позиции и последние сделки.\n\n"
        f"Баланс: {usd(BALANCE)}\n"
        f"Риск на сделку сейчас: {percent:g}% ({usd(BALANCE * percent / 100)})\n"
        f"Фильтры: R:R от 1:{MIN_RR:g}, цель 1:{PREFERRED_RR:g} и дальше, "
        f"минимум {REQUIRED_FACTORS} фактора, "
        f"плечо до {MAX_LEVERAGE:g}x\n\n"
        f"📡 Подписка: {subscription_state(message.chat.id)}\n"
        f"🆔 ID чата: {message.chat.id}\n"
        f"{persistence_hint(message.chat.id)}"
    )


def subscription_state(chat_id) -> str:
    """Короткий статус чата для ответов бота."""
    try:
        if signal_log.is_subscribed(chat_id):
            return "включена ✅"
        known = any(cid == chat_id for cid, _ in signal_log.all_chats())
        return "выключена 🔕" if known else "не подключена"
    except Exception:  # noqa: BLE001
        log.exception("не удалось получить статус подписки")
        return "статус недоступен"


def persistence_hint(chat_id) -> str:
    """Готовая строка для дашборда Render — подписка переживает деплой.

    Чаты, которые Telegram не нашёл при старте, в подсказку не попадают —
    иначе бот советовал бы скопировать в переменную мусор.
    """
    known = [cid for cid in env_chat_ids() if _env_status.get(cid) != BAD_CHAT]
    if chat_id in known:
        return "✅ TELEGRAM_CHAT_IDS уже задана — подписка переживёт любой деплой."
    merged = ",".join(str(item) for item in dict.fromkeys([*known, chat_id]))
    return (
        "⚠️ Деплой Render стирает базу чатов. Чтобы подписка переживала рестарт, "
        "добавь на Render переменную\n"
        f"TELEGRAM_CHAT_IDS={merged}\n"
        "(Environment → Environment Variables → Save, бот перезапустится сам)."
    )


@dp.message(Command("signals", "подписка"))
async def signals_cmd(message: Message):
    """Подписка на автосигналы: /signals on | off | статус без аргумента."""
    parts = (message.text or "").split()
    arg = parts[1].lower() if len(parts) > 1 else ""
    chat_id = message.chat.id

    if arg in ("on", "вкл", "enable", "start"):
        try:
            signal_log.subscribe(chat_id)
        except Exception:  # noqa: BLE001
            log.exception("не удалось включить подписку чата %s", chat_id)
            await message.answer("❌ Не получилось включить подписку, подробности в логе.")
            return
        await message.answer(
            "✅ Подписка включена — сигналы будут приходить сами.\n"
            f"Чат: {chat_id}\n"
            f"Условие отправки: {CONFIRM_REQUIRED} одинаковых направления за "
            f"{CONFIRM_WINDOW_SEC // 60} мин, повтор не чаще {REPEAT_SIGNAL_SEC // 60} мин.\n"
            f"Проверка рынка: каждые {CHECK_INTERVAL_MIN} мин "
            f"(+{CHECK_DELAY_SEC} с после закрытия свечи).\n\n"
            f"{persistence_hint(chat_id)}"
        )
        return

    if arg in ("off", "выкл", "disable", "stop"):
        try:
            known = any(cid == chat_id for cid, _ in signal_log.all_chats())
            was_on = signal_log.unsubscribe(chat_id)
        except Exception:  # noqa: BLE001
            log.exception("не удалось выключить подписку чата %s", chat_id)
            await message.answer("❌ Не получилось выключить подписку, подробности в логе.")
            return
        if was_on:
            await message.answer(
                f"🔕 Подписка выключена для чата {chat_id}.\n"
                "Включить снова: /signals on"
            )
        elif known:
            await message.answer(
                f"Чат {chat_id} и так не подписан. Включить: /signals on"
            )
        else:
            await message.answer(
                f"Чат {chat_id} не подписан. Включить: /signals on"
            )
        return

    # Без аргументов — статус.
    try:
        pairs = signal_log.all_chats()
    except Exception:  # noqa: BLE001
        log.exception("не удалось получить чаты")
        pairs = []
    active = [cid for cid, enabled in pairs if enabled]
    state = subscription_state(chat_id)
    env_set = bool(env_chat_ids())
    await message.answer(
        "📡 Подписка на сигналы\n\n"
        f"Чат {chat_id}: {state}\n"
        f"Чатов с подпиской: {len(active)} из {len(pairs)}\n\n"
        f"Расписание: каждые {CHECK_INTERVAL_MIN} мин (+{CHECK_DELAY_SEC} с после свечи)\n"
        f"Подтверждение: {CONFIRM_REQUIRED} направления за {CONFIRM_WINDOW_SEC // 60} мин\n"
        f"Повтор сигнала: не чаще {REPEAT_SIGNAL_SEC // 60} мин\n"
        f"Факторы: минимум {REQUIRED_FACTORS}, встречный голос запрещает\n"
        f"{env_status_line()}\n\n"
        "/signals on — включить, /signals off — выключить\n"
        f"{'' if env_set else persistence_hint(chat_id)}"
    )


def remember(message: Message) -> None:
    """Запоминает чат — без этого автопроверке некому будет писать."""
    try:
        signal_log.save_chat(message.chat.id)
    except Exception:  # noqa: BLE001
        log.exception("не удалось запомнить чат")


# --- Paper trading: настоящие ордера на Bybit ---------------------------
# Включается ключами из окружения; /paper on|off переключает на лету и
# запоминает выбор в базе. Позиция живёт на бирже, поэтому переживает
# и рестарт процесса, и стирание файловой системы на Render.
PAPER_DEFAULT = os.getenv("PAPER_TRADING", "1") != "0"
CLOSE_BUTTONS = InlineKeyboardMarkup(
    inline_keyboard=[[InlineKeyboardButton(text="🛑 Закрыть позицию", callback_data="close_position")]]
)


def paper_enabled() -> bool:
    """Выставлять ли ордера на биржу. Ключи обязательны — иначе только сигнал."""
    stored = signal_log.get_setting("paper")
    if stored is None:
        return PAPER_DEFAULT and bybit.enabled()
    return stored == "on" and bybit.enabled()


async def place_paper_order(trade: dict, signal_id=None) -> str:
    """Ставит ордер по подтверждённому сигналу. Возвращает текст для чата.

    Пустая строка — paper trading выключен, молчим. Любая проблема
    превращается в понятную строку: сигнал всё равно уходит в чат,
    а упавший ордер не должен его отменять.
    """
    if not paper_enabled():
        return ""
    if not bybit.enabled():
        return "⚠️ Paper trading включён, но ключи Bybit не заданы — ордер не выставлен."

    try:
        open_positions = await bybit.positions()
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось прочитать позиции с биржи")
        return f"⚠️ Ордер не выставлен: биржа недоступна ({exc})."

    if open_positions:
        position = open_positions[0]
        return (
            f"⏸ Позиция уже открыта ({position['side']} {position['size']}), "
            "новый ордер не ставил."
        )

    try:
        info = await bybit.instrument_info()
        price = await bybit.ticker_price()
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось получить данные биржи")
        return f"⚠️ Ордер не выставлен: данные биржи недоступны ({exc})."

    drift = abs(price - trade["entry"]) / trade["entry"]
    if drift > MAX_ENTRY_DRIFT:
        return (
            f"⚠️ Ордер не выставлен: цена ушла на {drift * 100:.2f}% от входа "
            f"(лимит {MAX_ENTRY_DRIFT * 100:.1f}%)."
        )

    qty, problem = bybit.build_qty(trade["size"], price, info)
    if problem:
        return f"⚠️ Ордер не выставлен: {problem}."

    try:
        await bybit.set_leverage(trade["leverage"])
        order_id = await bybit.open_position(
            trade["direction"], qty, trade["entry"], trade["sl"], trade["tp"], info["tick"]
        )
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось выставить ордер на бирже")
        return f"⚠️ Ордер не выставлен: {exc}"

    try:
        signal_log.log_position(
            signal_id=signal_id,
            mode=bybit.mode(),
            direction=trade["direction"],
            qty=bybit.num(qty),
            price=price,
            entry=trade["entry"],
            sl=trade["sl"],
            tp=trade["tp"],
            leverage=trade["leverage"],
            order_id=order_id,
        )
    except Exception:  # noqa: BLE001
        log.exception("ордер выставлен, но записать его в журнал не удалось")

    log.info(
        "ордер выставлен (%s): %s %s qty=%s вход=%s стоп=%s цель=%s плечо=%sx id=%s",
        bybit.mode(), trade["direction"], SYMBOL, bybit.num(qty),
        trade["entry"], trade["sl"], trade["tp"], trade["leverage"], order_id,
    )
    return "\n".join(
        [
            f"📋 Ордер выставлен на Bybit {bybit.mode()}",
            f"Market {trade['direction']} {bybit.num(qty)} {SYMBOL.split('/')[0]} ≈ {usd(price)}",
            f"Стоп {usd(trade['sl'])} · Цель {usd(trade['tp'])} · Плечо {trade['leverage']:g}x",
            f"ID: {order_id}",
        ]
    )


async def paper_stats() -> str:
    """Реализованный результат с биржи — то, чего не было в /stats."""
    if not bybit.enabled():
        return ""
    try:
        deals = await bybit.closed_pnl(100)
    except Exception as exc:  # noqa: BLE001
        log.warning("не удалось получить закрытые сделки: %s", exc)
        return ""
    if not deals:
        return ""
    total = sum(deal["pnl"] for deal in deals)
    wins = sum(1 for deal in deals if deal["pnl"] > 0)
    return "\n".join(
        [
            "",
            "📈 Реальный paper trading (Bybit " + bybit.mode() + ")",
            f"Закрыто сделок: {len(deals)}",
            f"Прибыльных: {wins} ({wins / len(deals) * 100:.0f}%)",
            f"Реализованный PnL: {usd(total)}",
        ]
    )


async def analyze(progress=None) -> dict:
    """Полный проход: данные рынка → модель → расчёт риска.

    Один и тот же код идут вручную (/predict) и по расписанию.
    progress — необязательный колбэк для промежуточных статусов,
    в фоновом режиме его не передают.

    Возвращает:
      kind   — signal | wait | nodata | data_error | groq_error | parse_error
      score  — сколько сигналов этого направления встретилось за окно
               подтверждения (0 для не-сигналов)
      text   — готовое сообщение для пользователя
      trade  — расчёт (для лога)
      context, raw — контекст рынка и сырой ответ модели (для лога)
    """
    try:
        context = await build_market_context(SYMBOL)
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось собрать рыночные данные")
        trade = {"direction": "DATA_ERROR", "reason": str(exc)}
        log_signal(trade, {})
        return {
            "kind": "data_error",
            "score": register_verdict("data_error"),
            "text": f"❌ Не удалось собрать рыночные данные:\n{exc}",
            "trade": trade,
            "context": {},
            "raw": "",
        }

    log.info(
        "факторы: %s | ошибки: %s",
        ", ".join(context["available"]) or "нет",
        context["errors"] or "нет",
    )

    if len(context["available"]) < REQUIRED_FACTORS:
        # Без 3 источников сигнал невозможен — не тратим запрос к модели впустую.
        trade = no_data_trade(context)
        log_signal(trade, context)
        return {
            "kind": "nodata",
            "score": register_verdict("nodata"),
            "text": data_report(context),
            "trade": trade,
            "context": context,
            "raw": "",
        }

    decision = decide_direction(context)
    if decision["direction"] == "WAIT":
        # Голосов не набралось — запрос к модели не нужен: её субъективное
        # мнение и давало перевороты направления за минуту.
        trade = {"direction": "WAIT", "reason": decision["reason"]}
        log_signal(trade, context)
        return {
            "kind": "wait",
            "score": register_verdict("wait"),
            "text": format_trade(trade),
            "trade": trade,
            "context": context,
            "raw": "",
        }

    if progress:
        await progress(
            f"🧠 Подбираю уровни для {decision['direction']} через {GROQ_MODEL}..."
        )

    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": build_system_prompt(
                    decision["direction"],
                    context["available"],
                    BALANCE,
                    context.get("price"),
                )},
                {"role": "user", "content": context["text"]},
            ],
            temperature=0.05,
            response_format={"type": "json_object"},
        )
        raw = response.choices[0].message.content or ""
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось вызвать нейросеть")
        trade = {"direction": "GROQ_ERROR", "reason": str(exc)}
        log_signal(trade, context)
        return {
            "kind": "groq_error",
            "score": register_verdict("groq_error"),
            "text": f"❌ Ошибка вызова нейросети:\n{exc}",
            "trade": trade,
            "context": context,
            "raw": "",
        }

    try:
        setup = parse_setup(raw)
        setup["atr_pct"] = context["atr_pct"]
        # Направление, факторы и требование конфлюэнции уже решил код —
        # ответ модели их не может изменить.
        setup["direction"] = decision["direction"]
        setup["factors"] = decision["confirmed"]
        setup["required"] = decision["required"]
        trade = build_trade(
            setup, context["available"], BALANCE, context.get("price")
        )
        trade["votes"] = decision["reason"]
        if trade["direction"] == "WAIT":
            trade["reason"] = f"{trade['reason']}\n{decision['reason']}"
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("не удалось разобрать ответ модели: %s | raw=%s", exc, raw)
        trade = {"direction": "PARSE_ERROR", "reason": str(exc)}
        log_signal(trade, context, raw)
        return {
            "kind": "parse_error",
            "score": register_verdict("parse_error"),
            "text": f"❌ Модель ответила не по формату: {exc}",
            "trade": trade,
            "context": context,
            "raw": raw,
        }

    log_signal(trade, context, raw)
    kind = "signal" if trade["direction"] in ("LONG", "SHORT") else "wait"
    return {
        "kind": kind,
        "score": register_verdict(kind, trade["direction"]),
        "text": format_trade(trade),
        "trade": trade,
        "context": context,
        "raw": raw,
    }


@dp.message(Command("predict"))
async def predict_cmd(message: Message):
    remember(message)
    msg = await message.answer("📊 Собираю данные: стакан, деривативы, индикаторы, новости...")

    async def progress(text: str) -> None:
        await safe_edit(msg, text)

    result = await analyze(progress)
    if result["kind"] == "signal":
        # Пользователь уже увидел сигнал — фоновой задаче дублировать его не нужно.
        note_sent(result["trade"]["direction"])
    await safe_edit(msg, result["text"])


@dp.message(Command("history"))
async def history_cmd(message: Message):
    """Последние записи из лога сигналов."""
    remember(message)
    try:
        rows = signal_log.recent(8)
        total = signal_log.total()
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось прочитать лог сигналов")
        await message.answer(f"❌ Не удалось прочитать лог: {exc}")
        return

    if not rows:
        await message.answer("📜 Лог пуст — сигналов ещё не было.")
        return

    await message.answer(f"{format_history(rows)}\n\nВсего записей: {total}")


@dp.message(Command("stats"))
async def stats_cmd(message: Message):
    """Сводка для анализа paper trading."""
    remember(message)
    try:
        data = signal_log.stats()
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось прочитать статистику")
        await message.answer(f"❌ Не удалось прочитать статистику: {exc}")
        return

    if not data["signals"]:
        await message.answer(
            "📊 Сделок пока нет — статистика появится после первых сигналов."
        )
        return

    await message.answer(
        "\n".join(
            [
                "📊 Статистика сигналов",
                "",
                f"Сделок (LONG/SHORT): {data['signals']}",
                f"Пропущено (WAIT): {data['waits']}",
                f"Суммарный профит: {usd(data['total_profit'] or 0)}",
                f"Средний профит на сделку: {usd(data['avg_profit'] or 0)}",
                f"Средняя ширина стопа: {data['avg_stop']:.2f}%",
                f"Средний R:R: {data['avg_rr']:.2f}",
            ]
        )
        + "\n"
        + await paper_stats()
    )


# --- Paper trading: команды и кнопки ------------------------------------

async def paper_status_text() -> str:
    """Что сейчас с paper trading: режим, ключи, баланс, позиция."""
    lines = [f"📒 Paper trading — Bybit {bybit.mode()}", ""]
    lines.append(
        f"Режим: {'включён ✅' if paper_enabled() else 'выключен 🔕'}"
    )
    if not bybit.enabled():
        lines.append("Ключи BYBIT_KEY / BYBIT_SECRET: не заданы ⚠️")
        lines.append("Без них бот шлёт только сигналы, ордера не ставит.")
    else:
        lines.append("Ключи: заданы ✅")
        try:
            balance = await bybit.wallet_balance()
            lines.append(
                f"Баланс счёта: {usd(balance['wallet'])} "
                f"(equity {usd(balance['equity'])}, {balance['account_type']})"
            )
        except Exception as exc:  # noqa: BLE001
            lines.append(f"Баланс: недоступен ({exc})")
        try:
            open_positions = await bybit.positions()
        except Exception as exc:  # noqa: BLE001
            lines.append(f"Позиции: недоступны ({exc})")
        else:
            if open_positions:
                for position in open_positions:
                    lines.append(
                        f"Открыта: {position['side']} {position['size']} по "
                        f"{usd(position['entry'])}, плечо {position['leverage']:g}x, "
                        f"PnL {usd(position['pnl'])}"
                    )
            else:
                lines.append("Открытых позиций нет")
    lines += [
        "",
        "/paper on — включить, /paper off — выключить",
        "/positions — открытые и последние сделки",
    ]
    return "\n".join(lines)


@dp.message(Command("paper", "бумага"))
async def paper_cmd(message: Message):
    """Paper trading: /paper on | off | статус без аргумента."""
    remember(message)
    parts = (message.text or "").split()
    arg = parts[1].lower() if len(parts) > 1 else ""

    if arg in ("on", "вкл", "enable", "start"):
        if not bybit.enabled():
            await message.answer(
                "❌ Ключи Bybit не заданы. Добавь на Render переменные "
                "BYBIT_KEY и BYBIT_SECRET (тестнет: testnet.bybit.com → API), "
                "потом повтори /paper on."
            )
            return
        signal_log.set_setting("paper", "on")
        await message.answer(
            "✅ Paper trading включён — подтверждённый сигнал выставит ордер "
            f"на Bybit {bybit.mode()}.\n\n" + await paper_status_text()
        )
        return

    if arg in ("off", "выкл", "disable", "stop"):
        signal_log.set_setting("paper", "off")
        await message.answer(
            "🔕 Paper trading выключен — бот продолжит слать сигналы, "
            "но ордера ставить не будет.\n\n" + await paper_status_text()
        )
        return

    await message.answer(await paper_status_text())


@dp.message(Command("positions", "позиции"))
async def positions_cmd(message: Message):
    """Открытые позиции и последние закрытые сделки — с самой биржи."""
    remember(message)
    lines = ["📗 Позиции и сделки", ""]

    if not bybit.enabled():
        lines.append("Ключи Bybit не заданы — биржевая статистика недоступна.")
    else:
        try:
            open_positions = await bybit.positions()
        except Exception as exc:  # noqa: BLE001
            lines.append(f"Открытые позиции: недоступны ({exc})")
        else:
            if open_positions:
                for position in open_positions:
                    lines += [
                        f"Открыта {position['side']} {position['size']} BTC",
                        f"  вход {usd(position['entry'])} → сейчас {usd(position['mark'])}",
                        f"  плечо {position['leverage']:g}x · PnL {usd(position['pnl'])}",
                        f"  стоп {usd(position['sl'])} · цель {usd(position['tp'])}",
                    ]
            else:
                lines.append("Открытых позиций нет")

        try:
            deals = await bybit.closed_pnl(5)
        except Exception as exc:  # noqa: BLE001
            lines.append(f"Закрытые сделки: недоступны ({exc})")
        else:
            if deals:
                lines += ["", "Последние закрытые:"]
                for deal in deals:
                    stamp = datetime.fromtimestamp(
                        deal["closed_at"] / 1000
                    ).astimezone().strftime("%d.%m %H:%M")
                    lines.append(
                        f"  {stamp}  {deal['side']} {deal['qty']}  "
                        f"PnL {usd(deal['pnl'])}"
                    )
            else:
                lines += ["", "Закрытых сделок пока нет"]

    journal = signal_log.positions_log(5)
    if journal:
        lines += ["", "Журнал ордеров:"]
        for row in journal:
            lines.append(
                f"  {row['opened_at'][:16]}  {row['direction']} {row['qty']} "
                f"@ {row['price']}  {row['order_id'] or ''}"
            )

    await message.answer("\n".join(lines))


@dp.callback_query(F.data == "close_position")
async def close_position_cb(callback: CallbackQuery):
    """Кнопка «Закрыть позицию» под сигналом: снимает ордера и закрывает."""
    if not bybit.enabled():
        await callback.answer("Ключи Bybit не заданы", show_alert=True)
        return
    try:
        open_positions = await bybit.positions()
    except Exception as exc:  # noqa: BLE001
        await callback.answer(f"Биржа недоступна: {exc}", show_alert=True)
        return
    if not open_positions:
        await callback.answer("Открытых позиций нет", show_alert=True)
        return

    position = open_positions[0]
    try:
        await bybit.cancel_all()
        await bybit.close_position()
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось закрыть позицию")
        await callback.answer(f"Не удалось закрыть: {exc}", show_alert=True)
        return

    try:
        deals = await bybit.closed_pnl(3)
        last = deals[0] if deals else None
    except Exception:  # noqa: BLE001
        last = None

    text = f"🛑 Позиция закрыта вручную: {position['side']} {position['size']} BTC"
    if last:
        text += f"\nРеализованный PnL: {usd(last['pnl'])}"
    await callback.message.answer(text)
    await callback.answer("Закрыто")


# --- Автопроверка по расписанию -----------------------------------------
_auto = {
    "direction": None,   # напр. последний отправленный сигнал
    "sent_at": 0.0,      # monotonic-время отправки (анти-спам)
    "failures": 0,       # подряд неудачных проверок
    "alerted": False,     # предупреждение об отказе уже уходило
    "seen": [],          # [(направление, момент)] — окно подтверждения
}
_background_tasks: set = set()


def register_verdict(kind: str, direction=None) -> int:
    """Ведёт окно последних сигналов и считает, сколько их одного направления.

    За две минуты модель успевает пройти SHORT → WAIT → LONG, поэтому одного
    появления мало. Считаем повтор в пределах CONFIRM_WINDOW_SEC — даже если
    между ними были WAIT: они не подтверждают, но и не обнуляют, иначе при
    75 % случаев «нет данных о направлении» подтверждение не наберётся никогда.
    """
    now = time.monotonic()
    window = _auto["seen"]
    window[:] = [item for item in window if now - item[1] <= CONFIRM_WINDOW_SEC]
    if kind != "signal":
        return 0
    window.append((direction, now))
    return sum(1 for item in window if item[0] == direction)


def note_sent(direction: str) -> None:
    """Помечает, что сигнал уже показан — чтобы фоновая задача не дублировала."""
    _auto["direction"] = direction
    _auto["sent_at"] = time.monotonic()


def seed_auto_state() -> None:
    """После рестарта не повторяет сигнал, который уже был в логе."""
    try:
        direction = signal_log.last_signal_direction()
    except Exception:  # noqa: BLE001
        log.exception("не удалось прочитать последний сигнал из лога")
        return
    if direction:
        # Время считаем «сейчас»: после перезапуска дубль не нужен.
        note_sent(direction)


async def restore_subscriptions() -> None:
    """Поднимает подписки из переменной окружения и отсеивает чужие ID.

    Переменную на Render заполняют руками, поэтому в неё с легкостью
    попадает опечатка или ID, скопированный из чужого лога: такой чат
    подписываем только после того, как Telegram подтвердил его наличие.
    Сетевая ошибка — не приговор: чат подписывается всё равно, статус
    остаётся «не проверен», чтобы обрыв связи не лишил пользователя сигналов.
    """
    ids = env_chat_ids()
    if not ids:
        log.warning(
            "TELEGRAM_CHAT_IDS не задана — после каждого деплоя подписку "
            "придётся включать заново командой /signals on"
        )
        return

    restored = []
    for chat_id in ids:
        try:
            await bot.get_chat(chat_id)
        except Exception as exc:  # noqa: BLE001
            message = str(exc).lower()
            if any(word in message for word in (
                "chat not found",
                "peer_id_invalid",
                "user is deactivated",
                "bot was blocked",
            )):
                _env_status[chat_id] = BAD_CHAT
                log.warning(
                    "чат %s из TELEGRAM_CHAT_IDS не существует в Telegram — "
                    "подписку не восстанавливаю, убери его из переменной",
                    chat_id,
                )
                continue
            _env_status[chat_id] = "не проверен"
            log.warning(
                "не удалось проверить чат %s (%s) — восстанавливаю без проверки",
                chat_id,
                exc,
            )
        else:
            _env_status[chat_id] = "ok"

        try:
            signal_log.subscribe(chat_id)
        except Exception:  # noqa: BLE001
            log.exception("не удалось восстановить подписку чата %s", chat_id)
            continue
        restored.append(chat_id)

    log.info("подписки восстановлены: %s (проверка: %s)", restored, dict(_env_status))


def env_status_line() -> str:
    """Строка про переменную окружения с проверкой каждого чата внутри неё."""
    ids = env_chat_ids()
    if not ids:
        return "Автовосстановление: TELEGRAM_CHAT_IDS не задана ⚠️"
    parts = []
    for cid in ids:
        status = _env_status.get(cid)
        if status == "ok":
            parts.append(f"{cid} ✅")
        elif status == BAD_CHAT:
            parts.append(f"{cid} ❌ {BAD_CHAT}")
        else:
            parts.append(f"{cid} … не проверен")
    tail = "; убери их из переменной" if BAD_CHAT in (_env_status.get(cid) for cid in ids) else ""
    return f"Автовосстановление: TELEGRAM_CHAT_IDS задана ✅\n{', '.join(parts)}{tail}"


def seconds_until_next_check(moment=None) -> float:
    """Ждём границу интервала плюс задержку — свеча должна закрыться.

    Интервал 15 → проверки в 21:15:45, 21:30:45, 21:45:45.
    """
    now = moment or datetime.now().astimezone()
    interval = max(CHECK_INTERVAL_MIN, 1)
    base = now.replace(minute=(now.minute // interval) * interval, second=0, microsecond=0)
    candidate = base + timedelta(seconds=CHECK_DELAY_SEC)
    if candidate <= now:
        candidate = base + timedelta(minutes=interval, seconds=CHECK_DELAY_SEC)
    return max((candidate - now).total_seconds(), 1.0)


async def broadcast(text: str, keyboard=None) -> None:
    """Шлёт сообщение во все чаты, с которыми бот уже разговаривал."""
    try:
        targets = signal_log.chats()
    except Exception:  # noqa: BLE001
        log.exception("не удалось прочитать список чатов")
        return
    if not targets:
        log.warning("автосообщение некому отправить — чаты не запомнены")
        return
    for chat_id in targets:
        try:
            await bot.send_message(chat_id, text, reply_markup=keyboard)
        except Exception:  # noqa: BLE001
            log.exception("не удалось отправить сообщение в чат %s", chat_id)


async def auto_check() -> None:
    """Одна проверка по расписанию: проанализировать и решить, слать ли."""
    # Помним тик до вызова модели: если та зависнет, возраст пойдёт в рост,
    # и /health честно ответит 503 вместо вечно зелёного 200.
    _health["checks"] += 1
    _health["last_check_at"] = time.time()
    _health["last_check_kind"] = "running"
    result = await analyze()
    _health["last_check_kind"] = result["kind"]
    kind = result["kind"]

    if kind in ("signal", "wait"):
        _auto["failures"] = 0
        _auto["alerted"] = False

    if kind != "signal":
        if kind == "wait":
            return  # молчим: ждём, пока появятся факторы
        _auto["failures"] += 1
        log.warning(
            "автопроверка не удалась (%s), подряд: %s",
            kind, _auto["failures"],
        )
        if _auto["failures"] >= AUTO_FAILURE_ALERT and not _auto["alerted"]:
            _auto["alerted"] = True
            await broadcast(
                "⚠️ Автопроверка не работает "
                f"{_auto['failures']} раза подряд.\n\n{result['text']}"
            )
        return

    direction = result["trade"]["direction"]
    score = result.get("score", 0)  # посчитан в analyze() через register_verdict
    if score < CONFIRM_REQUIRED:
        # Один раз за окно показалось — ждём повтора. Именно так из лога
        # уходит переворот SHORT → WAIT → LONG, который появился трижды за 2 минуты.
        log.info(
            "сигнал %s не подтверждён (%s из %s за %s мин) — молчим",
            direction, score, CONFIRM_REQUIRED, CONFIRM_WINDOW_SEC // 60,
        )
        return

    same = direction == _auto["direction"]
    recent = time.monotonic() - _auto["sent_at"] < REPEAT_SIGNAL_SEC
    if same and recent:
        log.info("сигнал %s уже показан, не повторяю", direction)
        return
    note_sent(direction)
    log.info(
        "сигнал %s подтверждён (%s за %s мин) — отправляю",
        direction, score, CONFIRM_WINDOW_SEC // 60,
    )
    # Ордер ставится до отправки: если биржа отказалась, причина
    # попадает в то же сообщение, что и сигнал.
    paper_note = await place_paper_order(result["trade"])
    await broadcast(
        result["text"] + (f"\n\n{paper_note}" if paper_note else ""),
        keyboard=CLOSE_BUTTONS if paper_note.startswith("📋") else None,
    )


def next_check_delay() -> float:
    """Ждать до следующей проверки; падение расчёта не должно убивать цикл."""
    try:
        return seconds_until_next_check()
    except Exception:  # noqa: BLE001
        log.exception("не удалось вычислить время следующей проверки")
        return max(CHECK_INTERVAL_MIN, 1) * 60


async def scheduler() -> None:
    """Фоновая задача: проверяет рынок по расписанию, пока жив процесс.

    Тело цикла целиком под try: задача не должна умереть молча — иначе
    веб-сервер продолжит отвечать 200, а сигналы просто перестанут ходить.
    """
    log.info(
        "автопроверка включена: каждые %s мин (+%s с после закрытия свечи)",
        CHECK_INTERVAL_MIN,
        CHECK_DELAY_SEC,
    )
    while True:
        delay = next_check_delay()
        log.info("следующая автопроверка через %.1f мин", delay / 60)
        try:
            await asyncio.sleep(delay)
            await auto_check()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            # Цикл не прерываем: следующий круг сам выждет ближайшую границу
            # интервала, поэтому падение не ускоряет и не сдвигает проверки.
            log.exception("автопроверка упала — жду следующего интервала")


async def run_polling() -> None:
    """Держит long polling живым: сбой сети не должен убивать процесс.

    Без этого любая ошибка Telegram вылетала бы из main() и процесс
    завершался бы вместе с веб-сервером — Render получил бы 5xx, и бот
    лежал бы до ручного рестарта.
    """
    delay = POLLING_RETRY_BASE_SEC
    while True:
        started = time.monotonic()
        try:
            await dp.start_polling(bot)
            log.warning("polling завершился сам — перезапускаю")
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("polling упал, перезапуск через %.0f с", delay)
        # Долго прожил — значит, дело было не в токене, обнуляем backoff.
        if time.monotonic() - started > 120:
            delay = POLLING_RETRY_BASE_SEC
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)


async def main():
    await start_web_server()
    seed_auto_state()
    try:
        # Проверка ID через Telegram — не дольше 25 с, чтобы старт не завис.
        await asyncio.wait_for(restore_subscriptions(), timeout=25)
    except Exception:  # noqa: BLE001
        log.exception("подписки из TELEGRAM_CHAT_IDS восстановить не удалось")
    log.info(
        "Бот запущен: баланс %s, риск %s%%, модель %s",
        usd(BALANCE),
        risk_percent(BALANCE),
        GROQ_MODEL,
    )
    if CHECK_INTERVAL_MIN > 0:
        _background_tasks.add(asyncio.create_task(scheduler()))
    else:
        log.info("автопроверка выключена: CHECK_INTERVAL_MIN=0")
    await run_polling()


if __name__ == "__main__":
    asyncio.run(main())
