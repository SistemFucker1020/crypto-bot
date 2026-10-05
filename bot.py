import os
import json
import time
import asyncio
import logging
from datetime import datetime, timedelta

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import Message
from groq import AsyncGroq

from market import build_market_context
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
REQUIRED_FACTORS = 3              # конфлюэнция: минимум подтверждённых факторов

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


async def handle_health_check(request):
    return web.Response(text="Bot is running!")


async def start_web_server():
    global _runner, _site
    app = web.Application()
    app.router.add_get("/", handle_health_check)
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


def build_trade(setup: dict, available: list, balance: float) -> dict:
    """Считает риск, позицию, плечо и профит кодом — модель ими не управляет."""
    reason = setup["reason"]

    if setup["direction"] == "WAIT":
        return {"direction": "WAIT", "reason": reason or "Подходящего сетапа нет."}

    # Конфлюэнция: минимум REQUIRED_FACTORS, но не больше, чем есть источников.
    required = min(REQUIRED_FACTORS, len(available))
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
    spread = abs(entry - sl)
    if entry <= 0 or spread <= 0:
        return {
            "direction": "WAIT",
            "reason": f"Модель вернула некорректные уровни (entry={entry}, sl={sl}).",
        }

    atr_pct = setup.get("atr_pct")
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
        f"✅ Факторы: {factors}",
        f"💡 Причина: {trade['reason']}",
    ]
    return "\n".join(lines)


def build_system_prompt(available: list, balance: float) -> str:
    percent = risk_percent(balance)
    return (
        "Ты — строгий финансовый аналитик и алгоритмический трейдер.\n"
        f"- Баланс: {usd(balance)}.\n"
        f"- Риск на сделку: {percent:g}% ({usd(balance * percent / 100)}).\n"
        f"- Risk/Reward: жёсткий минимум 1:{MIN_RR:g}, но приоритет — максимум "
        f"прибыли. Целись в 1:{PREFERRED_RR:g} и дальше (1:8–1:10), пока уровень "
        "реально существует на графике. Не срезай TP ради вероятности попадания.\n"
        "- Размер позиции, плечо и риск считает система — ты их не указываешь.\n\n"
        "Доступные источники: " + ", ".join(available) + ".\n\n"
        "Правила:\n"
        "- Подтверждай фактор ТОЛЬКО если его блок есть в данных и реально "
        "поддерживает сетап.\n"
        f"- Если подтверждено меньше {REQUIRED_FACTORS} факторов — direction: WAIT.\n"
        "- Уровни задавай от структуры рынка (уровни, EMA, ATR, стенки), не выдумывай.\n"
        f"- Стоп не ближе {min_stop_pct(balance):.2f}% от цены входа: при более "
        f"тесном стопе позиция упрётся в потолок плеча {MAX_LEVERAGE:g}x, "
        "риск будет меньше бюджета, а профит ниже возможного.\n"
        "- Не придумывай уровни, которых нет в данных.\n"
        "- Пиши поле reason строго на русском языке.\n\n"
        "Ответь СТРОГО одним JSON-объектом, без markdown и пояснений вокруг:\n"
        '{"direction": "LONG|SHORT|WAIT", "entry": <число>, "sl": <число>, '
        '"tp": <число>, "confirmed_factors": ["orderbook","derivatives",'
        '"technicals","news"], "reason": "<2-3 предложения>"}\n'
        "Если сетапа нет — direction: WAIT, entry/sl/tp = 0, "
        "confirmed_factors = []."
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
        "📜 /history — прошлые сигналы и что из них вышло.\n\n"
        f"Баланс: {usd(BALANCE)}\n"
        f"Риск на сделку сейчас: {percent:g}% ({usd(BALANCE * percent / 100)})\n"
        f"Фильтры: R:R от 1:{MIN_RR:g}, цель 1:{PREFERRED_RR:g} и дальше, "
        f"минимум {REQUIRED_FACTORS} фактора, "
        f"плечо до {MAX_LEVERAGE:g}x"
    )


def remember(message: Message) -> None:
    """Запоминает чат — без этого автопроверке некому будет писать."""
    try:
        signal_log.save_chat(message.chat.id)
    except Exception:  # noqa: BLE001
        log.exception("не удалось запомнить чат")


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

    if progress:
        await progress(f"🧠 Анализирую через {GROQ_MODEL}...")

    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": build_system_prompt(context["available"], BALANCE)},
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
        trade = build_trade(setup, context["available"], BALANCE)
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
    )


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


async def broadcast(text: str) -> None:
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
            await bot.send_message(chat_id, text)
        except Exception:  # noqa: BLE001
            log.exception("не удалось отправить сообщение в чат %s", chat_id)


async def auto_check() -> None:
    """Одна проверка по расписанию: проанализировать и решить, слать ли."""
    result = await analyze()
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
    await broadcast(result["text"])


async def scheduler() -> None:
    """Фоновая задача: проверяет рынок по расписанию, пока жив процесс."""
    log.info(
        "автопроверка включена: каждые %s мин (+%s с после закрытия свечи)",
        CHECK_INTERVAL_MIN,
        CHECK_DELAY_SEC,
    )
    while True:
        delay = seconds_until_next_check()
        log.info("следующая автопроверка через %.1f мин", delay / 60)
        await asyncio.sleep(delay)
        try:
            await auto_check()
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001
            log.exception("автопроверка упала")


async def main():
    await start_web_server()
    seed_auto_state()
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
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
