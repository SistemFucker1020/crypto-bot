import os
import json
import asyncio
import logging

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command
from aiogram.types import Message
from groq import AsyncGroq

from market import build_market_context

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
    percent = risk_percent(BALANCE)
    await message.answer(
        "👋 Привет! Я крипто-аналитик.\n"
        "Отправь /predict — соберу стакан, деривативы, индикаторы и новости, "
        "и дам торговый сетап.\n\n"
        f"Баланс: {usd(BALANCE)}\n"
        f"Риск на сделку сейчас: {percent:g}% ({usd(BALANCE * percent / 100)})\n"
        f"Фильтры: R:R от 1:{MIN_RR:g}, цель 1:{PREFERRED_RR:g} и дальше, "
        f"минимум {REQUIRED_FACTORS} фактора, "
        f"плечо до {MAX_LEVERAGE:g}x"
    )


@dp.message(Command("predict"))
async def predict_cmd(message: Message):
    msg = await message.answer("📊 Собираю данные: стакан, деривативы, индикаторы, новости...")

    try:
        context = await build_market_context(SYMBOL)
    except Exception as exc:  # noqa: BLE001
        log.exception("не удалось собрать рыночные данные")
        await safe_edit(msg, f"❌ Не удалось собрать рыночные данные:\n{exc}")
        return

    log.info(
        "факторы: %s | ошибки: %s",
        ", ".join(context["available"]) or "нет",
        context["errors"] or "нет",
    )

    if len(context["available"]) < REQUIRED_FACTORS:
        # Без 3 источников сигнал невозможен — не тратим запрос к модели впустую.
        await safe_edit(msg, data_report(context))
        return

    await safe_edit(msg, f"🧠 Анализирую через {GROQ_MODEL}...")

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
        await safe_edit(msg, f"❌ Ошибка вызова нейросети:\n{exc}")
        return

    try:
        setup = parse_setup(raw)
        setup["atr_pct"] = context["atr_pct"]
        trade = build_trade(setup, context["available"], BALANCE)
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("не удалось разобрать ответ модели: %s | raw=%s", exc, raw)
        await safe_edit(msg, f"❌ Модель ответила не по формату: {exc}")
        return

    await safe_edit(msg, format_trade(trade))


async def main():
    await start_web_server()
    log.info(
        "Бот запущен: баланс %s, риск %s%%, модель %s",
        usd(BALANCE),
        risk_percent(BALANCE),
        GROQ_MODEL,
    )
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
