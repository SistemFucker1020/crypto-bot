import os
import json
import asyncio
import logging

from aiohttp import web
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message
import ccxt.async_support as ccxt
from groq import AsyncGroq

# --- Настройки ---------------------------------------------------------
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
        + ". Задай их в настройках хостинга (Koyeb)."
    )

# Торговые параметры
DEPOSIT = 100.0                                  # депозит пользователя, $
RISK_PCT = 3.0                                   # риск на сделку, % от депозита
RISK_AMOUNT = DEPOSIT * RISK_PCT / 100           # = $3 на сделку
MIN_RR = 3.0                                     # минимальное Risk/Reward
GROQ_MODEL = "llama-3.1-8b-instant"
SYMBOL = "BTC/USDT"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("crypto-bot")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
groq_client = AsyncGroq(api_key=GROQ_API_KEY)


# Фиктивный веб-сервер, чтобы хостинг не закрывал сервис по таймауту портов
async def handle_health_check(request):
    return web.Response(text="Bot is running!")


async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()


# --- Рыночные данные ---------------------------------------------------
async def get_market_data(symbol: str = SYMBOL) -> str:
    exchange = ccxt.mexc()
    try:
        ticker = await exchange.fetch_ticker(symbol)
    finally:
        await exchange.close()  # закроется всегда, даже при исключении

    return (
        f"Пара: {symbol}\n"
        f"Последняя цена: ${ticker.get('last')}\n"
        f"Изменение за 24ч: ${ticker.get('percentage')}%\n"
        f"Максимум за 24ч: ${ticker.get('high')}\n"
        f"Минимум за 24ч: ${ticker.get('low')}"
    )


SYSTEM_PROMPT = (
    "Ты — строгий финансовый аналитик и алгоритмический трейдер.\n"
    f"- Депозит пользователя: ${DEPOSIT:.0f}.\n"
    f"- Риск на сделку: строго {RISK_PCT:g}% (${RISK_AMOUNT:.0f}).\n"
    f"- Соотношение Risk/Reward: минимум 1:{MIN_RR:g}.\n"
    "- Размер позиции посчитает система, ты его не указываешь.\n\n"
    "Ответь СТРОГО одним JSON-объектом, без markdown и пояснений вокруг:\n"
    '{"direction": "LONG|SHORT|WAIT", "entry": <число>, "sl": <число>, '
    '"tp": <число>, "reason": "<2-3 предложения про тренд или уровни>"}\n'
    "Если подходящего сетапа нет — direction: WAIT, а entry/sl/tp поставь 0."
)


# --- Разбор и расчёт ---------------------------------------------------
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

    return {
        "direction": direction,
        "entry": number("entry"),
        "sl": number("sl"),
        "tp": number("tp"),
        "reason": str(data.get("reason", "")).strip(),
    }


def build_trade(setup: dict) -> dict:
    """Считает размер позиции и R:R кодом — нейросеть ими не управляет."""
    reason = setup["reason"]

    if setup["direction"] == "WAIT":
        return {"direction": "WAIT", "reason": reason or "Подходящего сетапа нет."}

    entry, sl, tp = setup["entry"], setup["sl"], setup["tp"]
    spread = abs(entry - sl)

    if entry <= 0 or spread <= 0:
        return {
            "direction": "WAIT",
            "reason": f"Модель вернула некорректные уровни (entry={entry}, sl={sl}).",
        }

    rr = abs(tp - entry) / spread
    if rr < MIN_RR:
        return {
            "direction": "WAIT",
            "reason": f"R/R {rr:.2f} ниже требуемых 1:{MIN_RR:g} — сетап отброшен.\n{reason}",
        }

    return {
        "direction": setup["direction"],
        "entry": entry,
        "sl": sl,
        "tp": tp,
        "size": RISK_AMOUNT / spread,   # так, чтобы стоп съел ровно $3
        "rr": rr,
        "sl_pct": spread / entry * 100,
        "reason": reason,
    }


def usd(value: float) -> str:
    return f"${value:,.2f}"


def format_trade(trade: dict) -> str:
    header = f"📈 Анализ {GROQ_MODEL}\n"

    if trade["direction"] == "WAIT":
        return f"{header}\n⏸ WAIT\n\n{trade['reason']}"

    asset = SYMBOL.split("/")[0]
    return "\n".join(
        [
            header,
            f"Направление: {trade['direction']}",
            f"Вход: {usd(trade['entry'])}",
            f"Стоп: {usd(trade['sl'])} ({trade['sl_pct']:.1f}% от входа)",
            f"Тейк: {usd(trade['tp'])}",
            "",
            f"Размер позиции: {trade['size']:.6f} {asset} ({usd(trade['size'] * trade['entry'])})",
            f"Риск на сделку: {usd(RISK_AMOUNT)} ({RISK_PCT:g}% от {usd(DEPOSIT)})",
            f"R:R: 1:{trade['rr']:.2f}",
            "",
            f"Обоснование: {trade['reason']}",
        ]
    )


# --- Команды -----------------------------------------------------------
@dp.message(Command("start"))
async def start_cmd(message: Message):
    await message.answer(
        "👋 Привет! Я крипто-аналитик.\n"
        "Отправь /predict — дам торговый сетап по BTC/USDT."
    )


@dp.message(Command("predict"))
async def predict_cmd(message: Message):
    msg = await message.answer("📊 Получаю данные с биржи MEXC...")

    try:
        market_data = await get_market_data()
    except Exception as e:
        log.exception("не удалось получить данные с биржи")
        await msg.edit_text(f"❌ Не удалось получить данные с MEXC:\n{e}")
        return

    await msg.edit_text("🧠 Анализирую данные...")

    try:
        response = await groq_client.chat.completions.create(
            model=GROQ_MODEL,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": market_data},
            ],
            temperature=0.2,
        )
        raw = response.choices[0].message.content or ""
    except Exception as e:
        log.exception("не удалось вызвать нейросеть")
        await msg.edit_text(f"❌ Ошибка вызова нейросети:\n{e}")
        return

    try:
        trade = build_trade(parse_setup(raw))
    except (ValueError, json.JSONDecodeError) as e:
        log.warning("не удалось разобрать ответ модели: %s | raw=%s", e, raw)
        await msg.edit_text(f"❌ Модель ответила не по формату: {e}")
        return

    await msg.edit_text(format_trade(trade))


async def main():
    await start_web_server()
    log.info("Бот запущен")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
