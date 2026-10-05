import os
import asyncio
import ccxt.async_support as ccxt
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from groq import Groq

# Ключи автоматически подтянет Koyeb из настроек
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
groq_client = Groq(api_key=GROQ_API_KEY)


async def get_bybit_market_data(symbol: str = "BTC/USDT"):
    exchange = ccxt.bybit()
    try:
        ohlcv = await exchange.fetch_ohlcv(symbol, timeframe="15m", limit=5)
        ticker = await exchange.fetch_ticker(symbol)
        await exchange.close()

        last_price = ticker["last"]
        candles_summary = "\n".join(
            [f"Свеча: Open={c[1]}, High={c[2]}, Low={c[3]}, Close={c[4]}" for c in ohlcv]
        )
        return f"Пара: {symbol}\nТекущая цена: ${last_price}\nПоследние 5 свечей (15m):\n{candles_summary}"
    except Exception as e:
        await exchange.close()
        return f"Ошибка получения данных с Bybit: {e}"


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer(
        "👋 **Трейдинг-бот запущен в облаке 24/7!**\n\n"
        "Нейросеть Qwen 2.5 готова к анализу.\n"
        "Отправь команду /predict, чтобы получить торговый сетап по BTC/USDT."
    )


@dp.message(Command("predict"))
async def cmd_predict(message: types.Message):
    status_msg = await message.answer("📊 Запрашиваем данные с биржи Bybit...")

    market_data = await get_bybit_market_data("BTC/USDT")

    await status_msg.edit_text("🧠 Нейросеть Qwen 2.5 просчитывает риски...")

    system_prompt = (
        "Ты — строгий финансовый аналитик и алгоритмический трейдер.\n"
        "Твои правила:\n"
        "- Депозит пользователя: $100.\n"
        "- Риск на сделку: строго 3% ($3).\n"
        "- Соотношение Risk/Reward: минимум 1:3.\n\n"
        "Проанализируй предоставленные свечи и ответь СТРОГО по шаблону:\n"
        "1. Направление: [LONG / SHORT / WAIT]\n"
        "2. Точка входа (Entry): [Цена]\n"
        "3. Stop-Loss: [Цена под расчет 3% риска]\n"
        "4. Take-Profit: [Цена под R/R 1:3]\n"
        "5. Обоснование: [2-3 предложения по FVG, уровням или тренду]"
    )

    try:
        response = groq_client.chat.completions.create(
            model="qwen-2.5-32b",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": market_data},
            ],
            temperature=0.2,
        )

        ai_analysis = response.choices[0].message.content
        await status_msg.edit_text(f"📈 **Анализ Qwen 2.5:**\n\n{ai_analysis}")

    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка вызова нейросети: {e}")


async def main():
    print("🚀 Бот запущен в облаке 24/7!")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())