import os
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiohttp import web
import ccxt.async_support as ccxt
from groq import AsyncGroq

# Инициализация API
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")

bot = Bot(token=TELEGRAM_TOKEN)
dp = Dispatcher()
groq_client = AsyncGroq(api_key=GROQ_API_KEY)

# Фиктивный веб-сервер, чтобы Render не закрывал сервис по таймауту портов
async def handle_health_check(request):
    return web.Response(text="Bot is running!")

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv("PORT", 10000))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

@dp.message(Command("start"))
async def start_cmd(message: Message):
    await message.answer("👋 Привет! Я крипто-аналитик. Отправь /predict для анализа рынка.")

@dp.message(Command("predict"))
async def predict_cmd(message: Message):
    msg = await message.answer("📊 Получаю данные с биржи Binance...")
    try:
        exchange = ccxt.binance()
        ticker = await exchange.fetch_ticker('BTC/USDT')
        ohlcv = await exchange.fetch_ohlcv('BTC/USDT', timeframe='1h', limit=12)
        await exchange.close()

        prompt = f"""
        Проанализируй текущую ситуацию по BTC/USDT:
        Текущая цена: ${ticker['last']}
        Изменение за 24ч: {ticker['percentage']}%
        Максимум за 24ч: ${ticker['high']}
        Минимум за 24ч: ${ticker['low']}
        
        Дай краткий прогноз и торговую рекомендацию (Long/Short/Wait).
        """

        await msg.edit_text("🧠 Анализирую данные с помощью Qwen 2.5...")
        response = await groq_client.chat.completions.create(
            model="qwen-2.5-32b",
            messages=[{"role": "user", "content": prompt}]
        )
        await msg.edit_text(response.choices[0].message.content)
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка: {str(e)}")

async def main():
    await start_web_server()
    print("🚀 Бот и веб-сервер успешно запущены!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
