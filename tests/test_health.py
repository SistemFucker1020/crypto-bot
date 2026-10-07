"""Тесты живости бота: /health, смертность scheduler и polling, чат.

Запуск из корня репозитория:

    .venv/bin/python tests/test_health.py

Тесты не ходят в сеть за рыночными данными и не шлют сообщения:
лог сводится во временный файл, Telegram и Groq не трогаются.
"""
import asyncio
import json
import os
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_env() -> None:
    """Читает .env (на Render его нет — там переменные заданы в настройках)."""
    path = ROOT / ".env"
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


load_env()
os.environ["DB_PATH"] = os.path.join(tempfile.gettempdir(), "health-test.db")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1001234567890")
os.environ.setdefault("POLLING_RETRY_BASE_SEC", "0.02")
os.environ["PORT"] = str(18000 + os.getpid() % 900)
if os.path.exists(os.environ["DB_PATH"]):
    os.remove(os.environ["DB_PATH"])
sys.path.insert(0, str(ROOT))

import bot  # noqa: E402
import signal_log  # noqa: E402

failures = []


def check(name, cond, extra=""):
    print(("OK        " if cond else "ПРОВАЛЕНО ") + name + (f" — {extra}" if extra else ""))
    if not cond:
        failures.append(name)


def payload(resp) -> str:
    text = getattr(resp, "text", None)
    if isinstance(text, str):
        return text
    body = getattr(resp, "body", None)
    if isinstance(body, (bytes, bytearray)):
        return body.decode()
    if isinstance(body, str):
        return body
    raise AssertionError(f"не удалось прочитать ответ: {resp!r}")


async def test_handlers():
    fresh = await bot.handle_status(None)
    check("/health свежий -> 200", fresh.status == 200, str(fresh.status))
    data = json.loads(payload(fresh))
    check("/health status=ok", data["status"] == "ok", str(data))
    check("/health interval_min=15", data["interval_min"] == 15, str(data))

    bot._health["last_check_at"] = time.time() - 46 * 60
    stale = await bot.handle_status(None)
    check("автопроверка мертва 46 мин -> 503", stale.status == 503, str(stale.status))
    data = json.loads(payload(stale))
    check("stale падает с возрастом и причиной",
          data["status"] == "stale" and data["last_check_age_sec"] > 2700, str(data))

    saved_interval, bot.CHECK_INTERVAL_MIN = bot.CHECK_INTERVAL_MIN, 0
    off = await bot.handle_status(None)
    check("CHECK_INTERVAL_MIN=0 -> 200 даже при старом тике", off.status == 200, str(off.status))
    bot.CHECK_INTERVAL_MIN = saved_interval

    bot._health["last_check_at"] = time.time()
    again = await bot.handle_status(None)
    check("после восстановления тика снова 200", again.status == 200, str(again.status))

    plain = await bot.handle_health_check(None)
    check("/ отвечает Bot is running!", plain.text == "Bot is running!", plain.text)


async def test_real_http():
    await bot.start_web_server()
    from aiohttp import ClientSession

    port = os.environ["PORT"]
    async with ClientSession() as session:
        async with session.get(f"http://127.0.0.1:{port}/") as resp:
            text = await resp.text()
            check("HTTP / -> 200", resp.status == 200 and text == "Bot is running!",
                  f"{resp.status} {text!r}")
        async with session.get(f"http://127.0.0.1:{port}/health") as resp:
            data = await resp.json()
            check("HTTP /health -> 200 ok", resp.status == 200 and data["status"] == "ok",
                  f"{resp.status} {data}")
        bot._health["last_check_at"] = time.time() - 46 * 60
        async with session.get(f"http://127.0.0.1:{port}/health") as resp:
            data = await resp.json()
            check("HTTP /health мёртвый тик -> 503",
                  resp.status == 503 and data["status"] == "stale", str(resp.status))
    bot._health["last_check_at"] = time.time()
    await bot._runner.cleanup()


def test_next_check_delay():
    original = bot.seconds_until_next_check

    def broken(moment=None):
        raise RuntimeError("часы сломались")

    bot.seconds_until_next_check = broken
    delay = bot.next_check_delay()
    check("сломанный расчёт времени -> пауза интервала",
          delay == max(bot.CHECK_INTERVAL_MIN, 1) * 60, str(delay))
    bot.seconds_until_next_check = original
    check("нормальный расчёт времени жив", bot.next_check_delay() > 0)


async def test_scheduler_survives():
    calls = {"n": 0}
    original_seconds = bot.seconds_until_next_check
    original_check = bot.auto_check

    def fast(moment=None):
        return 0.02

    async def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("модель упала")
        raise asyncio.CancelledError

    bot.seconds_until_next_check = fast
    bot.auto_check = flaky
    try:
        await asyncio.wait_for(bot.scheduler(), 2)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    bot.seconds_until_next_check = original_seconds
    bot.auto_check = original_check
    check("scheduler пережил два падения auto_check и продолжил",
          calls["n"] >= 3, f"вызовов: {calls['n']}")


async def test_polling_survives():
    calls = {"n": 0}

    class FakeDispatcher:
        async def start_polling(self, token):
            calls["n"] += 1
            if calls["n"] <= 3:
                raise RuntimeError("сеть Telegram недоступна")
            await asyncio.Event().wait()

    original_dp = bot.dp
    bot.dp = FakeDispatcher()
    try:
        await asyncio.wait_for(bot.run_polling(), 2)
    except (asyncio.TimeoutError, asyncio.CancelledError):
        pass
    bot.dp = original_dp
    check("polling пережил три обрыва и не умер", calls["n"] >= 4, f"вызовов: {calls['n']}")


def test_chat_restore():
    bot.seed_auto_state()
    check("чат восстановлен из TELEGRAM_CHAT_ID",
          int(os.environ["TELEGRAM_CHAT_ID"]) in signal_log.chats(), str(signal_log.chats()))


async def test_start_command():
    sent = []
    chat_id = int(os.environ["TELEGRAM_CHAT_ID"])

    class FakeChat:
        id = chat_id

    class FakeMessage:
        chat = FakeChat()

        async def answer(self, text):
            sent.append(text)

    await bot.start_cmd(FakeMessage())
    text = sent[0] if sent else ""
    check("/start показывает ID чата", str(chat_id) in text, text[-160:])
    check("/start подсказывает TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID=" in text)


async def main():
    test_next_check_delay()
    await test_handlers()
    await test_real_http()
    await test_scheduler_survives()
    await test_polling_survives()
    test_chat_restore()
    await test_start_command()
    print("---")
    print("провалено: " + (", ".join(failures) if failures else "ничего"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
