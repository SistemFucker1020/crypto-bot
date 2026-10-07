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


async def test_chat_restore():
    class FakeBot:
        """get_chat отвечает «chat not found» только для чатов из bad."""

        bad: set = set()

        async def get_chat(self, chat_id):
            if chat_id in self.bad:
                raise RuntimeError("Bad Request: chat not found")
            return chat_id

    fake = FakeBot()
    original_bot = bot.bot
    original_env = bot.TELEGRAM_CHAT_IDS
    bot.bot = fake
    try:
        for cid, _ in list(signal_log.all_chats()):
            signal_log.unsubscribe(cid)

        await bot.restore_subscriptions()
        restored = bot.env_chat_ids()
        check("чаты из переменной окружения разобраны", restored == [-1001234567890], str(restored))
        check("чат восстановлен после проверки в Telegram",
              all(signal_log.is_subscribed(cid) for cid in restored), str(signal_log.all_chats()))
        check("результат проверки запомнен",
              bot._env_status.get(-1001234567890) == "ok", str(bot._env_status))

        # Мусорный ID в переменной: Telegram говорит «нет такого чата».
        bot.TELEGRAM_CHAT_IDS = "-1001234567890,424242"
        fake.bad = {424242}
        signal_log.unsubscribe(-1001234567890)
        await bot.restore_subscriptions()
        check("несуществующий чат не подписывается",
              not signal_log.is_subscribed(424242) and signal_log.is_subscribed(-1001234567890),
              str(signal_log.all_chats()))
        check("мусорный ID помечен как несуществующий",
              bot._env_status.get(424242) == bot.BAD_CHAT, str(bot._env_status))
        check("подсказка не советует мусорный ID",
              "424242" not in bot.persistence_hint(777), bot.persistence_hint(777)[:130])
        check("подсказка для чата из переменной — уже задана",
              "уже задана" in bot.persistence_hint(-1001234567890),
              bot.persistence_hint(-1001234567890)[:80])
        check("статус /signals показывает несуществующий чат",
              "424242 ❌ не найден" in bot.env_status_line(), bot.env_status_line())

        # Обрыв сети — не приговор: подписка ставится, статус «не проверен».
        bot.TELEGRAM_CHAT_IDS = "777"

        async def unreachable(chat_id):
            raise OSError("Temporary failure in name resolution")

        fake.get_chat = unreachable
        signal_log.unsubscribe(777)
        await bot.restore_subscriptions()
        check("сетевая ошибка не отменяет подписку",
              signal_log.is_subscribed(777) and bot._env_status.get(777) == "не проверен",
              str(bot._env_status))
    finally:
        bot.bot = original_bot
        bot.TELEGRAM_CHAT_IDS = original_env
        bot._env_status.clear()
        signal_log.unsubscribe(777)


def test_subscription():
    for cid, _ in list(signal_log.all_chats()):
        signal_log.unsubscribe(cid)

    signal_log.save_chat(111)
    check("/start подписывает чат", signal_log.is_subscribed(111), str(signal_log.all_chats()))
    check("chats() отдаёт только подписанных",
          signal_log.chats() == [cid for cid, on in signal_log.all_chats() if on],
          str(signal_log.chats()))

    check("/signals off выключает", signal_log.unsubscribe(111) is True)
    check("выключенный чат убран из рассылки", not signal_log.is_subscribed(111))
    check("выключенный чат виден в статусе",
          [(cid, on) for cid, on in signal_log.all_chats() if cid == 111] == [(111, False)],
          str(signal_log.all_chats()))
    check("повторный off не считается ошибкой", signal_log.unsubscribe(111) is False)
    check("незнакомый чат в off -> False", signal_log.unsubscribe(222) is False)

    signal_log.subscribe(111)
    check("/signals on включает снова", signal_log.is_subscribed(111) and 111 in signal_log.chats())
    rows_111 = [cid for cid, _ in signal_log.all_chats() if cid == 111]
    check("повторный on не дублирует чат",
          rows_111 == [111], f"строк с чатом 111: {rows_111}")

    # Старая база без колонки enabled должна дочитываться, а не падать.
    import sqlite3
    legacy = os.path.join(tempfile.gettempdir(), "legacy-chats.db")
    if os.path.exists(legacy):
        os.remove(legacy)
    conn = sqlite3.connect(legacy)
    conn.execute("CREATE TABLE chats (chat_id INTEGER PRIMARY KEY, created_at TEXT NOT NULL)")
    conn.execute("INSERT INTO chats VALUES (42, '2026-10-01T00:00:00Z')")
    conn.commit()
    signal_log._ensure_enabled_column(conn)
    has_column = {row[1] for row in conn.execute("PRAGMA table_info(chats)")}
    enabled = conn.execute("SELECT enabled FROM chats WHERE chat_id = 42").fetchone()[0]
    conn.close()
    check("миграция старой базы добавляет enabled",
          "enabled" in has_column and enabled == 1, f"{has_column} enabled={enabled}")


async def test_signals_command():
    sent = []

    class FakeChat:
        id = 777

    class FakeMessage:
        chat = FakeChat()
        text = "/signals on"

        async def answer(self, text):
            sent.append(text)

    msg = FakeMessage()
    await bot.signals_cmd(msg)
    check("/signals on включает и отвечает",
          "Подписка включена" in sent[-1] and signal_log.is_subscribed(777), sent[-1][:80])

    msg.text = "/signals off"
    await bot.signals_cmd(msg)
    check("/signals off выключает",
          "выключена" in sent[-1] and not signal_log.is_subscribed(777), sent[-1][:80])

    msg.text = "/signals"
    await bot.signals_cmd(msg)
    check("/signals без аргументов даёт статус",
          "Чатов с подпиской" in sent[-1] and "Расписание" in sent[-1], sent[-1][:120])
    check("статус видит выключенный чат", "Чат 777: выключена" in sent[-1], sent[-1][:200])

    check("подсказка для незнакомого чата содержит переменную",
          f"TELEGRAM_CHAT_IDS=" in bot.persistence_hint(777), bot.persistence_hint(777)[:80])
    check("подсказка для чата из переменной — уже задана",
          "уже задана" in bot.persistence_hint(bot.env_chat_ids()[0]),
          bot.persistence_hint(bot.env_chat_ids()[0])[:80])
    signal_log.unsubscribe(777)


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
    check("/start упоминает /signals", "/signals" in text, text[-200:])
    check("/start даёт строку переменной или подтверждение",
          "TELEGRAM_CHAT_IDS=" in text or "уже задана" in text, text[-200:])


async def main():
    test_next_check_delay()
    await test_handlers()
    await test_real_http()
    await test_scheduler_survives()
    await test_polling_survives()
    await test_chat_restore()
    test_subscription()
    await test_signals_command()
    await test_start_command()
    print("---")
    print("провалено: " + (", ".join(failures) if failures else "ничего"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
