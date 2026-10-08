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


def test_bybit_helpers():
    import importlib

    saved = {name: os.environ.get(name) for name in ("BYBIT_KEY", "BYBIT_SECRET")}
    try:
        os.environ["BYBIT_KEY"] = "  key-with-spaces \n"
        os.environ["BYBIT_SECRET"] = " secret \t"
        importlib.reload(bot.bybit)
        check("ключ и секрет обрезаются при старте",
              bot.bybit.KEY == "key-with-spaces" and bot.bybit.SECRET == "secret",
              f"{bot.bybit.KEY!r} {bot.bybit.SECRET!r}")
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        importlib.reload(bot.bybit)

    payload = "1700000000000my_key5000symbol=BTCUSDT&side=Buy"
    check("подпись Bybit — HMAC-SHA256 по правилу биржи",
          bot.bybit.sign(payload, "secret123")
          == "b0a55adcaae86138f6e15eb09706559680dfd582536de4f3de4b1f178f28b020",
          bot.bybit.sign(payload, "secret123"))

    check("стоп лонга округляется вниз (не к входу)",
          bot.bybit.round_down(84254.45, 0.1) == 84254.4,
          str(bot.bybit.round_down(84254.45, 0.1)))
    check("стоп шорта округляется вверх",
          bot.bybit.round_up(84254.45, 0.1) == 84254.5,
          str(bot.bybit.round_up(84254.45, 0.1)))
    check("число уходит без хвостовых нулей", bot.bybit.num(0.01200) == "0.012",
          bot.bybit.num(0.01200))

    info = {"step": 0.001, "min_qty": 0.001, "min_notional": 5.0}
    qty, problem = bot.bybit.build_qty(0.01193, 84254.4, info)
    check("количество режется вниз до шага лота",
          problem is None and qty == 0.011, f"{qty} {problem}")
    qty, problem = bot.bybit.build_qty(0.0004, 84254.4, info)
    check("объём меньше шага лота отклоняется с причиной",
          qty is None and "шага лота" in problem, problem or "")
    qty, problem = bot.bybit.build_qty(0.001, 1.0, info)
    check("объём меньше минимума биржи отклоняется",
          qty is None and "минимума биржи" in problem, problem or "")


async def test_bybit_errors():
    """Пустой ответ от 401 должен звучать человечески, а не как падение JSON."""
    bybit = bot.bybit
    original_aiohttp = bybit.aiohttp

    class FakeResponse:
        def __init__(self, status, body):
            self.status = status
            self._body = body

        async def text(self, *, encoding=None, errors="strict"):
            return self._body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

    class FakeSession:
        def __init__(self, status, body):
            self.status, self.body = status, body

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def request(self, *args, **kwargs):
            return FakeResponse(self.status, self.body)

    class FakeAiohttp:
        def __init__(self, status, body):
            self.ClientSession = lambda **kwargs: FakeSession(status, body)

    cases = (
        (401, "", "HTTP 401"),
        (401, "", "ключ отклонён"),
        (401, "", "testnet.bybit.com"),
        (200, "<html>oops", "не JSON"),
        (200, '{"retCode":10001,"retMsg":"sign error, please check"}', "sign error"),
    )
    try:
        for status, body, expected in cases:
            bybit.aiohttp = FakeAiohttp(status, body)
            try:
                await bybit._request("GET", "/v5/diagnostic", signed=False)
            except bybit.BybitError as exc:
                check(f"ответ {status} → понятная ошибка: {expected[:34]}",
                      expected in str(exc), str(exc)[:150])
            else:
                check(f"ответ {status} → ошибка", False, "исключение не поднято")
    finally:
        bybit.aiohttp = original_aiohttp


async def test_paper_trading():
    bybit = bot.bybit
    names = (
        "enabled", "mode", "positions", "instrument_info", "ticker_price",
        "set_leverage", "open_position", "closed_pnl", "wallet_balance",
        "cancel_all", "close_position",
    )
    original = {name: getattr(bybit, name) for name in names}
    previous_setting = signal_log.get_setting("paper")
    calls = {"leverage": [], "orders": [], "closed": []}

    trade = {
        "direction": "LONG", "entry": 84000.0, "sl": 83500.0, "tp": 86500.0,
        "size": 0.0119, "leverage": 5.0,
    }

    async def no_positions():
        return []

    async def one_position():
        return [{
            "side": "Buy", "size": 0.011, "entry": 84000.0, "mark": 84100.0,
            "leverage": 5.0, "value": 924.0, "pnl": 1.0, "liq": 0.0,
            "sl": 83500.0, "tp": 86500.0,
        }]

    async def info():
        return {"min_qty": 0.001, "step": 0.001, "min_notional": 5.0, "tick": 0.1}

    async def price():
        return 84254.4

    async def set_lev(leverage):
        calls["leverage"].append(leverage)

    async def open_pos(side, qty, entry, sl, tp, tick=0.1):
        calls["orders"].append((side, qty, entry, sl, tp, tick))
        return "OID-777"

    async def deals(limit=20):
        return [{
            "pnl": 12.5, "side": "Buy", "qty": 0.011, "entry": 84000.0,
            "exit": 86000.0, "fee": 0.1, "closed_at": 1791371662000,
            "reason": "Market",
        }]

    async def wallet():
        return {"equity": 100.0, "wallet": 100.0, "available": 80.0,
                "account_type": "UNIFIED"}

    async def cancel():
        calls["closed"].append("cancel")

    async def close():
        calls["closed"].append("close")

    bybit.enabled = lambda: True
    bybit.mode = lambda: "testnet"
    bybit.positions = no_positions
    bybit.instrument_info = info
    bybit.ticker_price = price
    bybit.set_leverage = set_lev
    bybit.open_position = open_pos
    bybit.closed_pnl = deals
    bybit.wallet_balance = wallet
    bybit.cancel_all = cancel
    bybit.close_position = close

    try:
        signal_log.set_setting("paper", "off")
        note = await bot.place_paper_order(trade)
        check("paper trading выключен — ордер не ставится",
              note == "" and not calls["orders"], note)

        signal_log.set_setting("paper", "on")
        note = await bot.place_paper_order(trade)
        check("подтверждённый сигнал даёт ордер",
              "Ордер выставлен" in note and "OID-777" in note,
              note.replace("\n", " | "))
        check("плечо выставлено до входа", calls["leverage"] == [5.0],
              str(calls["leverage"]))
        check("в ордер ушло посчитанное количество",
              calls["orders"][0][1] == 0.011, str(calls["orders"]))
        check("ордер записан в журнал",
              bool(signal_log.positions_log(1))
              and signal_log.positions_log(1)[0]["order_id"] == "OID-777",
              str(signal_log.positions_log(1)))

        bybit.positions = one_position
        note = await bot.place_paper_order(trade)
        check("при открытой позиции второй ордер не ставится",
              "уже открыта" in note, note)
        bybit.positions = no_positions

        async def far_price():
            return 90000.0

        bybit.ticker_price = far_price
        note = await bot.place_paper_order(trade)
        check("цена ушла дальше лимита — вход блокируется",
              "ушла" in note, note)
        bybit.ticker_price = price

        async def broken(*args, **kwargs):
            raise bybit.BybitError("Margin is insufficient")

        bybit.open_position = broken
        note = await bot.place_paper_order(trade)
        check("ошибка биржи попадает в сообщение, а не роняет сигнал",
              "не выставлен" in note and "Margin" in note, note)
        bybit.open_position = open_pos

        stats = await bot.paper_stats()
        check("/stats получает реализованный PnL с биржи",
              "Реализованный PnL" in stats and "$12.50" in stats,
              stats.replace("\n", " | "))

        sent = []

        class FakeChat:
            id = 777

        class FakeMessage:
            chat = FakeChat()
            text = "/paper"

            async def answer(self, text):
                sent.append(text)

        message = FakeMessage()
        await bot.paper_cmd(message)
        check("/paper без аргументов показывает статус",
              "Paper trading" in sent[-1] and "Режим" in sent[-1],
              sent[-1][:130])

        message.text = "/paper off"
        await bot.paper_cmd(message)
        check("/paper off выключает и запоминает",
              signal_log.get_setting("paper") == "off" and "выключен" in sent[-1],
              sent[-1][:100])

        message.text = "/paper on"
        await bot.paper_cmd(message)
        check("/paper on включает", signal_log.get_setting("paper") == "on")

        message.text = "/positions"
        await bot.positions_cmd(message)
        check("/positions показывает позиции, сделки и журнал",
              "Открытых позиций нет" in sent[-1]
              and "Последние закрытые" in sent[-1]
              and "Журнал ордеров" in sent[-1], sent[-1][:250])

        class FakeCallbackMessage:
            def __init__(self):
                self.texts = []

            async def answer(self, text):
                self.texts.append(text)

        class FakeCallback:
            def __init__(self):
                self.alerts = []
                self.sent_message = FakeCallbackMessage()

            async def answer(self, text=None, show_alert=False):
                self.alerts.append(text)

            @property
            def message(self):
                return self.sent_message

        bybit.positions = one_position
        callback = FakeCallback()
        await bot.close_position_cb(callback)
        check("кнопка снимает ордера и закрывает позицию",
              calls["closed"] == ["cancel", "close"]
              and callback.sent_message.texts
              and "закрыта" in callback.sent_message.texts[0],
              f"{calls['closed']} {callback.sent_message.texts}")
        check("закрытие показывает реализованный PnL",
              "$12.50" in callback.sent_message.texts[0],
              str(callback.sent_message.texts))
        bybit.positions = no_positions

        bybit.enabled = lambda: False
        callback = FakeCallback()
        await bot.close_position_cb(callback)
        check("без ключей кнопка отвечает отказом",
              callback.alerts and "не заданы" in callback.alerts[0],
              str(callback.alerts))
    finally:
        for name in names:
            setattr(bybit, name, original[name])
        signal_log.set_setting("paper", previous_setting or "off")


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
    check("/start упоминает /paper и /positions",
          "/paper" in text and "/positions" in text, text[-260:])
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
    test_bybit_helpers()
    await test_bybit_errors()
    await test_paper_trading()
    await test_start_command()
    print("---")
    print("провалено: " + (", ".join(failures) if failures else "ничего"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
