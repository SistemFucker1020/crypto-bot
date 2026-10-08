"""Лог торговых сигналов в SQLite.

Каждый вызов /predict оставляет строку: настоящий сигнал, WAIT, случай
«нет данных» и даже ответ модели не в том формате. Нужен, чтобы через
2-3 недели paper trading можно было посмотреть, как часто бот прав и
какая средняя доходность.

База лежит рядом с bot.py, путь меняется переменной DB_PATH.

ВНИМАНИЕ: на бесплатном Render диск эфемерный — база стирается при
каждом деплое и перезапуске. Постоянным архивом остаётся переписка
с ботом, здесь — рабочая копия для анализа.
"""

import os
import sqlite3
from contextlib import closing
from datetime import datetime

DB_PATH = os.getenv(
    "DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db"),
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    symbol TEXT NOT NULL,
    direction TEXT NOT NULL,
    entry REAL,
    sl REAL,
    tp REAL,
    rr REAL,
    stop_pct REAL,
    risk_usd REAL,
    risk_pct REAL,
    profit_usd REAL,
    position_usd REAL,
    leverage REAL,
    margin_usd REAL,
    capped INTEGER,
    factors TEXT,
    sources TEXT,
    balance REAL,
    reason TEXT,
    raw TEXT
)
"""

_COLUMNS = (
    "created_at",
    "symbol",
    "direction",
    "entry",
    "sl",
    "tp",
    "rr",
    "stop_pct",
    "risk_usd",
    "risk_pct",
    "profit_usd",
    "position_usd",
    "leverage",
    "margin_usd",
    "capped",
    "factors",
    "sources",
    "balance",
    "reason",
    "raw",
)

# Направления, которые мы вообще пишем в базу.
DIRECTIONS = ("LONG", "SHORT", "WAIT", "NODATA", "PARSE_ERROR")

# Переживает рестарт настройки, которые меняют сами пользователи (/paper).
# Render стирает базу — тогда действует значение по умолчанию из окружения.
_SETTINGS_SCHEMA = """
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
)
"""

# Журнал выставленных ордеров: что именно биржа приняла и по какому сигналу.
# Статус позиции бот берёт с биржи, а не отсюда — базу Render стирает.
_POSITIONS_SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id INTEGER,
    opened_at TEXT NOT NULL,
    mode TEXT,
    direction TEXT NOT NULL,
    qty TEXT NOT NULL,
    price REAL,
    entry REAL,
    sl REAL,
    tp REAL,
    leverage REAL,
    order_id TEXT
)
"""


def get_setting(key: str, default=None):
    """Настройка из базы или значение по умолчанию, если её ещё не меняли."""
    try:
        with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
            conn.execute(_SETTINGS_SCHEMA)
            row = conn.execute(
                "SELECT value FROM settings WHERE key = ?", (str(key),)
            ).fetchone()
    except sqlite3.Error:
        return default
    return row[0] if row else default


def set_setting(key: str, value) -> None:
    """Меняет настройку. Повторная запись перетирает предыдущую."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_SETTINGS_SCHEMA)
        conn.execute(
            "INSERT INTO settings (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
            "updated_at = excluded.updated_at",
            (str(key), str(value), now_iso()),
        )
        conn.commit()


def log_position(**fields) -> int:
    """Журнал выставленного ордера, возвращает id записи."""
    columns = (
        "signal_id", "opened_at", "mode", "direction", "qty",
        "price", "entry", "sl", "tp", "leverage", "order_id",
    )
    values = {name: fields.get(name) for name in columns}
    if not values["opened_at"]:
        values["opened_at"] = now_iso()
    placeholders = ", ".join("?" for _ in columns)
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_POSITIONS_SCHEMA)
        cursor = conn.execute(
            f"INSERT INTO positions ({', '.join(columns)}) VALUES ({placeholders})",
            tuple(values[name] for name in columns),
        )
        conn.commit()
        return cursor.lastrowid


def positions_log(limit: int = 10) -> list:
    """Последние выставленные ордера — для /positions, когда биржа недоступна."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_POSITIONS_SCHEMA)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM positions ORDER BY id DESC LIMIT ?", (int(limit),)
        ).fetchall()
    return [dict(row) for row in rows]


def now_iso() -> str:
    """Момент записи с часовым поясом сервера — чтобы время было однозначным."""
    return datetime.now().astimezone().isoformat(timespec="seconds")


def record(**fields) -> int:
    """Пишет одну запись, возвращает её id. Таблицу создаёт при первом вызове."""
    values = {name: fields.get(name) for name in _COLUMNS}
    if values["capped"] is not None:
        values["capped"] = int(bool(values["capped"]))
    if values["reason"]:
        values["reason"] = str(values["reason"])[:2000]

    placeholders = ", ".join("?" for _ in _COLUMNS)
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_SCHEMA)
        cursor = conn.execute(
            f"INSERT INTO signals ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
            tuple(values[name] for name in _COLUMNS),
        )
        conn.commit()
        return cursor.lastrowid


def recent(limit: int = 8) -> list:
    """Последние записи, новые первыми."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute(_SCHEMA)
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def total() -> int:
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_SCHEMA)
        return conn.execute("SELECT COUNT(*) FROM signals").fetchone()[0]


_CHATS_SCHEMA = """
CREATE TABLE IF NOT EXISTS chats (
    chat_id INTEGER PRIMARY KEY,
    created_at TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1
)
"""


def _ensure_enabled_column(conn) -> None:
    """Старая база (без enabled) дочитывается миграцией, а не падением."""
    columns = {row[1] for row in conn.execute("PRAGMA table_info(chats)")}
    if "enabled" not in columns:
        conn.execute("ALTER TABLE chats ADD COLUMN enabled INTEGER NOT NULL DEFAULT 1")


def _open_chats_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(_CHATS_SCHEMA)
    _ensure_enabled_column(conn)
    return conn


def save_chat(chat_id) -> None:
    """Запоминает чат (/start). Уже отключённый чат отключённым и остаётся."""
    with closing(_open_chats_conn()) as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chats (chat_id, created_at, enabled) VALUES (?, ?, 1)",
            (int(chat_id), now_iso()),
        )
        conn.commit()


def subscribe(chat_id) -> None:
    """Включает подписку (/signals on): создаёт чат или снова включает."""
    with closing(_open_chats_conn()) as conn:
        conn.execute(
            "INSERT INTO chats (chat_id, created_at, enabled) VALUES (?, ?, 1) "
            "ON CONFLICT(chat_id) DO UPDATE SET enabled = 1",
            (int(chat_id), now_iso()),
        )
        conn.commit()


def unsubscribe(chat_id) -> bool:
    """Выключает подписку (/signals off). True — чат был подписан и теперь выключен."""
    with closing(_open_chats_conn()) as conn:
        cursor = conn.execute(
            "UPDATE chats SET enabled = 0 WHERE chat_id = ? AND enabled = 1",
            (int(chat_id),),
        )
        conn.commit()
        return cursor.rowcount > 0


def chats() -> list:
    """Чаты, куда рассылать автопроверку, — только с включённой подпиской."""
    with closing(_open_chats_conn()) as conn:
        rows = conn.execute(
            "SELECT chat_id FROM chats WHERE enabled = 1 ORDER BY chat_id"
        ).fetchall()
    return [row[0] for row in rows]


def all_chats() -> list:
    """[(chat_id, enabled)] — для статуса подписки в /signals."""
    with closing(_open_chats_conn()) as conn:
        rows = conn.execute(
            "SELECT chat_id, enabled FROM chats ORDER BY chat_id"
        ).fetchall()
    return [(row[0], bool(row[1])) for row in rows]


def is_subscribed(chat_id) -> bool:
    """Подключён ли этот чат к рассылке."""
    with closing(_open_chats_conn()) as conn:
        row = conn.execute(
            "SELECT enabled FROM chats WHERE chat_id = ?", (int(chat_id),)
        ).fetchone()
    return bool(row) and bool(row[0])


def last_signal_direction():
    """Направление последнего LONG/SHORT — чтобы не слать дубль после рестарта."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_SCHEMA)
        row = conn.execute(
            "SELECT direction FROM signals "
            "WHERE direction IN ('LONG', 'SHORT') "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return row[0] if row else None


def stats() -> dict:
    """Сводка по сигналам: сколько сделок, средний профит, доля прибыльных."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_SCHEMA)
        row = conn.execute(
            """
            SELECT COUNT(*) AS signals,
                   AVG(profit_usd) AS avg_profit,
                   SUM(profit_usd) AS total_profit,
                   AVG(stop_pct) AS avg_stop,
                   AVG(rr) AS avg_rr
            FROM signals
            WHERE direction IN ('LONG', 'SHORT')
            """
        ).fetchone()
        waits = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE direction = 'WAIT'"
        ).fetchone()[0]
    return {
        "signals": row[0] or 0,
        "avg_profit": row[1],
        "total_profit": row[2],
        "avg_stop": row[3],
        "avg_rr": row[4],
        "waits": waits,
    }
