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
    created_at TEXT NOT NULL
)
"""


def save_chat(chat_id) -> None:
    """Запоминает чат, куда можно слать автосигналы. Повтор — не ошибка."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_CHATS_SCHEMA)
        conn.execute(
            "INSERT OR IGNORE INTO chats (chat_id, created_at) VALUES (?, ?)",
            (int(chat_id), now_iso()),
        )
        conn.commit()


def chats() -> list:
    """Чаты, куда рассылать автопроверку."""
    with closing(sqlite3.connect(DB_PATH, timeout=10)) as conn:
        conn.execute(_CHATS_SCHEMA)
        rows = conn.execute("SELECT chat_id FROM chats ORDER BY chat_id").fetchall()
    return [row[0] for row in rows]


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
