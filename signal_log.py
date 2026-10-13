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

import json
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
    # --- Качество сигнала (миграция внизу: старая база дочитывается) ---
    "timeframe",          # таймфрейм свечи, под которой собран сигнал
    "candle_close_at",    # закрытие этой свечи (формирование, без будущих данных)
    "formed_at",          # момент формирования сигнала
    "expires_at",         # TTL: срок действия сигнала
    "sent_at",            # момент отправки в чат
    "score",              # сила сетапа 0-100 (НЕ вероятность успеха)
    "score_breakdown",    # json: вклад каждой группы факторов
    "coverage",           # доля веса факторов с реальными данными
    "regime",             # режим рынка (TREND/RANGE/...)
    "regime_flags",       # json: признаки волатильности/новостного риска
    "snapshot",           # json: снимок рынка на момент формирования
    "costs",              # json: комиссии/funding/slippage (оценка)
    "gross_planned",      # плановая валовая прибыль
    "net_planned",        # плановая чистая (за вычетом оценки издержек)
    "status",             # sent|manual|cancelled|expired|score_blocked|regime_blocked
    "status_reason",      # причина отмены/блокировки
    "recheck",            # json: результат повторной проверки перед отправкой
)

# Добавление колонок в существующую таблицу. Удалений нет — история цела.
_SIGNAL_COLUMNS_DDL = {
    "timeframe": "TEXT",
    "candle_close_at": "TEXT",
    "formed_at": "TEXT",
    "expires_at": "TEXT",
    "sent_at": "TEXT",
    "score": "REAL",
    "score_breakdown": "TEXT",
    "coverage": "REAL",
    "regime": "TEXT",
    "regime_flags": "TEXT",
    "snapshot": "TEXT",
    "costs": "TEXT",
    "gross_planned": "REAL",
    "net_planned": "REAL",
    "status": "TEXT",
    "status_reason": "TEXT",
    "recheck": "TEXT",
}

# Эти поля храним как json-строки; принимаем и готовую строку, и dict.
_JSON_COLUMNS = frozenset({"score_breakdown", "regime_flags", "snapshot", "costs", "recheck"})

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


def _ensure_signals_columns(conn) -> None:
    """Миграция signals: добавляет недостающие колонки, ничего не удаляя.

    Старая база (до TTL/Score/режима) читается и пишется как обычно —
    новые колонки просто появляются с NULL, прошлые записи не перетираются.
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(signals)")}
    for name, ddl in _SIGNAL_COLUMNS_DDL.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE signals ADD COLUMN {name} {ddl}")


def _open_signals_conn() -> None:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute(_SCHEMA)
    _ensure_signals_columns(conn)
    return conn


def record(**fields) -> int:
    """Пишет одну запись, возвращает её id. Таблицу создаёт при первом вызове."""
    values = {name: fields.get(name) for name in _COLUMNS}
    if values["capped"] is not None:
        values["capped"] = int(bool(values["capped"]))
    if values["reason"]:
        values["reason"] = str(values["reason"])[:2000]
    for name in _JSON_COLUMNS:
        if isinstance(values.get(name), (dict, list, tuple)):
            values[name] = json.dumps(values[name], ensure_ascii=False)

    placeholders = ", ".join("?" for _ in _COLUMNS)
    with closing(_open_signals_conn()) as conn:
        cursor = conn.execute(
            f"INSERT INTO signals ({', '.join(_COLUMNS)}) VALUES ({placeholders})",
            tuple(values[name] for name in _COLUMNS),
        )
        conn.commit()
        return cursor.lastrowid


def update_status(signal_id, status: str, reason: str = "", sent_at=None) -> None:
    """Помечает исход сигнала: отправлен, отменён, истёк, заблокирован.

    Пишет только колонки статуса — снимок рынка, score и уровни в записи
    остаются ровно теми, что были в момент формирования.
    """
    if not signal_id:
        return
    try:
        with closing(_open_signals_conn()) as conn:
            conn.execute(
                "UPDATE signals SET status = ?, status_reason = ?, sent_at = "
                "COALESCE(?, sent_at) WHERE id = ?",
                (str(status), str(reason or "")[:2000], sent_at, int(signal_id)),
            )
            conn.commit()
    except sqlite3.Error:
        # Статус не критичен для работы бота: сигнал уже отправлен/отменён.
        pass


def recent(limit: int = 8) -> list:
    """Последние записи, новые первыми."""
    with closing(_open_signals_conn()) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM signals ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(row) for row in rows]


def total() -> int:
    with closing(_open_signals_conn()) as conn:
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
    """Направление последнего ОТПРАВЛЕННОГО LONG/SHORT — не слать дубль.

    Заблокированные (score/regime) и отменённые записи не считаются:
    сигнал, который не дошёл до чата, не должен подавлять повтор того же
    направления после рестарта.
    """
    with closing(_open_signals_conn()) as conn:
        row = conn.execute(
            "SELECT direction FROM signals "
            "WHERE direction IN ('LONG', 'SHORT') "
            "AND (status IS NULL OR status IN ('sent', 'manual', 'formed')) "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return row[0] if row else None


def stats() -> dict:
    """Сводка по сигналам: сколько сделок, средний профит, доля прибыльных.

    Разделяем сформированные LONG/SHORT, отправленные, отменённые и
    заблокированные записи. Плановые суммы (gross/net) — оценка, а не
    реализованный результат: его даёт только /stats через закрытые сделки.
    """
    with closing(_open_signals_conn()) as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS signals,
                   AVG(profit_usd) AS avg_profit,
                   SUM(profit_usd) AS total_profit,
                   AVG(stop_pct) AS avg_stop,
                   AVG(rr) AS avg_rr,
                   AVG(gross_planned) AS avg_gross,
                   AVG(net_planned) AS avg_net,
                   AVG(score) AS avg_score
            FROM signals
            WHERE direction IN ('LONG', 'SHORT')
            """
        ).fetchone()
        waits = conn.execute(
            "SELECT COUNT(*) FROM signals WHERE direction = 'WAIT'"
        ).fetchone()[0]
        counts = {
            status: conn.execute(
                "SELECT COUNT(*) FROM signals WHERE status = ?", (status,)
            ).fetchone()[0]
            for status in (
                "formed", "sent", "manual", "cancelled", "expired",
                "score_blocked", "regime_blocked",
            )
        }
    return {
        "signals": row[0] or 0,
        "avg_profit": row[1],
        "total_profit": row[2],
        "avg_stop": row[3],
        "avg_rr": row[4],
        "avg_gross": row[5],
        "avg_net": row[6],
        "avg_score": row[7],
        "waits": waits,
        "sent": counts["sent"],
        "manual": counts["manual"],
        "cancelled": counts["cancelled"],
        "expired": counts["expired"],
        "score_blocked": counts["score_blocked"],
        "regime_blocked": counts["regime_blocked"],
    }
