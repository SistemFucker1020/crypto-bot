"""Тесты сигнального конвейера: уровни, TTL, повторная проверка, Score, режим.

Запуск из корня репозитория:

    .venv/bin/python tests/test_signal_quality.py

Тесты не ходят в сеть: контексты рынка собираются синтетически, упавшие
источники эмулируются словарями ошибок. База — временный файл.
"""
import asyncio
import os
import pathlib
import sys
import tempfile
from datetime import datetime, timedelta

ROOT = pathlib.Path(__file__).resolve().parent.parent


def load_env() -> None:
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
os.environ["DB_PATH"] = os.path.join(tempfile.gettempdir(), "quality-test.db")
os.environ.setdefault("TELEGRAM_CHAT_ID", "-1001234567890")
os.environ["PORT"] = str(19000 + os.getpid() % 900)
if os.path.exists(os.environ["DB_PATH"]):
    os.remove(os.environ["DB_PATH"])
sys.path.insert(0, str(ROOT))

import bot          # noqa: E402
import bybit        # noqa: E402
import costs        # noqa: E402
import levels       # noqa: E402
import regime       # noqa: E402
import setup_score  # noqa: E402
import signal_log   # noqa: E402

failures = []


def check(name, cond, extra=""):
    print(("OK        " if cond else "ПРОВАЛЕНО ") + name + (f" — {extra}" if extra else ""))
    if not cond:
        failures.append(name)


# --- Синтетический рыночный контекст ---------------------------------------

def make_context(direction="LONG", price=10000.0, atr14=30.0, fetched_age_sec=1):
    """Полный контекст, на котором конфлюэнция даёт direction."""
    if direction == "LONG":
        technicals = {
            "15m": {"close": price, "price": price, "ema20": price * 0.99,
                    "ema50": price * 0.98, "ema200": price * 0.97,
                    "rsi14": 58.0, "atr14": atr14, "atr_pct": 0.3,
                    "adx": 30.0, "forming": False,
                    "candle_close_at": "2026-10-13T12:15:00+03:00",
                    "swing_highs": [price * 1.01, price * 1.02],
                    "swing_lows": [price * 0.985, price * 0.99],
                    "levels": {"support": [{"price": price * 0.99, "touches": 2}],
                               "resistance": [{"price": price * 1.03, "touches": 1},
                                              {"price": price * 1.05, "touches": 3}]},
                    "range_low": price * 0.98, "range_high": price * 1.04,
                    "vol_ratio": 1.4, "taker_flow": 0.58, "closes_20": price * 0.99},
            "1h": {"close": price, "price": price, "ema20": price * 0.99,
                   "ema50": price * 0.98, "ema200": price * 0.97,
                   "rsi14": 60.0, "atr14": atr14 * 2, "atr_pct": 0.6,
                   "adx": 28.0, "forming": False,
                   "candle_close_at": "2026-10-13T12:00:00+03:00",
                   "swing_highs": [], "swing_lows": [],
                   "levels": {"support": [], "resistance": []},
                   "range_low": price * 0.95, "range_high": price * 1.06,
                   "vol_ratio": 1.2, "taker_flow": None, "closes_20": price * 0.98},
        }
        scores = {"orderbook": 1, "derivatives": 1, "technicals": 1, "news": 0}
    else:
        technicals = {
            "15m": {"close": price, "price": price, "ema20": price * 1.01,
                    "ema50": price * 1.02, "ema200": price * 1.03,
                    "rsi14": 42.0, "atr14": atr14, "atr_pct": 0.3,
                    "adx": 30.0, "forming": False,
                    "candle_close_at": "2026-10-13T12:15:00+03:00",
                    "swing_highs": [price * 1.01, price * 1.015],
                    "swing_lows": [price * 0.985, price * 0.99],
                    "levels": {"support": [{"price": price * 0.98, "touches": 2}],
                               "resistance": [{"price": price * 1.01, "touches": 3}]},
                    "range_low": price * 0.97, "range_high": price * 1.02,
                    "vol_ratio": 1.4, "taker_flow": 0.42, "closes_20": price * 1.01},
            "1h": {"close": price, "price": price, "ema20": price * 1.01,
                   "ema50": price * 1.02, "ema200": price * 1.03,
                   "rsi14": 40.0, "atr14": atr14 * 2, "atr_pct": 0.6,
                   "adx": 28.0, "forming": False,
                   "candle_close_at": "2026-10-13T12:00:00+03:00",
                   "swing_highs": [], "swing_lows": [],
                   "levels": {"support": [], "resistance": []},
                   "range_low": price * 0.94, "range_high": price * 1.05,
                   "vol_ratio": 1.2, "taker_flow": None, "closes_20": price * 1.02},
        }
        scores = {"orderbook": -1, "derivatives": -1, "technicals": -1, "news": 0}

    fetched = (datetime.now().astimezone()
               - timedelta(seconds=fetched_age_sec)).isoformat(timespec="seconds")
    return {
        "available": ["orderbook", "derivatives", "technicals", "news"],
        "atr_pct": 0.3,
        "price": price,
        "scores": scores,
        "errors": {},
        "blocks": {
            "orderbook": {"imbalance_pct": 62.0 if direction == "LONG" else 38.0,
                          "walls": [], "bid_usd": 1, "ask_usd": 1, "band_pct": 0.2},
            "derivatives": {"funding_pct": 0.01, "ls_ratio": 0.9, "oi_btc": 1.0,
                            "oi_usd": 10000.0},
            "technicals": technicals,
            "news": {"headlines": ["Спокойные новости"]},
        },
        "candle_close_at": "2026-10-13T12:15:00+03:00",
        "fetched_at": fetched,
        "timeframe": "15m",
    }


def full_setup(direction="LONG", entry=10000.0, balance=100.0):
    """Готовый setup, проходящий конфлюэнцию и R:R."""
    if direction == "LONG":
        sl, tp = 9900.0, 10400.0
    else:
        sl, tp = 10100.0, 9600.0
    return {"direction": direction, "entry": entry, "sl": sl, "tp": tp,
            "reason": "тест", "factors": {"orderbook", "derivatives", "technicals"},
            "required": 3}


# --- Уровни (этап 4) --------------------------------------------------------

def test_levels():
    t15 = make_context("LONG")["blocks"]["technicals"]
    r = levels.compute_levels("LONG", 10000, 10000, t15, min_rr=3, min_stop_pct=0.5)
    check("уровни LONG: стоп < вход < тейк, R:R >= 3",
          "sl" in r and r["sl"] < 10000 < r["tp"] and r["rr"] >= 3, str(r))
    check("уровни LONG: стоп за поддержкой с буфером", r.get("sl", 0) < 9900, str(r))

    ts = make_context("SHORT")["blocks"]["technicals"]
    r2 = levels.compute_levels("SHORT", 10000, 10000, ts, min_rr=3, min_stop_pct=0.5)
    check("уровни SHORT: тейк < вход < стоп, R:R >= 3",
          "sl" in r2 and r2["tp"] < 10000 < r2["sl"] and r2["rr"] >= 3, str(r2))
    check("уровни SHORT: стоп за сопротивлением", r2.get("sl", 0) > 10100, str(r2))

    # Нет реальной цели с MIN_RR -> WAIT, TP не растягивается
    tight = {"15m": {"atr14": 20, "levels": {"support": [{"price": 9900, "touches": 1}],
                                             "resistance": [{"price": 10080, "touches": 1}]},
                     "range_low": 9900, "range_high": 10080}, "1h": {}}
    r3 = levels.compute_levels("LONG", 10000, 10000, tight, atr=20, min_rr=3, min_stop_pct=0.5)
    check("нет цели с R:R>=3 -> WAIT (TP не растягивается)",
          "error" in r3 and "R:R" in r3["error"], str(r3))

    r4 = levels.compute_levels("LONG", 10000, 10000, {"15m": {}, "1h": {}}, atr=None)
    check("нет данных -> WAIT, не произвольные уровни", "error" in r4, str(r4))

    # Минимальная ширина стопа (потолок плеча)
    t_tight = {"15m": {"atr14": 5, "levels": {"support": [{"price": 9999, "touches": 1}],
                                              "resistance": [{"price": 10400, "touches": 1}]},
                       "range_low": 9990, "range_high": 10400}, "1h": {}}
    r5 = levels.compute_levels("LONG", 10000, 10000, t_tight, atr=5, min_rr=3, min_stop_pct=0.5)
    check("теский стоп расширяется до минимума", r5.get("stop_pct", 0) >= 0.5, str(r5))

    r6 = levels.compute_levels("LONG", 10000, 10000, t15, min_rr=3,
                               min_stop_pct=0.5, regime="HIGH_VOLATILITY")
    check("HIGH_VOLATILITY расширяет буфер стопа",
          "sl" in r6 and r6["sl"] < r["sl"], f"{r6.get('sl')} vs {r['sl']}")

    r7 = levels.compute_levels("DOG", 10000, 10000, t15)
    check("неизвестное направление -> ошибка", "error" in r7, str(r7))


# --- Издержки и чистый риск (этап 3) ---------------------------------------

def test_costs():
    r = costs.estimate("LONG", 100, 110, 1000, hold_hours=8)
    check("LONG: чистая прибыль меньше валовой",
          r["net"] < r["gross"] and r["estimate"] is True, str(r))
    r2 = costs.estimate("SHORT", 100, 110, 1000, hold_hours=8)
    check("SHORT: рост цены — убыток, funding шорт получает",
          r2["gross"] < 0 and r2["funding"] < 0, str(r2))

    for balance in (100.0, 500.0, 10000.0):
        t = bot.build_trade(full_setup(), ["orderbook", "derivatives", "technicals"],
                            balance)
        check(f"чистый убыток по стопу <= {costs.NET_RISK_MAX_PCT:g}% "
              f"(баланс {balance:g}$)",
              t["net_risk_pct"] <= costs.NET_RISK_MAX_PCT + 1e-6,
              f"{t['net_risk_pct']:.3f}%")
    check("предохранитель режет риск-тиер выше потолка",
          costs.net_budget_usd(100.0, 8.0) == costs.NET_RISK_MAX_PCT)
    check("чистая плановая прибыль всегда ниже валовой",
          t["net_profit"] < t["profit"])


# --- TTL (этап 5) ------------------------------------------------------------

def test_ttl():
    check("TTL задан для 15m", bot.SIGNAL_TTL_SEC["15m"] > 0,
          str(bot.SIGNAL_TTL_SEC))

    trade = full_setup()
    bot.attach_signal_meta(trade, make_context())
    check("метаданные TTL заполнены",
          trade["formed_at"] and trade["expires_at"] and trade["timeframe"] == "15m",
          str({k: trade.get(k) for k in ("formed_at", "expires_at", "timeframe")}))
    result = {"kind": "signal", "signal_id": 0}
    bot.check_ttl(trade, result)
    check("свежий сигнал проходит TTL", result["kind"] == "signal", str(trade.get("reason")))

    # Истёкший: направление гасится, статус expired в журнале
    sid = signal_log.record(created_at=signal_log.now_iso(), symbol="BTC/USDT",
                            direction="LONG", status="formed")
    trade2 = full_setup()
    bot.attach_signal_meta(trade2, make_context())
    trade2["expires_at"] = (datetime.now().astimezone()
                            - timedelta(seconds=5)).isoformat()
    result2 = {"kind": "signal", "signal_id": sid}
    bot.check_ttl(trade2, result2)
    check("истёкший TTL гасит сигнал", result2["kind"] == "wait"
          and trade2["direction"] == "WAIT", str(trade2.get("reason")))
    row = signal_log.recent(1)[0]
    check("истёкший TTL помечен expired в журнале",
          row["status"] == "expired" and "TTL" in (row["status_reason"] or ""), str(row["status"]))

    # Повторная проверка: истёкший сигнал не регистрируется в окно подтверждения
    before = bot.register_verdict("signal", "LONG")
    trade3 = full_setup()
    bot.attach_signal_meta(trade3, make_context())
    trade3["expires_at"] = (datetime.now().astimezone()
                            - timedelta(seconds=5)).isoformat()
    bot.check_ttl(trade3, {"kind": "signal", "signal_id": 0})
    check("истёкший TTL не подтверждает направление в окне",
          bot.register_verdict("wait") == 0)

    # Paper entry: по истёкшему TTL ордер не ставится (без ключей — до ключей)
    saved = bybit.enabled
    bybit.enabled = lambda: True
    try:
        note = asyncio.run(bot.place_paper_order(trade3))
        check("paper entry отклоняет истёкший TTL", "TTL" in note, note)
    finally:
        bybit.enabled = saved


# --- Повторная проверка (этап 5) --------------------------------------------

def make_result(context, direction="LONG"):
    trade = full_setup(direction)
    bot.attach_signal_meta(trade, context)
    return {"kind": "signal", "trade": trade, "context": context,
            "signal_id": 0, "text": "", "raw": ""}


def test_recheck():
    calls = {"ctx": []}
    good = make_context("LONG")

    async def ok_context(symbol="BTC/USDT"):
        return good

    async def run_recheck(ctx_factory, result):
        original = bot.build_market_context
        bot.build_market_context = ctx_factory
        try:
            return await bot.recheck_signal(result)
        finally:
            bot.build_market_context = original

    ok, reason = asyncio.run(run_recheck(ok_context, make_result(good)))
    check("свежий рынок -> повторная проверка пройдена", ok, reason)

    # 1) направление перевернулось
    flipped = make_context("SHORT")
    ok, reason = asyncio.run(run_recheck(
        lambda s="BTC/USDT": _async_return(flipped), make_result(good)))
    check("смена направления -> отмена", not ok and "направление" in reason, reason)

    # 2) цена ушла дальше дрейфа
    drifted = make_context("LONG", price=10300.0)
    ok, reason = asyncio.run(run_recheck(
        lambda s="BTC/USDT": _async_return(drifted), make_result(good)))
    check("дрейф цены -> отмена", not ok and "ушла" in reason, reason)

    # 3) источников стало меньше трёх
    poor = make_context("LONG")
    poor["available"] = ["orderbook"]
    ok, reason = asyncio.run(run_recheck(
        lambda s="BTC/USDT": _async_return(poor), make_result(good)))
    check("источников меньше REQUIRED_FACTORS -> отмена",
          not ok and "источников" in reason, reason)

    # 4) данные устарели
    stale = make_context("LONG", fetched_age_sec=int(bot.RECHECK_MAX_AGE_SEC) + 60)
    ok, reason = asyncio.run(run_recheck(
        lambda s="BTC/USDT": _async_return(stale), make_result(good)))
    check("устаревшие данные -> отмена", not ok and "старше" in reason, reason)

    # 5) биржа упала целиком
    async def broken(symbol="BTC/USDT"):
        raise RuntimeError("биржа легла")

    ok, reason = asyncio.run(run_recheck(broken, make_result(good)))
    check("сбой источника данных -> отмена", not ok and "недоступны" in reason, reason)

    # 6) Score упал ниже порога
    weak = make_context("LONG")
    weak["blocks"]["technicals"]["15m"]["taker_flow"] = 0.40
    weak["blocks"]["technicals"]["15m"]["vol_ratio"] = 0.5
    weak["blocks"]["orderbook"]["imbalance_pct"] = 50.0
    weak["blocks"]["derivatives"]["funding_pct"] = 0.08
    ok, reason = asyncio.run(run_recheck(
        lambda s="BTC/USDT": _async_return(weak), make_result(good)))
    check("Score упал ниже порога -> отмена",
          not ok and ("Score" in reason or "покрытие" in reason), reason)


def _async_return(value):
    async def inner(symbol="BTC/USDT"):
        return value
    return inner()


# --- Режим рынка (этап 6) ----------------------------------------------------

def test_regime():
    ctx = make_context("LONG")
    r = regime.detect(ctx)
    check("трендовый контекст -> TREND", r["mode"] == "TREND", str(r))

    ctx["blocks"]["technicals"]["15m"]["adx"] = 10.0
    r2 = regime.detect(ctx)
    check("низкий ADX -> RANGE", r2["mode"] == "RANGE", str(r2))

    ctx["blocks"]["technicals"]["15m"]["adx"] = 21.0
    r3 = regime.detect(ctx)
    check("переходная зона ADX -> UNKNOWN", r3["mode"] == "UNKNOWN", str(r3))

    ctx["blocks"]["technicals"]["15m"]["adx"] = 30.0
    ctx["blocks"]["technicals"]["15m"]["atr_pct"] = 0.9
    r4 = regime.detect(ctx)
    check("высокий ATR -> HIGH_VOLATILITY важнее тренда",
          r4["mode"] == "HIGH_VOLATILITY" and r4["flags"]["high_volatility"], str(r4))

    ctx["blocks"]["news"]["headlines"] = ["Binance hacked, $200M stolen"]
    r5 = regime.detect(ctx)
    check("новостной маркер -> NEWS_EVENT с флагом",
          r5["mode"] == "NEWS_EVENT" and r5["flags"]["news_event"], str(r5))

    r6 = regime.detect({})
    check("пустой контекст -> UNKNOWN без исключений", r6["mode"] == "UNKNOWN", str(r6))

    check("по умолчанию ничего не блокируется",
          regime.detect(ctx)["blocked"] is False and not regime.BLOCKED,
          str(regime.BLOCKED))


# --- Score (этап 7) ----------------------------------------------------------

def test_score():
    ctx = make_context("LONG")
    r = setup_score.score_setup(ctx, "LONG")
    check("сильный LONG: score >= 70, ok", r["ok"] and r["score"] >= 70, str(r))
    check("ликвидации честно без данных (coverage < 1)",
          r["breakdown"]["liquidations"]["available"] is False
          and r["coverage"] < 1.0, str(r["coverage"]))
    check("score не утверждает вероятность (ключ detail без %)",
          "вероят" not in str(r["breakdown"]).lower(), "")

    r2 = setup_score.score_setup(ctx, "SHORT")
    check("встречный SHORT: score мал, отказ", not r2["ok"] and r2["score"] <= 20, str(r2))

    r3 = setup_score.score_setup({"available": [], "scores": {}, "blocks": {}}, "LONG")
    check("нет данных -> отказ по покрытию", not r3["ok"] and "покрытие" in r3["reason"],
          r3["reason"])

    flat = make_context("LONG")
    f = flat["blocks"]["technicals"]["15m"]
    f.update({"ema20": 10000, "ema50": 10000, "ema200": 10000,
              "taker_flow": 0.5, "vol_ratio": 0.8})
    flat["blocks"]["orderbook"]["imbalance_pct"] = 50.0
    r4 = setup_score.score_setup(flat, "LONG")
    check("нейтральный рынок: полное покрытие, но score 0 -> отказ по Score",
          r4["coverage"] > 0.8 and not r4["ok"] and "Score" in r4["reason"], str(r4))

    # Обязательные группы в разбивке
    for group in setup_score.GROUPS:
        check(f"группа Score присутствует: {group}", group in r["breakdown"], "")


# --- Журнал: миграции и статусы (этап 2) -------------------------------------

def test_signal_log():
    sid = signal_log.record(
        created_at=signal_log.now_iso(), symbol="BTC/USDT", direction="LONG",
        entry=10000, sl=9900, tp=10400, rr=4.0, stop_pct=1.0,
        risk_usd=4.0, risk_pct=4.0, profit_usd=16.0, position_usd=400,
        leverage=4.0, margin_usd=100, capped=0, factors="orderbook",
        sources="orderbook,technicals", balance=100, reason="тест", raw="",
        timeframe="15m", candle_close_at="2026-10-13T12:15:00+03:00",
        formed_at="2026-10-13T12:15:30+03:00",
        expires_at="2026-10-13T12:45:30+03:00",
        score=75.0, score_breakdown={"market_structure": {"points": 20}},
        coverage=0.9, regime="TREND", regime_flags={"high_volatility": False},
        snapshot={"price": 10000, "adx": 30}, costs={"fees": 1.2, "estimate": True},
        gross_planned=16.0, net_planned=14.5, status="formed",
    )
    row = next(r for r in signal_log.recent(10) if r["id"] == sid)
    check("все новые поля пишутся",
          row["timeframe"] == "15m" and row["score"] == 75.0
          and row["regime"] == "TREND" and row["coverage"] == 0.9
          and row["status"] == "formed" and row["expires_at"], str(row))

    signal_log.update_status(sid, "sent", "подтверждён", sent_at=signal_log.now_iso())
    row = next(r for r in signal_log.recent(10) if r["id"] == sid)
    check("update_status: sent + sent_at, снимок не тронут",
          row["status"] == "sent" and row["sent_at"]
          and row["snapshot"] == '{"price": 10000, "adx": 30}', str(row))

    s = signal_log.stats()
    check("stats: sent=1, сформированные считаются",
          s["sent"] == 1 and s["signals"] >= 1, str(s))
    check("stats: score/режим в сводке", "avg_score" in s and "regime_blocked" in s, str(s))

    # Статус cancelled не ломает антидубль последнего отправленного
    check("last_signal_direction видит отправленный",
          signal_log.last_signal_direction() == "LONG",
          str(signal_log.last_signal_direction()))

    sid2 = signal_log.record(created_at=signal_log.now_iso(), symbol="BTC/USDT",
                             direction="SHORT", status="score_blocked")
    check("заблокированный не подавляет направление",
          signal_log.last_signal_direction() == "LONG",
          str(signal_log.last_signal_direction()))


# --- Интеграция risk engine ---------------------------------------------------

def test_build_trade():
    avail = ["orderbook", "derivatives", "technicals"]
    t = bot.build_trade(full_setup("LONG"), avail, 100.0)
    check("LONG: позиция, плечо, уровни согласованы",
          t["direction"] == "LONG" and t["sl"] < t["entry"] < t["tp"]
          and 0 < t["leverage"] <= bot.MAX_LEVERAGE, str(t))
    t2 = bot.build_trade(full_setup("SHORT"), avail, 100.0)
    check("SHORT: уровни согласованы",
          t2["direction"] == "SHORT" and t2["tp"] < t2["entry"] < t2["sl"], str(t2))

    bad = full_setup("LONG")
    bad["tp"] = 10100.0                        # R:R = 1.0 < MIN_RR
    t3 = bot.build_trade(bad, avail, 100.0)
    check("R:R ниже MIN_RR -> WAIT", t3["direction"] == "WAIT"
          and "R/R" in t3["reason"], str(t3))

    drift = {"direction": "LONG", "entry": 10300.0, "sl": 10200.0, "tp": 10700.0,
             "reason": "тест", "factors": {"orderbook", "derivatives", "technicals"},
             "required": 3}
    t4 = bot.build_trade(drift, avail, 100.0, price=10000.0)
    check("вход далее MAX_ENTRY_DRIFT -> WAIT", t4["direction"] == "WAIT", str(t4))

    narrow = full_setup("LONG")
    narrow["sl"], narrow["tp"] = 9995.0, 10400.0  # стоп 0.05% < MIN_ATR_PCT
    t5 = bot.build_trade({**narrow, "atr_pct": 0.04}, avail, 100.0)
    check("ATR ниже порога -> WAIT", t5["direction"] == "WAIT", str(t5))

    few = full_setup("LONG")
    few["factors"] = {"orderbook"}
    t6 = bot.build_trade(few, avail, 100.0)
    check("конфлюэнция: 1 из 3 -> WAIT", t6["direction"] == "WAIT", str(t6))


# --- Некорректные API-ответы ---------------------------------------------------

def test_bad_api():
    try:
        bot.parse_setup("тут вообще нет json")
        check("parse_setup: нет JSON -> ValueError", False)
    except ValueError:
        check("parse_setup: нет JSON -> ValueError", True)

    try:
        bot.parse_setup('{"direction": "SIDEWAYS", "entry": 1}')
        check("parse_setup: неизвестное направление -> ValueError", False)
    except ValueError:
        check("parse_setup: неизвестное направление -> ValueError", True)

    try:
        bot.parse_setup('{"direction": "LONG", "entry": "abc"}')
        check("parse_setup: entry не число -> ValueError", False)
    except ValueError:
        check("parse_setup: entry не число -> ValueError", True)

    r2 = bot.parse_setup('{"direction": "LONG", "entry": 10000, "sl": 0, "tp": 0}')
    check("parse_setup: нулевые SL/TP допустимы (их считает код)",
          r2["sl"] == 0.0 and r2["tp"] == 0.0, str(r2))

    t = bot.build_trade({"direction": "WAIT", "reason": "нет сетапа"},
                        ["orderbook"], 100.0)
    check("setup WAIT проходит без исключений", t["direction"] == "WAIT", str(t))

    r3 = regime.detect({"blocks": {"technicals": {"15m": {"adx": "мусор"}}}})
    check("мусорный ADX -> UNKNOWN, не исключение", r3["mode"] == "UNKNOWN", str(r3))


# --- Запуск -------------------------------------------------------------------

def main():
    test_levels()
    test_costs()
    test_ttl()
    test_recheck()
    test_regime()
    test_score()
    test_signal_log()
    test_build_trade()
    test_bad_api()

    print("---")
    if failures:
        print(f"Провалено {len(failures)} проверок:")
        for name in failures:
            print(f"  • {name}")
        sys.exit(1)
    print("провалено: ничего")


if __name__ == "__main__":
    main()
