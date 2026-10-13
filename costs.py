"""Оценка издержек сделки: комиссии, funding, проскальзывание.

Считает только Python, значения — из конфигурации (env). До закрытия
сделки точных ставок не существует, поэтому ВСЁ здесь — явно помеченная
ОЦЕНКА, а не факт: фактическую комиссию биржа отдаёт в closed_pnl
(поле fee) и в /stats она показывается отдельно.

Ключевое следствие — чистый убыток по стопу. Валовой риск = позиция ×
доля стопа, но издержки считаются от НОМИНАЛА позиции, а номинал при
тесном стопе大きく (позиция = риск / стоп). Поэтому размер позиции
считается от чистого бюджета:

    position = net_budget / (stop_fraction + cost_rate)

тогда чистый убыток на стопе равен бюджету, а не превышает его.
NET_RISK_MAX_PCT — жёсткий потолок независимо от RISK_TIERS.
"""

import os

# --- Конфигурация (все ставки в % ) --------------------------------------
# Тейкер: вход market + выход market/стоп. Для BTC perpetual это типовые
# значения; на конкретной бирже меняются переменной, не правкой кода.
FEE_TAKER_PCT = float(os.getenv("FEE_TAKER_PCT", "0.05"))
# Проскальзывание на одну сторону (спред + движение цены за время исполнения).
SLIPPAGE_PCT = float(os.getenv("SLIPPAGE_PCT", "0.02"))
# Ставка funding за интервал, если позиция переживёт его начисление.
FUNDING_PCT = float(os.getenv("FUNDING_PCT", "0.01"))
FUNDING_INTERVAL_H = float(os.getenv("FUNDING_INTERVAL_H", "8"))
# Плановое удержание для оценки funding в сигнале (по умолчанию один интервал).
PLAN_HOLD_H = float(os.getenv("PLAN_HOLD_H", "8"))
# Жёсткий потолок чистого убытка по стопу, % баланса. Не зависит от
# RISK_TIERS: даже если те подняли, позиция ужмётся так, чтобы чистый
# убыток по стопу не превысил этот процент.
NET_RISK_MAX_PCT = float(os.getenv("NET_RISK_MAX_PCT", "5"))

LIVE = ("LONG", "SHORT")


def _check_direction(direction: str) -> None:
    if direction not in LIVE:
        raise ValueError(f"направление должно быть LONG или SHORT, получено {direction!r}")


def cost_rate(hold_hours: float = None) -> float:
    """Суммарные издержки за удержание как ДОЛЯ номинала (вход + выход).

    Комиссия и проскальзывание — по обеим сторонам, funding — за сколько
    интервалов продержится позиция. Для оценки риска funding берётся в
    худшем случае (заплатили бы), поэтому чистый убыток не занижается.
    """
    hold = PLAN_HOLD_H if hold_hours is None else float(hold_hours)
    intervals = max(hold, 0.0) / FUNDING_INTERVAL_H if FUNDING_INTERVAL_H > 0 else 0.0
    fees = 2 * FEE_TAKER_PCT / 100
    slippage = 2 * SLIPPAGE_PCT / 100
    funding = FUNDING_PCT / 100 * intervals
    return fees + slippage + funding


def estimate(direction: str, entry: float, exit_price: float,
             notional: float, hold_hours: float = None) -> dict:
    """Валовый и чистый результат гипотетической сделки. Оценка, не факт.

    Учитываются LONG/SHORT, номинал позиции и направление влияния funding:
    при положительной ставке лонг платит за удержание, шорт получает.
    Плечо напрямую в арифметику не входит — издержки считаются от
    номинала, плечо лишь задаёт, какой этот номинал при данной марже.
    """
    _check_direction(direction)
    if entry <= 0:
        raise ValueError("entry должна быть положительной")

    sign = 1 if direction == "LONG" else -1
    gross = (exit_price - entry) / entry * notional * sign

    # Выходной номинал отличается от входного (цена ушла), поэтому
    # комиссию и slippage считаем от фактических номиналов сторон.
    exit_notional = notional * exit_price / entry
    fees = (notional + exit_notional) * FEE_TAKER_PCT / 100
    slippage = (notional + exit_notional) * SLIPPAGE_PCT / 100

    hold = PLAN_HOLD_H if hold_hours is None else float(hold_hours)
    intervals = max(hold, 0.0) / FUNDING_INTERVAL_H if FUNDING_INTERVAL_H > 0 else 0.0
    # funding_paid > 0 — заплатили (сокращает прибыль), < 0 — получили.
    funding_paid = notional * (FUNDING_PCT / 100) * intervals * sign

    net = gross - fees - slippage - funding_paid
    return {
        "gross": gross,
        "fees": fees,
        "slippage": slippage,
        "funding": funding_paid,
        "net": net,
        "hold_hours": hold,
        "estimate": True,   # ставки оценочные, факт — только у закрытой сделки
    }


def net_budget_usd(balance: float, risk_percent: float) -> float:
    """Чистый бюджет риска в долларах, ограниченный NET_RISK_MAX_PCT."""
    percent = min(float(risk_percent), NET_RISK_MAX_PCT)
    return balance * percent / 100


def position_for_budget(net_budget_usd: float, stop_fraction: float,
                        hold_hours: float = None) -> float:
    """Номинал позиции, при котором чистый убыток на стопе = бюджет.

    Чистый убыток = позиция × (доля стопа + доля издержек), отсюда
    позиция = бюджет / (стоп + издержки). Чем уже стоп, тем меньше
    номинал — поэтому «тесный стоп + 10x» больше не раздувает
    комиссии до превышения бюджета.
    """
    denominator = stop_fraction + cost_rate(hold_hours)
    if denominator <= 0:
        raise ValueError("доля стопа и издержек должны быть положительными")
    return net_budget_usd / denominator
