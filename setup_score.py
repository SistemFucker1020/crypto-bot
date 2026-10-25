"""Score сетапа 0–100 по семи группам факторов (детерминированный Python).

Score — ЭТО НЕ ВЕРОЯТНОСТЬ УСПЕХА. Это сила согласованности независимых
групп факторов с выбранным направлением, ничего больше. 80 баллов не
значит «80% прибыльных»: калибровки на исторической выборке здесь нет,
и утверждать такое было бы обманом.

Семь групп (вес по умолчанию в скобках, меняются env SCORE_W_*):

  market_structure (20) — порядок EMA 15m + структура свайпов (HH/HL vs LH/LL)
  order_flow       (15) — доля покупателя в market-ордерах (тейкер-флоу, CVD-прокси)
  oi_funding       (15) — funding и лонг/шорт: контрариан-перегрев
  liquidations     (10) — ликвидации; БЕСПЛАТНОГО источника нет → всегда без баллов
  volume           (10) — объём подтверждает движение цены (vol_ratio + направление)
  htf_trend        (15) — тренд старшего ТФ (1h EMA 20/50/200)
  orderbook        (15) — дисбаланс стакана ±0.2%

Правила честности:

  * Группа начисляет баллы ТОЛЬКО при реальных, свежих данных. Нет
    данных (или они из технических ошибок источника) — группа не участвует
    вовсе, а её доля снижает coverage (уверенность оценки).
  * Внутри группы коррелированные индикаторы объединяются в ОДНУ оценку:
    цена>EMA20 и цена>EMA50 — это одна «структура», а не два подтверждения.
    Независимыми считаются только группы между собой.
  * Встречное подтверждение (голосует против направления) вычитает баллы:
    чем больше вес встречных групп, тем ниже score.
  * coverage = доля веса групп с данными. SCORE_MIN_COVERAGE ниже — сигнал
    отклоняется: «нет данных» не равно «рынок нейтрален».
  * Порог отправки SCORE_MIN — настраивается. REQUIRED_FACTORS
    (конфлюэнция) НЕ заменяется и остаётся отдельным воротом в bot.py.
"""

import os

# --- Веса групп (сумма 100 по умолчанию) ---------------------------------
WEIGHTS = {
    "market_structure": float(os.getenv("SCORE_W_STRUCTURE", "20")),
    "order_flow":       float(os.getenv("SCORE_W_ORDERFLOW", "15")),
    "oi_funding":       float(os.getenv("SCORE_W_OI", "15")),
    "liquidations":     float(os.getenv("SCORE_W_LIQ", "10")),
    "volume":           float(os.getenv("SCORE_W_VOLUME", "10")),
    "htf_trend":        float(os.getenv("SCORE_W_HTF", "15")),
    "orderbook":        float(os.getenv("SCORE_W_BOOK", "15")),
}

# Порог отправки и минимальное покрытие данных (в долях веса, 0–1).
# 50 вместо 60: при 60 сигналы почти не проходили (неделями молчал) —
# порог ослаблен, но не отменён: встречные группы всё ещё режут Score.
SCORE_MIN = float(os.getenv("SCORE_MIN", "50"))
SCORE_MIN_COVERAGE = float(os.getenv("SCORE_MIN_COVERAGE", "0.6"))

# Пороги голосов факторов (в % или долях, как у источника).
BOOK_BULL = 56.0        # дисбаланс стакана: быки (доля %)
BOOK_BEAR = 44.0        # медведи
FLOW_BULL = 0.55        # доля тейкер-покупок
FLOW_BEAR = 0.45
FUNDING_HOT = 0.05      # % за 8ч: перегрев лонгов (контрариан — медведи)
FUNDING_COLD = -0.05    # перегрев шортов
LS_BULL = 1.15          # лонг/шорт ниже — позиционирование контратрендовое
LS_BEAR = 0.85
VOL_CONFIRM = 1.1       # vol_ratio >= — объём подтверждает движение

GROUPS = tuple(WEIGHTS)


def score_setup(context: dict, direction: str) -> dict:
    """Score 0–100 для направления LONG/SHORT. Возвращает:

      score     — 0–100 (не вероятность!)
      coverage  — доля веса групп с реальными данными (0–1)
      breakdown — по каждой группе: голос, доступность, вклад, пояснение
      ok        — score >= SCORE_MIN И coverage >= SCORE_MIN_COVERAGE
      reason    — причина отказа, если ok = False

    Никогда не бросает исключений: отсутствие данных → группа без баллов.
    """
    blocks = (context.get("blocks") or {})
    available = set(context.get("available") or [])
    scores = context.get("scores") or {}

    votes = {}
    votes["market_structure"] = _structure_vote(blocks, direction)
    votes["order_flow"] = _order_flow_vote(blocks.get("technicals") or {}, direction)
    votes["oi_funding"] = _oi_funding_vote(blocks.get("derivatives") or {},
                                           direction, available)
    votes["liquidations"] = (None, "бесплатного источника ликвидаций нет — "
                                   "группа честно без баллов")
    votes["volume"] = _volume_vote(blocks.get("technicals") or {}, direction)
    votes["htf_trend"] = _htf_vote(blocks.get("technicals") or {}, direction)
    votes["orderbook"] = _orderbook_vote(blocks.get("orderbook") or {},
                                         direction, available)

    total_weight = sum(WEIGHTS.values()) or 1.0
    points, breakdown, used_weight = 0.0, {}, 0.0
    for name in GROUPS:
        supported, detail = votes[name]
        weight = WEIGHTS[name]
        if supported is None:
            # Данных нет: без баллов, вес уходит в coverage-штраф.
            breakdown[name] = {"weight": weight, "available": False,
                               "support": None, "points": 0.0, "detail": detail}
            continue
        contribution = weight * supported
        points += contribution
        used_weight += weight
        breakdown[name] = {"weight": weight, "available": True,
                           "support": round(supported, 3),
                           "points": round(contribution, 2), "detail": detail}

    raw = 100.0 * points / total_weight
    score = max(0.0, min(100.0, raw))
    coverage = used_weight / total_weight

    ok = True
    reason = ""
    if coverage < SCORE_MIN_COVERAGE:
        ok = False
        reason = (
            f"покрытие данных {coverage:.0%} ниже минимального "
            f"{SCORE_MIN_COVERAGE:.0%} — оценка неполна, сигнал отклонён"
        )
    elif score < SCORE_MIN:
        ok = False
        reason = (
            f"Score {score:.0f} < порога {SCORE_MIN:g} — "
            f"согласованность факторов с {direction} слабая"
        )

    return {
        "score": round(score, 1),
        "coverage": round(coverage, 3),
        "breakdown": breakdown,
        "ok": ok,
        "reason": reason,
    }


# --- Группы факторов --------------------------------------------------------
# Возвращают (support, detail) или (None, detail) при отсутствии данных.
# support: +1 подтверждает направление, -1 встречный, 0 нейтрально.

def _structure_vote(blocks: dict, direction: str) -> tuple:
    """Порядок EMA 15m И структура свайпов — одна коррелированная группа."""
    frame = (blocks.get("technicals") or {}).get("15m") or {}
    if not frame:
        return None, "нет свечей 15m"
    votes = []
    close = frame.get("close")
    emas = (frame.get("ema20"), frame.get("ema50"), frame.get("ema200"))
    if close is not None and all(e is not None for e in emas):
        if close > emas[0] > emas[1] > emas[2]:
            votes.append(1)
        elif close < emas[0] < emas[1] < emas[2]:
            votes.append(-1)
        else:
            votes.append(0)
    highs = frame.get("swing_highs") or []
    lows = frame.get("swing_lows") or []
    if len(highs) >= 2 and len(lows) >= 2:
        hh = highs[-1] > highs[-2]
        hl = lows[-1] > lows[-2]
        lh = highs[-1] < highs[-2]
        ll = lows[-1] < lows[-2]
        if hh and hl:
            votes.append(1)
        elif lh and ll:
            votes.append(-1)
        else:
            votes.append(0)
    if not votes:
        return None, "нет EMA и свайпов"
    bull = sum(votes) / len(votes)
    support = bull if direction == "LONG" else -bull
    return support, f"структура 15m (EMA+свайпы): {bull:+.2f}"


def _order_flow_vote(technicals: dict, direction: str) -> tuple:
    """Тейкер-флоу (доля покупателя) — прокси CVD. Есть только у Binance."""
    frame = technicals.get("15m") or {}
    flow = frame.get("taker_flow")
    if flow is None:
        return None, "тейкер-флоу недоступен (источник без этой колонки)"
    if flow >= FLOW_BULL:
        bull = min(1.0, (flow - FLOW_BULL) / 0.1 + 0.5)
    elif flow <= FLOW_BEAR:
        bull = max(-1.0, -(FLOW_BEAR - flow) / 0.1 - 0.5)
    else:
        bull = 0.0
    support = bull if direction == "LONG" else -bull
    return support, f"доля тейкер-покупок {flow:.1%}"


def _oi_funding_vote(derivatives: dict, direction: str, available: set) -> tuple:
    """Funding и лонг/шорт — контрариан (перегрев против движения)."""
    if "derivatives" not in available or not derivatives:
        return None, "деривативы недоступны"
    votes = []
    funding = derivatives.get("funding_pct")
    if funding is not None:
        if funding >= FUNDING_HOT:
            votes.append(-1)   # перегрев лонгов — медведи
        elif funding <= FUNDING_COLD:
            votes.append(1)    # перегрев шортов — быки
        else:
            votes.append(0)
    ratio = derivatives.get("ls_ratio")
    if ratio is not None:
        # Контрариан: много лонгов (ratio выше) — перегрев против быков,
        # много шортов — перегрев против медведей.
        if ratio >= LS_BULL:
            votes.append(-1)
        elif ratio <= LS_BEAR:
            votes.append(1)
        else:
            votes.append(0)
    if not votes:
        return None, "нет funding и лонг/шорт"
    bull = sum(votes) / len(votes)
    support = bull if direction == "LONG" else -bull
    return support, f"funding {funding:+.4f}%, лонг/шорт {ratio if ratio is not None else 'н/д'}"


def _volume_vote(technicals: dict, direction: str) -> tuple:
    """Объём подтверждает движение цены (иначе движение слабое)."""
    frame = technicals.get("15m") or {}
    ratio = frame.get("vol_ratio")
    close, base = frame.get("close"), frame.get("closes_20")
    if ratio is None or close is None or base is None:
        return None, "нет объёма или базы сравнения"
    if ratio < VOL_CONFIRM:
        return 0.0, f"объём ×{ratio} — движение не подтверждено"
    bull = 1.0 if close > base else -1.0
    support = bull if direction == "LONG" else -bull
    return support, f"объём ×{ratio} в направлении цены"


def _htf_vote(technicals: dict, direction: str) -> tuple:
    """Тренд старшего ТФ: порядок EMA на 1h."""
    frame = technicals.get("1h") or {}
    close = frame.get("close")
    emas = (frame.get("ema20"), frame.get("ema50"), frame.get("ema200"))
    if close is None or not all(e is not None for e in emas):
        return None, "нет 1h-EMA"
    if close > emas[0] > emas[1] > emas[2]:
        bull = 1.0
    elif close < emas[0] < emas[1] < emas[2]:
        bull = -1.0
    else:
        bull = 0.0
    support = bull if direction == "LONG" else -bull
    return support, f"тренд 1h: {'up' if bull > 0 else 'down' if bull < 0 else 'нет'}"


def _orderbook_vote(orderbook: dict, direction: str, available: set) -> tuple:
    """Дисбаланс стакана в полосе ±0.2%."""
    if "orderbook" not in available or not orderbook:
        return None, "стакан недоступен"
    imbalance = orderbook.get("imbalance_pct")
    if imbalance is None:
        return None, "нет дисбаланса стакана"
    if imbalance >= BOOK_BULL:
        bull = 1.0
    elif imbalance <= BOOK_BEAR:
        bull = -1.0
    else:
        bull = 0.0
    support = bull if direction == "LONG" else -bull
    return support, f"дисбаланс стакана {imbalance:.0f}%"
