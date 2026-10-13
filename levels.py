"""Динамические уровни SL/TP от структуры рынка.

Модель подбирала TP «под R:R» — ради цифры 1:8 цель уводили за реальные
уровни, а стоп ставили так, что шум его задевал. Здесь считает код:

  SL  — за уровнем отмены торговой идеи (ближайшая поддержка/сопротивление
        или swing-экстремум) плюс волатильный буфер; никогда не ближе
        min_stop_pct от входа (иначе позиция упирается в потолок плеча).
  TP  — первый РЕАЛЬНЫЙ уровень в направлении сделки (уровни из свайпов,
        стенки стакана, граница диапазона), дающий R:R не ниже min_rr.
        Искусственное растяжение невозможно: выбирается только среди
        существующих на графике цен.
  Не нашлось ни уровня отмены, ни цели с min_rr — ошибка → WAIT.

Вход (entry) продолжает подбирать модель по данным, а риск, позицию и
плечо, как и раньше, считает только Risk Engine в bot.py.

Все уровни — рекомендации расчёта, а не гарантия исполнения: на пробое
цена проходит мимо, стоп может закрыться хуже заявленного.
"""

import os

# Буфер за уровнем отмены: в ATR (шум не задевает стоп) и минимум в % цены.
SL_ATR_BUFFER = float(os.getenv("SL_ATR_BUFFER", "0.5"))
SL_MIN_BUFFER_PCT = float(os.getenv("SL_MIN_BUFFER_PCT", "0.15"))
# Если уровня ниже входа нет вовсе — инвалидация по волатильности.
INVALIDATION_ATR = float(os.getenv("INVALIDATION_ATR", "1.5"))
# Множитель буфера при повышенной волатильности (задаётся снаружи
# по режиму рынка, см. regime.HIGH_VOLATILITY).
VOL_MULT = {"HIGH_VOLATILITY": 1.5}


def compute_levels(direction, entry, price, technicals, walls=None,
                   min_rr=3.0, min_stop_pct=0.5, atr=None,
                   regime=None):
    """Возвращает {"sl", "tp", "rr", "stop_pct", "notes"} либо {"error"}.

    direction     — LONG/SHORT (направление уже решил код конфлюэнции)
    entry         — вход модели (ровно его и защищаем уровнями)
    price         — текущая цена (для корректности кандидатов)
    technicals    — блоки technicals из market: {"15m": {...}, "1h": {...}}
    walls         — стенки стакана [{"side","price","usd"}] или None
    min_rr        — MIN_RR из bot.py (минимальный допустимый R:R)
    min_stop_pct  — min_stop_pct(balance) в процентах (потолок плеча)
    atr           — ATR (15m предпочтительнее) для волатильного буфера
    regime        — режим рынка: меняет только размер буфера
    """
    if direction not in ("LONG", "SHORT"):
        return {"error": f"неизвестное направление {direction!r}"}
    if entry <= 0 or price <= 0:
        return {"error": "вход или цена не положительны — уровни не считать"}

    frames = [
        technicals.get("15m") or {},
        technicals.get("1h") or {},
    ]
    if atr is None:
        for frame in frames:
            if frame.get("atr14"):
                atr = frame["atr14"]
                break

    mult = VOL_MULT.get(regime, 1.0)
    notes = []

    # --- Кандидаты уровней ------------------------------------------------
    supports, resistances = [], []
    for frame in frames:
        levels = frame.get("levels") or {}
        supports += [l["price"] for l in levels.get("support") or []]
        resistances += [l["price"] for l in levels.get("resistance") or []]
        # Границы диапазона 60 свечей — реальные цели движения.
        if frame.get("range_low"):
            supports.append(frame["range_low"])
        if frame.get("range_high"):
            resistances.append(frame["range_high"])

    for wall in walls or []:
        side, wprice = wall.get("side"), wall.get("price")
        if not side or not wprice:
            continue
        (supports if side == "BUY" else resistances).append(wprice)

    # Свежие swing-экстремумы — дубли уровнями не бывают (set от кратности).
    supports = sorted(set(supports), reverse=True)   # ближайший снизу — первый
    resistances = sorted(set(resistances))           # ближайший сверху — первый

    # --- SL: уровень отмены + волатильный буфер ---------------------------
    if direction == "LONG":
        below = [lvl for lvl in supports if lvl < entry]
        if below:
            level = below[0]                          # ближайшая поддержка
            buffer = max((atr or 0) * SL_ATR_BUFFER * mult,
                         price * SL_MIN_BUFFER_PCT / 100)
            sl = level - buffer
            notes.append(f"стоп за поддержкой {level:,.2f} (+буфер {buffer:,.2f})")
        elif atr:
            sl = entry - atr * INVALIDATION_ATR
            notes.append(f"уровней ниже входа нет — инвалидация по ATR {atr:,.2f}")
        else:
            return {"error": "нет уровня отмены ниже входа и нет ATR — стоп не обосновать"}
        # Не сужать стоп ниже min_stop_pct: иначе упор в потолок плеча.
        if (entry - sl) / entry * 100 < min_stop_pct:
            sl = entry * (1 - min_stop_pct / 100)
            notes.append(f"стоп расширен до минимума {min_stop_pct:g}% (потолок плеча)")
        above = [lvl for lvl in resistances if lvl > entry]
        stop_dist = entry - sl
        candidates = above
        flip = 1
    else:  # SHORT
        above = [lvl for lvl in resistances if lvl > entry]
        if above:
            level = above[0]                          # ближайшее сопротивление
            buffer = max((atr or 0) * SL_ATR_BUFFER * mult,
                         price * SL_MIN_BUFFER_PCT / 100)
            sl = level + buffer
            notes.append(f"стоп за сопротивлением {level:,.2f} (+буфер {buffer:,.2f})")
        elif atr:
            sl = entry + atr * INVALIDATION_ATR
            notes.append(f"уровней выше входа нет — инвалидация по ATR {atr:,.2f}")
        else:
            return {"error": "нет уровня отмены выше входа и нет ATR — стоп не обосновать"}
        if (sl - entry) / entry * 100 < min_stop_pct:
            sl = entry * (1 + min_stop_pct / 100)
            notes.append(f"стоп расширен до минимума {min_stop_pct:g}% (потолок плеча)")
        below = [lvl for lvl in supports if lvl < entry]
        stop_dist = sl - entry
        candidates = below
        flip = -1

    if stop_dist <= 0:
        return {"error": "стоп оказался по ту сторону входа — уровни противоречивы"}

    # --- TP: первый реальный уровень с требуемым R:R ----------------------
    tp = None
    for level in candidates:
        rr = flip * (level - entry) / stop_dist
        if rr >= min_rr:
            tp = level
            break
    if tp is None:
        if not candidates:
            return {"error": "в направлении сделки нет реальных целей (уровней) — WAIT"}
        nearest = candidates[0]
        best_rr = flip * (nearest - entry) / stop_dist
        return {
            "error": (
                f"ближайшая цель {nearest:,.2f} даёт R:R {best_rr:.2f} < "
                f"{min_rr:g}: тянуть TP за реальные уровни запрещено — WAIT"
            ),
        }

    rr = flip * (tp - entry) / stop_dist
    stop_pct = stop_dist / entry * 100
    notes.append(f"цель по реальному уровню {tp:,.2f} (R:R {rr:.2f})")

    # --- Проверка корректного расположения --------------------------------
    if direction == "LONG" and not (sl < entry < tp):
        return {"error": f"уровни не согласованы LONG: стоп {sl} вход {entry} тейк {tp}"}
    if direction == "SHORT" and not (tp < entry < sl):
        return {"error": f"уровни не согласованы SHORT: тейк {tp} вход {entry} стоп {sl}"}
    if rr < min_rr:
        return {"error": f"R:R {rr:.2f} ниже требуемых {min_rr:g} — WAIT"}

    return {
        "sl": round(sl, 2),
        "tp": round(tp, 2),
        "rr": round(rr, 4),
        "stop_pct": round(stop_pct, 4),
        "notes": "; ".join(notes),
    }
