#!/bin/bash
# Первый канал живости Render (второй — GitHub Actions, .github/workflows/keep-alive.yml).
# Ходит на /health каждые INTERVAL секунд: Render засыпает после 15 минут
# без входящего трафика. Если /health отдаёт 404 (старая версия) — берём /.
#
# Почему timeout вместо одного --max-time: после загрузки машины curl
# зависал в DNS на ~50 минут, --max-time такое не обрывает, и пингер молча
# простаивал. timeout убивает процесс целиком, что бы ни случилось.

URL="https://crypto-bot-im58.onrender.com/health"
FALLBACK="https://crypto-bot-im58.onrender.com/"
LOG="/home/admin/keepalive/keepalive.log"
BODY="/home/admin/keepalive/keepalive.body"
INTERVAL=240

ping_once() {
    local target="$1" raw
    raw=$(timeout 40 curl -sS --connect-timeout 10 --max-time 30 \
             -o "$BODY" -w "%{http_code}" "$target" 2>/dev/null || true)
    case "$raw" in
        [0-9][0-9][0-9]) printf '%s' "$raw" ;;
        *) printf '000' ;;
    esac
}

while true; do
    code=$(ping_once "$URL")
    if [ "$code" = "404" ]; then
        code=$(ping_once "$FALLBACK")
    fi
    echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) HTTP $code $(head -c 160 "$BODY" 2>/dev/null | tr -d '\n')" >>"$LOG"
    # Лог не разрастается: держим последние 1500 строк.
    if [ "$(wc -l <"$LOG")" -gt 2000 ]; then
        tail -n 1500 "$LOG" >"$LOG.tmp" && mv "$LOG.tmp" "$LOG"
    fi
    sleep "$INTERVAL"
done
