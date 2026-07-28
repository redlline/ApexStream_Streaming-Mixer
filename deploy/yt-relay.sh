#!/bin/bash
# Релей YouTube → MediaMTX (SRT push), для ApexStream.
#
# Зачем отдельный процесс, а не ссылка прямо во входе потока: yt-dlp отдаёт
# ВРЕМЕННУЮ ссылку (живёт около 6 часов), после чего она мертва. Если поставить
# её входом потока, эфир упадёт и сам не поднимется — движок будет долбиться в
# протухший URL. Здесь же протухание чинится переполучением ссылки: наш поток
# видит короткий пропал сигнала, показывает заглушку и подхватывает обратно.
#
# Использование:  yt-relay.sh <youtube-url> <streamid> [srt-порт]
# Поток в микшере потом читается как  rtsp://127.0.0.1:8554/<streamid>

set -u
URL="${1:?укажите ссылку на YouTube}"
SID="${2:?укажите streamid}"
PORT="${3:-8890}"

log(){ echo "[$(date '+%F %T')] $*"; }

while true; do
  log "резолвлю ссылку: $URL"
  # берём готовый прогрессивный/HLS-вариант; -g отдаёт прямой URL манифеста
  M=$(yt-dlp -g -f 'best[protocol^=m3u8]/best' "$URL" 2>/dev/null | head -1)

  if [ -z "$M" ]; then
    log "не удалось получить ссылку — повтор через 20с"
    sleep 20
    continue
  fi

  EXP=$(echo "$M" | grep -oP 'expire/\K[0-9]+' || true)
  [ -n "${EXP:-}" ] && log "ссылка действует до $(date -d "@$EXP" '+%F %T')"
  log "запускаю ffmpeg → srt://127.0.0.1:$PORT (streamid=publish:$SID)"

  # -c copy: YouTube отдаёт h264+aac, перекодировать незачем — экономим CPU и
  # не теряем качество. Микшер всё равно кодирует заново уже со своими слоями.
  # -reconnect: короткие сетевые заминки не должны ронять весь релей.
  ffmpeg -hide_banner -loglevel warning \
         -reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 \
         -i "$M" -c copy -f mpegts \
         "srt://127.0.0.1:$PORT?streamid=publish:$SID" 2>&1 | tail -3

  log "ffmpeg завершился (протухла ссылка или обрыв) — перезапуск через 5с"
  sleep 5
done
