#!/bin/sh
# fs-watermark: one run per file newer than the cursor.
# Cursor: highest mtime (unix s). run_key <path>:<mtime> so a file that
# changes again gets a new run. Gap: a half written file looks newer than
# the cursor; the duplicate run is a no-op at commit (run_keys dedup).
set -eu

DIR="${WATCH_DIR:?WATCH_DIR is required}"
cursor="${PACEQ_CURSOR:-0}"
max="${PACEQ_MAX_TRIGGERS:-100}"

triggers=""
newest="$cursor"
n=0
for f in $(find "$DIR" -type f 2>/dev/null | sort); do
  [ "$n" -ge "$max" ] && break
  mtime=$(stat -c %Y "$f")
  [ "$mtime" -le "$cursor" ] && continue
  triggers="${triggers}${triggers:+,}$(printf '{"run_key":"%s:%s","params":{"path":"%s"}}' "$f" "$mtime" "$f")"
  [ "$mtime" -gt "$newest" ] && newest="$mtime"
  n=$((n + 1))
done

if [ "$n" -eq 0 ]; then
  printf '{"cursor":"%s","triggers":[],"skip_reason":"no files newer than %s"}\n' "$cursor" "$cursor"
else
  printf '{"cursor":"%s","triggers":[%s],"skip_reason":null}\n' "$newest" "$triggers"
fi