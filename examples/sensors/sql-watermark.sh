#!/bin/sh
# sql-watermark: one run per new row over a monotone column.
# Cursor: highest committed id. run_key <table>:<id> so one row fires once
# per epoch. Gap: non monotone columns (updates) replay; an autoincrement id
# beats updated_at. Exit 75 (transient): a briefly unreadable DB counts no fault.
set -eu

DB="${WATCH_DB:?WATCH_DB is required}"
TABLE="${WATCH_TABLE:?WATCH_TABLE is required}"
COL="${WATCH_COL:-id}"
cursor="${PACEQ_CURSOR:-0}"
max="${PACEQ_MAX_TRIGGERS:-100}"

ids=$(sqlite3 -noheader "$DB" "SELECT $COL FROM $TABLE WHERE $COL > $cursor ORDER BY $COL LIMIT $max;" 2>/dev/null) || exit 75

triggers=""
newest="$cursor"
n=0
for id in $ids; do
  [ "$n" -ge "$max" ] && break
  triggers="${triggers}${triggers:+,}$(printf '{"run_key":"%s:%s","params":{"id":"%s"}}' "$TABLE" "$id" "$id")"
  newest="$id"
  n=$((n + 1))
done

if [ "$n" -eq 0 ]; then
  printf '{"cursor":"%s","triggers":[],"skip_reason":"no rows newer than %s"}\n' "$cursor" "$cursor"
else
  printf '{"cursor":"%s","triggers":[%s],"skip_reason":null}\n' "$newest" "$triggers"
fi