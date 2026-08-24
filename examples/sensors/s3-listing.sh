#!/bin/sh
# s3-listing: one run per object in lexicographic key order.
# Cursor: last object key. run_key = the key, so dedup and cursor agree.
# Truncation: PACEQ_MAX_TRIGGERS caps one tick; the cursor only advances to
# the last key actually listed, so a capped tick re lists the rest next poll.
# The cursor test lives in awk (POSIX sh has no string comparison).
set -eu

BUCKET="${WATCH_BUCKET:?WATCH_BUCKET is required}"
cursor="${PACEQ_CURSOR:-}"
max="${PACEQ_MAX_TRIGGERS:-100}"

keys=$(aws s3 ls "s3://$BUCKET" --recursive 2>/dev/null \
  | awk -v c="$cursor" -v cap="$max" '{ if ((c == "") || ($4 > c)) { if (n >= cap) exit; print $4; n++ } }') || exit 75

triggers=""
newest="$cursor"
n=0
for key in $keys; do
  triggers="${triggers}${triggers:+,}$(printf '{"run_key":"%s","params":{"key":"s3://%s/%s"}}' "$key" "$BUCKET" "$key")"
  newest="$key"
  n=$((n + 1))
done

if [ "$n" -eq 0 ]; then
  printf '{"cursor":"%s","triggers":[],"skip_reason":"no keys newer than %s"}\n' "$cursor" "$cursor"
else
  printf '{"cursor":"%s","triggers":[%s],"skip_reason":null}\n' "$newest" "$triggers"
fi