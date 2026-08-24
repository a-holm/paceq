#!/bin/sh
# http-etag: one run when a URL changes its ETag (or body hash).
# Cursor: the ETag value, or a sha256 of the body when none. run_key = the
# ETag, so cursor and dedup agree. Gap: no ETag re downloads every poll.
# Exit 75 is transient (EX_TEMPFAIL): a network blip never trips the breaker.
set -eu

URL="${WATCH_URL:?WATCH_URL is required}"
cursor="${PACEQ_CURSOR:-}"

tmp=$(mktemp 2>/dev/null || printf '/tmp/http-etag.XXXXXX')
trap 'rm -f "$tmp"' EXIT HUP INT TERM

if ! curl -fsSI --max-time 10 "$URL" >"$tmp" 2>/dev/null; then exit 75; fi
etag=$(awk 'tolower($1)=="etag:"{print $2}' <"$tmp" | tr -d '\r"')

if [ -z "$etag" ]; then
  if ! curl -fsS --max-time 20 "$URL" >"$tmp" 2>/dev/null; then exit 75; fi
  etag=$(sha256sum <"$tmp" | cut -d' ' -f1)
fi

if [ "$etag" = "$cursor" ]; then
  printf '{"cursor":"%s","triggers":[],"skip_reason":"etag unchanged"}\n' "$cursor"
else
  printf '{"cursor":"%s","triggers":[{"run_key":"%s","params":{"url":"%s","etag":"%s"}}],"skip_reason":null}\n' \
    "$etag" "$etag" "$URL" "$etag"
fi