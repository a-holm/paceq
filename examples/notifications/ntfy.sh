#!/bin/sh
# ntfy recipe: forward the raw event as one push.
# NTFY_URL must be exported before paceq starts, e.g. https://ntfy.sh/mysite
set -eu
url="${NTFY_URL:?NTFY_URL is not set}"
curl -fsS \
  -H "Title: paceq ${PULSEQ_SUBJECT}: ${PULSEQ_EVENT}" \
  --data-binary @- \
  "$url" > /dev/null
echo "$PULSEQ_EVENT for $PULSEQ_SUBJECT was delivered"
exit 0
