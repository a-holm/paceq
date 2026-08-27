#!/bin/sh
# Slack incoming-webhook recipe. Slack wants its own JSON envelope, so this
# script composes one from the two variables the contract guarantees.
# SLACK_WEBHOOK_URL must be exported before paceq starts.
#
# Safe interpolation note: PULSEQ_SUBJECT and PULSEQ_TARGET are paceq names
# (lower case letters, digits, underscores, dashes) and PULSEQ_EVENT comes
# from a closed vocabulary, so none of them can break out of the quotes
# below. Never build JSON from PULSEQ_* values that carry free text - put
# those in the body via stdin instead.
set -eu
url="${SLACK_WEBHOOK_URL:?SLACK_WEBHOOK_URL is not set}"
body=$(printf '{"text":"paceq %s on %s: %s"}' \
  "$PULSEQ_EVENT" "$PULSEQ_SUBJECT" "$PULSEQ_TARGET")
curl -fsS \
  -H "Content-Type: application/json" \
  --data-binary "$body" \
  "$url" > /dev/null
echo "slack accepted $PULSEQ_EVENT for $PULSEQ_SUBJECT"
