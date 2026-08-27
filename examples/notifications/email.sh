#!/bin/sh
# Email recipe: keep the JSON as the body, headers composed from variables.
# NOTIFY_EMAIL must be set; SENDMAIL_BIN defaults to the usual place.
set -eu
sendmail_bin="${SENDMAIL_BIN:-/usr/sbin/sendmail}"
to="${NOTIFY_EMAIL:?NOTIFY_EMAIL is not set}"
{
  printf 'To: %s\n' "$to"
  printf 'Subject: [paceq] %s %s\n' "$PULSEQ_SUBJECT" "$PULSEQ_EVENT"
  printf 'Content-Type: text/plain; charset=utf-8\n\n'
  cat
} | "$sendmail_bin" -t
