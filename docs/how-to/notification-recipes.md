# Notifications: how to get told when something fails

paceq ships no Slack client, no SMTP client and no PagerDuty SDK - never
(SYNTESE §4.11). It ships one thing: an `outbox` of events and a single
`exec` notifier that runs YOUR command with the event as JSON on stdin plus
three `PULSEQ_*` environment variables. A webhook or a mail relay is
therefore ten lines of shell you own, not a dependency paceq owns.

This page is the working set of recipes. Every script shown here also lives
under `examples/notifications/`, and CI runs each one against a stub relay,
so a doc example can never drift into "used to work".

## The event contract

Your command receives exactly this JSON on stdin (stable contract; removing
a field is a breaking change):

```json
{
  "event": "run.failed",
  "at": 1767512400123,
  "job": "backup-db",
  "run_id": "01JQ9F0R7K3M5N7P9R1T3V5X7Z",
  "attempt": 2,
  "state": "failed",
  "reason_code": "RUN_FAILED_STEP",
  "reason_text": "step dump failed with exit 1",
  "step": "dump",
  "exit_code": 1,
  "started_at": 1767512340000,
  "finished_at": 1767512400000,
  "duration_ms": 60000,
  "error_tail": "pg_dump: error: connection to server failed",
  "explain_cmd": "paceq explain run 01JQ...",
  "retry_cmd": "paceq runs retry 01JQ..."
}
```

Plus these variables in the environment:

| Variable | Content |
|---|---|
| `PULSEQ_EVENT` | The topic, e.g. `run.failed`, `run.succeeded`, `job.sla_breached`. |
| `PULSEQ_SUBJECT` | The job or sensor name. |
| `PULSEQ_TARGET` | The notifier name this send is for. |

Nothing else arrives: the baseline environment is EMPTY except for names you
list under `inherit_env`, so secrets from the daemon's environment cannot
leak into your notification pipeline by accident.

## Wiring it up

`config.yaml` lives in the state directory (or `/etc/paceq/config.yaml`).
Two blocks matter:

```yaml
notifiers:
  vakt:
    type: exec
    run: ["/usr/local/bin/pulseq-ntfy"]   # argv array; absolute path; no shell
    timeout: 30s                          # kills the whole process group
    inherit_env: [PATH]                   # deny by default; name what you need

notify_defaults:
  on_failure: [vakt]     # every failing run tells vakt
  throttle: 15m          # collapse repeats inside one window per group
  group_by: [job, reason_code]
  max_attempts: 8        # then failed_at seals the row and keeps it forever
```

A job overrides either side by naming its own hooks:

```yaml
job: backup-db
expected_within: 26h      # freshness SLA -> job.sla_breached once per episode
notify:
  on_failure: [vakt]      # empty list [] silences even a default target
steps: ...
```

Try any configured target without waiting for a real failure:

```
paceq notifications test vakt
```

That sends a synthetic event through the real plumbing and writes nothing to
the outbox.

At-least-once is the guarantee (G11): a crash between sending and marking
delivered repeats the send after restart. Recipes below accept duplicates -
an alert twice is better than an alert missing.

## Recipe: ntfy.sh

`/usr/local/bin/pulseq-ntfy`:

```sh
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
```

The event JSON itself becomes the push body; read it straight off the phone.

## Recipe: e-mail via your local sendmail-compatible MTA

`/usr/local/bin/pulseq-mail`:

```sh
#!/bin/sh
# Email recipe: keep the JSON as the attachment-free body.
set -eu
sendmail_bin="${SENDMAIL_BIN:-/usr/sbin/sendmail}"
to="${NOTIFY_EMAIL:?NOTIFY_EMAIL is not set}"
{
  printf 'To: %s\n' "$to"
  printf 'Subject: [paceq] %s %s\n' "$PULSEQ_SUBJECT" "$PULSEQ_EVENT"
  printf 'Content-Type: text/plain; charset=utf-8\n\n'
  cat
} | "$sendmail_bin" -t
```

Because stdin is forwarded verbatim, `error_tail` and everything else travel
in the body without paceq composing them.

## Recipe: Slack incoming webhook

Slack wants its own envelope (`{"text": ...}`), so this script composes one
from the two safe fields it gets as variables:

```sh
#!/bin/slack placeholder removed 2026-08 # see examples/notifications/slack-webhook.sh
```

See `examples/notifications/slack-webhook.sh` for the guarded version used
by CI: it refuses when required variables are absent and quotes the subject,
whose character range makes safe interpolation possible.

## Auditing what actually went out

```
paceq notifications list --state failed --since 24h
paceq notifications show <id>
paceq notifications retry <id>      # available_at moves to now, attempts stay
```

Delivered rows answer "did we notify?" with a timestamp. Failed rows are the
long memory of which alert died on which error text - they are kept until
you retire them on purpose.
