# Sensor contract

This page is the one reference a sensor author needs. It freezes the contract
between paceq and a sensor subprocess at v0.1: the inbound object, the
environment, the one JSON object on stdout, the exit code table, the resource
limits and the guarantees. Anything [M3-07] or [M5-08] builds on this page, and
nothing here changes without a contract break.

## What a sensor is

A sensor is a program. paceq starts it as a subprocess in its own process
group when the sensor is due, feeds it the inbound contract on stdin and as
environment, waits for one JSON object on stdout, and classifies the run into
a tick verdict. A sensor never runs in process, never through a shell, and
never waits in line behind another sensor: each one runs in its own goroutine,
and no two runs of the same sensor overlap.

## Inbound contract

The object is written to the sensor's standard input as a single JSON value,
and every field is also set as an environment variable, so a one liner and a
program read the same facts.

| JSON field | Type | Env | Meaning |
|---|---|---|---|
| `sensor` | string | `PACEQ_SENSOR` | the sensor's name |
| `job` | string | `PACEQ_JOB` | the job this sensor belongs to |
| `cursor` | string or null | `PACEQ_CURSOR` | the last committed cursor, empty on the first run |
| `last_tick_at` | integer or null | `PACEQ_LAST_TICK_AT` | unix ms of the last committed tick |
| `now` | integer | `PACEQ_NOW` | unix ms wall time of this evaluation |
| `max_triggers` | integer | `PACEQ_MAX_TRIGGERS` | the most triggers this run may return |
| `deadline_ms` | integer | `PACEQ_DEADLINE_MS` | unix ms by which the sensor should answer |
| `dry_run` | boolean | `PACEQ_DRY_RUN` | `0` or `1`; 1 means evaluate and report only |

```json
{
  "sensor": "dropzone",
  "job": "import-file",
  "cursor": "2026-08-21/03-11-02.csv",
  "last_tick_at": 1761040800000,
  "now": 1761040860000,
  "max_triggers": 100,
  "deadline_ms": 1761040890000,
  "dry_run": false
}
```

## Environment

The sensor's environment is deny by default. Only these layers are visible:
the fixed baseline (`PATH`, and `HOME`, `TZ`, `LANG` when the daemon set them),
the `PACEQ_` contract keys above, and any variable the sensor declared in its
own definition. A variable set in the daemon's environment is not visible
unless the sensor declares it. The `PACEQ_` prefix is reserved; a sensor may
not override a contract key.

## Out contract

Exactly one JSON object on standard output:

```json
{
  "cursor": "2026-08-21/09-20-03.csv",
  "triggers": [
    {"run_key": "2026-08-21/09-14-51.csv", "params": {"key": "s3://bucket/09-14-51.csv"}},
    {"run_key": "2026-08-21/09-20-03.csv", "params": {"key": "s3://bucket/09-20-03.csv"}}
  ],
  "skip_reason": null
}
```

`cursor` names the position the next run starts from, `triggers` names the
runs to create, and `skip_reason` explains a quiet run. One JSON object is the
only accepted form in the MVP; history and summarising are `jq`'s business.

## Exit codes and verdicts

| Situation | Tick outcome | reason code |
|---|---|---|
| exit 0, at least one trigger | triggered | trigger-level codes |
| exit 0, no triggers, a `skip_reason` | skipped | `TICK_SKIPPED_SENSOR` |
| exit 0, empty stdout | skipped | `TICK_SKIPPED_SENSOR` (fixed text) |
| exit 0, unreadable JSON | errored | `TICK_ERROR_SENSOR_OUTPUT` |
| exit 75 | errored | `TICK_ERROR_SENSOR_FAILED`, transient |
| exit 64 | errored | `TICK_ERROR_CONFIG` |
| any other nonzero exit | errored | `TICK_ERROR_SENSOR_FAILED` |
| timed out (deadline overshot) | errored | `TICK_ERROR_SENSOR_TIMEOUT` |
| stdout larger than the limit | errored | `TICK_ERROR_SENSOR_OUTPUT` |
| `skip_reason` and triggers both set | triggered | triggers win; the reason is kept as a note |

Whitespace alone is invalid output, not silence: an empty stdout is a skip,
but a stream of spaces is unreadable JSON and an error.

## Limits and guarantees

- stdout is capped at 1 MiB; a larger stream is an errored outcome and never
  a truncated parse.
- The last 4 KiB of stderr is kept on the tick, so a sensor that dies rarely
  explains itself at its own beginning.
- Timeout is hard and mandatory: a sensor that overshoots its deadline is
  killed with SIGTERM to its whole process group and SIGKILL after the grace.
- A hanging sensor never delays the loop or the other sensors; it occupies its
  own slot until its deadline, and the scheduler loop is never on its path.
- The same sensor never runs twice at once.
- Concurrent evaluations are capped globally (default 4).
- The cursor never moves on a failed or skipped run.

## A full five line sensor

A dropzone sensor that answers with one new file:

```sh
#!/bin/sh
f=$(ls /inbox/new/*.csv 2>/dev/null | head -1)
if [ -z "$f" ]; then
  echo '{"skip_reason":"no new files"}'
  exit 0
fi
printf '{"cursor":"%s","triggers":[{"run_key":"%s"}]}' "$f" "$f"
```

[M3-07]: ./PLAN.md
[M5-08]: ./PLAN.md