# Monitoring

paceq exposes Prometheus text-format metrics and ships alert rules, so "is
everything healthy" is a scrape instead of a guess. Metrics answer from the
running daemon; [status](../reference/status-contract.md) answers from the
database with the daemon down.

## Turning on /metrics

The endpoint is **opt-in and loopback-only**:

```bash
paceq serve --metrics-listen 127.0.0.1:9753
```

A non-loopback bind is refused at startup (exit 2): a daemon that came up
without the metrics surface its operator configured for is worse than one
that refuses to start. Scrape it through SSH tunneling or a proxy that
handles auth; do not widen the bind. When the daemon also runs with
`--socket <path>`, `/metrics` answers over that unix socket too.

## What is exposed

One static hand-rolled exposition - no client library in the dependency
budget. The families:

| Metric | Meaning |
|---|---|
| `pulseq_build_info` | version, commit - which binary is running |
| `pulseq_daemon_start_timestamp_seconds` | when this instance started |
| `pulseq_tick_total` | ticks processed |
| `pulseq_tick_lag_seconds` | how late ticks run under load (best effort) |
| `pulseq_last_tick_timestamp_seconds` / `pulseq_next_tick_timestamp_seconds` / `pulseq_tick_interval_seconds` | the scheduler's pulse |
| `pulseq_lease_reclaims_total` | expired leases the reaper took back |
| `pulseq_runs_by_state`, `pulseq_run_total` | queue depth and run counts by state |
| `pulseq_db_write_wait_seconds_max`, `pulseq_db_busy_total` | write-lock pressure |
| `pulseq_db_size_bytes`, `pulseq_wal_size_bytes` | disk footprint of state |
| `pulseq_last_success_timestamp_seconds`, `pulseq_job_freshness_sla_seconds` | per-job freshness: last real success vs the job's declared `expected_within` |
| `pulseq_sensor_cursor_age_seconds`, `pulseq_sensor_consecutive_failures`, `pulseq_instigator_paused` | sensor health |
| `pulseq_outage_seconds_total` | accumulated daemon-outage time |
| `pulseq_backup_last_success_timestamp_seconds`, `pulseq_backup_last_verified` | the nightly backup's age and verified flag |
| `pulseq_gc_last_success_timestamp_seconds` | retention's last pass |

A naming note, so nobody "fixes" it twice: the metric prefix is `pulseq_`,
the project's name at the time the families were frozen. The product renamed
to paceq ([ADR-0002](../adr/0002-product-name.md)); renaming public metric
names breaks every dashboard, so the prefix stays until a deliberate
breaking change.

## Shipped alert rules

[deploy/pulseq-alerts.yml](../../deploy/pulseq-alerts.yml) carries the rules
the project itself runs, each with a severity and an annotation saying what
to do:

| Alert | Fires when | Severity |
|---|---|---|
| `PulseqJobStale` | `time() - last_success > freshness_sla` for any job declaring `expected_within` | warning |
| `PulseqTickStalled` | no tick for 3 intervals | warning |
| `PulseqWALGrowth` | WAL over 64 MiB (checkpoint trouble) | warning |
| `PulseqBackupStale` | last backup older than 36h | critical |
| `PulseqBackupUnverified` | newest backup never passed verification | critical |
| `PulseqSensorErrorRate` | a sensor's consecutive failures pass 5 | warning |
| `PulseqQueueBacklog` | more than 50 queued runs | warning |
| `PulseqDaemonFlapping` | 4+ restarts in 30 minutes | critical |

Validate and test them the way CI does:

```bash
promtool check rules deploy/pulseq-alerts.yml
promtool test rules deploy/pulseq-alerts.test.yml
```

Both gates skip loudly without promtool on your machine and always run in CI,
pinned to the same promtool release the Makefile names.

## A job declares its own SLA

Every generic rule above covers every job forever because jobs carry
`expected_within`: one line in the job file feeds
`pulseq_job_freshness_sla_seconds`, and `PulseqJobStale` starts watching.
No expectation declared, no SLA series, no alarm - an absent expectation
never renders as zero.
