# Job file reference

A job file is YAML, one job per file, loaded by `paceq apply` as an immutable
version - editing a file and re-applying makes a new version, never a mutation
of history. Every key, its type, its default and its limit are on this page.
`paceq validate` checks the same rules the parser enforces and names every
finding; when a key is misspelled the error suggests what you probably meant
(`cmd` → `run`, `depends_on` → `needs`, and friends).

Names (job, step, schedule, sensor) match `^[a-z0-9][a-z0-9_-]{0,63}$`: lower
case, no spaces, because a name appears on a command line, in a directory name
and in a URL. Durations accept Go syntax: `30s`, `10m`, `1h`.

## The whole format at a glance

```yaml
name: nightly-report          # required, unique across the catalog
description: What this job is for.

env:                          # job environment, most specific layer
  REPORT_BUCKET: ops-reports
env_file: secrets.env         # mode 0600 exactly, or refused
inherit_env: [HOME, AWS_REGION]  # named variables only; deny by default otherwise
workdir: /srv/reports
timeout: 1h                   # default 1h, ceiling 24h
expected_within: 26h          # freshness SLA: feeds pulseq_job_freshness_sla_seconds + alerting
max_concurrent: 1             # default 1: no overlapping runs unless you say so
max_parallel: 4               # steps of one run in flight at once; default 4, max 64
concurrency_key: tenant       # see "Concurrency keys" below
on_conflict: defer            # defer | skip (with a concurrency_key)

steps:
  - name: main                # required per step
    run: ["/usr/local/bin/report", "--full"]   # argv, never a shell string
    shell: false              # true hands `run` to /bin/sh -c (warns)
    workdir: /tmp/spool       # must exist before the step starts
    timeout: 10m              # this step alone, inside the job's ceiling
    needs: [extract]          # static DAG edges; failed upstream => skipped
    retry:
      max: 3                  # retries after the first attempt
      backoff: exponential    # exponential | fixed
      initial: 30s            # first delay (default 30s)
      max_delay: 10m          # ceiling for exponential growth (default 10m)
      jitter: full            # full | none (default full)

schedules:
  - name: daily               # required per schedule
    cron: "17 2 * * *"        # five fields, or @daily/@hourly/... equivalents
    timezone: Europe/Oslo     # default UTC; explicit beats ambient
    overlap: skip             # skip | queue when max_concurrent is held

sensors:
  - name: dropzone            # required, unique across every job
    kind: exec                # the only kind in v0.1
    run: ["./sensors/new-files.sh"]
    workdir: .
    env:                      # extra variables for the sensor subprocess only
      WATCH_DIR: /srv/dropzone
    interval: 30s             # evaluation cadence; floor 1s
    min_interval: 1s          # absolute lower bound between two starts
    timeout: 30s              # per evaluation; 1s..5m
    max_triggers_per_tick: 100   # burst chunking; up to 10000
    paused: false             # initial state on materialisation
    description: React to files landing in the dropzone.
```

## Jobs

| Key | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | unique across the catalog; the name every command takes |
| `description` | string | | shown by `paceq ls` and `explain` |
| `env` | map[string]string | | layer 5 of the [step contract](step-contract.md); may not set `PACEQ_*` |
| `env_file` | path | | relative to the project root; mode must be exactly 0600 |
| `inherit_env` | list[string] | | variables copied from the daemon's own environment; `PACEQ_*` refused |
| `workdir` | path | project root | where steps start unless a step overrides |
| `timeout` | duration | `1h` | ceiling `24h`; a longer job is a service, not a scheduled job |
| `expected_within` | duration | unset | freshness SLA. Unset means no expectation and no SLA series; an absent expectation never renders as zero |
| `max_concurrent` | int | `1` | runs of this job in flight at once; the opposite of cron, which overlaps freely |
| `max_parallel` | int | `4` | steps of one run in flight at once; 1..64 |
| `concurrency_key` | scalar or map | none | mutual exclusion, see below |
| `on_conflict` | `defer` \| `skip` | `defer` | what a trigger does when another run holds the key |

### Concurrency keys

Without a key, every run of a job is unlimited apart from `max_concurrent`.
With one, runs that resolve to the same key value are mutually exclusive -
the general form of the flock wrapper cron users write by hand. Exactly one
form applies:

```yaml
concurrency_key: nightly           # constant: every run has this key
concurrency_key:
  param: tenant                    # params["tenant"] of the trigger
concurrency_key:
  from: run_key                    # the trigger's dedup key: each event its own class
```

A param form that resolves to an empty value means the run has no key and is
unlimited - normal for sensors whose payload simply lacked the field. Key
values cap at 200 characters. `on_conflict` decides what the second fire does:
`defer` (default) queues it to start when a slot frees; `skip` stands it down,
recorded with its reason like every other decision.

## Steps

| Key | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | unique within the job |
| `run` | list[string] | required | **argv**, never a shell string: nothing is ever split, quoted or expanded by a shell. The first element must be an **absolute path** - paceq runs the process itself, there is no shell to search PATH (`PQ1012` guards this) |
| `shell` | bool | `false` | hands `run` to `/bin/sh -c`. An explicit opt-in that carries a validation warning wherever it appears |
| `workdir` | path | job's `workdir` | must exist; a missing directory is a spawn failure with zero side effects |
| `timeout` | duration | job's `timeout` | this step's own ceiling inside it |
| `retry` | map | none | see below |
| `needs` | list[string] | [] | static DAG edges, known at parse time. A failed step marks everything downstream `skipped` with reason codes; there is no `continue_on_error` in 1.0 |

Steps run when their `needs` have succeeded; independent steps share the run
up to `max_parallel`. Retry policy:

| Key | Default | Notes |
|---|---|---|
| `max` | 0 (no retries) | attempts after the first |
| `backoff` | `exponential` | `exponential` \| `fixed` |
| `initial` | `30s` | first delay |
| `max_delay` | `10m` | exponential growth stops here |
| `jitter` | `full` | `full` \| `none` |

An exit code of 75 (`EX_TEMPFAIL`) marks the failure *transient*: it is the
step telling paceq "this is temporary", so it retries under the step's `retry`
policy like any other failure. See the
[step contract](step-contract.md) for how outcomes map to reason codes.

## Schedules

| Key | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | `<job>.<schedule>` everywhere in the CLI |
| `cron` | string | required | five fields; seconds are not part of the grammar |
| `timezone` | IANA name | `UTC` | UTC rather than the daemon's local zone, so a schedule cannot mean different things on different machines. DST is explicit: spring-forward gaps and fall-back duplicates follow a documented policy per schedule, recorded as tick reasons either way |
| `overlap` | `skip` \| `queue` | `skip` | what happens when the fire time arrives and `max_concurrent` is already held: stand down (like `flock -n`) or queue the run into the future. Either way the decision lands in the history with its reason |

`paceq schedules preview <job>.<daily>` shows the upcoming firings without
running anything.

## Sensors

| Key | Type | Default | Notes |
|---|---|---|---|
| `name` | string | required | unique across every job; the sensor row's primary key |
| `kind` | `exec` | `exec` | the only kind in v0.1; anything else is a validation error naming the release that brings it |
| `run` | list[string] | required | argv, like steps. A relative `argv[0]` resolves against the evaluation's working directory - the project root when you run the CLI there |
| `workdir` | path | the evaluator's working directory | where the sensor process starts; must be absolute when set |
| `env` | map[string]string | | variables for the sensor subprocess only, on top of the fixed baseline; may not set `PACEQ_*` |
| `interval` | duration | | how often the sensor is evaluated; floor 1s |
| `min_interval` | duration | `1s` | absolute lower bound between two starts even across retries |
| `timeout` | duration | `30s` | ceiling on one evaluation; 1s..5m. A sensor that needs longer should chunk via `max_triggers_per_tick` instead |
| `max_triggers_per_tick` | int | `100` | how many triggers one evaluation may admit; up to 10000. The chunking knob that keeps a backfill burst from flooding the queue |
| `paused` | bool | `false` | initial state on first materialisation; pausing later survives re-apply |
| `description` | string | | |

The wire format a sensor speaks - JSON on stdin, JSON on stdout, the
`PACEQ_*` environment, exit codes - is frozen separately in the
[sensor contract](sensor-contract.md). How its cursor and run keys behave:
[cursor vs run_key](../cursor-vs-run-key.md).

## What a job file is not

No templating, no expression language, no loops over values, no includes. The
file is data, and everything dynamic lives in the steps themselves - that is
what keeps `paceq validate` able to say exactly what a file means.
