# Troubleshooting

paceq never guesses: every evaluation, trigger, run and step stores its
outcome with a reason code, and `explain` reads those rows back. So most
troubleshooting starts the same way - run this first, then find your symptom
below:

```bash
paceq explain job <name> --since 48h
```

Each section below is anchored to a scenario in the explain checklist
(`internal/explain`), which CI keeps in lockstep with the
[reason catalogue](reference/reason-codes.md): if the code can store it, this
page can explain it. Every code links its full entry via
`paceq error <code>`.

## "My job did not run last night"

Start here. Then the most common causes:

### The schedule was paused

`TICK_SKIPPED_PAUSED`. Pausing is deliberate state, not an error. Resume it:

```bash
paceq schedules resume <job>.<schedule>     # or: paceq sensors resume <sensor>
```

### The previous run was still going

`TICK_SKIPPED_OVERLAP`: `max_concurrent` is **1 by default**, so while one run
is in flight the next fire stands down instead of stampeding. The tick row
carries the blocking `run_id`. Raise `max_concurrent` in the job file if the
overlap is actually safe.

### Another run holds the concurrency key

`TRIGGER_REJECTED_CONCURRENCY_KEY`: with a `concurrency_key`, runs resolving
to the same value exclude each other; `on_conflict: skip` stands the new
trigger down (with `defer`, the default, it queues instead). The trigger's
`reason_data` names the key and the blocking run.

### The daemon was down at the fire time

`TICK_MISSED_DAEMON_DOWN`: gap detection noticed no daemon was running and
recorded the window as missed. If catchup should have replayed it, see the
catchup settings below.

### The clock jumped past the fire time

`TICK_MISSED_CLOCK_JUMP`. Recorded rather than silently skipped.

### Catchup decided against replaying

Three codes, three policies: `TICK_SKIPPED_CATCHUP_DISABLED` (catchup is off),
`TICK_SKIPPED_CATCHUP_LAST_ONLY` (older due ticks were discarded, only the
newest replays) and `TICK_SKIPPED_CATCHUP_WINDOW` (the due tick is older than
the window; `reason_data` carries `scheduled_for` and `window_ms`). Which one
applies is a configuration choice you make explicitly - paceq never invents
one.

### DST did something to the local time

`TICK_SKIPPED_DST_NONEXISTENT` (spring forward: 02:30 does not exist) and
`TICK_SKIPPED_DST_DUPLICATE` (fall back: 02:30 happens twice). Each carries
the offending `local_time`. Schedules are explicit about their zone for
exactly this reason.

### A second instance lost the race

`TICK_MISSED_LEASE_LOST`: another daemon held the lease for this state
directory. Two daemons on one state directory is a configuration error;
the fencing makes it safe, the reason code makes it visible.

## "My sensor stopped firing"

### The sensor failed, timed out, or wrote bad output

`TICK_ERROR_SENSOR_FAILED` (non-zero exit or panic; `exit_code` stored),
`TICK_ERROR_SENSOR_TIMEOUT` (overshot its deadline; `timeout_ms`) and
`TICK_ERROR_SENSOR_OUTPUT` (invalid JSON or over the stdout cap; `bytes`,
`limit`). Reproduce without writing anything:

```bash
paceq sensors test <sensor>
```

### The sensor kept failing and the breaker tripped

`TICK_ERROR_SENSOR_FAILED` with the sensor paused by its breaker: repeated
failures trip a cooldown, and a half-open probe decides when it may try
again. Fix the underlying failure; the breaker opens back up on its own.

### The trigger was deduplicated away

`TRIGGER_DEDUPED_RUN_KEY`: that exact unit of work fired before. This is the
dedup gate working, not stuck. If you genuinely want the replay, read
[cursor vs run_key](cursor-vs-run-key.md) first - the two reset
independently, and which one you move decides what happens.

## "The run went red - why?"

Run-level answers come from the run's timeline; step-level ones from inside
it:

```bash
paceq explain run <id-or-prefix>
paceq logs <run-id>
```

| You see | It means | First thing to do |
|---|---|---|
| `RUN_FAILED_STEP` | a step ended red and dragged the run with it | find the step below |
| `RUN_TIMED_OUT` | the job's deadline hit | raise `timeout` or make the work smaller |
| `RUN_CANCELLED_MANUAL` | someone cancelled it | `runs cancel` is always recorded |
| `RUN_POISONED` | quarantined after repeated crash-loop failures | fix, then reopen |
| `RUN_REOPENED_OPERATOR` | an operator retry reopened it | history shows both attempts |

And per step:

| Code | Meaning | First thing to do |
|---|---|---|
| `STEP_FAILED_NONZERO_EXIT` | exited N ≠ 0 (`transient: true` when N=75) | the log has the answer: `paceq logs <run>` |
| `STEP_FAILED_SIGNAL` | killed by a signal, reported as 128+signal | who sent it? OOM killer is the classic |
| `STEP_FAILED_TIMEOUT` | deadline hit, process group killed | grandchildren die too - check for stragglers |
| `STEP_FAILED_SPAWN` | never started (missing binary, missing workdir, unreadable env_file); zero side effects | retrying is safe once the cause is fixed |
| `STEP_RETRIES_EXHAUSTED` | the retry policy ran out | widen `retry.max` only if the failures were transient |
| `STEP_SKIPPED_RUN_TIMED_OUT` | the run spent its deadline before this step started | raise `timeout`; nothing here failed |
| `STEP_SKIPPED_UPSTREAM_FAILED` | an upstream step failed | fix upstream; this step never started |
| `STEP_SKIPPED_UPSTREAM_SKIPPED` | skipped because *its* upstream was skipped | same, one level up |
| `STEP_SKIPPED_REPLAY_REUSED` | a replay reused the earlier attempt's result | nothing to do - reuse is recorded, not hidden |
| `STEP_FAILED_EXECUTOR_LOST` / `STEP_CANCELLED` | executor died mid-flight / cancellation won | the reaper hands these back; see [guarantees](guarantees.md) G5 |

When everything succeeded but the question is whether it *should have run*:
`RUN_SUCCEEDED` on the job timeline, with `freshness_state` in the summary,
answers "when did it last do real work".

## Nothing matches what I am seeing

- `paceq error <code>` prints the catalogue entry: long explanation plus
  remediation, straight from the source of truth.
- `paceq error --list -o json` gives machines the whole catalogue.
- `paceq doctor` checks the installation itself.
- `paceq fsck` checks the state database against its invariants.
