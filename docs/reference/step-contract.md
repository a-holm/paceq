# The step contract

Every step of every run executes the same way: paceq starts your command as a
subprocess in its own process group, hands it a documented environment, waits
under a deadline, and records an outcome with a reason code. This page is that
contract. It is frozen at v0.1: nothing here changes without a contract break,
so you can build against it.

See also the [sensor contract](sensor-contract.md) - the sibling contract for
the subprocesses that decide when to run.

## How the command runs

- `run` is **argv**, never a string. Nothing is split, quoted or expanded by a
  shell, so there are no injection surprises and no shell-dependent quoting.
  Opt into a shell explicitly with `shell: true`, which carries a validation
  warning wherever it appears.
- Each step gets its own **process group**. A timeout kill addresses the whole
  group, so grandchildren do not survive their parent.
- The working directory is the job's `workdir` (or the project root), then the
  step's own `workdir` if it sets one. It must exist; a missing workdir is a
  spawn failure before any side effect happens.
- Time comes from the job's `timeout` unless the step sets its own `timeout`.

## Environment: deny by default

A job does not inherit the daemon's environment. "Works in my shell, fails in
cron" is the number one cron trap, and it is solved by making the environment
explicit instead of inheriting more. The child environment is built in layers;
a key set by a later layer replaces the same key from an earlier one:

1. **Baseline**: `PATH` is a fixed default (`/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin`), never inherited from the daemon. `HOME`, `TZ` and `LANG` pass through from the runner process when they are set there. Nothing else survives.
2. **The context contract** below, set on every run.
3. **`inherit_env`**: only the named variables, copied from the daemon's environment when they exist there.
4. **`env_file`**: a `KEY=VALUE` file that must have mode `0600` exactly; a looser mode is refused (fail closed).
5. **`env`**: the job's own map, the most specific layer, wins over every other user layer.

The `PACEQ_` prefix is reserved. No user layer may set, inherit or read from a
file a variable with that prefix; declaring one is a validation error.

## The context contract

Set on every step of every run:

| Variable | Meaning |
|---|---|
| `PACEQ_RUN_ID` | the run's ULID |
| `PACEQ_JOB` | job name |
| `PACEQ_STEP` | step name |
| `PACEQ_ATTEMPT` | 1-based attempt number |
| `PACEQ_RUN_KEY` | the trigger's dedup key, empty for manual runs |
| `PACEQ_IDEMPOTENCY_KEY` | sha256(run_id + ":" + step_name), first 32 hex characters; stable across retries and duplicate starts of the same step in the same run - use it to make steps idempotent |
| `PACEQ_SCHEDULED_FOR` | RFC3339 UTC instant this run was scheduled for, empty for manual runs |
| `PACEQ_PARAMS` | the trigger's params as a JSON object (`{}` when none) |
| `PACEQ_OUTPUT` | path of the NDJSON file the step may write artifacts and params to; created by the runner before the command starts |
| `PACEQ_INPUTS` | merged JSON from upstream steps, `{}` when there are none |
| `PACEQ_INPUTS_FILE` | set instead of `PACEQ_INPUTS` being inline once inputs cross 128 KiB; `PACEQ_INPUTS` is then the literal `null` |

### Making a step idempotent

paceq's delivery guarantee is at-least-once ([why](../guarantees.md)), so a
step can start twice under unlucky timing. Deduplicate on the key:

```bash
#!/bin/sh
lock="/var/lock/$PACEQ_IDEMPOTENCY_KEY"
[ -e "$lock" ] && { echo "already done"; exit 0; }
do_the_work && touch "$lock"
```

## Exit codes and outcomes

The runner maps how a process died onto one outcome, each with its own reason
code, so [explain](../troubleshooting.md) can tell "command not found" from
"exit 1" from "killed by a signal" from "hit the deadline":

| Outcome | When | Reason code |
|---|---|---|
| Succeeded | exit 0 | `STEP_SUCCEEDED` |
| Failed | exit N ≠ 0 | `STEP_FAILED_NONZERO_EXIT` |
| Failed (transient) | exit 75 (`EX_TEMPFAIL`) | retried per the step's `retry` policy |
| Signalled | killed by a signal | `STEP_FAILED_SIGNAL` (exit reported as 128+signal) |
| Timed out | deadline hit, process group killed | `STEP_FAILED_TIMEOUT` |
| Spawn failed | the process never started (missing binary, no execute permission, missing workdir, refused path, unreadable env file) | `STEP_FAILED_SPAWN`; no side effects happened, so a retry is always safe |

Exit 1 and exit 5 mean different things at the *command* level too:
[exit codes](exit-codes.md).

## Writing outputs

A step may append NDJSON lines to `$PACEQ_OUTPUT`. Each line is exactly one of
two contract shapes - unknown fields, both shapes on one line, or a non-object
line is a contract break that surfaces with a warning, never a silent guess:

- `{"artifact": {...}}` registers an artifact reference. The object carries a
  `name`, a `uri` (absolute path or scheme URI), and optionally `size_bytes`,
  `checksum` and `media_type`.
- `{"params": {...}}` publishes parameters downstream steps read through
  `$PACEQ_INPUTS`.

Anything a step writes to stdout or stderr goes to its log file, readable with
`paceq logs <run>` - never parsed by paceq.

## Limits

- Job timeout default 1h, ceiling 24h. A longer job is a service, and paceq
  does not supervise services.
- Step timeout, when set, bounds that step alone inside the job's ceiling.
- The environment baseline is fixed; a job needing more must name it through
  `inherit_env`, `env_file` or `env`.
