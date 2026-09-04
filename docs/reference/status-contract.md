# The status contract

`paceq status` is the command that is run most often and read fastest. This
document freezes the parts of it that scripts and monitoring are written
against: the exit codes, the job states, and the shape of `--json`. Nothing
here changes without a schema_version bump and a golden update.

## Exit codes

The exit code is half of the command's answer, because its second caller is
a cron line or an MOTD script that reads nothing else.

| Code | Meaning |
|---|---|
| 0 | Everything is inside its window: no unconfirmed failure in the last 24 hours, no stuck runs, no breached freshness SLA. |
| 5 | At least one job needs attention: an unconfirmed failure, a stuck run, or a breached SLA. |
| 1 | paceq itself failed (the state database is unavailable or corrupt). |
| 3 | A reference was given and nothing matches it. |
| 2 | The command line itself is wrong. |

**A paused job never moves the exit code away from 0.** Pausing is a
deliberate operator decision, not a deviation: it renders with its own mark
and stays out of the deviation count. A monitoring setup that wants to treat
a paused job as an incident has to say so itself; status refuses to guess,
because a permanently red MOTD line teaches people to ignore red.

The distinction between 1 and 5 is the whole point (03 section 7.2): a script
must be able to tell "paceq is broken" from "a job failed". `paceq status;
echo $?` in an MOTD hook answers exactly what the operator needs, without
parsing any text.

## Job states

One state per job, one line per job, deviations sorted first:

| State | Meaning | Deviation |
|---|---|---|
| `ok` | The newest finished run succeeded (or an older failure was later confirmed by a success). | no |
| `idle` | The job exists but no run has ever finished. | no |
| `paused` | An operator paused the job; schedules stand down, manual runs still go. | **never** |
| `failed` | The newest finished run failed and nothing after it confirms recovery. Age does not expire the verdict - only a later successful run does; `summary.failed_24h` scopes the counter to the last day, which is the window the exit-0 promise quotes. | yes |
| `stuck` | A run is still marked running while its lease has expired - the executor died mid-flight. | yes |
| `sla_breached` | The newest successful run is older than the job's freshness SLA. Reserved until the spec gains a freshness field; the exit-code path already treats it as a deviation. | yes |

"Unconfirmed failure" is time-based on purpose: the MVP has no
acknowledgement mechanism, so confirmation means "a successful run happened
after the failure". Reading only each job's newest finished run makes the
check exact without new state.

## Hints

Every deviation carries exactly one hint, and the hint is always a runnable
command - `paceq explain job <name>` - never prose. Anything healthy carries
no hint at all: a permanent hint footer would be noise, and noise gets
ignored.

## The JSON document

`-o json` writes one object with `schema_version: 1`. Timestamps are RFC3339
strings in UTC. The overview document carries `daemon`, `summary` and `jobs`;
`daemon.up` says whether a daemon holds this state directory, read from the
open session row while answering, so it is honest even when the daemon went
down between two cron lines and whether or not that daemon was given a socket
to listen on. A reference report adds
`subject` and kind-specific facts (`schedule`, `sensor`, `run`). Fields a
subject does not have are absent, never null-filled.

The golden tests under `internal/cli/testdata/script/status.txtar` pin this
shape; a change here fails them loudly by design.
