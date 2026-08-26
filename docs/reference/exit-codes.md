# Exit codes

Every `paceq` command answers with one of these. They are a public contract
from the first release and never change without a major version, because
scripts are written against them.

| Code | Meaning | |
|---|---|---|
| 0 | success | |
| 1 | paceq itself failed | a bug, an I/O error, an unexpected state - not your command line's fault |
| 2 | wrong arguments or flags | the help text named in the error is the fix |
| 3 | the resource asked for does not exist | unknown job, sensor or run |
| 4 | validation failed | a job file did not parse or broke a rule; the diagnostics name every finding |
| 5 | the job failed, and paceq was waiting for it | `paceq run` only: the step ran and told you so itself |
| 6 | busy | another process holds the state, or a concurrency limit was reached |
| 7 | timed out | |
| 8 | interrupted | SIGINT/SIGTERM while paceq was working |

## The difference between 1 and 5

This distinction is what makes moving a crontab to paceq safe.

Under cron, every failure looks the same: the job's exit code disappears into
a mail nobody reads, and cron itself reports success. With paceq:

- **Exit 1 means paceq failed.** The scheduler, the store, the runner - the
  infrastructure had a problem. It was not your job's fault, and retrying your
  job is not the first thing to do; read what paceq says
  (`paceq explain job <name>`), check [troubleshooting](../troubleshooting.md).
- **Exit 5 means the job failed.** The machinery worked end to end: the job
  started, ran, and exited non-zero. Your script's problem, your script's fix -
  with its log attached (`paceq logs <run>`).

A wrapper script around `paceq run` can therefore trust the exit code it sees:
`paceq run nightly || page someone` pages on *both* kinds of failure, while
`test $? -eq 5` reacts only to the job's own verdict.

For monitoring, `paceq status` folds the same information into its own exit
code contract: see the [status contract](status-contract.md).
