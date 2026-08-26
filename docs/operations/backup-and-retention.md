# Backup and retention

paceq's state is one SQLite database under `.paceq/` plus per-run log files
under `.paceq/logs/`. Retention bounds how much history is kept; backups are
how you survive everything else. This page covers both, and the one command
you must never run on a live database.

## What retention keeps

`paceq prune` applies the retention policies. Each kind of history has both
a time horizon and a keep-minimum, and deletion stops at whichever keeps
more:

| What | Time horizon | Keep minimum |
|---|---|---|
| Runs | **90 days** | newest **50 per job** |
| Ticks | **90 days** | newest **200 per source** |
| Skipped ticks | **7 days** | - |
| Dedup run_keys | **365 days** | - |
| Log date shards | **14 days** empty-or-stale shards go whole | - |
| Daemon sessions | **90 days** | newest **50** |

So a job that ran once 100 days ago still shows its one run (the keep
minimum wins), while a chatty job's millionth tick from March is gone (the
horizon wins).

Deletion is batched - 200 rows per transaction with a 50 ms pause - so the
write lock is held for milliseconds at a time and the daemon barely notices.
Estimate before you commit:

```bash
paceq prune --dry-run     # prints exactly what a real pass would delete
paceq prune               # applies it
```

The daemon's nightly maintenance runs the same pass by itself and returns up
to 2000 free pages a night through incremental vacuum; `prune` is the
on-demand version of the same machinery.

### The 365-day footnote that matters

Run keys outlive runs: 90 days of run history, 365 days of dedup memory.
A `run_key` that reappears after its key has been pruned is treated as new
and fires again. If your sources can legitimately re-present old events
after a year (restored archives, re-imported rows), either make the step
idempotent ([how](../guarantees.md)) or re-present them through a path that
expects dedup. The gate only remembers what it is still told to remember.

## Taking a backup

**Never `cp` the database file while anything can write it.** A copy taken
while the WAL holds un-checkpointed frames is not a backup, it is a
corruption lottery. The safe options, best first:

1. **Let the daemon's nightly backup do it.** Every night the janitor takes a
   consistent copy of the database with `VACUUM INTO` into
   `.paceq/backups/state-<timestamp>.db`, **verifies** the copy (quick_check
   nightly, full `integrity_check` weekly and whenever no deep check has ever
   passed), records the outcome in the database for `paceq doctor` to show,
   deletes an unverified copy rather than keeping it, and rotates the
   generations down to 14. An unverified copy is worse than none; this is
   the difference between a backup habit and a backup.
2. **Stop, then copy** - the boring, correct option:

   ```bash
   systemctl stop paceq          # or Ctrl-C the serve process
   cp -a .paceq /backup/paceq-$(date +%F)/
   systemctl start paceq
   ```

3. **SQLite's own online backup** while the daemon runs:

   ```bash
   sqlite3 .paceq/state.db ".backup '/backup/state-$(date +%F).db'"
   ```

   Consistent snapshot, no stop needed. Copy `.paceq/logs/` separately.

The nightly copy lives on the same filesystem as the database by default -
that protects against corruption and operator error, not against disk loss,
so ship copies off the machine too. And before every upgrade or schema
migration, take a fresh one: the daemon's pre-migration copy is a matter of
correctness, not a backup strategy - see [upgrading](upgrading.md).

Restore is the reverse: stop everything, put the database (and logs) back,
start again, then prove the world with `paceq fsck`.

## Shrinking the file

Deletes leave free pages behind; they do not shrink the file. Nightly
incremental vacuum handles ordinary upkeep. After very large deletions:

```bash
paceq db compact --i-know-this-blocks
```

A full VACUUM needs an exclusive lock and roughly twice the database in free
disk space, and blocks every writer until done - including the daemon. That
is why the flag asks you to mean it.

## Keeping evidence past retention

Retention deletes history; an export keeps proof:

```bash
paceq export run 01M0XREMEESCC64DYGJJ88AMF3
```

writes `<run id>.tar.gz`: the run, steps, dependencies, events and artifacts
from the database, the trigger and tick that caused it, the exact job version
it executed, every log attempt, and a `manifest.json` with each file's
sha256. Audit trails outlive the retention window because they were exported,
not because they were hoped for.

## What to monitor about all this

Disk usage of `.paceq/` and the freshness of your last verified backup are
operational facts like any other. `paceq doctor` reports the last backup
attempt, its status and its path; `/metrics` carries the run surfaces worth
watching - see [monitoring](monitoring.md) - and per-job
`expected_within` turns "the nightly report has gone quiet" into an alert
instead of a discovery.
