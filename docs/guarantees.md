# Guarantees and non-guarantees

This document is the semantic contract for paceq. Honesty about guarantees is a feature, not a disclaimer (plans: 00 §3.8). It is written before the engine exists, so read it as what the implementation must satisfy: each guarantee is a property a test can falsify, and each invariant is a SQL query that must return zero rows.

The non-guarantees carry the same weight as the guarantees. A promise we deliberately withhold is part of the contract too.

## Guarantees

Numbered G1 to G10 (plans: 02 §4.1).

| Id | Guarantee |
|---|---|
| G1 | **Durable intent, within the fsync policy.** When a command returns OK, the decision is committed to the write-ahead log. Nothing is buffered in memory waiting to be written. The one window where a committed transaction can still be lost is stated in full under "What `synchronous=NORMAL` means" below, and it is a window that costs re-evaluation, never a lost decision. |
| G2 | **At most one run per trigger identity.** For any `(source, epoch, run_key)` there is at most one run, enforced by a UNIQUE index in the database, not by application logic (plans: 00 §3.22). Combined with a trigger that is retried until it succeeds, that yields exactly one run for that identity. |
| G3 | **At-least-once start of a step.** A step that reaches a terminal state has been executed at least once. In the crash window between spawning the process and committing its result, a step can be executed more than once. |
| G4 | **No lost sensor triggers.** A sensor cursor never advances unless every trigger derived from that interval is committed in the same transaction. A crash gives replay, never loss. |
| G5 | **Progress.** A run in a non-terminal state will, given that the daemon restarts, either make progress or reach a terminal state in finite time. No run hangs forever. Expired leases are reclaimed by the reaper, which bumps the epoch and requeues or fails the run with a reason code. |
| G6 | **At most one concurrent execution per step.** Enforced by `flock` on the state directory, which is absolute on a single node, and by lease plus fencing in general, given that the lease TTL exceeds the longest stall. |
| G7 | **Monotone fencing.** A write from an owner holding an outdated `lease_epoch` updates zero rows. The stale owner self-fences: it kills its process group and releases everything. |
| G8 | **Cancellation is monotone.** A cancellation request can be set and never cleared. A cancelled run never starts a new attempt. |
| G9 | **Deterministic scheduling.** The set of `scheduled_for` instants for a given expression, timezone and interval does not depend on when the scheduler actually ran. Two daemons with the same configuration and different uptime produce identical tick sets. |
| G10 | **Complete audit trail.** Every state transition has exactly one `run_events` row, committed in the same transaction as the transition. Every terminal state carries a `reason_code`, and that is enforced in CI. |

## Non-guarantees

These are promises paceq does not make (plans: 02 §4.2, 00 §4.11).

- **No exactly-once side effects.** paceq guarantees that a step starts at least once. It cannot guarantee that the effects of a step happen exactly once, because it does not control those effects. Make your step idempotent. Every step receives a stable idempotency key that survives retries and duplicate deliveries, so a step that needs deduplication has a key to deduplicate on (plans: 11 §5.4). exactly-once is never promised anywhere else in this project, and a feature request asking for it is refused with a link to this line.
- **No real-time precision.** The guarantee is "not before `scheduled_for`". There is a best-effort tick lag metric, and no upper bound on delay under load.
- **No at-least-once completion.** A run interrupted by a crash is requeued or terminated with a reason; it is not silently restarted from the beginning as if nothing happened. At-least-once applies to starting a step, not to finishing one.
- **No global ordering** between independent jobs. Order exists inside a run through step dependencies, and nowhere else.
- **No protection against disk corruption** beyond what SQLite itself provides. Take backups. The daemon takes a consistent copy before each schema migration, but that is not a backup strategy.
- **No guarantee that the last log lines before a power cut survive.** Logs are files, written with buffered I/O and never fsynced per line. Each line carries a sequence number, so loss and truncation are detectable rather than invisible (plans: 00 §3.9).
- **No sub-second scheduling and no queue semantics.** paceq is a scheduler. See [SCOPE.md](../SCOPE.md).

## What `synchronous=NORMAL` means

paceq runs SQLite in WAL mode with `synchronous=NORMAL` as the default (plans: 00 §3.8, 00 §3.11). This is a deliberate choice, and it is only defensible because this section states what it costs.

With `NORMAL`, SQLite does not fsync the write-ahead log on every commit. The concrete consequence:

- A **process crash cannot lose a committed transaction.** The data is in the operating system's page cache, and the kernel outlives the process. Every crash-recovery scenario in the design that involves the daemon dying is unaffected.
- A **power cut or an OS crash can roll back the most recently committed transactions.** The bytes never reached the disk.

A rolled-back transaction is consistent, not corrupt, and that is what makes the default safe:

1. A tick, the triggers it produced and the sensor cursor are committed atomically, in one transaction. A rollback cannot leave an advanced cursor with missing triggers, or triggers with a stale cursor. Either all of it is there or none of it is.
2. A rollback therefore means re-evaluation, not loss. The sensor is asked again from the older cursor.
3. Re-evaluation deduplicates. The same trigger identity produces the same `run_key`, and the UNIQUE index in G2 rejects the second one, so a replayed trigger does not become a second run.
4. Schedule ticks are deterministic (G9). A schedule `run_key` is derived from the schedule name and the scheduled instant, so a lost tick is re-materialized identically on the next pass.

The whole system is at-least-once with reconciliation. `NORMAL` costs at most a repeat of work the system is already built to repeat, and it buys a commit rate 10 to 100 times higher than `FULL`.

`FULL` is available as a configuration key for anyone who wants to pay for it. It fsyncs on every commit and closes the power-cut window. It does not change any guarantee above; it narrows the window in which G1 falls back on re-evaluation.

## Cursor and run_key are two notions with two reset flags

A sensor runs a dedup identity that is a triple: `(source_id, epoch, run_key)`. The `run_key` is the natural identity of what changed (the file, the row); the epoch is the operator's way to say "start over" without deleting anything. This is what G2's uniqueness cites, and it is what makes the "reset gives replay" promise real: 50 new files give 50 runs, an immediate rerun gives 0 runs, and a reset gives 50 new runs.

The cursor and the dedup key must therefore never be coupled implicitly (10 `§5` F4c). Moving the cursor is not a reset, and a reset is not merely a cursor move. Each command moves exactly the flags it names:

| Command | cursor | dedup_epoch | run_keys | Effect |
|---|---|---|---|---|
| `sensors reset <s>` | NULL | +1 | kept | Full replay from the start, against a fresh epoch |
| `sensors reset <s> --cursor <v>` | v | +1 | kept | Replay from a chosen point |
| `sensors reset <s> --forget-run-keys` | NULL | +1 | deleted for that sensor | Full replay plus a clean dedup table |
| `sensors cursor set <s> <v>` | v | unchanged | kept | Spool the cursor without replay; old run keys still dedup |

The epoch bump is `O(1)` and reversible in practice (the old rows stay), while deleting run keys is `O(n)` and irreversible, so reset never deletes unless you ask. This is why a reset bumps the epoch rather than clearing the table: it preserves the dedup history and only makes old keys irrelevant.

`run_keys` keeps its rows for 365 days, longer than runs (90 days). A `run_key` that reappears after the retention window is treated as new and fires a new run. That is a documented consequence, not a silent one: the gate only remembers what it is still told to remember.

## Invariants

Machine-checkable properties, enforced by `paceq fsck` as SQL and asserted continuously in the property tests (plans: 02 §4.3, 00 §4.6). A violation is recorded as an integrity event, not swallowed.

| Id | Invariant |
|---|---|
| I1 | No run in `running` without a lease owner and a lease that has not expired. |
| I2 | No run in a terminal state has a step in `running`. |
| I3 | The trigger identity is unique: `(source, epoch, run_key)` has at most one run. Enforced by the database and verified anyway. |
| I4 | At most one active attempt per `(run_id, step_name)`. |
| I5 | Attempt numbers are dense from 1 with no gaps, per `(run_id, step_name)`. |
| I6 | At most one tick per `(source_kind, source_name, scheduled_for)`. Enforced by the database. |
| I7 | Every sensor cursor value has a matching finished tick whose `cursor_after` equals it. A cursor cannot have advanced without a committed tick. |
| I8 | A step in `running` has every step it `needs` in `succeeded`. |
| I9 | The step graph of a run is acyclic and every dependency names a step that exists in that run. |
| I10 | A run in `succeeded` has no failed step. A run in `failed` has at least one failed step or a run-level failure reason. |
| I11 | `lease_epoch` is non-decreasing, verified against the `run_events` history. |
| I12 | The number of active runs per `(job, concurrency_key)` never exceeds the job's `max_concurrent`. |
| I13 | Timestamps are ordered: finished at or after started, started at or after created, and every timestamp is greater than zero. |
| I14 | A deferred run has a `defer_reason`. A skipped tick has a reason code. There is no state in the system whose explanation is "unknown". |
| I15 | The `run_events` chain for a run is contiguous: the number of state-change rows matches the transitions derived from the from-state and to-state chain. |
| I16 | No active lease is held by a daemon session that has stopped or gone stale. |
