-- 0018_integrity.sql - the fsck sweep's indexes and the integrity event log
-- (M6-06, issue #49).
--
-- Two concerns, one migration, because they land together: the sweep that
-- writes integrity events is the same sweep whose queries the indexes serve.
--
-- The indexes make every invariant query index-supported: the acceptance
-- criterion is an EXPLAIN QUERY PLAN per invariant with no SCAN TABLE runs,
-- so a sweep over a database of a million ticks and a hundred thousand runs
-- stays inside its five second per-statement budget. Three of them are
-- covering or partial indexes that exist for the sweep alone; their columns
-- are exactly the columns the check reads, nothing more.
--
-- integrity_events is the systemhendelser side: one row per invariant with
-- findings per sweep, so "the database is inconsistent" is a fact with a
-- history (what broke, how bad, when it was seen), not a log line nobody
-- keeps. It is standalone, like the outbox: no foreign keys, so it reads
-- even when the rows it talks about have been retained away.

-- I3: the dedup gate's promise, queryable. Retention prunes run_keys by age;
-- this index lets the sweep group what remains without touching the runs
-- table body.
CREATE INDEX idx_runs_run_key ON runs (job_name, run_key);

-- I13: the stamp monotonicity check reads four columns of every run. The
-- covering index turns that read into an index-only scan.
CREATE INDEX idx_runs_fsck_stamps ON runs (created_at, started_at, finished_at, id);

-- I11: the sweep reads every run's current fencing token to compare it with
-- the token history in its events. Two columns, index-only.
CREATE INDEX idx_runs_fsck_epoch ON runs (lease_epoch, id);

-- I14: queued rows held into the future without a defer reason. The partial
-- predicate is the check's own predicate, so the plan never leaves the index.
CREATE INDEX idx_runs_fsck_defer ON runs (id, available_at, created_at)
  WHERE state = 'queued' AND (defer_reason IS NULL OR defer_reason = '');

-- The reason rule's runs arm: terminal rows sitting without a usable code.
-- The schema CHECK refuses new ones; the sweep still looks, for rows written
-- before the constraint existed.
CREATE INDEX idx_runs_fsck_reason ON runs (id, reason_code)
  WHERE state IN ('succeeded', 'failed', 'cancelled')
    AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN');

-- One row per invariant with findings, per sweep that saw them.
--   severity: the catalogue grade at the time of the sweep.
--   violations: how many rows broke the invariant. Never zero: a sweep with
--     nothing to say writes nothing.
--   detail_json: the subjects, capped, for the report that reads history
--     without a database to re-sweep.
CREATE TABLE integrity_events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  at          INTEGER NOT NULL,
  invariant   TEXT    NOT NULL,
  severity    TEXT    NOT NULL CHECK (severity IN ('warning', 'serious', 'critical')),
  violations  INTEGER NOT NULL CHECK (violations > 0),
  detail_json TEXT    NOT NULL DEFAULT '{}'
) STRICT;

CREATE INDEX idx_integrity_events_at ON integrity_events (at);
