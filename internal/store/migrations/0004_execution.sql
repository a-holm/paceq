-- 0004_execution: what actually ran. A run is one attempt at one job version,
-- its steps are frozen at materialisation and so are the edges between them, so
-- the claim predicate reads state and never the current spec.
--
-- Three rules the CHECKs carry, and internal/model carries the same ones. Both
-- ends enforce them because one end is always the one that has the bug.
--
--   * Deferred is not a state. A held back run is queued with available_at in
--     the future and a defer_reason that says why.
--   * A row that is finished says why it is finished. Terminal without a reason
--     code is how a reason catalogue rots into UNKNOWN inside a year.
--   * A run cannot finish before it starts.

CREATE TABLE runs (
  id                  TEXT    PRIMARY KEY,
  job_name            TEXT    NOT NULL REFERENCES jobs (name) ON DELETE CASCADE,
  -- No ON DELETE: a version a run points at may not be deleted. Versions are
  -- tiny and are never swept, and without one a replay means nothing.
  job_version_id      TEXT    NOT NULL REFERENCES job_versions (id),
  trigger_id          TEXT    REFERENCES triggers (id) ON DELETE SET NULL,
  origin              TEXT    NOT NULL,
  run_key             TEXT,
  state               TEXT    NOT NULL,
  available_at        INTEGER NOT NULL,
  defer_reason        TEXT,
  scheduled_for       INTEGER,
  params_json         TEXT    NOT NULL DEFAULT '{}',
  concurrency_key     TEXT,
  attempt             INTEGER NOT NULL DEFAULT 0,
  max_attempts        INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
  lease_owner         TEXT,
  -- The fencing token. Every result write proves the lease is still its own by
  -- carrying it, so a slow worker cannot overwrite what its replacement wrote.
  lease_epoch         INTEGER NOT NULL DEFAULT 0,
  lease_expires_at    INTEGER,
  heartbeat_at        INTEGER,
  cancel_requested_at INTEGER,
  cancel_reason       TEXT,
  crash_count         INTEGER NOT NULL DEFAULT 0,
  replay_of           TEXT    REFERENCES runs (id) ON DELETE SET NULL,
  reason_code         TEXT,
  reason_text         TEXT,
  reason_data         TEXT,
  error               TEXT,
  error_tail          TEXT,
  created_at          INTEGER NOT NULL,
  started_at          INTEGER,
  finished_at         INTEGER,
  duration_ms         INTEGER,
  updated_at          INTEGER NOT NULL,
  CHECK (origin IN ('schedule', 'sensor', 'manual', 'retry', 'replay', 'backfill')),
  CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
  CHECK (defer_reason IS NOT NULL OR available_at <= created_at OR state <> 'queued'),
  CHECK (state IN ('queued', 'running') OR reason_code IS NOT NULL),
  CHECK (finished_at IS NULL OR started_at IS NULL OR finished_at >= started_at)
) STRICT;

CREATE INDEX idx_runs_claim    ON runs (available_at, id) WHERE state = 'queued';
CREATE INDEX idx_runs_reaper   ON runs (lease_expires_at) WHERE state = 'running';
CREATE INDEX idx_runs_active   ON runs (job_name) WHERE state IN ('queued', 'running');
CREATE INDEX idx_runs_history  ON runs (job_name, id DESC);
CREATE INDEX idx_runs_finished ON runs (finished_at) WHERE finished_at IS NOT NULL;

-- The concurrency limit is this index. There is no application check to race
-- with it: a second active run on one key fails to insert.
CREATE UNIQUE INDEX ux_runs_conc_key ON runs (concurrency_key)
  WHERE concurrency_key IS NOT NULL AND state IN ('queued', 'running');

CREATE TABLE steps (
  run_id          TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  name            TEXT    NOT NULL,
  -- Position in the spec. M1 runs steps in this order; M4 reads step_deps and
  -- keeps idx only as the tie break between steps that are equally ready.
  idx             INTEGER NOT NULL,
  state           TEXT    NOT NULL,
  attempt         INTEGER NOT NULL DEFAULT 0,
  max_attempts    INTEGER NOT NULL DEFAULT 1 CHECK (max_attempts >= 1),
  next_attempt_at INTEGER,
  started_at      INTEGER,
  finished_at     INTEGER,
  duration_ms     INTEGER,
  exit_code       INTEGER,
  signal          TEXT,
  error           TEXT,
  reason_code     TEXT,
  reason_text     TEXT,
  reason_data     TEXT,
  -- The log itself is a file. Only where it is, how big it got and whether it
  -- was cut off belong in the database.
  log_path        TEXT,
  log_bytes       INTEGER NOT NULL DEFAULT 0,
  log_truncated   INTEGER NOT NULL DEFAULT 0 CHECK (log_truncated IN (0, 1)),
  error_tail      TEXT,
  PRIMARY KEY (run_id, name),
  CHECK (state IN ('pending', 'running', 'succeeded', 'failed', 'skipped', 'cancelled')),
  CHECK (state IN ('pending', 'running') OR reason_code IS NOT NULL)
) STRICT;

CREATE INDEX idx_steps_claimable ON steps (run_id, idx) WHERE state = 'pending';
CREATE INDEX idx_steps_retry     ON steps (next_attempt_at)
  WHERE state = 'pending' AND next_attempt_at IS NOT NULL;

-- The edges as they were when the run was materialised. Freezing them is what
-- keeps a spec edit from changing which steps an in flight run is waiting for.
CREATE TABLE step_deps (
  run_id     TEXT NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  step_name  TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  PRIMARY KEY (run_id, step_name, depends_on)
) STRICT, WITHOUT ROWID;

-- The upstream direction: which steps a finished step unblocks.
CREATE INDEX idx_step_deps_upstream ON step_deps (run_id, depends_on);

-- Append only, one row per state transition, written in the same transaction as
-- the transition itself. This is the backbone of explain: a transition that is
-- not here did not happen as far as the operator can ever tell.
--
-- AUTOINCREMENT rather than a plain rowid: retention deletes old runs, and a
-- reused id would make two different events share an identity in anything that
-- has already read one of them.
CREATE TABLE run_events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  run_id      TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  -- Steps are keyed by (run_id, name), so this is the name alone rather than a
  -- foreign key. An event about a step that no longer exists still reads.
  step_name   TEXT,
  at          INTEGER NOT NULL,
  kind        TEXT    NOT NULL,
  from_state  TEXT,
  to_state    TEXT,
  reason_code TEXT,
  actor       TEXT    NOT NULL DEFAULT 'system',
  detail_json TEXT    NOT NULL DEFAULT '{}'
) STRICT;

CREATE INDEX idx_run_events_run ON run_events (run_id, id);

CREATE TABLE artifacts (
  id         TEXT    PRIMARY KEY,
  run_id     TEXT    NOT NULL REFERENCES runs (id) ON DELETE CASCADE,
  step_name  TEXT,
  name       TEXT    NOT NULL,
  uri        TEXT    NOT NULL,
  size_bytes INTEGER,
  checksum   TEXT,
  meta_json  TEXT    NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL,
  UNIQUE (run_id, name)
) STRICT;
