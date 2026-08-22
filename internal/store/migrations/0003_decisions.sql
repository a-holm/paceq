-- 0003_decisions: what the system decided, whether or not it did anything. The
-- absence of a run is not data, so every due evaluation writes a tick and every
-- tick writes the triggers it produced, including the ones it threw away. The
-- tables land in M1 and the engines that fill them land in M2 and M3, because a
-- reason code retrofitted after the fact is a guess.
--
-- schedules and sensors are declarations, ticks and triggers are events. The
-- line between them is a foreign key, not a convention.

CREATE TABLE schedules (
  id                TEXT    PRIMARY KEY,
  job_name          TEXT    NOT NULL REFERENCES jobs (name) ON DELETE CASCADE,
  name              TEXT    NOT NULL,
  kind              TEXT    NOT NULL CHECK (kind IN ('cron', 'interval')),
  expr              TEXT    NOT NULL,
  -- The one place a timezone is stored, because a cron expression has to be
  -- interpreted. Everything else is ordered, and ordering needs no timezone.
  timezone          TEXT    NOT NULL DEFAULT 'UTC',
  spring_forward    TEXT    NOT NULL DEFAULT 'skip'
                    CHECK (spring_forward IN ('skip', 'shift')),
  fall_back         TEXT    NOT NULL DEFAULT 'first'
                    CHECK (fall_back IN ('first', 'both')),
  catchup           TEXT    NOT NULL DEFAULT 'skip'
                    CHECK (catchup IN ('skip', 'last', 'all')),
  catchup_limit     INTEGER NOT NULL DEFAULT 10 CHECK (catchup_limit >= 0),
  catchup_window_ms INTEGER NOT NULL DEFAULT 86400000,
  paused            INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  last_tick_at      INTEGER,
  next_tick_at      INTEGER NOT NULL,
  created_at        INTEGER NOT NULL,
  updated_at        INTEGER NOT NULL,
  UNIQUE (job_name, name)
) STRICT;

-- Partial: the scheduler asks what is due, and a paused schedule can never be.
-- The query stays a range scan over a handful of rows however many paused
-- schedules the database holds.
CREATE INDEX idx_schedules_due ON schedules (next_tick_at) WHERE paused = 0;

CREATE TABLE sensors (
  name                  TEXT    PRIMARY KEY,
  job_name              TEXT    NOT NULL REFERENCES jobs (name) ON DELETE CASCADE,
  -- One adapter: a subprocess that writes JSON to stdout. A second kind is a
  -- migration and a new evaluator, not a flag somebody sets by accident.
  kind                  TEXT    NOT NULL DEFAULT 'exec' CHECK (kind = 'exec'),
  exec_json             TEXT    NOT NULL,
  interval_ms           INTEGER NOT NULL CHECK (interval_ms >= 1000),
  min_interval_ms       INTEGER NOT NULL DEFAULT 1000,
  timeout_ms            INTEGER NOT NULL DEFAULT 30000 CHECK (timeout_ms > 0),
  max_triggers_per_tick INTEGER NOT NULL DEFAULT 100 CHECK (max_triggers_per_tick > 0),
  paused                INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  paused_reason         TEXT,
  cursor                TEXT,
  cursor_updated_at     INTEGER,
  -- Bumped when an operator resets the cursor. It is part of the dedup key, so
  -- a reset makes every run key new again instead of replaying into a table
  -- that has already seen all of them and silently dropping the lot.
  dedup_epoch           INTEGER NOT NULL DEFAULT 0,
  consecutive_failures  INTEGER NOT NULL DEFAULT 0,
  next_eval_at          INTEGER NOT NULL,
  created_at            INTEGER NOT NULL,
  updated_at            INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_sensors_due ON sensors (next_eval_at) WHERE paused = 0;

-- One row per evaluation that was due. A loop waking up and finding nothing due
-- is not a tick and writes nothing, or the history drowns in its own heartbeat.
--
-- UNIQUE (source_kind, source_name, scheduled_for) carries two semantics with
-- one constraint. A schedule sets scheduled_for and gets exactly one tick per
-- logical slot, so double firing is structurally impossible even with two
-- evaluators racing. A sensor leaves it NULL, and NULL is distinct from NULL in
-- a unique index, so it may tick without limit.
CREATE TABLE ticks (
  id                TEXT    PRIMARY KEY,
  source_kind       TEXT    NOT NULL
                    CHECK (source_kind IN ('schedule', 'sensor', 'manual')),
  source_name       TEXT    NOT NULL,
  scheduled_for     INTEGER,
  started_at        INTEGER NOT NULL,
  -- Repeated identical skips coalesce onto one row: last_started_at moves and
  -- repeat_count grows, so a sensor that finds nothing for a day is one legible
  -- row rather than 2880 of them.
  last_started_at   INTEGER NOT NULL,
  finished_at       INTEGER,
  duration_ms       INTEGER,
  repeat_count      INTEGER NOT NULL DEFAULT 1 CHECK (repeat_count >= 1),
  outcome           TEXT    NOT NULL DEFAULT 'running'
                    CHECK (outcome IN ('running', 'triggered', 'skipped', 'error', 'missed')),
  reason_code       TEXT,
  reason_text       TEXT,
  reason_data       TEXT,
  trigger_count     INTEGER NOT NULL DEFAULT 0,
  deduped_count     INTEGER NOT NULL DEFAULT 0,
  cursor_before     TEXT,
  cursor_after      TEXT,
  -- Not a foreign key on purpose: sessions are swept on their own horizon, and
  -- a tick has to keep saying which process decided it after that sweep.
  daemon_session_id TEXT,
  UNIQUE (source_kind, source_name, scheduled_for),
  -- An outcome nobody acted on always says why. Explain has no other source.
  CHECK (outcome IN ('running', 'triggered') OR reason_code IS NOT NULL)
) STRICT;

CREATE INDEX idx_ticks_source ON ticks (source_kind, source_name, started_at DESC);
CREATE INDEX idx_ticks_bad    ON ticks (started_at DESC) WHERE outcome IN ('error', 'missed');
CREATE INDEX idx_ticks_age    ON ticks (started_at);

CREATE TABLE triggers (
  id          TEXT    PRIMARY KEY,
  tick_id     TEXT    NOT NULL REFERENCES ticks (id) ON DELETE CASCADE,
  job_name    TEXT    NOT NULL REFERENCES jobs (name) ON DELETE CASCADE,
  run_key     TEXT,
  params_json TEXT    NOT NULL DEFAULT '{}',
  created_at  INTEGER NOT NULL,
  outcome     TEXT    NOT NULL CHECK (outcome IN ('accepted', 'deduped', 'rejected')),
  reason_code TEXT,
  reason_text TEXT,
  -- On a deduped trigger this points at the original run, which is the answer
  -- to the only question anyone asks about a deduped trigger.
  run_id      TEXT    REFERENCES runs (id) ON DELETE SET NULL,
  CHECK (outcome = 'accepted' OR reason_code IS NOT NULL)
) STRICT;

CREATE INDEX idx_triggers_tick ON triggers (tick_id);
CREATE INDEX idx_triggers_job  ON triggers (job_name, created_at DESC);

-- The long lived dedup table. It outlives retention of triggers and runs,
-- because deleting a key is what lets an old trigger fire a second time, and it
-- holds no foreign key for the same reason.
--
-- epoch is the sensor's dedup_epoch, and 0 for schedules and manual runs. Its
-- presence in the key is what makes a cursor reset safe.
CREATE TABLE run_keys (
  source_id     TEXT    NOT NULL,
  epoch         INTEGER NOT NULL,
  run_key       TEXT    NOT NULL,
  first_seen_at INTEGER NOT NULL,
  run_id        TEXT,
  PRIMARY KEY (source_id, epoch, run_key)
) STRICT, WITHOUT ROWID;

CREATE INDEX idx_run_keys_age ON run_keys (first_seen_at);
