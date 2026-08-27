-- 0014_shadow: skyggemodus (#32). Shadow mode runs the whole scheduler
-- honestly - every due evaluation becomes a tick row with the decision it
-- would have made - and replaces only the last step: no run, no trigger,
-- no run key, no process.
-- +paceq rebuild

-- The rebuild: ticks.outcome gains 'shadow_triggered'. A would-have-run
-- evaluation claims its fire-time under the same UNIQUE gate as a real run
-- but stores that nothing executed; skip decisions keep their real reason
-- codes unchanged in both modes. SQLite cannot ALTER a CHECK, so the table
-- copies, and triggers references ticks so foreign keys have to be off.
--
-- Everything else here is additive:
--
--   * schedules.shadow marks a schedule as shadowed at row level, so the
--     loop reads one flag next to paused instead of consulting YAML it
--     cannot see. A global --shadow on serve stays a process property.
--   * shadow_observations holds cron starts observed from outside (journald
--     or a syslog file), the comparison side of `paceq shadow report`.

ALTER TABLE schedules ADD COLUMN shadow INTEGER NOT NULL DEFAULT 0
  CHECK (shadow IN (0, 1));

CREATE TABLE ticks_shadow (
  id                TEXT    PRIMARY KEY,
  source_kind       TEXT    NOT NULL
                    CHECK (source_kind IN ('schedule', 'sensor', 'manual')),
  source_name       TEXT    NOT NULL,
  scheduled_for     INTEGER,
  started_at        INTEGER NOT NULL,
  last_started_at   INTEGER NOT NULL,
  finished_at       INTEGER,
  duration_ms       INTEGER,
  repeat_count      INTEGER NOT NULL DEFAULT 1 CHECK (repeat_count >= 1),
  outcome           TEXT    NOT NULL DEFAULT 'running'
                    CHECK (outcome IN ('running', 'triggered', 'skipped',
                                       'error', 'missed', 'shadow_triggered')),
  reason_code       TEXT,
  reason_text       TEXT,
  reason_data       TEXT,
  trigger_count     INTEGER NOT NULL DEFAULT 0,
  deduped_count     INTEGER NOT NULL DEFAULT 0,
  cursor_before     TEXT,
  cursor_after      TEXT,
  daemon_session_id TEXT,
  UNIQUE (source_kind, source_name, scheduled_for),
  CHECK (outcome IN ('running', 'triggered', 'shadow_triggered')
         OR reason_code IS NOT NULL)
) STRICT;

INSERT INTO ticks_shadow
  (id, source_kind, source_name, scheduled_for, started_at, last_started_at,
   finished_at, duration_ms, repeat_count, outcome, reason_code, reason_text,
   reason_data, trigger_count, deduped_count, cursor_before, cursor_after,
   daemon_session_id)
SELECT id, source_kind, source_name, scheduled_for, started_at, last_started_at,
       finished_at, duration_ms, repeat_count, outcome, reason_code, reason_text,
       reason_data, trigger_count, deduped_count, cursor_before, cursor_after,
       daemon_session_id
  FROM ticks;

DROP TABLE ticks;
ALTER TABLE ticks_shadow RENAME TO ticks;

CREATE INDEX idx_ticks_source ON ticks (source_kind, source_name, started_at DESC);
CREATE INDEX idx_ticks_bad    ON ticks (started_at DESC) WHERE outcome IN ('error', 'missed');
CREATE INDEX idx_ticks_age    ON ticks (started_at);

CREATE TABLE shadow_observations (
  id          INTEGER PRIMARY KEY,
  observed_at INTEGER NOT NULL,
  source      TEXT    NOT NULL CHECK (source IN ('journald', 'file', 'manual')),
  raw         TEXT    NOT NULL,
  command     TEXT,
  cron_user   TEXT,
  job_name    TEXT,
  UNIQUE(source, observed_at, raw)
) STRICT;

CREATE INDEX idx_shadow_obs_job ON shadow_observations(job_name, observed_at);
