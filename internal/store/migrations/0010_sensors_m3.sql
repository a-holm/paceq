-- 0010_sensors_m3: completes the sensors table for M3-01 and opens the sensor
-- lifecycle journal.
--
-- The sensors table arrived in 0003_decisions carrying the evaluation columns
-- it needs from day one (cursor, dedup_epoch, consecutive_failures, the due
-- index). Here it gains the M3 definition and state columns that apply
-- materialises, and the journal a removed sensor writes to. Nothing in this
-- file touches a column that already exists: every ALTER is additive.

ALTER TABLE sensors ADD COLUMN last_error TEXT;
ALTER TABLE sensors ADD COLUMN last_eval_at INTEGER;
ALTER TABLE sensors ADD COLUMN breaker_opened_at INTEGER;
ALTER TABLE sensors ADD COLUMN spec_json TEXT NOT NULL DEFAULT '{}';

-- Invariant: a paused sensor always says why. The application refuses a pause
-- without a reason; this is the second lock so a stray write cannot leave a
-- paused row nobody can explain (07 section 7).
CREATE TRIGGER sensors_paused_needs_reason
BEFORE UPDATE OF paused ON sensors
WHEN NEW.paused = 1 AND (NEW.paused_reason IS NULL OR NEW.paused_reason = '')
BEGIN
  SELECT RAISE(ABORT, 'paused sensor uten paused_reason');
END;

-- The definition-change analogue of run_events: one row per lifecycle change.
-- A removed sensor is recorded here because nothing else in the schema owns
-- the act of going away, and the run_keys it seeded are kept so a sensor that
-- comes back under the same name does not replay a burst of old keys.
CREATE TABLE sensor_events (
  id          TEXT    PRIMARY KEY,
  sensor_name TEXT    NOT NULL,
  job_name    TEXT    NOT NULL,
  kind        TEXT    NOT NULL CHECK (kind IN ('removed', 'created', 'spec_changed')),
  at          INTEGER NOT NULL,
  detail_json TEXT
) STRICT;

CREATE INDEX idx_sensor_events_sensor ON sensor_events (sensor_name, at DESC);