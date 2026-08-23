-- 0009_sensor_commit: the cursor_version CAS guard for the atomic sensor
-- commit. CommitSensorTick refuses to advance a sensor past a cursor_version
-- it did not read: a late commit from an evaluation that was already overtaken
-- (lease lost, restart took over) bumps the version again and is detected by
-- the WHERE clause matching zero rows. The column exists so the check can be
-- one UPDATE, never a read-then-write (11 section 5.3).

ALTER TABLE sensors ADD COLUMN cursor_version INTEGER NOT NULL DEFAULT 0;