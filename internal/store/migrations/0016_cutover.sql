-- 0016_cutover: the operator actions that switch a machine from cron to
-- paceq. A cutover edits the one file in the whole product paceq writes to
-- outside its own state directory, so every line it touches is recorded
-- with who and when, the same audit quality the run_events table gives a
-- run (06 section 2). The trail answers the only question that matters
-- after a surprise: what exactly did paceq do to my crontab, and when.
--
-- The conventions hold as stated in 0001_init: STRICT, unix milliseconds
-- UTC, status-like columns TEXT with a CHECK. One row per line changed;
-- a cutover of three lines is three rows, because the questions come one
-- line at a time.

CREATE TABLE cutover_events (
  id          TEXT    PRIMARY KEY,
  -- 'cutover' comments a line out, 'rollback' puts it back. A rollback
  -- --from is one row per restore it performed, or a single row with
  -- line_number 0 when it restored a whole backup file.
  action      TEXT    NOT NULL CHECK (action IN ('cutover', 'rollback')),
  -- The paceq job the line belonged to, from the marker. A whole-file
  -- restore carries the empty string: no one line, no one job.
  job_name    TEXT    NOT NULL,
  -- One-based line number in the crontab as it stood before the change.
  line_number INTEGER NOT NULL CHECK (line_number >= 0),
  -- The line verbatim: the commented-out line for a cutover, the restored
  -- line for a rollback.
  line_text   TEXT    NOT NULL,
  actor       TEXT    NOT NULL,
  -- The backup that was written before the change, so the trail can walk
  -- backwards through the whole history without leaving the table.
  backup_path TEXT    NOT NULL,
  -- A forced cutover overrode the fence that wants one successful run
  -- first. The fact survives here even when the report's warning scrolled
  -- away.
  forced      INTEGER NOT NULL CHECK (forced IN (0, 1)),
  created_at  INTEGER NOT NULL
) STRICT;

CREATE INDEX idx_cutover_events_time ON cutover_events (created_at DESC);
CREATE INDEX idx_cutover_events_job  ON cutover_events (job_name, created_at DESC);
