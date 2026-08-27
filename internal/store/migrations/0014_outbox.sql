-- 0014_outbox.sql - the notification outbox (#29, M6-01).
--
-- SYNTESE section 4.2 fixes the table's name and core; the issue folds plan
-- 06 section 3.1's fields into it (subject, target) and derives row state
-- from delivered_at/attempts/failed_at instead of a state string. Three
-- indexes: uniqueness on the dedup key ("exactly one warning"), the pending
-- claim order, and the audit path by subject.
--
-- No foreign keys anywhere: the outbox is standalone so it can move to its
-- own database file later without rewriting history.

CREATE TABLE outbox (
  id           INTEGER PRIMARY KEY,
  topic        TEXT    NOT NULL,               -- run.failed | run.succeeded | job.sla_breached
  subject      TEXT    NOT NULL,               -- job or sensor name (filtering + group_by)
  target       TEXT    NOT NULL,               -- named notifier from configuration
  payload      TEXT    NOT NULL,               -- JSON, serialised once at insertion
  dedup_key    TEXT    NOT NULL,               -- topic|subject|target|<event key>
  created_at   INTEGER NOT NULL,
  available_at INTEGER NOT NULL,
  attempts     INTEGER NOT NULL DEFAULT 0,
  delivered_at INTEGER,
  failed_at    INTEGER,                        -- permanent give-up after max_attempts
  last_error   TEXT
) STRICT;

CREATE UNIQUE INDEX ux_outbox_dedup ON outbox(dedup_key);
CREATE INDEX idx_outbox_pending ON outbox(available_at, id)
  WHERE delivered_at IS NULL AND failed_at IS NULL;
CREATE INDEX idx_outbox_subject ON outbox(subject, created_at DESC);

-- Throttle/group_by bookkeeping. One row per (topic, target, group). The
-- opener_id names the outbox row that carries the window's single delivery;
-- suppressed counts the events collapsed into that delivery while it was in
-- flight or done. Keeping this beside outbox (not as columns on it) keeps the
-- locked outbox shape untouched and the counters mutable without touching
-- audit rows.
CREATE TABLE outbox_windows (
  topic      TEXT    NOT NULL,
  target     TEXT    NOT NULL,
  group_key  TEXT    NOT NULL,
  opener_id  INTEGER NOT NULL,
  opened_at  INTEGER NOT NULL,
  suppressed INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (topic, target, group_key)
) STRICT;

CREATE INDEX idx_outbox_windows_opener ON outbox_windows(opener_id);

-- One row per job currently past its expected_within deadline: episode state
-- for "one job.sla_breached per breach episode, not per check". A present row
-- means breaching; deleting it means the job succeeded again.
CREATE TABLE sla_episodes (
  job         TEXT    PRIMARY KEY,
  breached_at INTEGER NOT NULL
) STRICT;
