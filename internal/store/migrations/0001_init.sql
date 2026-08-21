-- 0001_init: the infrastructure tables. They belong to no single feature, and
-- everything else in the schema rests on them: database identity, role
-- ownership and gap detection.
--
-- Conventions, stated once for the whole schema and documented in doc.go: every
-- table is STRICT, every time column is INTEGER unix milliseconds UTC, every
-- status is TEXT with a CHECK.
--
-- schema_migrations and schema_migration_lock are absent on purpose. The
-- migration engine creates them before it applies this file, because the ledger
-- and the lock have to exist before the first migration can record itself.

-- Database identity: created_at, boot_id and the version that created the
-- schema. Key-value with a text key, so the rowid would be pure overhead.
CREATE TABLE meta (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
) STRICT, WITHOUT ROWID;

-- Role leases: one row per singleton role, 'scheduler', 'sensor', 'reaper'.
-- epoch is the fencing token, monotone per name, and every write made under a
-- lease carries it so a holder whose lease expired cannot corrupt state.
--
-- The migrator's own lock is not here. It has to exist before this migration
-- runs, so the engine owns it.
CREATE TABLE leases (
  name        TEXT    PRIMARY KEY,
  holder      TEXT    NOT NULL,
  epoch       INTEGER NOT NULL CHECK (epoch > 0),
  acquired_at INTEGER NOT NULL,
  expires_at  INTEGER NOT NULL
) STRICT, WITHOUT ROWID;

-- One row per daemon start, with a heartbeat. This is the evidence gap
-- detection reads: without a session history there is no way to tell later that
-- the daemon was down, so the table exists from the first schema.
CREATE TABLE daemon_sessions (
  id           TEXT    PRIMARY KEY,
  version      TEXT    NOT NULL,
  boot_id      TEXT,
  pid          INTEGER NOT NULL,
  started_at   INTEGER NOT NULL,
  last_seen_at INTEGER NOT NULL,
  stopped_at   INTEGER,
  stop_reason  TEXT CHECK (stop_reason IS NULL OR stop_reason IN ('clean', 'crash', 'replaced'))
) STRICT;

-- Partial index: the open session is the one row every start looks for, and the
-- index holds only the rows that can match.
CREATE INDEX daemon_sessions_open
  ON daemon_sessions (started_at DESC) WHERE stopped_at IS NULL;

-- Periods where the daemon provably made no decisions.
CREATE TABLE outages (
  id           INTEGER PRIMARY KEY,
  from_ts      INTEGER NOT NULL,
  to_ts        INTEGER NOT NULL,
  detected_at  INTEGER NOT NULL,
  kind         TEXT    NOT NULL CHECK (kind IN ('crash', 'clean', 'clock_jump', 'boot')),
  prev_session TEXT REFERENCES daemon_sessions (id),
  missed_ticks INTEGER NOT NULL DEFAULT 0 CHECK (missed_ticks >= 0),
  CHECK (to_ts >= from_ts)
) STRICT;

CREATE INDEX outages_by_time ON outages (from_ts DESC);
