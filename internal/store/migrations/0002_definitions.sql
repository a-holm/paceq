-- 0002_definitions: the declarative side. A job is what an operator names; a
-- job version is one immutable snapshot of the spec it was loaded from, and
-- every run points at exactly the version it ran. Editing a spec therefore
-- cannot rewrite what an old run did.
--
-- The conventions hold as stated in 0001_init and in doc.go: every table is
-- STRICT, every time column is INTEGER unix milliseconds UTC, every status is
-- TEXT with a CHECK.
--
-- Two departures from plan 07 section 3.2, both decided in issue #50. Ids are
-- ULIDs rather than an integer key beside a public one: one id world is simpler
-- and the volume is low. Step definitions are not a table here, they live in
-- spec_json and are frozen per run in 0004_execution, which is what makes the
-- claim predicate stateless.

CREATE TABLE jobs (
  name               TEXT    PRIMARY KEY,
  -- Deferred because the two tables point at each other: a job's first version
  -- cannot exist before the job row, and the job cannot name it before it does.
  current_version_id TEXT    REFERENCES job_versions (id) DEFERRABLE INITIALLY DEFERRED,
  description        TEXT    NOT NULL DEFAULT '',
  paused             INTEGER NOT NULL DEFAULT 0 CHECK (paused IN (0, 1)),
  max_concurrent     INTEGER NOT NULL DEFAULT 1 CHECK (max_concurrent > 0),
  source_path        TEXT,
  created_at         INTEGER NOT NULL,
  updated_at         INTEGER NOT NULL
) STRICT;

-- Immutable: a new spec is a new row, never an update. UNIQUE (job_name,
-- spec_hash) is the whole of idempotent reload. Loading the same file twice
-- conflicts instead of inventing a version nobody wrote, so no application
-- check has to decide whether the spec changed.
CREATE TABLE job_versions (
  id          TEXT    PRIMARY KEY,
  job_name    TEXT    NOT NULL REFERENCES jobs (name) ON DELETE CASCADE,
  version     INTEGER NOT NULL,
  spec_hash   TEXT    NOT NULL,
  spec_json   TEXT    NOT NULL,
  source_path TEXT,
  created_at  INTEGER NOT NULL,
  UNIQUE (job_name, version),
  UNIQUE (job_name, spec_hash)
) STRICT;

-- Descending, because the only question asked of this table at runtime is which
-- version is the newest one for a job.
CREATE INDEX idx_job_versions_job ON job_versions (job_name, version DESC);
