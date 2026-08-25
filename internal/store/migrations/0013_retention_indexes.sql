-- 0013_retention_indexes: the two foreign keys that point at runs without an
-- index of their own (issue #36).
--
-- Both edges are ON DELETE SET NULL, so every deleted run forces SQLite to
-- find the child rows that cite it. Without an index that search is a full
-- table scan PER DELETED ROW: retention's 500-row batch would scan runs and
-- triggers 500 times each, which is exactly the "writing that holds the lock
-- for seconds" this issue exists to prevent. With the indexes each lookup is
-- one seek, so the cascade cost follows the batch size, not the table size.

CREATE INDEX idx_runs_replay_of ON runs (replay_of)
  WHERE replay_of IS NOT NULL;

CREATE INDEX idx_triggers_run_id ON triggers (run_id)
  WHERE run_id IS NOT NULL;
