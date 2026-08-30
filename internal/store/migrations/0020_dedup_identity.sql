-- 0020_dedup_identity.sql - the I3 sweep's index, on the table that owns the
-- dedup identity (issue #200).
--
-- I3 asks whether any run is claimed by more than one dedup identity. The
-- identity is (source_id, epoch, run_key), which is the run_keys primary key,
-- so the sweep groups run_keys by run_id. A secondary index on a WITHOUT
-- ROWID table carries the primary key columns as its row reference, so this
-- one index answers the whole statement without reading the table body.
CREATE INDEX idx_run_keys_run_id ON run_keys (run_id) WHERE run_id IS NOT NULL;

-- idx_runs_run_key served the previous I3 statement, which grouped runs by
-- (job_name, run_key). Two runs share that pair every time a sensor reset
-- replays a key into a new epoch and every time a pruned run key fires again,
-- both documented behaviours, so the statement is gone and no reader is left.
DROP INDEX idx_runs_run_key;
