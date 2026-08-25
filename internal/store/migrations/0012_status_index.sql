-- 0012_status_index: the one index `paceq status` needs (issue #30).
--
-- Status answers "what did every job last do" by reading, per job, its newest
-- FINISHED run. The history index idx_runs_history (job_name, id DESC) finds
-- the newest run of any state; picking the newest finished one needs
-- finished_at in the ordering too. This partial index carries exactly that
-- shape, so the status join is one seek per job however long the history is:
--
--   CREATE INDEX runs_by_job_finished ...   -- the shape issue #30 prescribes
--
-- The id DESC tail breaks ties between runs that finished on the same
-- millisecond, which a frozen test clock produces constantly: without it the
-- answer would depend on rowid order and the goldens would flip. It also lets
-- SQLite satisfy ORDER BY finished_at DESC, id DESC straight from the index,
-- with no sort node in the query plan.

CREATE INDEX idx_runs_job_finished ON runs (job_name, finished_at DESC, id DESC)
  WHERE finished_at IS NOT NULL;
