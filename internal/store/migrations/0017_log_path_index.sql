-- 0017_log_path_index: the one lookup the disk-guard's shard pruning does
-- (issue #44).
--
-- Clearing steps.log_path after a date shard is removed must never scan the
-- steps table: the write that removes a shard happens under disk pressure,
-- which is exactly when the write lock must stay short. The partial index
-- keeps the UPDATE at one seek per affected row and makes the "no step names
-- a file that is gone" rule cheap enough to hold every time.

CREATE INDEX idx_steps_log_path ON steps (log_path)
  WHERE log_path IS NOT NULL;
