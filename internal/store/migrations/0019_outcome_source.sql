-- 0019_outcome_source: where a step's verdict came from. Three values:
--
--   direct      the executor waited on the process and wrote what it saw;
--   spool       the verdict was read from the attempt's result spool file
--               (issue #39), which the child wrote before it exited — the
--               answer to crash window W8, where the executor died between
--               the child's exit and the verdict's commit;
--   reconciled  recovery assumed the outcome without a source: the executor
--               is gone, nothing was on file, and the only honest verdict is
--               the one that says so (STEP_FAILED_EXECUTOR_LOST).
--
-- The column exists so "why was this not run again?" has a stored answer
-- instead of a re-derived one (00-SYNTESE 4.7). It is additive and nullable:
-- every row written before this migration carries NULL, which reads as the
-- direct era — there was no shim, so the executor's own wait was the only
-- source there ever was.

ALTER TABLE steps ADD COLUMN outcome_source TEXT
  CHECK (outcome_source IN ('direct', 'spool', 'reconciled'));
