-- 0021_skipped_tick_age.sql - the age index skipped-tick retention reads
-- (issue #256).
--
-- Skip retention ages a row by last_started_at, because a coalesced row keeps
-- its first evaluation in started_at. idx_ticks_age is on started_at, so the
-- dry-run count lost its index and fell to a full scan of the highest-volume
-- table in the database. This index restores an index path and, because it
-- carries only the rows the predicate selects, answers the count without
-- touching the table at all.
--
-- Partial rather than plain: the predicate always pins outcome = 'skipped',
-- the index then costs nothing on a triggered or errored tick, and SQLite can
-- use it as a covering index. idx_ticks_age stays: PruneTicksBatch and its
-- estimate still walk started_at.
CREATE INDEX idx_ticks_skipped_age ON ticks (last_started_at)
  WHERE outcome = 'skipped';
