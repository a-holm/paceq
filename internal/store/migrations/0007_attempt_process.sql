-- 0007_attempt_process: the persisted identity of a running attempt's
-- process. Startup reconciliation (#62) sweeps /proc for orphaned process
-- groups carrying PACEQ_RUN_ID, and the sweep must never signal an innocent
-- process on a hunch. The baseline is what turns "this pid exists" into
-- "this pid is the attempt we started": field 22 of /proc/<pid>/stat, the
-- kernel's start time in clock ticks, saved beside the pid at spawn.
--
-- A recycled pid would carry a different start ticks value than the one on
-- file, so the mismatch is the refusal. The columns stay after the attempt
-- ends on purpose: a dead attempt's baseline is exactly what the sweep
-- verifies a survivor against.
--
-- Additive and nullable, like 0005 before it: every row written before this
-- migration carries NULL and reads as "no process was recorded", which is
-- simply a step the sweep has no opinion about.

ALTER TABLE steps ADD COLUMN pid INTEGER;
ALTER TABLE steps ADD COLUMN pid_start_ticks INTEGER;
