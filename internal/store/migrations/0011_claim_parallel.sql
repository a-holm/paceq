-- 0012_claim_parallel: the M4-02 parallel budget.
--
-- max_parallel is how many steps of one run may run at the same time. The
-- value lives on the run because the claim predicate enforces the cap inside
-- the same BEGIN IMMEDIATE transaction that picks the step: the COUNT variant
-- keeps the correctness in the database where the rest of the write model
-- keeps its (07 section 4.2), so a lost counter in an executor can never let a
-- run exceed its budget.
--
-- Additive and nullable: a run that predates the column carries the default,
-- four steps at once, and the executor that claims reads the number back with
-- the claim. Nothing that wrote a run before this migration changes meaning.
ALTER TABLE runs ADD COLUMN max_parallel INTEGER NOT NULL DEFAULT 4
  CHECK (max_parallel >= 1);