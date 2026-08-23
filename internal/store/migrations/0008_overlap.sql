-- 0007_overlap: the per schedule overlap policy (#68). skip stands down when
-- the job's max_concurrent is already held; queue materialises the run anyway,
-- deferred into the future with a defer_reason. The CHECK keeps the set closed
-- the same way spring_forward and fall_back are kept.

ALTER TABLE schedules ADD COLUMN overlap TEXT NOT NULL DEFAULT 'skip'
  CHECK (overlap IN ('skip', 'queue'));
