-- 0005_engine: attribution for a cancellation request.
--
-- A cancellation is a request (02 section 5.8): the CLI writes
-- cancel_requested_at durably, and whoever observes it later effectuates the
-- stop. The run_events row for the cancellation carries an actor, and the
-- engine can only name the requester if the request stored who it was.
-- cancel_requested_at and cancel_reason landed with 0004; the person column
-- was missed there and every cancellation event said "system".
--
-- Additive and nullable: a run nobody asked to cancel carries NULL, exactly
-- like the two columns beside it.

ALTER TABLE runs ADD COLUMN cancel_requested_by TEXT;
