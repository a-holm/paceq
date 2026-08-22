-- 0006_lease_events: the audit trail of role leases.
--
-- A role lease changes hands three ways that matter later: someone took it
-- from empty, someone lost it to a live rival, and someone took it over from
-- a holder whose time had run out. The leases table itself only ever shows
-- the present: one row per role, overwritten in place by every renewal. An
-- operator asking "who was leader between the crash and the restart" needs
-- the moments, not the present, so each transition lands here as its own row
-- with a distinct reason code from the closed catalogue.
--
-- The rows are written by whoever holds the loop (internal/leases), one small
-- write per transition and none while leadership is steady. Nothing reads
-- them on a hot path; explain surfaces them when a human asks.
--
-- AUTOINCREMENT rather than a plain rowid, same rule as run_events: retention
-- may delete old rows, and a reused id would let two different moments share
-- an identity in anything that already read one of them.
CREATE TABLE lease_events (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  at          INTEGER NOT NULL,
  lease       TEXT    NOT NULL,
  holder      TEXT    NOT NULL,
  epoch       INTEGER NOT NULL CHECK (epoch > 0),
  reason_code TEXT    NOT NULL,
  detail_json TEXT    NOT NULL DEFAULT '{}'
) STRICT;

CREATE INDEX idx_lease_events_lease ON lease_events (lease, id);
