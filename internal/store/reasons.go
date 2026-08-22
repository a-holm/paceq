package store

import (
	"context"
	"fmt"
)

// UnexplainedReasonSQL is the audit query behind the reason code rule (06
// section 2.1): it lists every terminal run, step, tick and trigger that sits
// in the database without a usable reason code. "Without a usable code" means
// NULL, empty, or the literal UNKNOWN, which is the value a rotting catalogue
// reaches for; the catalogue itself holds no UNKNOWN code, so a stored one is
// always a bug.
//
// The schema CHECKs refuse the NULL and empty cases at write time. This query
// exists for the cases a CHECK cannot see: rows written before a constraint
// existed, and the UNKNOWN case, which no CHECK names. It returns zero rows on
// a healthy database, and testutil.AssertNoUnknownReasons runs it as the
// teardown assertion of every integration test from M1 on. It later becomes
// part of `paceq fsck` (M1-12).
const UnexplainedReasonSQL = `SELECT 'run', id FROM runs
 WHERE state IN ('succeeded', 'failed', 'cancelled')
   AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')
UNION ALL
SELECT 'step', run_id || '/' || name FROM steps
 WHERE state IN ('succeeded', 'failed', 'skipped', 'cancelled')
   AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')
UNION ALL
SELECT 'tick', id FROM ticks
 WHERE outcome IN ('skipped', 'error', 'missed')
   AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')
UNION ALL
SELECT 'trigger', id FROM triggers
 WHERE outcome IN ('deduped', 'rejected')
   AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')`

// UnexplainedReason is one row the audit query returns: a terminal object
// sitting without a usable reason code.
type UnexplainedReason struct {
	// Kind is which table the row lives in: "run", "step", "tick" or
	// "trigger".
	Kind string
	// Key names the row the way an operator would: the id, or run and step
	// name together for a step.
	Key string
}

// UnexplainedReasons runs the audit query and returns every terminal run,
// step, tick and trigger stored without a usable reason code. A healthy
// database returns nothing, and `paceq fsck` (M1-12) will print exactly this
// list.
func (s *Store) UnexplainedReasons(ctx context.Context) ([]UnexplainedReason, error) {
	rs, err := s.w.QueryContext(ctx, UnexplainedReasonSQL)
	if err != nil {
		return nil, fmt.Errorf("audit reason codes: %w", err)
	}
	defer rs.Close()
	var rows []UnexplainedReason
	for rs.Next() {
		var r UnexplainedReason
		if err := rs.Scan(&r.Kind, &r.Key); err != nil {
			return nil, fmt.Errorf("audit reason codes: %w", err)
		}
		rows = append(rows, r)
	}
	return rows, rs.Err()
}
