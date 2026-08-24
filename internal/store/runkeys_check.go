package store

import (
	"context"
	"fmt"
)

// DoubleCompletedRunKeys names every dedup key that two or more succeeded
// runs share. It is the explicit form of the crash suite's fourth invariant
// (issue #20): at-least-once execution may duplicate work, never duplicate a
// completed run behind one key. Fsck does not carry this check, because the
// invariant is about the relationship between the run_keys decision table and
// the runs it produced, and the harness asserts it directly after every
// crash.
//
// A key belongs to exactly one decision, so one succeeded run is the most a
// healthy database can hold behind it. Failed or cancelled runs do not
// count: an operator may legitimately see a failed attempt and rerun under a
// new decision, and a reopened run stays one row.
//
// Empty keys are skipped. A manual run carries no key at all, and an empty
// string column would otherwise fold every such run into one group.
func (s *Store) DoubleCompletedRunKeys(ctx context.Context) ([]string, error) {
	rows, err := s.w.QueryContext(ctx, `SELECT run_key FROM runs
WHERE run_key <> '' AND state = 'succeeded'
GROUP BY run_key HAVING COUNT(*) > 1
ORDER BY run_key`)
	if err != nil {
		return nil, fmt.Errorf("sweep for doubled run keys: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, fmt.Errorf("sweep for doubled run keys: %w", err)
		}
		out = append(out, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sweep for doubled run keys: %w", err)
	}
	return out, rows.Close()
}
