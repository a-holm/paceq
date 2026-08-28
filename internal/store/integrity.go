package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// The integrity event log (M6-06): what the fsck sweep found, when, and how
// bad it was. A violation that only prints is a rumour; a violation in the
// database is a fact with a history, and the metrics and health surfaces
// read their numbers from here instead of re-running the sweep.
//
// One row per invariant that had findings, per sweep that saw them. A sweep
// with nothing to say writes nothing, so the log's silence is meaningful:
// no rows at the head means the newest sweep was clean.

// integritySubjectCap is how many violating subjects one event row carries in
// its detail. A finding can name a million rows; the event names the first
// ones and the count, which is what "how bad is it" needs.
const integritySubjectCap = 5

// IntegrityFinding is one invariant's outcome from one sweep.
type IntegrityFinding struct {
	// Invariant is the catalogue ID: "I3", "reason".
	Invariant string
	// Severity is the catalogue grade the row was found under.
	Severity Severity
	// Violations is how many rows broke the invariant. Never zero: a clean
	// invariant produces no finding.
	Violations int
	// Subjects names the first breaking rows, capped at integritySubjectCap.
	Subjects []string
}

// RecordIntegrityFindings writes one sweep's findings in one transaction, so
// a reader never sees half a sweep. The store's clock stamps the rows unless
// the caller names a moment; the hourly sweep stamps through its own clock so
// a test can pin the timeline.
func (s *Store) RecordIntegrityFindings(ctx context.Context, at time.Time, findings []IntegrityFinding) error {
	if at.IsZero() {
		at = s.clk.Now().UTC()
	}
	atMilli := at.UnixMilli()
	if len(findings) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, f := range findings {
			if f.Violations <= 0 {
				continue
			}
			subjects := f.Subjects
			if len(subjects) > integritySubjectCap {
				subjects = subjects[:integritySubjectCap]
			}
			detail, err := json.Marshal(map[string]any{"subjects": subjects})
			if err != nil {
				return fmt.Errorf("record integrity finding %s: %w", f.Invariant, err)
			}
			if _, err := tx.Exec(`INSERT INTO integrity_events
(at, invariant, severity, violations, detail_json)
VALUES (?, ?, ?, ?, ?)`,
				atMilli, f.Invariant, f.Severity.String(), f.Violations, string(detail)); err != nil {
				return fmt.Errorf("record integrity finding %s: %w", f.Invariant, err)
			}
		}
		return nil
	})
}

// MetricsIntegrityViolation is one cell of the newest recorded sweep: how
// many rows broke one invariant, and at what grade.
type MetricsIntegrityViolation struct {
	Invariant  string
	Severity   string
	Violations int64
}

// MetricsIntegrityViolations returns the findings of the newest sweep that
// wrote any, in one statement. The gauge family therefore answers "what is
// broken right now, as far as the last sweep knows" without running the sweep
// at scrape time - a scrape that swept would be the long lived reader 07
// section 6.4 forbids.
func (s *Store) MetricsIntegrityViolations(ctx context.Context) ([]MetricsIntegrityViolation, error) {
	var out []MetricsIntegrityViolation
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT invariant, severity, violations
FROM integrity_events
WHERE at = (SELECT MAX(at) FROM integrity_events)`)
		if err != nil {
			return fmt.Errorf("read the newest integrity findings: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var f MetricsIntegrityViolation
			if err := rows.Scan(&f.Invariant, &f.Severity, &f.Violations); err != nil {
				return fmt.Errorf("scan an integrity finding: %w", err)
			}
			out = append(out, f)
		}
		return rows.Err()
	})
	return out, err
}

// MetricsFsckLastRun returns when the newest recorded sweep ran. The second
// return is false while no sweep has ever written, and the gauge family stays
// absent: no series without truth.
func (s *Store) MetricsFsckLastRun(ctx context.Context) (time.Time, bool, error) {
	var milli sql.NullInt64
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT MAX(at) FROM integrity_events`)
		if err != nil {
			return fmt.Errorf("read the last sweep stamp: %w", err)
		}
		defer func() { _ = rows.Close() }()
		if !rows.Next() {
			return nil
		}
		if err := rows.Scan(&milli); err != nil {
			return fmt.Errorf("scan the last sweep stamp: %w", err)
		}
		return rows.Err()
	})
	if err != nil || !milli.Valid {
		return time.Time{}, false, err
	}
	return time.UnixMilli(milli.Int64), true, nil
}
