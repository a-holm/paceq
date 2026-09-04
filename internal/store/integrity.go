package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

// The integrity event log (M6-06): what the fsck sweep found, when, and how
// bad it was. A violation that only prints is a rumour; a violation in the
// database is a fact with a history, and the metrics and health surfaces
// read their numbers from here instead of re-running the sweep.
//
// One row per invariant that had findings, per sweep that saw them, plus one
// meta stamp per sweep whatever it found. The stamp is what makes the log
// readable: silence in the event table alone cannot tell a clean sweep from
// a sweep that never ran, and the gauges have to answer both questions.

// MetaFsckLastSweepAtMs is the meta key carrying the unix-millisecond stamp
// of the newest completed sweep, clean or not. It is the identity of that
// sweep: the violation gauge reads the event rows stamped with it, and the
// timestamp gauge reads it directly.
const MetaFsckLastSweepAtMs = "fsck_last_sweep_at_ms"

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

// RecordIntegritySweep writes one sweep - the fact that it ran and whatever
// it found - in one transaction, so a reader never sees half a sweep. A sweep
// that found nothing still records itself, because "nothing is broken" and
// "nobody has looked" are different answers and the gauges are asked for
// both. The store's clock stamps the sweep unless the caller names a moment;
// the hourly sweep stamps through its own clock so a test can pin the
// timeline.
func (s *Store) RecordIntegritySweep(ctx context.Context, at time.Time, findings []IntegrityFinding) error {
	if at.IsZero() {
		at = s.clk.Now().UTC()
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		atMilli, err := sweepStamp(tx, at.UnixMilli())
		if err != nil {
			return err
		}
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
		if _, err := tx.Exec(`INSERT INTO meta (key, value) VALUES (?, ?)
ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
			MetaFsckLastSweepAtMs, strconv.FormatInt(atMilli, 10)); err != nil {
			return fmt.Errorf("stamp the integrity sweep: %w", err)
		}
		return nil
	})
}

// sweepStamp keeps every sweep's stamp strictly newer than the one before it.
// The stamp is the sweep's identity, so two sweeps sharing one would make the
// newer report the older one's findings. A stamp nothing can parse reads as
// absent, so a hand edit costs one sweep rather than every sweep after it.
func sweepStamp(tx *sql.Tx, at int64) (int64, error) {
	var raw string
	err := tx.QueryRow(`SELECT value FROM meta WHERE key = ?`, MetaFsckLastSweepAtMs).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return at, nil
	}
	if err != nil {
		return 0, fmt.Errorf("read the last sweep stamp: %w", err)
	}
	prev, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil || at > prev {
		return at, nil
	}
	return prev + 1, nil
}

// MetricsIntegrityViolation is one cell of the newest recorded sweep: how
// many rows broke one invariant, and at what grade.
type MetricsIntegrityViolation struct {
	Invariant  string
	Severity   string
	Violations int64
}

// MetricsIntegrityViolations returns the findings of the newest sweep, in one
// statement. It selects on that sweep's stamp rather than on the newest row,
// so a clean sweep empties the family instead of leaving the last failure
// standing for ever. The gauge therefore answers "what is broken right now,
// as far as the last sweep knows" without running the sweep at scrape time -
// a scrape that swept would be the long lived reader 07 section 6.4 forbids.
func (s *Store) MetricsIntegrityViolations(ctx context.Context) ([]MetricsIntegrityViolation, error) {
	var out []MetricsIntegrityViolation
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT invariant, severity, violations
FROM integrity_events
WHERE at = (SELECT CAST(value AS INTEGER) FROM meta WHERE key = ?)`, MetaFsckLastSweepAtMs)
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

// MetricsFsckLastRun returns when the newest sweep ran, clean or not. The
// second return is false while no sweep has ever run, and the gauge family
// stays absent: no series without truth. It reads the sweep stamp rather than
// the event log, because the log holds only the sweeps that found something.
func (s *Store) MetricsFsckLastRun(ctx context.Context) (time.Time, bool, error) {
	raw, found, err := s.MetaValue(ctx, MetaFsckLastSweepAtMs)
	if err != nil || !found {
		return time.Time{}, false, err
	}
	milli, perr := strconv.ParseInt(raw, 10, 64)
	if perr != nil {
		return time.Time{}, false, fmt.Errorf("read the last sweep stamp: %w", perr)
	}
	return time.UnixMilli(milli), true, nil
}
