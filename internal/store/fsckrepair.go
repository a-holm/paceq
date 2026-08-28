package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/paceq/internal/model"
)

// fsck --repair (M6-06): the conservative half of the sweep. Repair is always
// "set the row to what the system itself would have written through ordinary
// reconciliation" - the same shapes the reaper and the drain write, attributed
// to fsck instead. It never deletes, never edits run_keys, and never rewrites
// history: the only rows it appends are its own audit events, each carrying
// actor "fsck" so the repair trail reads like any other transition trail.
//
// Three rules fix the rest:
//
//   - A database with critical findings is repaired only after the operator
//     confirms. Criticals mean a hand edit or corruption; repairing around
//     one silently would hide exactly the fact the severity exists to keep
//     visible.
//   - Reason codes are stamped only on runs and steps, whose codes are
//     display metadata for a verdict the row already carries. Ticks and
//     triggers stay the operator's: their reason codes are decisions, and
//     inventing one would be writing history.
//   - Every repair runs in its own transaction and re-checks its predicate
//     inside that transaction, so a row that moved between the sweep and the
//     repair is simply left alone.

// fsckActor is the actor name every repair event carries, so the audit trail
// answers "who did this" with the tool that did it.
const fsckActor = "fsck"

// RepairOutcome is what repairing one invariant did.
type RepairOutcome struct {
	// Invariant is the catalogue ID that was repaired.
	Invariant string
	// Repaired counts rows the repair put back in line.
	Repaired int
	// Skipped counts rows the repair left for the operator, with the reason
	// in Note.
	Skipped int
	// Note says why anything was skipped. Empty when nothing was.
	Note string
}

// RepairConfirmError reports critical findings under an unconfirmed repair.
// The CLI turns it into the preservation-and-confirm guidance the startup
// refusal names; the errors.As chain exists so no caller has to string-match.
type RepairConfirmError struct {
	// Critical holds the critical findings that require the confirmation.
	Critical []Violation
}

func (e *RepairConfirmError) Error() string {
	n := 0
	for _, v := range e.Critical {
		if v.Severity == Critical {
			n++
		}
	}
	return fmt.Sprintf("%s: repairing a state with critical invariant violations needs "+
		"an explicit --confirm, because the damage is not something paceq can repair "+
		"around silently", count(n, "critical finding"))
}

// FsckRepair sweeps, then repairs what is safely repairable. only limits the
// work to named invariants (the --only list; empty means all). confirm is the
// operator's explicit --confirm, required while any critical finding stands.
//
// The sweep and the repairs are two phases: the repairs re-check their own
// predicates, so the sweep is only the map of what to look at. The return
// carries one outcome per invariant that had findings, in no order.
func (s *Store) FsckRepair(ctx context.Context, only []string, confirm bool) ([]RepairOutcome, error) {
	violations, err := s.Fsck(ctx)
	if err != nil {
		return nil, err
	}
	if err := validRepairScope(only); err != nil {
		return nil, err
	}

	var critical []Violation
	byCheck := map[string][]Violation{}
	for _, v := range violations {
		if len(only) > 0 && !containsString(only, v.Check) {
			continue
		}
		if v.Severity == Critical {
			critical = append(critical, v)
		}
		byCheck[v.Check] = append(byCheck[v.Check], v)
	}
	if len(critical) > 0 && !confirm {
		return nil, &RepairConfirmError{Critical: critical}
	}

	var out []RepairOutcome
	if len(byCheck["I1"]) > 0 {
		o, err := s.repairOrphanedRunning(ctx)
		if err != nil {
			return nil, err
		}
		o.Invariant = "I1"
		out = append(out, o)
	}
	if len(byCheck["I2"]) > 0 {
		o, err := s.repairStrandedSteps(ctx)
		if err != nil {
			return nil, err
		}
		o.Invariant = "I2"
		out = append(out, o)
	}
	if len(byCheck["I14"]) > 0 {
		o, err := s.repairUnexplainedDeferral(ctx)
		if err != nil {
			return nil, err
		}
		o.Invariant = "I14"
		out = append(out, o)
	}
	if len(byCheck["reason"]) > 0 {
		o, err := s.repairLegacyReasons(ctx)
		if err != nil {
			return nil, err
		}
		o.Invariant = "reason"
		out = append(out, o)
	}
	return out, nil
}

// repairScope is every invariant --repair knows how to touch. The CLI's
// --only validates against it plus the catalogue, so a typo fails loudly
// instead of repairing nothing quietly.
var repairScope = []string{"I1", "I2", "I14", "reason"}

func validRepairScope(only []string) error {
	for _, name := range only {
		if _, ok := invariantByID(name); !ok {
			ids := ""
			for _, inv := range Invariants {
				if ids != "" {
					ids += ", "
				}
				ids += inv.ID
			}
			return fmt.Errorf("unknown invariant %q; the catalogue holds: %s", name, ids)
		}
	}
	return nil
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// repairOrphanedRunning puts every running-without-live-lease run back in the
// queue the way the reaper's requeue arm does: the epoch rises, the crash
// counts, the token lands in the event history, and the run waits out a
// backoff. A row whose crash budget is spent is left for the reaper, which
// owns the decision to fail it into the quarantine.
func (s *Store) repairOrphanedRunning(ctx context.Context) (RepairOutcome, error) {
	var o RepairOutcome
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		cut := s.clk.Now().UTC().Add(-DefaultClockSkewAllowance).UnixMilli()
		rows, err := tx.Query(`SELECT id, lease_epoch, crash_count FROM runs
WHERE state = 'running'
	AND (lease_owner IS NULL OR lease_owner = ''
		OR lease_expires_at IS NULL
		OR lease_expires_at <= ?)`, cut)
		if err != nil {
			return fmt.Errorf("repair I1: find the orphaned runs: %w", err)
		}
		type candidate struct {
			id      string
			epoch   int64
			crashes int
		}
		var found []candidate
		for rows.Next() {
			var c candidate
			if err := rows.Scan(&c.id, &c.epoch, &c.crashes); err != nil {
				_ = rows.Close()
				return fmt.Errorf("repair I1: %w", err)
			}
			found = append(found, c)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("repair I1: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("repair I1: %w", err)
		}

		now := s.clk.Now().UTC()
		for _, c := range found {
			if c.crashes+1 > DefaultMaxCrashCount {
				o.Skipped++
				continue
			}
			if _, _, err := model.NextRunState(model.RunRunning, model.EvLeaseExpired, model.Guards{
				LeaseValid:      false,
				CrashBudgetLeft: true,
			}); err != nil {
				o.Skipped++
				continue
			}
			repaired, err := repairRequeueRunTx(tx, c.id, c.epoch, now)
			if err != nil {
				return err
			}
			if repaired {
				o.Repaired++
			} else {
				o.Skipped++ // the row moved since the sweep; leave it be
			}
		}
		return nil
	})
	return o, err
}

// repairStrandedSteps closes the steps that outlived their run, through the
// same transitions a cancel uses: a running step goes to cancelled, a pending
// one to skipped, both carrying the step-level cancel code. The run row is
// terminal already and stays exactly as it is.
func (s *Store) repairStrandedSteps(ctx context.Context) (RepairOutcome, error) {
	var o RepairOutcome
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT s.run_id, s.name, s.state FROM steps s
JOIN runs r ON r.id = s.run_id
WHERE s.state IN ('pending', 'running')
	AND r.state IN ('succeeded', 'failed', 'cancelled')`)
		if err != nil {
			return fmt.Errorf("repair I2: find the stranded steps: %w", err)
		}
		type stranded struct {
			runID, name, state string
		}
		var found []stranded
		for rows.Next() {
			var f stranded
			if err := rows.Scan(&f.runID, &f.name, &f.state); err != nil {
				_ = rows.Close()
				return fmt.Errorf("repair I2: %w", err)
			}
			found = append(found, f)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("repair I2: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("repair I2: %w", err)
		}

		now := s.clk.Now().UTC()
		for _, f := range found {
			var (
				repaired bool
				err      error
			)
			switch model.StepState(f.state) {
			case model.StepRunning:
				repaired, err = repairCancelRunningStepTx(tx, f.runID, f.name, now)
			case model.StepPending:
				repaired, err = repairSkipPendingStepTx(tx, f.runID, f.name, now)
			default:
				o.Skipped++
				continue
			}
			if err != nil {
				return err
			}
			if repaired {
				o.Repaired++
			} else {
				o.Skipped++
			}
		}
		return nil
	})
	return o, err
}

// repairUnexplainedDeferral stamps the defer reason the CLI reports as "held
// for an unknown reason". The run stays queued and stays held; the repair only
// makes the hold legible. It writes a state-preserving event so the repair is
// visible in the history the way every other repair is.
func (s *Store) repairUnexplainedDeferral(ctx context.Context) (RepairOutcome, error) {
	var o RepairOutcome
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`SELECT id FROM runs
WHERE state = 'queued' AND available_at > created_at
	AND (defer_reason IS NULL OR defer_reason = '')`)
		if err != nil {
			return fmt.Errorf("repair I14: find the unexplained holds: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("repair I14: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("repair I14: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("repair I14: %w", err)
		}

		now := s.clk.Now().UTC()
		for _, id := range ids {
			repaired, err := repairStampDeferTx(tx, id, now)
			if err != nil {
				return err
			}
			if repaired {
				o.Repaired++
			} else {
				o.Skipped++
			}
		}
		return nil
	})
	return o, err
}

// repairLegacyReasons stamps the legacy code over the unusable reason values
// on terminal runs and steps. Ticks and triggers are counted as skipped: their
// reason codes are decisions the sweep cannot make for them.
func (s *Store) repairLegacyReasons(ctx context.Context) (RepairOutcome, error) {
	var o RepairOutcome
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		now := s.clk.Now().UTC()

		runs, err := tx.Query(`SELECT id, state FROM runs
WHERE state IN ('succeeded', 'failed', 'cancelled')
	AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')`)
		if err != nil {
			return fmt.Errorf("repair reason: find the legacy runs: %w", err)
		}
		type row struct {
			id, name, state string
		}
		var legacyRuns []row
		for runs.Next() {
			var r row
			if err := runs.Scan(&r.id, &r.state); err != nil {
				_ = runs.Close()
				return fmt.Errorf("repair reason: %w", err)
			}
			legacyRuns = append(legacyRuns, r)
		}
		if err := runs.Err(); err != nil {
			_ = runs.Close()
			return fmt.Errorf("repair reason: %w", err)
		}
		if err := runs.Close(); err != nil {
			return fmt.Errorf("repair reason: %w", err)
		}

		for _, r := range legacyRuns {
			repaired, err := repairStampRunReasonTx(tx, r.id, r.state, now)
			if err != nil {
				return err
			}
			if repaired {
				o.Repaired++
			} else {
				o.Skipped++
			}
		}

		steps, err := tx.Query(`SELECT s.run_id, s.name, s.state FROM steps s
WHERE s.state IN ('succeeded', 'failed', 'skipped', 'cancelled')
	AND (s.reason_code IS NULL OR s.reason_code = '' OR s.reason_code = 'UNKNOWN')`)
		if err != nil {
			return fmt.Errorf("repair reason: find the legacy steps: %w", err)
		}
		var legacySteps []row
		for steps.Next() {
			var r row
			if err := steps.Scan(&r.id, &r.name, &r.state); err != nil {
				_ = steps.Close()
				return fmt.Errorf("repair reason: %w", err)
			}
			legacySteps = append(legacySteps, r)
		}
		if err := steps.Err(); err != nil {
			_ = steps.Close()
			return fmt.Errorf("repair reason: %w", err)
		}
		if err := steps.Close(); err != nil {
			return fmt.Errorf("repair reason: %w", err)
		}
		for _, r := range legacySteps {
			repaired, err := repairStampStepReasonTx(tx, r.id, r.name, r.state, now)
			if err != nil {
				return err
			}
			if repaired {
				o.Repaired++
			} else {
				o.Skipped++
			}
		}

		// Ticks and triggers keep their history: the sweep still reports them,
		// but the repair does not invent decisions on their behalf. Count them
		// so the outcome's skipped number names what is left for the operator.
		for _, arm := range []struct{ table, outcomes string }{
			{"ticks", "('skipped', 'error', 'missed')"},
			{"triggers", "('deduped', 'rejected')"},
		} {
			var n int
			if err := tx.QueryRow(`SELECT COUNT(*) FROM ` + arm.table + `
WHERE outcome IN ` + arm.outcomes + `
	AND (reason_code IS NULL OR reason_code = '' OR reason_code = 'UNKNOWN')`).Scan(&n); err != nil {
				return fmt.Errorf("repair reason: count the legacy %s: %w", arm.table, err)
			}
			o.Skipped += n
		}
		return nil
	})
	return o, err
}

// count is the shared "1 run, 2 runs" renderer for messages.
func count(n int, word string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", word)
	}
	return fmt.Sprintf("%d %ss", n, word)
}
