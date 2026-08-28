package engine

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spool"
	"github.com/a-holm/paceq/internal/store"
)

// recoverPoll is how often Recover re-reads a lease that has not expired yet.
// The wait exists for correctness, not for speed: recovery may not touch a
// run whose executor could still be alive, so it waits out the lease and
// polls rather than sleeping one long block, which keeps tests fast and a
// production caller responsive to context cancellation.
const recoverPoll = 10 * time.Millisecond

// Recover closes out what a dead executor left behind and makes the run
// claimable again. It is the restart half of the guarantee the crash harness
// (#75) proves: a run interrupted by SIGKILL converges on restart, without an
// invariant violation and without inventing a verdict.
//
// Four steps, in this order:
//
//  1. Wait for the abandoned lease to expire. A lease that still lives may
//     belong to a process that is only slow, and two executors on one run is
//     the one outcome fencing exists to prevent.
//  2. Settle what the dead attempts' shims wrote: a result file in the
//     spool (issue #39) is what really happened, and it is committed with
//     outcome_source='spool' — the difference between rerunning a
//     data-warehouse load and not (crash window W8).
//  3. Close every step the dead attempt left running without a result. Each
//     goes through the machine as a failed attempt with
//     STEP_FAILED_EXECUTOR_LOST and outcome_source='reconciled': the verdict
//     was lost with the executor, so recovery records exactly that instead
//     of guessing. A step with attempts left comes back pending for its next
//     attempt; otherwise it fails and the run will follow.
//  4. Requeue the run through the store's lease_expired transition, which
//     bumps the epoch, counts the crash and writes defer_reason.
//
// A run that is not running needs nothing and is returned untouched, so a
// caller may call Recover before every ExecuteRun without checking first.
func (e *Engine) Recover(ctx context.Context, runID string) (string, error) {
	if err := e.waitForDeadLease(ctx, runID); err != nil {
		return "", err
	}

	detail, err := e.Store.GetRun(ctx, runID)
	if err != nil {
		return "", fmt.Errorf("recover run %s: %w", runID, err)
	}
	if detail.Run.State != string(model.RunRunning) {
		return detail.Run.State, nil
	}

	now := e.Clock.Now().UTC()
	for _, step := range detail.Steps {
		if model.StepState(step.State) != model.StepRunning {
			continue
		}
		// What the shim wrote wins over what recovery would assume. A
		// file that cannot be committed here — stale epoch, unknown
		// attempt, a lease that somehow came back to life — is left for
		// the reconciler's own pass, which archives it with an event;
		// this loop then treats the attempt as sourceless, which is the
		// honest reading of "the file did not say".
		settled, err := e.settleFromSpool(ctx, runID, step)
		if err != nil {
			return "", fmt.Errorf("recover step %s of run %s: %w", step.Name, runID, err)
		}
		if settled {
			continue
		}
		outcome := storeStepOutcomeLost(now, e.Owner)
		// Recovery writes without a holder on purpose: the lease is dead,
		// which the store verifies inside the same transaction.
		if err := e.Store.RecordStepOutcome(ctx, runID, step.Name, outcome, store.LeaseRef{}); err != nil {
			return "", fmt.Errorf("recover step %s of run %s: %w", step.Name, runID, err)
		}
	}

	if err := e.Store.RequeueCrashedRun(ctx, runID); err != nil {
		return "", fmt.Errorf("recover run %s: %w", runID, err)
	}
	return string(model.RunQueued), nil
}

// waitForDeadLease blocks until the run's lease has lapsed or the context
// ends. An expired lease is evidence enough that the previous owner is gone;
// the state directory lock, which a crashed process cannot keep, backs it up.
func (e *Engine) waitForDeadLease(ctx context.Context, runID string) error {
	for {
		detail, err := e.Store.GetRun(ctx, runID)
		if err != nil {
			return fmt.Errorf("recover run %s: %w", runID, err)
		}
		if detail.Run.State != string(model.RunRunning) {
			return nil // nothing is claimed; nothing to wait for
		}
		if detail.Run.LeaseExpiresAt.IsZero() || !detail.Run.LeaseExpiresAt.After(e.Clock.Now()) {
			return nil
		}
		timer := e.Clock.NewTimer(recoverPoll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf("recover run %s: %w", runID, ctx.Err())
		case <-timer.C:
		}
	}
}

// storeStepOutcomeLost builds the verdict recovery writes over a dead
// attempt. The event is a failure because the machine retries failures and
// never retries cancellations; the code says plainly why no exit code, signal
// or log tail accompanies it, and the outcome source names the verdict for
// what it is: assumed, not observed (#39).
func storeStepOutcomeLost(now time.Time, owner string) store.StepOutcome {
	return store.StepOutcome{
		Event:         string(model.EvStepFailed),
		ReasonCode:    reason.STEPFailedExecutorLost,
		FinishedAt:    now,
		DetailJSON:    detailJSON(map[string]any{"recovered_by": owner}),
		OutcomeSource: "reconciled",
	}
}

// settleFromSpool commits one running step's outcome from the result file
// its shim left behind, if there is one and it still belongs to this
// attempt. It reports whether the step was settled here.
//
// A spool-committed success still publishes what the step wrote (#13): the
// output file is on disk under the run's directory, and the verdict commit
// that lands the step on succeeded is the one that may name its artifacts.
// A publication failure degrades: the step stays succeeded with no
// artifacts, because a crashed executor's output contract cannot be
// resurrected by refusing the honest verdict.
func (e *Engine) settleFromSpool(ctx context.Context, runID string, step store.Step) (bool, error) {
	if e.SpoolDir == "" {
		return false, nil
	}
	path := filepath.Join(e.SpoolDir, spool.FileName(runID, step.Name, step.Attempt))
	res, err := spool.ReadResult(path)
	if err != nil {
		return false, nil // no file, or one the format cannot vouch for
	}
	outcome, err := store.SpoolVerdict(res)
	if err != nil {
		return false, nil
	}
	// Publication (#13), before the commit so the artifacts land in the
	// verdict's own transaction, exactly as the live path does. A read or
	// collection failure degrades to no artifacts: the verdict stays what
	// the file said, and the output contract of a crashed executor cannot
	// be resurrected by refusing it.
	if res.Outcome == spool.OutcomeSucceeded && e.StateDir != "" {
		outputPath := filepath.Join(e.StateDir, "runs", runID,
			fmt.Sprintf("%s.%d.output.ndjson", step.Name, step.Attempt))
		if parsed, err := runner.ReadStepOutput(outputPath); err == nil {
			if arts, warnings, err := e.collectPublications(ctx, runID, step.Name, step.Index, parsed); err == nil {
				outcome.Artifacts = arts
				outcome.DetailJSON = publicationDetail(outcome.DetailJSON, parsed.Params, warnings)
			}
		}
	}
	err = e.Store.CommitSpooledOutcome(ctx, store.SpoolOutcome{
		RunID:      runID,
		Step:       step.Name,
		Attempt:    step.Attempt,
		ClaimEpoch: res.ClaimEpoch,
		BootID:     res.BootID,
		Outcome:    outcome,
	})
	switch {
	case err == nil:
	case errors.Is(err, store.ErrEpochMismatch),
		errors.Is(err, store.ErrUnknownAttempt),
		errors.Is(err, store.ErrLeaseLost):
		return false, nil // not this loop's file to settle
	default:
		return false, err
	}
	// The attempt is settled on the row; the file has told its story.
	if err := spool.Remove(path); err != nil {
		return false, err
	}
	return true, nil
}
