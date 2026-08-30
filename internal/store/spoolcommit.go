package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/spool"
)

// The spool committer (issue #39). A result file from a dead attempt's shim
// is only as good as the checks that admit it: the attempt must still be the
// one running, and the lease it carried must still be the one on the row.
// Both checks live here, in the same transaction as the verdict write, so a
// file can never be consumed twice and a stale file can never overwrite what
// a newer attempt wrote.

// ErrEpochMismatch means the result's fencing token does not match the run's
// current lease epoch: the attempt was reclaimed, and the file is a voice
// from a run that moved on without it.
var ErrEpochMismatch = errors.New("the result belongs to a lease that has moved on")

// ErrUnknownAttempt means nothing running matches the result's identity: the
// run is gone, the step is not running, or the attempt number is not the one
// on the row. The file is not a lie — it is a leftover.
var ErrUnknownAttempt = errors.New("no running attempt matches the result")

// SpoolOutcome is one consumed result file, translated into what the store
// needs to settle it: the identity the file claims, the fencing token it
// carried, the boot it was written on, and the verdict itself.
type SpoolOutcome struct {
	RunID      string
	Step       string
	Attempt    int
	ClaimEpoch int64
	BootID     string
	Outcome    StepOutcome
}

// CommitSpooledOutcome settles one attempt from its spool result. The checks
// and the write are one transaction, so the file's whole admission test is
// atomic with the commit:
//
//   - the run must exist and its lease epoch must equal the one the result
//     carries — the same CAS the reaper's fencing gives every other writer;
//     a mismatch is ErrEpochMismatch and the caller archives the file;
//   - the step must be running at the attempt the result names — anything
//     else is ErrUnknownAttempt, including a verdict that already landed,
//     which is what makes consuming twice harmless;
//   - the lease must be dead, unless the result was written on another
//     boot: a changed boot id is the strongest evidence there is, and it
//     voids a still-unexpired lease the same way it voids the orphan sweep's
//     wait. A live lease with no boot change means a live executor owns the
//     attempt, and the file stays where it is.
//
// A passed check commits through the same verdict write every other outcome
// takes, with the caller's outcome source ('spool') on the row.
func (s *Store) CommitSpooledOutcome(ctx context.Context, in SpoolOutcome) error {
	now := s.clk.Now().UTC()
	finishedAt := in.Outcome.FinishedAt
	if finishedAt.IsZero() {
		finishedAt = now
	}
	detail := in.Outcome.DetailJSON
	if detail == "" {
		detail = "{}"
	}

	return s.withTx(ctx, func(tx *sql.Tx) error {
		run, err := readRunTx(tx, in.RunID)
		if errors.Is(err, ErrRunNotFound) {
			return fmt.Errorf("commit the spooled result of %s on run %s: %w", in.Step, in.RunID, ErrUnknownAttempt)
		}
		if err != nil {
			return err
		}
		if run.LeaseEpoch != in.ClaimEpoch {
			return fmt.Errorf("commit the spooled result of %s on run %s: %w (result epoch %d, row epoch %d)",
				in.Step, in.RunID, ErrEpochMismatch, in.ClaimEpoch, run.LeaseEpoch)
		}

		// The boot fact: a result from another boot proves the machine
		// restarted, which proves every holder dead at once. The same
		// rule the startup sweep applies to orphaned processes applies
		// to the lease here.
		leaseVoid := false
		if in.BootID != "" {
			var stored sql.NullString
			if err := tx.QueryRow(`SELECT value FROM meta WHERE key = ?`, bootKey).Scan(&stored); err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("read the recorded boot id: %w", err)
			}
			if stored.Valid && stored.String != "" && stored.String != in.BootID {
				leaseVoid = true
			}
		}
		if !leaseVoid {
			if err := checkLeaseTx(tx, in.RunID, LeaseRef{}, now); err != nil {
				// A live lease means a live executor owns this
				// attempt's story; the file stays for it.
				return err
			}
		} else if run.State != string(model.RunRunning) {
			return fmt.Errorf("commit the spooled result of %s on run %s: %w (the run is %s)",
				in.Step, in.RunID, ErrUnknownAttempt, run.State)
		}

		step, err := readStepTx(tx, in.RunID, in.Step)
		if errors.Is(err, ErrRunNotFound) {
			return fmt.Errorf("commit the spooled result of %s on run %s: %w", in.Step, in.RunID, ErrUnknownAttempt)
		}
		if err != nil {
			return err
		}
		if step.State != "running" || step.Attempt != in.Attempt {
			return fmt.Errorf("commit the spooled result of %s on run %s: %w (step is %s at attempt %d)",
				in.Step, in.RunID, ErrUnknownAttempt, step.State, step.Attempt)
		}

		return applyStepOutcomeTx(tx, in.RunID, in.Step, in.Outcome, finishedAt, detail)
	})
}

// SpoolVerdict reads one result file as the verdict the step machine takes.
// The judgment itself is not made here: it comes from StepVerdict, the table
// the live executor reads too, so the same exit fact cannot read one way when
// the executor saw it and another way when this file is all that is left of
// it. What this function adds is what only a file can say — its stamps, its
// exit code, and that the verdict was read rather than watched. Log facts
// stay empty on purpose: the shim cannot know how the log ended, and recovery
// does not guess.
//
// An outcome word the format does not define is an admission error; the
// caller archives the file rather than guessing.
func SpoolVerdict(res spool.Result) (StepOutcome, error) {
	event, code, ok := StepVerdict(res.Outcome, res.KilledBy == spool.KilledByCancel)
	if !ok {
		return StepOutcome{}, fmt.Errorf("%w: unknown outcome %q", spool.ErrFormat, res.Outcome)
	}
	out := StepOutcome{
		Event:         event,
		ReasonCode:    code,
		FinishedAt:    msToTime(res.EndedAt),
		DetailJSON:    string(res.ReasonData),
		OutcomeSource: "spool",
	}
	if event != string(model.EvCancelObserved) {
		// A cancellation names no signal on either path: the number the
		// kernel delivered is how the cancellation was carried out, not
		// what happened to the step.
		out.Signal = res.Signal
	}
	switch res.Outcome {
	case spool.OutcomeSucceeded, spool.OutcomeFailed:
		code := res.ExitCode
		out.ExitCode = &code
	}
	return out, nil
}

// msToTime reads unix milliseconds back as a UTC time. Zero stays zero: an
// attempt that never managed to stamp its end has no time to claim, and the
// store fills its own.
func msToTime(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

// RecordSpoolArchive writes the warn event that says a result file was moved
// aside instead of committed, and why. A rejected result nobody can read
// about later is indistinguishable from a lost one, which is exactly the
// silence the spool was built to end (02 R2). The run must exist — the event
// hangs off it; a file naming no known run is archived with only the log for
// company.
//
// The event is run level, the step's name riding in the detail: the
// event-chain invariant (I15) reads each step's partition as one transition
// story, and an annotation about a rejected file is not a step transition.
// The orphan-kill event is run level for the same reason.
func (s *Store) RecordSpoolArchive(ctx context.Context, runID, step, file, why string) error {
	now := s.clk.Now().UTC()
	detail, err := json.Marshal(map[string]string{"step": step, "file": file, "why": why})
	if err != nil {
		return fmt.Errorf("encode the spool archive detail: %w", err)
	}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRow(`SELECT state FROM runs WHERE id = ?`, runID).Scan(&state); err != nil {
			return fmt.Errorf("read the state of run %s: %w", runID, err)
		}
		return appendRunEvent(tx, RunEvent{
			RunID:      runID,
			At:         now,
			Kind:       "run.spool_archived",
			FromState:  state,
			ToState:    state,
			Actor:      "reconcile",
			DetailJSON: string(detail),
		})
	})
	if err != nil {
		return fmt.Errorf("record the spool archive of %s on run %s: %w", file, runID, err)
	}
	return nil
}
