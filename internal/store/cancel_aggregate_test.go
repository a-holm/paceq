package store_test

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// #170: a cancelled run's stored state is what its steps aggregate to (I10).
//
// The soak found four runs sitting in cancelled whose steps added up to failed,
// and one whose steps added up to succeeded. Every one of them came from the
// same mistake: the writer took the state from the request instead of from the
// steps. Three writers complete a cancellation - the holder observing one, the
// reaper completing one whose owner died, and the claim sweep closing one that
// was still queued - and each is covered below, because a fix in one of them
// leaves the invariant breakable through the other two.
//
// The shape that breaks it is a run that already carries a failed step and
// still has work pending: the pending step closes as cancelled, the failure
// stays, and failed outranks cancelled.

// forkSpec is two independent steps. Failing one leaves the other pending
// rather than skipped: nothing needs the failure, so skip propagation does not
// reach it, which is what leaves a mixed aggregate behind.
const forkSpec = `{"name":"fork","max_concurrent":2,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"boom","run":["/bin/false"],"shell":false},` +
	`{"name":"later","run":["/bin/true"],"shell":false}]}`

// reusedSpec is one step, used for the replay shape: every step already
// succeeded, so a cancellation finds nothing left to cancel.
const reusedSpec = `{"name":"reused","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"only","run":["/bin/true"],"shell":false}]}`

func TestObserveRunCancelRecordsTheFailureItArrivedAfter(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "fork", forkSpec)
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "boom", ref); err != nil {
		t.Fatalf("start boom: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "boom", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(1),
		FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("fail boom: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop it"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if err := s.ObserveRunCancel(ctx, runID, ref, "cli:1000", reason.RUNCancelledManual); err != nil {
		t.Fatalf("ObserveRunCancel: %v", err)
	}

	assertRunMatchesItsSteps(t, ctx, s, runID, model.RunFailed, reason.RUNFailedStep)
	if got := mustStep(t, ctx, s, runID, "later").State; got != string(model.StepCancelled) {
		t.Errorf("later = %s, want cancelled: a step that never started under a cancellation is cancelled", got)
	}
}

func TestReaperRecordsTheFailureTheCancelArrivedAfter(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "fork", forkSpec)
	ref := store.LeaseRef{Owner: "doomed", Epoch: 1}

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "boom", ref); err != nil {
		t.Fatalf("start boom: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "boom", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(1),
		FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("fail boom: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop it"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)

	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped %+v, want the one run", reaped)
	}
	if reaped[0].State != string(model.RunFailed) {
		t.Errorf("reaped state = %s, want failed: the reaper reports the verdict it wrote", reaped[0].State)
	}
	assertRunMatchesItsSteps(t, ctx, s, runID, model.RunFailed, reason.RUNFailedStep)
}

func TestQueuedCancelRecordsTheSuccessItArrivedAfter(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "reused", reusedSpec)
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}

	// Run it to success, then reopen it. The reopened run is queued again with
	// its one step still succeeded, which is the replay that reuses everything.
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "only", ref); err != nil {
		t.Fatalf("start only: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "only", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
		FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("succeed only: %v", err)
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("FinishRun: %v", err)
	}
	replay, err := s.MaterializeReplay(ctx, runID, store.ReplayOpts{FailedOnly: true, Actor: "cli:1000"})
	if err != nil {
		t.Fatalf("MaterializeReplay: %v", err)
	}
	if len(replay.Rerun) != 0 {
		t.Fatalf("replay reruns %v, want a replay that reuses everything", replay.Rerun)
	}
	if _, err := s.RequestCancel(ctx, replay.NewRunID, "cli:1000", "too late"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	// The claim sweep closes queued cancellations before it claims anything.
	if _, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: testOwner, Limit: 4}); err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	assertRunMatchesItsSteps(t, ctx, s, replay.NewRunID, model.RunSucceeded, reason.RUNSucceeded)
}

// assertRunMatchesItsSteps is the I10 assertion this issue exists for: the run
// row says what the steps add up to, the reason code names that verdict, and
// fsck - the production sweep, not a copy of it - agrees.
func assertRunMatchesItsSteps(t *testing.T, ctx context.Context, s *store.Store,
	runID string, want model.RunState, wantReason reason.Code,
) {
	t.Helper()

	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.State != string(want) {
		t.Errorf("run state = %s, want %s", detail.Run.State, want)
	}
	if detail.Run.ReasonCode != string(wantReason) {
		t.Errorf("reason_code = %q, want %q", detail.Run.ReasonCode, wantReason)
	}

	states := make([]model.StepState, 0, len(detail.Steps))
	for _, step := range detail.Steps {
		parsed, err := model.ParseStepState(step.State)
		if err != nil {
			t.Fatalf("parse the state of step %s: %v", step.Name, err)
		}
		states = append(states, parsed)
	}
	if agg := model.RunAggregate(states, false); agg != want {
		t.Errorf("the steps aggregate to %s, want %s (states %v)", agg, want, states)
	}

	// The sweep is the production one, not a copy of it, and it is read for
	// I10 alone: a replay that reuses a succeeded step trips I5 because the
	// reused row carries no attempt, which belongs to the replay path and is
	// #175. Widen this back to every check when that lands.
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("Fsck: %v", err)
	}
	for _, v := range violations {
		if v.Check == "I10" {
			t.Errorf("fsck %s on %s: %s", v.Check, v.Subject, v.Detail)
		}
	}
}
