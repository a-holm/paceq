package store_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// The retry slice of the transition layer: a step that fails with attempts
// left goes back to pending, parked until next_attempt_at, and the claim
// gate is the whole scheduler. These tests drive the store side of that
// bargain; the engine tests in internal/engine prove the loop end to end.

// A single step whose retry block buys three more attempts. The document is
// canonical v1 form, because the frozen bytes are read back through the
// strict reader.
const retryingStepSpec = `{"max_concurrent":1,"name":"retrying","schema":"paceq.job.v1",` +
	`"steps":[{"name":"build","run":["/bin/true"],"shell":false,` +
	`"retry":{"max":3,"backoff":"fixed","initial_ms":2000,"max_delay_ms":30000,"jitter":"none"}}],` +
	`"timeout_ms":3600000}`

func aRetryingRun(t *testing.T, s *store.Store) string {
	t.Helper()

	aCanonicalJob(t, s, "retrying", retryingStepSpec)
	out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "retrying",
	})
	if err != nil {
		t.Fatalf("materialize retrying: %v", err)
	}
	return out.Run.ID
}

func TestARetryPlanParksTheStepPastNextAttemptAt(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryingRun(t, s)
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}
	clk.Advance(time.Second)
	failedAt := clk.Now()
	due := failedAt.Add(2 * time.Second)

	err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		FinishedAt: failedAt,
		Retry: &store.RetryPlan{
			NextAttemptAt: due,
			ReasonCode:    reason.STEPRetryScheduled,
			DetailJSON: `{"attempt":1,"backoff_ms":2000,"next_attempt_at":` +
				i64(due.UnixMilli()) + `,"transient":true}`,
		},
	}, store.LeaseRef{Owner: testOwner, Epoch: 1})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "pending" {
		t.Errorf("state = %s, want pending: retry is a transition, not a state", step.State)
	}
	if step.Attempt != 1 || step.MaxAttempts != 4 {
		t.Errorf("attempt = %d/%d, want 1 of 4 (max 3 retries plus the first run)",
			step.Attempt, step.MaxAttempts)
	}
	if !step.NextAttemptAt.Equal(due) {
		t.Errorf("next_attempt_at = %s, want %s", step.NextAttemptAt, due)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "step.retry_scheduled" || last.FromState != "running" ||
		last.ToState != "pending" {
		t.Errorf("event = (%s %s->%s), want step.retry_scheduled running->pending",
			last.Kind, last.FromState, last.ToState)
	}
	if last.ReasonCode != string(reason.STEPRetryScheduled) {
		t.Errorf("event reason = %q, want %q", last.ReasonCode, reason.STEPRetryScheduled)
	}
	wantDetail := `{"attempt":1,"backoff_ms":2000,"next_attempt_at":` + i64(due.UnixMilli()) + `,"transient":true}`
	if last.DetailJSON != wantDetail {
		t.Errorf("detail = %s, want %s", last.DetailJSON, wantDetail)
	}

	// The gate is the scheduler: not runnable before the plan says so,
	// runnable the millisecond after.
	if _, ok, err := s.NextRunnableStep(ctx, runID); err != nil || ok {
		t.Errorf("NextRunnableStep = ok(%v) err(%v), want false while parked", ok, err)
	}
	wait, waiting, err := s.NextRetryWait(ctx, runID)
	if err != nil || !waiting {
		t.Fatalf("NextRetryWait = (%s, %v, %v), want parked for 2s", wait, waiting, err)
	}
	if wait != 2*time.Second {
		t.Errorf("wait = %s, want exactly 2s on a frozen clock", wait)
	}

	clk.Advance(1999 * time.Millisecond)
	if _, ok, err := s.NextRunnableStep(ctx, runID); err != nil || ok {
		t.Errorf("one millisecond short: NextRunnableStep = ok(%v) err(%v), want false", ok, err)
	}
	clk.Advance(time.Millisecond)
	if name, ok, err := s.NextRunnableStep(ctx, runID); err != nil || !ok || name != "build" {
		t.Errorf("past due: NextRunnableStep = (%q, %v) err(%v), want build", name, ok, err)
	}
	wait, waiting, err = s.NextRetryWait(ctx, runID)
	if err != nil || !waiting || wait != 0 {
		t.Errorf("past due: NextRetryWait = (%s, %v) err(%v), want zero and waiting", wait, waiting, err)
	}

	if err := s.StartStep(ctx, runID, "build", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("restart: %v", err)
	}
	step = mustStep(t, ctx, s, runID, "build")
	if step.Attempt != 2 {
		t.Errorf("attempt after restart = %d, want 2", step.Attempt)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

// Without a plan the old behaviour stands: runnable again at once. Nothing
// outside the engine hands plans in today, but the store does not invent
// delays either.
func TestWithoutAPlanTheRetryIsImmediatelyRunnable(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryingRun(t, s)
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}

	err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "pending" {
		t.Fatalf("state = %s, want pending", step.State)
	}
	if !step.NextAttemptAt.Equal(clk.Now()) {
		t.Errorf("next_attempt_at = %s, want now without a plan", step.NextAttemptAt)
	}
	if _, ok, err := s.NextRunnableStep(ctx, runID); err != nil || !ok {
		t.Errorf("NextRunnableStep = ok(%v) err(%v), want true at once", ok, err)
	}
	wait, waiting, err := s.NextRetryWait(ctx, runID)
	if err != nil || !waiting || wait != 0 {
		t.Errorf("NextRetryWait = (%s, %v) err(%v), want zero and waiting", wait, waiting, err)
	}
}

// A caller's veto ends the step even with budget on the row (#205). The
// budget answers "how many attempts is this failure worth"; it cannot answer
// "may this kind of ending be retried at all", and a kill by the run's own
// deadline is the ending where the two differ.
func TestAVetoedOutcomeEndsTheStepWithBudgetLeftOnTheRow(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryingRun(t, s)
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}
	before := mustStep(t, ctx, s, runID, "build")
	if before.Attempt >= before.MaxAttempts {
		t.Fatalf("attempt %d of %d leaves no budget to veto", before.Attempt, before.MaxAttempts)
	}

	err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:            "step_failed",
		ReasonCode:       reason.STEPFailedTimeout,
		FinishedAt:       clk.Now(),
		NoFurtherAttempt: true,
	}, store.LeaseRef{Owner: testOwner, Epoch: 1})
	if err != nil {
		t.Fatalf("RecordStepOutcome: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != "failed" {
		t.Fatalf("state = %s, want failed: the veto outranks the budget", step.State)
	}
	if step.ReasonCode != string(reason.STEPFailedTimeout) {
		t.Errorf("reason_code = %q, want %q", step.ReasonCode, reason.STEPFailedTimeout)
	}
	if _, waiting, err := s.NextRetryWait(ctx, runID); err != nil || waiting {
		t.Errorf("NextRetryWait = waiting(%v) err(%v), want nothing parked", waiting, err)
	}
	testutil.AssertNoUnknownReasons(t, ctx, s)
}

// A run with no parked steps reports no wait at all, which is what lets the
// executor stop instead of sleeping forever over nothing.
func TestNextRetryWaitWithNothingParkedReportsNothing(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	if wait, waiting, err := s.NextRetryWait(ctx, runID); err != nil || waiting || wait != 0 {
		t.Errorf("fresh run: NextRetryWait = (%s, %v) err(%v), want no wait", wait, waiting, err)
	}
}

// i64 renders an int64 for embedding in a canonical detail literal.
func i64(n int64) string {
	return strconv.FormatInt(n, 10)
}
