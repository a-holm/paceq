package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// M4-04: run level retry. One operator command reopens a terminal run in one
// transaction: the run goes back to queued under a raised fencing token, and
// its failed and skipped steps go back to pending with their attempt history
// intact and their budget raised. Succeeded steps are never touched; they are
// what the retry builds on.

// retryChainSpec is a five step chain. A retry of c must reach exactly
// c, d and e, never a or b.
const retryChainSpec = `{"name":"retrychain","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"a","run":["/bin/true"],"shell":false},` +
	`{"name":"b","needs":["a"],"run":["/bin/true"],"shell":false},` +
	`{"name":"c","needs":["b"],"run":["/bin/true"],"shell":false},` +
	`{"name":"d","needs":["c"],"run":["/bin/true"],"shell":false},` +
	`{"name":"e","needs":["d"],"run":["/bin/true"],"shell":false}]}`

// driveChain claims the chain once and walks it in order. failAt names the
// step that dies (everything below it is skipped by M4-03); empty means every
// step succeeds. Either way the run is finished, so a retry starts from a
// genuinely terminal row.
func driveChain(t *testing.T, ctx context.Context, s *store.Store, clk interface{ Now() time.Time },
	runID, failAt string,
) {
	t.Helper()

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}
	failed := ""
	for _, step := range []string{"a", "b", "c", "d", "e"} {
		if err := s.StartStep(ctx, runID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		out := store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: ptr(0), FinishedAt: clk.Now(),
		}
		if step == failAt {
			out = store.StepOutcome{
				Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
				ExitCode: ptr(9), FinishedAt: clk.Now(),
			}
			failed = step
		}
		if err := s.RecordStepOutcome(ctx, runID, step, out, ref); err != nil {
			t.Fatalf("record %s: %v", step, err)
		}
		if failed != "" {
			break
		}
	}
	code := reason.RUNSucceeded
	data := "{}"
	if failed != "" {
		code = reason.RUNFailedStep
		data = `{"step":"` + failed + `"}`
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{Code: code, Data: data}); err != nil {
		t.Fatalf("finish the chain: %v", err)
	}
}

// stepState is a tiny read back helper so assertions read like the table.
type stepState struct {
	state    string
	attempt  int
	max      int
	finished bool
}

func stepStates(t *testing.T, ctx context.Context, s *store.Store, runID string) map[string]stepState {
	t.Helper()
	detail := mustGetRun(t, ctx, s, runID)
	out := map[string]stepState{}
	for _, st := range detail.Steps {
		out[st.Name] = stepState{
			state:    st.State,
			attempt:  st.Attempt,
			max:      st.MaxAttempts,
			finished: !st.FinishedAt.IsZero(),
		}
	}
	return out
}

// TestReopenKeepsTheRunAndReopensOnlyFailedAndSkipped is AC-1: same id and
// run_key, failed and skipped steps pending again, succeeded steps exactly as
// they were.
func TestReopenKeepsTheRunAndReopensOnlyFailedAndSkipped(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)

	driveChain(t, ctx, s, clk, runID, "c")
	before := stepStates(t, ctx, s, runID)
	src := mustGetRun(t, ctx, s, runID)
	if src.State != "failed" {
		t.Fatalf("the staged run is %s, want failed", src.State)
	}

	res, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got := res.Reopened; len(got) != 3 || got[0] != "c" || got[1] != "d" || got[2] != "e" {
		t.Errorf("reopened = %v, want exactly [c d e]", got)
	}

	after := stepStates(t, ctx, s, runID)
	for _, name := range []string{"c", "d", "e"} {
		if after[name].state != "pending" {
			t.Errorf("%s = %s, want pending after a reopen", name, after[name].state)
		}
		if after[name].finished {
			t.Errorf("%s still carries a finished_at from the dead generation", name)
		}
	}
	for _, name := range []string{"a", "b"} {
		if after[name] != before[name] {
			t.Errorf("%s moved on reopen: %+v -> %+v", name, before[name], after[name])
		}
	}

	reopened := mustGetRun(t, ctx, s, runID)
	if reopened.ID != src.ID || reopened.RunKey != src.RunKey {
		t.Errorf("run identity changed: %s/%q -> %s/%q",
			src.ID, src.RunKey, reopened.ID, reopened.RunKey)
	}
	if reopened.State != string(model.RunQueued) {
		t.Errorf("reopened run state = %s, want queued", reopened.State)
	}
}

// TestReopenWritesTheOperatorEventAndRaisesTheEpoch is AC-3.
func TestReopenWritesTheOperatorEventAndRaisesTheEpoch(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "c")
	before := mustGetRun(t, ctx, s, runID)

	res, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if res.NewEpoch != before.LeaseEpoch+1 {
		t.Errorf("new epoch = %d, want %d", res.NewEpoch, before.LeaseEpoch+1)
	}

	after := mustGetRun(t, ctx, s, runID)
	if after.LeaseEpoch != res.NewEpoch {
		t.Errorf("row epoch = %d, want %d", after.LeaseEpoch, res.NewEpoch)
	}
	if after.LeaseOwner != "" || !after.LeaseExpiresAt.IsZero() {
		t.Errorf("reopened run still carries a lease: %q %v", after.LeaseOwner, after.LeaseExpiresAt)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var reopen *store.RunEvent
	for i := range events {
		if events[i].Kind == "operator_reopen" {
			reopen = &events[i]
		}
	}
	if reopen == nil {
		t.Fatal("no operator_reopen event was written")
	}
	if reopen.Actor != "cli:1000" {
		t.Errorf("event actor = %q, want cli:1000", reopen.Actor)
	}
	if reopen.FromState != string(model.RunFailed) || reopen.ToState != string(model.RunQueued) {
		t.Errorf("event states = %s->%s, want failed->queued", reopen.FromState, reopen.ToState)
	}
	if reopen.ReasonCode != string(reason.RUNReopenedOperator) {
		t.Errorf("event reason = %q, want %s", reopen.ReasonCode, reason.RUNReopenedOperator)
	}

	// The fencing token rides in the event detail, or I11 reads a history
	// that ends below the row's own epoch.
	var detail map[string]any
	if err := json.Unmarshal([]byte(reopen.DetailJSON), &detail); err != nil {
		t.Fatalf("event detail %q is not an object: %v", reopen.DetailJSON, err)
	}
	if v, ok := detail["lease_epoch"].(float64); !ok || int64(v) != res.NewEpoch {
		t.Errorf("event detail %v carries lease_epoch %v, want %d",
			detail, detail["lease_epoch"], res.NewEpoch)
	}
}

// TestReopenedStepsKeepTheirAttemptHistory pins design decision 6: the
// counter continues, the budget rises, and the next start is a real new
// attempt numbered straight on from the old ones (I5).
func TestReopenedStepsKeepTheirAttemptHistory(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "c")

	c := mustStep(t, ctx, s, runID, "c")
	if c.Attempt != 1 || c.MaxAttempts != 1 {
		t.Fatalf("staged c is attempt %d/%d, want 1/1", c.Attempt, c.MaxAttempts)
	}

	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{}); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	c = mustStep(t, ctx, s, runID, "c")
	if c.Attempt != 1 {
		t.Errorf("attempt = %d, want 1 preserved across the reopen", c.Attempt)
	}
	if c.MaxAttempts != 2 {
		t.Errorf("max_attempts = %d, want 2 after the default budget of one", c.MaxAttempts)
	}
	if !c.NextAttemptAt.IsZero() {
		t.Errorf("next_attempt_at = %v, want cleared so the claim gate admits at once", c.NextAttemptAt)
	}

	// The proof the budget is real: the engine can take the new attempt,
	// and its number follows the old ones without a gap.
	_, newEpoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil {
		t.Fatalf("claim the reopened run: %v", err)
	}
	if err := s.StartStep(ctx, runID, "c", store.LeaseRef{Owner: testOwner, Epoch: newEpoch}); err != nil {
		t.Fatalf("start the new attempt of c: %v", err)
	}
	c = mustStep(t, ctx, s, runID, "c")
	if c.Attempt != 2 || c.State != "running" {
		t.Errorf("new attempt of c = %d (%s), want 2 running", c.Attempt, c.State)
	}
}

// TestReopenWithStepTakesTheDownstreamClosure is AC-5: --step b reopens b and
// everything downstream of it, from the frozen edges of this run, and leaves
// upstream alone.
func TestReopenWithStepTakesTheDownstreamClosure(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "b")

	step := "b"
	res, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{OnlyStep: &step})
	if err != nil {
		t.Fatalf("reopen --step b: %v", err)
	}
	if got := res.Reopened; len(got) != 4 || got[0] != "b" || got[1] != "c" || got[2] != "d" || got[3] != "e" {
		t.Errorf("reopened = %v, want exactly [b c d e]", got)
	}
	states := stepStates(t, ctx, s, runID)
	if states["a"].state != "succeeded" {
		t.Errorf("a = %s, want untouched", states["a"].state)
	}
}

// TestReopenWithStepOnASucceededStepIsRefused: when nothing behind the named
// step is failed or skipped, there is nothing to reopen, and saying so beats
// queueing a run that would finish instantly.
func TestReopenWithStepOnASucceededStepIsRefused(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	// Two independent branches, so naming a step from the healthy branch is
	// a real mistake an operator can make on a failed run.
	runID := aDagRun(t, s, "isolated", isolatedSkipSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: testOwner, Epoch: 1}
	for _, step := range []string{"a", "c", "p"} {
		if err := s.StartStep(ctx, runID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		if err := s.RecordStepOutcome(ctx, runID, step, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: ptr(0), FinishedAt: clk.Now(),
		}, ref); err != nil {
			t.Fatalf("succeed %s: %v", step, err)
		}
	}
	if err := s.StartStep(ctx, runID, "q", ref); err != nil {
		t.Fatalf("start q: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "q", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: ptr(9), FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("fail q: %v", err)
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"q"}`,
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	step := "a"
	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{OnlyStep: &step}); !errors.Is(err, store.ErrNothingToReopen) {
		t.Fatalf("reopen the healthy branch = %v, want ErrNothingToReopen", err)
	}
	detail := mustGetRun(t, ctx, s, runID)
	if detail.State != "failed" {
		t.Errorf("the refused reopen moved the run to %s", detail.State)
	}
}

// TestReopenWithUnknownStepIsRefused names the difference between "that step
// does not exist" and "it has nothing to reopen".
func TestReopenWithUnknownStepIsRefused(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "c")

	step := "nosuch"
	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{OnlyStep: &step}); !errors.Is(err, store.ErrStepNotInThisRun) {
		t.Fatalf("reopen --step nosuch = %v, want ErrStepNotInThisRun", err)
	}
}

// TestReopenRefusesASucceededRun: retry is for a run that has something to
// redo. A succeeded run is redone by replay, which makes a new row.
func TestReopenRefusesASucceededRun(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "")

	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{}); !errors.Is(err, store.ErrRunNotRetryable) {
		t.Fatalf("reopen a succeeded run = %v, want ErrRunNotRetryable", err)
	}
	if got := mustGetRun(t, ctx, s, runID).State; got != "succeeded" {
		t.Errorf("the refused reopen moved the run to %s", got)
	}
}

// TestReopenRefusesANonTerminalRun keeps T14 out of reach for anything but a
// finished run: a queued or running run is somebody's live work.
func TestReopenRefusesANonTerminalRun(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)

	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{}); err == nil {
		t.Fatal("reopening a queued run succeeded, want a refusal")
	}
}

// TestOldHolderCannotWriteAfterAReopen is the fence (review focus 3): the
// epoch moved, so a zombie executor still holding last generation's token is
// shut out of every write.
func TestOldHolderCannotWriteAfterAReopen(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "c")

	before := mustGetRun(t, ctx, s, runID)
	zombie := store.LeaseRef{Owner: testOwner, Epoch: before.LeaseEpoch}
	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{}); err != nil {
		t.Fatalf("reopen: %v", err)
	}

	err := s.RecordStepOutcome(ctx, runID, "d", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
	}, zombie)
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("a write at the old epoch = %v, want ErrLeaseLost", err)
	}
	if got := mustStep(t, ctx, s, runID, "d").State; got != "pending" {
		t.Errorf("the fenced write moved d to %s", got)
	}
}

// TestReopenedRunSweepsClean is AC-12's store half: fsck and the reason rule
// all hold on a database right after a reopen.
func TestReopenedRunSweepsClean(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "retrychain", retryChainSpec)
	driveChain(t, ctx, s, clk, runID, "c")

	if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{}); err != nil {
		t.Fatalf("reopen: %v", err)
	}
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Fatalf("fsck after a reopen: %+v", violations)
	}
}
