package store_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// M4-03: skip propagation and run aggregation.
//
// When a step fails with retry exhausted, its whole downstream transitive
// closure is marked skipped inside the SAME transaction as the step's own
// failure. A run can therefore never be observed sitting with a failed step
// while a step that needs its output is still pending. The skip reason says
// whether a skipped step depended on the failure directly
// (STEP_SKIPPED_UPSTREAM_FAILED) or on another skip further up
// (STEP_SKIPPED_UPSTREAM_SKIPPED), and every skip's reason_data points back
// at the step that actually failed.

// aDagRun records a job whose spec has the given steps and materialises one
// manual run of it, returning the run id.
func aDagRun(t *testing.T, s *store.Store, name, specJSON string) string {
	t.Helper()
	aCanonicalJob(t, s, name, specJSON)
	out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: name,
	})
	if err != nil {
		t.Fatalf("materialise %s: %v", name, err)
	}
	return out.Run.ID
}

func countStepKind(events []store.RunEvent, step, kind string) int {
	n := 0
	for _, e := range events {
		if e.StepName == step && e.Kind == kind {
			n++
		}
	}
	return n
}

const chainSkipSpec = `{"name":"chain","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"a","run":["/bin/true"],"shell":false},` +
	`{"name":"b","needs":["a"],"run":["/bin/true"],"shell":false},` +
	`{"name":"c","needs":["b"],"run":["/bin/true"],"shell":false},` +
	`{"name":"d","needs":["c"],"run":["/bin/true"],"shell":false},` +
	`{"name":"e","needs":["d"],"run":["/bin/true"],"shell":false}]}`

const diamondSkipSpec = `{"name":"graph","max_concurrent":2,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"x","run":["/bin/true"],"shell":false},` +
	`{"name":"y","needs":["x"],"run":["/bin/true"],"shell":false},` +
	`{"name":"z","needs":["x"],"run":["/bin/true"],"shell":false},` +
	`{"name":"w","needs":["y","z"],"run":["/bin/true"],"shell":false}]}`

const isolatedSkipSpec = `{"name":"isolated","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"a","run":["/bin/true"],"shell":false},` +
	`{"name":"c","needs":["a"],"run":["/bin/true"],"shell":false},` +
	`{"name":"p","run":["/bin/true"],"shell":false},` +
	`{"name":"q","needs":["p"],"run":["/bin/true"],"shell":false}]}`

const pairSkipSpec = `{"name":"pair","max_concurrent":2,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"a","run":["/bin/true"],"shell":false},` +
	`{"name":"b","run":["/bin/true"],"shell":false},` +
	`{"name":"c","needs":["a"],"run":["/bin/true"],"shell":false}]}`

// TestSkipPropagationClosesTheWholeChain is the depth five case: a fails,
// every step that transitively needs a is skipped, never left pending and
// never mislabelled an error.
func TestSkipPropagationClosesTheWholeChain(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "chain", chainSkipSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "a", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start a: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "a", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(9),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("fail a: %v", err)
	}

	detail := mustGetRun(t, ctx, s, runID)
	for _, step := range detail.Steps {
		want := map[string]string{
			"a": "failed", "b": "skipped", "c": "skipped", "d": "skipped", "e": "skipped",
		}[step.Name]
		if step.State != want {
			t.Errorf("step %s = %s, want %s", step.Name, step.State, want)
		}
	}

	if got := mustStep(t, ctx, s, runID, "b").ReasonCode; got != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("b reason = %q, want %q", got, reason.STEPSkippedUpstreamFailed)
	}
	for _, indirect := range []string{"c", "d", "e"} {
		if got := mustStep(t, ctx, s, runID, indirect).ReasonCode; got != string(reason.STEPSkippedUpstreamSkipped) {
			t.Errorf("%s reason = %q, want %q", indirect, got, reason.STEPSkippedUpstreamSkipped)
		}
	}

	for _, name := range []string{"b", "c", "d", "e"} {
		step := mustStep(t, ctx, s, runID, name)
		var data map[string]any
		if err := json.Unmarshal([]byte(step.ReasonData), &data); err != nil {
			t.Fatalf("%s reason_data is not an object: %q", name, step.ReasonData)
		}
		if data["upstream"] != "a" {
			t.Errorf("%s reason_data.upstream = %v, want a", name, data["upstream"])
		}
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, name := range []string{"b", "c", "d", "e"} {
		if got := countStepKind(events, name, "step.skipped"); got != 1 {
			t.Errorf("%d step.skipped events for %s, want exactly 1 (the closure must write once)",
				got, name)
		}
	}
}

// TestSkipPropagationOnADiamondMarksEachReachedStepExactlyOnce pins the
// UNION shape: w is reached through both y and z, and the closure marks it
// once, never twice.
func TestSkipPropagationOnADiamondMarksEachReachedStepExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "graph", diamondSkipSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "x", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start x: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "x", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedSignal, FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("fail x: %v", err)
	}

	if got := mustStep(t, ctx, s, runID, "y").ReasonCode; got != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("y reason = %q, want %q", got, reason.STEPSkippedUpstreamFailed)
	}
	if got := mustStep(t, ctx, s, runID, "z").ReasonCode; got != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("z reason = %q, want %q", got, reason.STEPSkippedUpstreamFailed)
	}
	if got := mustStep(t, ctx, s, runID, "w").ReasonCode; got != string(reason.STEPSkippedUpstreamSkipped) {
		t.Errorf("w reason = %q, want %q", got, reason.STEPSkippedUpstreamSkipped)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if got := countStepKind(events, "w", "step.skipped"); got != 1 {
		t.Errorf("%d step.skipped events for w, want exactly 1 (the UNION must dedupe)", got)
	}
}

// TestSkipPropagationSparesAnIndependentBranch: a failure in one branch never
// touches the other. p and q are not downstream of a, so they stay pending
// and can still run to success; the run still fails because a failed.
func TestSkipPropagationSparesAnIndependentBranch(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "isolated", isolatedSkipSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "a", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start a: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "a", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(1),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("fail a: %v", err)
	}

	if got := mustStep(t, ctx, s, runID, "c").State; got != "skipped" {
		t.Errorf("c = %s, want skipped (it is downstream of a)", got)
	}
	if got := mustStep(t, ctx, s, runID, "p").State; got != "pending" {
		t.Errorf("p = %s, want pending (untouched by the other branch)", got)
	}
	if got := mustStep(t, ctx, s, runID, "q").State; got != "pending" {
		t.Errorf("q = %s, want pending (untouched by the other branch)", got)
	}

	// The independent branch still completes.
	if err := s.StartStep(ctx, runID, "p", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start p: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "p", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("succeed p: %v", err)
	}
	if err := s.StartStep(ctx, runID, "q", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start q: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "q", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("succeed q: %v", err)
	}

	if got := mustStep(t, ctx, s, runID, "p").State; got != "succeeded" {
		t.Errorf("p = %s, want succeeded", got)
	}
	if got := mustStep(t, ctx, s, runID, "q").State; got != "succeeded" {
		t.Errorf("q = %s, want succeeded", got)
	}

	state, err := s.FinishRun(ctx, runID, store.LeaseRef{Owner: testOwner, Epoch: 1}, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"a"}`,
	})
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	if state != "failed" {
		t.Errorf("run ended %s, want failed", state)
	}
}

// TestReconcileRunStatesConvergesAndIsIdempotent is the crash backstop: a run
// stranded as running (its executor died before the run verdict) is driven to
// the state its terminal steps aggregate to by one pass, and a second pass
// changes nothing.
func TestReconcileRunStatesConvergesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "conv", `{"name":"conv","max_concurrent":1,"timeout_ms":3600000,`+
		`"schema":"paceq.job.v1","steps":[`+
		`{"name":"one","run":["/bin/true"],"shell":false},`+
		`{"name":"two","needs":["one"],"run":["/bin/true"],"shell":false}]}`)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner, TTL: time.Duration(500) * time.Millisecond}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	for _, name := range []string{"one", "two"} {
		if err := s.StartStep(ctx, runID, name, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
	}
	out := store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
		FinishedAt: clk.Now(),
	}
	if err := s.RecordStepOutcome(ctx, runID, "one", out, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("succeed one: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "two", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(2),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("fail two: %v", err)
	}

	// Both steps are terminal but no FinishRun happened: the run is a stuck
	// "running" that recovery must converge. Its lease was allowed to lapse.
	clk.Advance(2 * time.Second)
	if got := mustGetRun(t, ctx, s, runID).Run.State; got != "running" {
		t.Fatalf("precondition run = %s, want running", got)
	}

	if err := s.ReconcileRunStates(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	run := mustGetRun(t, ctx, s, runID).Run
	if run.State != "failed" {
		t.Errorf("after reconcile run = %s, want failed", run.State)
	}
	if run.ReasonCode != string(reason.RUNFailedStep) {
		t.Errorf("reason_code = %q, want %q", run.ReasonCode, reason.RUNFailedStep)
	}
	if run.FinishedAt.IsZero() {
		t.Error("finished_at not set by reconcile")
	}
	if run.LeaseOwner != "" || !run.LeaseExpiresAt.IsZero() {
		t.Errorf("lease not cleared: owner %q", run.LeaseOwner)
	}

	// A second pass is a no-op.
	eventsBefore, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	clk.Advance(time.Minute)
	if err := s.ReconcileRunStates(ctx); err != nil {
		t.Fatalf("reconcile again: %v", err)
	}
	eventsAfter, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events again: %v", err)
	}
	if len(eventsAfter) != len(eventsBefore) {
		t.Errorf("events grew from %d to %d on the second reconcile", len(eventsBefore), len(eventsAfter))
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	for _, v := range violations {
		t.Errorf("fsck: %s on %s: %s", v.Check, v.Subject, v.Detail)
	}
	if len(violations) > 0 {
		t.Fatalf("fsck found %d violations after convergence", len(violations))
	}
}

// TestFsckI8DetectsARunningStepWithAnUnmetNeed lives in dag_fsck_internal_test.go
// (package store) because planting the row needs the store's writer.

// TestSkipPropagationLetsARunningSiblingFinish: when a step fails while an
// unrelated step is already running, the running step is allowed to finish.
// The closure closes what the failure closes; it never touches the live sibling.
func TestSkipPropagationLetsARunningSiblingFinish(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "pair", pairSkipSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Two siblings in flight at once: the serial engine never drives this,
	// but the write model must not forbid it either.
	if err := s.StartStep(ctx, runID, "a", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start a: %v", err)
	}
	if err := s.StartStep(ctx, runID, "b", store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("start b: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "a", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(5),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("fail a: %v", err)
	}
	if got := mustStep(t, ctx, s, runID, "c").State; got != "skipped" {
		t.Errorf("c = %s, want skipped", got)
	}
	if got := mustStep(t, ctx, s, runID, "b").State; got != "running" {
		t.Errorf("b = %s, want still running (the live sibling was not closed)", got)
	}
	if err := s.RecordStepOutcome(ctx, runID, "b", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
		FinishedAt: clk.Now(),
	}, store.LeaseRef{Owner: testOwner, Epoch: 1}); err != nil {
		t.Fatalf("succeed b: %v", err)
	}
	if got := mustStep(t, ctx, s, runID, "b").State; got != "succeeded" {
		t.Errorf("b = %s, want succeeded", got)
	}
}
