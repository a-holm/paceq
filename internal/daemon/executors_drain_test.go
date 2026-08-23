package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The pool's exit promise, proven at the row level. These rows replay the two
// ways an executor can stop driving a run without the run having ended: the
// engine giving up with its claim still standing (the shape behind the one
// loaded-suite drain failure this file pins), and a store call that fails
// once while the handback is landing. In both, the row, not the error, is
// what decides whether work is owed.

// stubEngine drives nothing and reports what it is told. It stands in for an
// engine that returned mid-flight, which is exactly what the real one does
// when the drain cut its context between the kill and the verdict write.
type stubEngine struct {
	err error
}

func (s stubEngine) ExecuteRun(context.Context, string) (string, error) {
	return "", s.err
}

// countingStore forwards to a real store and counts the handback calls, so a
// test can prove both that a retry happened and that a finished run was not
// touched at all.
type countingStore struct {
	*store.Store
	getRuns   int
	interrupt int
	requeues  int

	failFirstGetRun int
}

func (c *countingStore) GetRun(ctx context.Context, runID string) (store.RunDetail, error) {
	c.getRuns++
	if c.failFirstGetRun > 0 {
		c.failFirstGetRun--
		return store.RunDetail{}, errors.New("injected transient read failure")
	}
	return c.Store.GetRun(ctx, runID)
}

func (c *countingStore) InterruptStepForShutdown(ctx context.Context, runID, name string, code reason.Code) error {
	c.interrupt++
	return c.Store.InterruptStepForShutdown(ctx, runID, name, code)
}

func (c *countingStore) RequeueRunAfterDrain(ctx context.Context, runID string) error {
	c.requeues++
	return c.Store.RequeueRunAfterDrain(ctx, runID)
}

func newPoolFor(t *testing.T, eng runDriver, st drainStore) *executorPool {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return &executorPool{
		st:    st,
		eng:   eng,
		clk:   clock.System(),
		log:   log,
		slots: make(chan struct{}, 1),
	}
}

// TestThePoolHandsBackARunItsEngineAbandonedMidFlight pins the drain failure
// of the loaded suite: the executor died after claiming and starting the step
// but before any verdict, leaving attempt 1 and an empty reason on a running
// row. Whatever the engine reported and whatever the goroutine's context
// says, the row still says running, so the handback is owed. The background
// context is deliberate: the old code skipped the handback whenever its own
// context survived, which is how the row got stranded.
func TestThePoolHandsBackARunItsEngineAbandonedMidFlight(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	p := newPoolFor(t, stubEngine{err: errors.New("record the verdict: context canceled")}, st)
	p.execute(context.Background(), runID)

	detail, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the run is %s, want queued; an abandoned claim must be handed back",
			detail.Run.State)
	}
	if detail.Run.DeferReason != model.DeferReasonAfterShutdown {
		t.Errorf("defer_reason is %q, want %q", detail.Run.DeferReason, model.DeferReasonAfterShutdown)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("%d steps came back, want 1", len(detail.Steps))
	}
	step := detail.Steps[0]
	if step.State != string(model.StepPending) || step.Attempt != 0 ||
		step.ReasonCode != string(reason.RUNInterruptedShutdown) {
		t.Errorf("the step is %+v, want pending, attempt 0, reason %s",
			step, reason.RUNInterruptedShutdown)
	}
	requireDrainEvents(t, st, runID)
}

// TestAHandoverThatStumblesOnceStillLands proves the bounded retry: one lost
// store call during the handback must not cost the run its handback, because
// nobody ever looks again after the daemon exits.
func TestAHandoverThatStumblesOnceStillLands(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	cs := &countingStore{Store: st, failFirstGetRun: 1}
	p := newPoolFor(t, stubEngine{}, cs)
	if err := p.handBackWhenOwed(context.Background(), runID); err != nil {
		t.Fatalf("the handback gave up: %v", err)
	}
	if cs.getRuns < 2 {
		t.Errorf("%d reads, want at least 2: the first failed and the second must happen", cs.getRuns)
	}

	detail, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the run is %s, want queued after a retried handback", detail.Run.State)
	}
	requireDrainEvents(t, st, runID)
}

// TestAFinishedRunIsNeverTouchedByTheHandbackCheck is the other edge of the
// promise: a run that is not running owes nothing, and the check must not
// write a single row to say so.
func TestAFinishedRunIsNeverTouchedByTheHandbackCheck(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	spec := `{"schema":"paceq.job.v1","name":"quiet","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["/bin/true"],"shell":false}]}`
	ctx := context.Background()
	if _, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "quiet", SpecHash: "sha256:quiet", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := st.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "quiet"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}

	cs := &countingStore{Store: st}
	p := newPoolFor(t, stubEngine{}, cs)
	if err := p.handBackWhenOwed(ctx, res.Run.ID); err != nil {
		t.Fatalf("a queued run owes nothing, got %v", err)
	}
	if cs.interrupt != 0 || cs.requeues != 0 {
		t.Errorf("the check wrote rows for a run it did not owe: interrupt=%d requeue=%d",
			cs.interrupt, cs.requeues)
	}
	detail, err := st.GetRun(ctx, res.Run.ID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the unclaimed run moved to %s", detail.Run.State)
	}
}

// requireDrainEvents asserts the event pair every clean handback leaves
// behind: one interrupted step naming the shutdown, one drained run from
// running back to queued.
func requireDrainEvents(t *testing.T, st *store.Store, runID string) {
	t.Helper()
	events, err := st.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var sawInterrupt, sawDrained bool
	for _, e := range events {
		if e.Kind == "step.interrupted" && e.ReasonCode == string(reason.RUNInterruptedShutdown) {
			sawInterrupt = true
		}
		if e.Kind == "run.drained" && e.FromState == "running" && e.ToState == "queued" {
			sawDrained = true
		}
	}
	if !sawInterrupt || !sawDrained {
		t.Fatalf("events are incomplete: interrupt=%v drained=%v in %+v",
			sawInterrupt, sawDrained, events)
	}
}

// The pool keeps taking the real engine through its interface; this compile
// time check pins that the production wiring never drifts away from it.
var _ runDriver = (*engine.Engine)(nil)
