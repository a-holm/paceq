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

// stubEngine drives nothing and holds exactly one believed lease. It stands
// in for an engine that returned mid-flight, which is exactly what the real
// one does when the drain cut its context between the kill and the verdict
// write: the keeper still has the entry until release runs, and the handback
// is what clears it from the row.
type stubEngine struct {
	ref  store.LeaseRef
	held bool
	err  error

	// st forwards the handback to the real store when set.
	st *store.Store

	drains int
}

func (s *stubEngine) ExecuteRun(context.Context, string) (string, error) {
	return "", s.err
}

func (s *stubEngine) HeldLease(string) (store.LeaseRef, bool) {
	return s.ref, s.held
}

func (s *stubEngine) DrainRun(ctx context.Context, runID string, ref store.LeaseRef, code reason.Code) (bool, error) {
	s.drains++
	if s.err != nil && s.drains == 1 {
		return false, s.err
	}
	if s.st != nil {
		return s.st.DrainRun(ctx, runID, ref, code)
	}
	return false, nil
}

// storeDrainer forwards every handback to a real store under one fixed,
// believed token. It stands in for an engine whose keeper still holds the
// entry, without dragging a whole engine into the test.
type storeDrainer struct {
	st   *store.Store
	ref  store.LeaseRef
	held bool
}

func (d storeDrainer) HeldLease(string) (store.LeaseRef, bool) {
	return d.ref, d.held
}

func (d storeDrainer) DrainRun(ctx context.Context, runID string, ref store.LeaseRef, code reason.Code) (bool, error) {
	return d.st.DrainRun(ctx, runID, ref, code)
}

func newPoolFor(t *testing.T, eng runDriver, drainer leaseDrainer) *executorPool {
	t.Helper()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return &executorPool{
		eng:   eng,
		drain: drainer,
		clk:   clock.System(),
		log:   log,
		slots: make(chan struct{}, 1),
	}
}

// TestThePoolHandsBackARunItsEngineAbandonedMidFlight pins the drain failure
// of the loaded suite: the executor died after claiming and starting the step
// but before any verdict, leaving attempt 1 and an empty reason on a running
// row. Whatever the engine reported and whatever the goroutine's context
// says, the row still says running and the token still says ours, so the
// handback is owed. The background context is deliberate: the old code
// skipped the handback whenever its own context survived, which is how the
// row got stranded.
func TestThePoolHandsBackARunItsEngineAbandonedMidFlight(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	drainer := &stubEngine{ref: store.LeaseRef{Owner: "serve:test", Epoch: 1}, held: true, st: st}
	p := newPoolFor(t, &stubEngine{err: errors.New("record the verdict: context canceled")}, drainer)
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
	if detail.Run.LeaseEpoch != 2 {
		t.Errorf("lease_epoch is %d, want 2: the drained attempt stays fenced out",
			detail.Run.LeaseEpoch)
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

	drainer := &stubEngine{
		ref:  store.LeaseRef{Owner: "serve:test", Epoch: 1},
		held: true,
		err:  errors.New("injected transient write failure"),
		st:   st,
	}
	p := newPoolFor(t, &stubEngine{}, drainer)
	if err := p.handBackWhenOwed(context.Background(), runID); err != nil {
		t.Fatalf("the handback gave up: %v", err)
	}
	if drainer.drains < 2 {
		t.Errorf("%d drains, want at least 2: the first failed and the second must happen",
			drainer.drains)
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
// promise: an engine that holds nothing owes nothing, and the check must not
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

	drainer := &stubEngine{}
	p := newPoolFor(t, &stubEngine{}, drainer)
	if err := p.handBackWhenOwed(ctx, res.Run.ID); err != nil {
		t.Fatalf("a run we do not hold owes nothing, got %v", err)
	}
	if drainer.drains != 0 {
		t.Errorf("the check drained %d times for a lease it never held", drainer.drains)
	}
	detail, err := st.GetRun(ctx, res.Run.ID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the unclaimed run moved to %s", detail.Run.State)
	}
}

// TestAStaleBeliefDrainsNothing pins the fence on the drain path itself: a
// daemon that froze, lost its run to the reaper and thawed mid stop believes
// it still holds the lease, but the row disagrees inside the handback
// transaction, so nothing of the old attempt lands.
func TestAStaleBeliefDrainsNothing(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	// The reaper takes the run while our belief still names us as holder.
	clk.Advance(10 * time.Minute)
	reaped, err := st.ReapExpiredRuns(context.Background(), store.ReapOptions{})
	if err != nil {
		t.Fatalf("reap: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("reaped %+v, want the run", reaped)
	}

	drainer := &stubEngine{ref: store.LeaseRef{Owner: "serve:test", Epoch: 1}, held: true, st: st}
	p := newPoolFor(t, &stubEngine{}, drainer)
	if err := p.handBackWhenOwed(context.Background(), runID); err != nil {
		t.Fatalf("a stale handback must be quiet, got %v", err)
	}
	detail, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) || detail.Run.LeaseEpoch != 2 {
		t.Errorf("the stale drain moved the reaper's work: %+v", detail.Run)
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

// The pool keeps taking the real engine through its interfaces; these compile
// time checks pin that the production wiring never drifts away from them.
var (
	_ runDriver    = (*engine.Engine)(nil)
	_ leaseDrainer = (*engine.Engine)(nil)
)
