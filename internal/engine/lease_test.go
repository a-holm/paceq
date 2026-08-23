package engine_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The renewal goroutine and self fencing. These tests run real processes on
// the real clock with short leases: a frozen worker is an executor whose
// renewals never started, which is what a 61 second stop is to production.

// leaseFixture is one shared store and two engines over it. The first engine
// is the worker under test; the second plays the rest of the world.
type leaseFixture struct {
	t     *testing.T
	Store *store.Store
	A     *engine.Engine
	B     *engine.Engine
}

func newLeaseFixture(t *testing.T) *leaseFixture {
	t.Helper()

	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	s, err := store.Open(context.Background(), filepath.Join(stateDir, "state.db"), store.Options{})
	if err != nil {
		t.Fatalf("open the shared store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close the shared store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return &leaseFixture{
		t:     t,
		Store: s,
		A: &engine.Engine{
			Store:    s,
			LogRoot:  logsink.NewRoot(stateDir),
			Clock:    clock.System(),
			Owner:    "frozen-worker",
			LeaseTTL: 300 * time.Millisecond,
		},
		B: &engine.Engine{
			Store:              s,
			LogRoot:            logsink.NewRoot(stateDir),
			Clock:              clock.System(),
			Owner:              "successor",
			PollInterval:       10 * time.Millisecond,
			ClockSkewAllowance: 100 * time.Millisecond,
			RequeueBackoff:     50 * time.Millisecond,
		},
	}
}

func (f *leaseFixture) queuedRun(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	spec := `{"schema":"paceq.job.v1","name":"sleepy","max_concurrent":1,` +
		`"steps":[{"name":"work","run":["` + f.sleepCmd() + `","sleep","2s"],"shell":false}]}`
	if _, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "sleepy",
		SpecHash: "sha256:sleepy",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	version, err := f.Store.CurrentJobVersion(ctx, "sleepy")
	if err != nil {
		t.Fatalf("read the version: %v", err)
	}
	// The run and its step both carry an attempt budget: a spent budget
	// sends a reaped run to failed, and this fixture needs the other arm,
	// the requeue that hands the run to a new holder.
	out, err := f.Store.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "sleepy",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		MaxAttempts:  3,
		Steps:        []store.NewStep{{Name: "work", MaxAttempts: 3}},
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return out.ID
}

func (f *leaseFixture) sleepCmd() string { return fakecmd(f.t) }

// waitForStepState polls until the named step reaches the state, or fails the
// test inside the bound.
func waitForStepState(t *testing.T, s *store.Store, runID, name, want string) store.RunDetail {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		detail, err := s.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("read the run: %v", err)
		}
		for _, step := range detail.Steps {
			if step.Name == name && step.State == want {
				return detail
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("step %s of run %s never reached %s within the bound", name, runID, want)
	return store.RunDetail{}
}

func waitForRunState(t *testing.T, s *store.Store, runID, want string) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		detail, err := s.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("read the run: %v", err)
		}
		if detail.Run.State == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("run %s never reached %s within the bound", runID, want)
}

// TestAFrozenWorkerCannotOverwriteItsSuccessor is the issue's named scenario:
// the executor freezes mid step, the reaper hands the run to a successor, the
// frozen executor wakes and tries to write its verdict anyway. The verdict
// must be refused, discarded as an event, and the successor's world left
// standing.
func TestAFrozenWorkerCannotOverwriteItsSuccessor(t *testing.T) {
	f := newLeaseFixture(t)
	runID := f.queuedRun(t)

	// The frozen worker claims and blocks inside its step. No renewals are
	// running for it, which is precisely what a freeze is.
	doneA := make(chan error, 1)
	go func() {
		_, err := f.A.ExecuteRun(context.Background(), runID)
		doneA <- err
	}()
	waitForStepState(t, f.Store, runID, "work", "running")

	// Time passes; well past the ttl plus the skew the reaper takes the run
	// and a successor finishes it. The extra margin over the arithmetic
	// minimum is scheduling room, so a loaded machine cannot flake here.
	time.Sleep(900 * time.Millisecond)
	reaped, err := f.B.ReapExpiredRuns(context.Background())
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 1 || reaped[0].ID != runID {
		t.Fatalf("reaped %+v, want the sleeping run", reaped)
	}
	// The requeue backoff passes before the run is due again.
	time.Sleep(150 * time.Millisecond)
	stateB, err := f.B.ExecuteRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("the successor failed: %v", err)
	}
	if stateB != string(model.RunSucceeded) {
		t.Fatalf("the successor ended the run %s, want succeeded", stateB)
	}

	// The frozen worker wakes, tries to record its verdict and must learn it
	// lost the run rather than write over the successor.
	select {
	case err := <-doneA:
		if !errors.Is(err, store.ErrLeaseLost) {
			t.Fatalf("the frozen worker finished with %v, want ErrLeaseLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the frozen worker never came back")
	}

	detail, err := f.Store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back the run: %v", err)
	}
	if detail.Run.State != string(model.RunSucceeded) || detail.Run.ReasonCode != string(reason.RUNSucceeded) {
		t.Errorf("the run ended %s/%s, want the successor's clean success",
			detail.Run.State, detail.Run.ReasonCode)
	}

	events, err := f.Store.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var discarded bool
	for _, e := range events {
		if e.Kind == "run.result_discarded" && strings.Contains(e.DetailJSON, `"lease_epoch"`) {
			discarded = true
		}
		if e.Kind == "run.succeeded" && e.Actor == "frozen-worker" {
			t.Errorf("the frozen worker wrote a success event: %+v", e)
		}
	}
	if !discarded {
		t.Errorf("no run.result_discarded event in %+v", events)
	}
}

// TestARenewalKeepsTheLeaseWhileTheWorkBlocks pins the reason the renewal has
// its own goroutine: a long running step must not cost its executor the lease.
// With the ttl at 900ms and the work blocked far longer than that, the only
// way the run survives is renewal from the side.
func TestARenewalKeepsTheLeaseWhileTheWorkBlocks(t *testing.T) {
	f := newLeaseFixture(t)
	runID := f.queuedRun(t)

	hbCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = f.A.RunLeaseRenewals(hbCtx) }()

	doneA := make(chan error, 1)
	go func() {
		_, err := f.A.ExecuteRun(context.Background(), runID)
		doneA <- err
	}()
	waitForStepState(t, f.Store, runID, "work", "running")

	// Well past the original expiry, the lease must still be alive and
	// growing: proof the renewal ran while the work was blocked.
	deadline := time.Now().Add(2 * time.Second)
	var expires time.Time
	for time.Now().Before(deadline) {
		detail, err := f.Store.GetRun(context.Background(), runID)
		if err != nil {
			t.Fatalf("read the run: %v", err)
		}
		expires = detail.Run.LeaseExpiresAt
		if time.Now().Add(-500*time.Millisecond).Before(expires) && detail.Run.LeaseEpoch == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if time.Now().After(expires) {
		t.Fatalf("the lease expired at %s while the step was still blocked", expires)
	}

	// And the reaper, looking at the same moment, takes nothing.
	taken, err := f.B.ReapExpiredRuns(context.Background())
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(taken) != 0 {
		t.Fatalf("the reaper took %+v while the holder was renewing", taken)
	}

	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("the blocked run failed anyway: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run never finished")
	}
	stop()
	waitForRunState(t, f.Store, runID, "succeeded")
}

// TestACancelRidesTheRenewalAnswer proves there is no second cancellation
// channel: the request lands in the database, the next renewal carries it
// back, and the owner kills the process group and commits cancelled inside
// one renewal interval.
func TestACancelRidesTheRenewalAnswer(t *testing.T) {
	f := newLeaseFixture(t)
	runID := f.queuedRun(t)

	hbCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() { _ = f.A.RunLeaseRenewals(hbCtx) }()

	doneA := make(chan error, 1)
	go func() {
		_, err := f.A.ExecuteRun(context.Background(), runID)
		doneA <- err
	}()
	waitForStepState(t, f.Store, runID, "work", "running")

	started := time.Now()
	if _, err := f.Store.RequestCancel(context.Background(), runID, "cli:1000", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	waitForRunState(t, f.Store, runID, "cancelled")
	took := time.Since(started)
	if took > 3*time.Second {
		t.Fatalf("the cancel took %s to observe, want it inside a couple of renewal ticks", took)
	}

	select {
	case err := <-doneA:
		if err != nil {
			t.Fatalf("ExecuteRun reported %v on a clean cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteRun never returned after the cancel")
	}
	stop()
}
