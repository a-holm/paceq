package worker_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/worker"
)

// The pool is the M4-02 parallel executor. These tests drive a real diamond
// through it over a real SQLite file and read the timestamps back: the two
// loads must genuinely overlap, and notify must wait on both.

const diamondSpec = `{"max_concurrent":1,"name":"diamond","schema":"paceq.job.v1","timeout_ms":3600000,` +
	`"steps":[` +
	`{"name":"extract","run":["/bin/true"],"shell":false},` +
	`{"name":"transform","needs":["extract"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-warehouse","needs":["transform"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-cache","needs":["transform"],"run":["/bin/true"],"shell":false},` +
	`{"name":"notify","needs":["load-warehouse","load-cache"],"run":["/bin/true"],"shell":false}` +
	`]}`

const owner = "worker-p"

// code boxes an exit code into the pointer the outcome carries.
func code(n int) *int { return &n }

// loadSleep is how long each of the two independent loads stays in flight; it
// is the gap that makes their overlap measurable in wall time.
const loadSleep = 300 * time.Millisecond

// openedDiamond opens a real-clock store on a real file (timestamp overlap is
// measured wall time, so the clock must not be faked), seeds the diamond job,
// materialises and claims one run, and returns the live store with the run's
// lease and fencing token.
func openedDiamond(t *testing.T) (*store.Store, string, store.LeaseRef) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pool.db")
	s, err := store.Open(context.Background(), path, store.Options{Clock: clock.System()})
	if err != nil {
		t.Fatalf("open the store at %q: %v", path, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName: "diamond", SpecHash: "sha256:diamond", SpecJSON: diamondSpec, MaxConcurrent: 1,
	}); err != nil {
		t.Fatalf("seed the diamond job: %v", err)
	}
	out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{JobName: "diamond"})
	if err != nil {
		t.Fatalf("materialise the diamond run: %v", err)
	}
	runID := out.Run.ID
	state, epoch, err := s.ClaimRun(context.Background(), runID, store.LeaseInput{Owner: owner, TTL: time.Minute})
	if err != nil || state != "running" {
		t.Fatalf("claim the run: state=%q err=%v", state, err)
	}
	return s, runID, store.LeaseRef{Owner: owner, Epoch: epoch}
}

// recordingExecutor runs one claimed step to a success verdict through the
// store, sleeping the two loads so their overlap is real, and wakes the pool
// when a step finishes. It plays the runner's role behind the pool seam.
func recordingExecutor(s *store.Store, runID string, ref store.LeaseRef, bus *notify.Bus) worker.Executor {
	return func(ctx context.Context, c *store.ClaimedStep) error {
		if c.Name == "load-warehouse" || c.Name == "load-cache" {
			time.Sleep(loadSleep)
		}
		err := s.RecordStepOutcome(ctx, runID, c.Name, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: code(0),
		}, ref)
		if err == nil && bus != nil {
			bus.Notify(notify.TopicStepReady)
		}
		return err
	}
}

func readSteps(t *testing.T, s *store.Store, runID string) map[string]store.Step {
	t.Helper()
	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	out := make(map[string]store.Step, len(detail.Steps))
	for _, st := range detail.Steps {
		out[st.Name] = st
	}
	return out
}

// TestDiamondLoadsRunInParallel: the two loads overlap in time, which is the
// point of the diamond. The pool admits both after transform, runs them
// together, and notify only starts once both have finished.
func TestDiamondLoadsRunInParallel(t *testing.T) {
	s, runID, ref := openedDiamond(t)
	bus := notify.New()
	p := worker.New(s, bus, recordingExecutor(s, runID, ref, bus), clock.System(), 8)
	p.Tick = 30 * time.Millisecond

	if err := p.Run(context.Background(), runID, ref); err != nil {
		t.Fatalf("the pool did not drain the diamond: %v", err)
	}
	steps := readSteps(t, s, runID)
	for _, name := range []string{"extract", "transform", "load-warehouse", "load-cache", "notify"} {
		if steps[name].State != "succeeded" {
			t.Fatalf("step %s ended in %s, want succeeded", name, steps[name].State)
		}
	}

	wh, ca := steps["load-warehouse"], steps["load-cache"]
	if wh.StartedAt.IsZero() || ca.StartedAt.IsZero() || wh.FinishedAt.IsZero() || ca.FinishedAt.IsZero() {
		t.Fatalf("the loads are missing start/finish stamps: %+v / %+v", wh, ca)
	}
	overlap := wh.StartedAt.Before(ca.FinishedAt) && ca.StartedAt.Before(wh.FinishedAt)
	if !overlap {
		t.Fatalf("the loads did not overlap: load-warehouse [%s,%s], load-cache [%s,%s]",
			wh.StartedAt, wh.FinishedAt, ca.StartedAt, ca.FinishedAt)
	}
	nf := steps["notify"]
	if !nf.StartedAt.After(wh.FinishedAt) || !nf.StartedAt.After(ca.FinishedAt) {
		t.Fatalf("notify started before both loads finished: %s vs %s/%s", nf.StartedAt, wh.FinishedAt, ca.FinishedAt)
	}
}

// TestDiamondRunsOnTheTickerAlone: with the bus off the same diamond still
// drains; the ticker is the safety net that carries correctness when no wake
// ever arrives. It is slower by a tick per dwell, which is why the pool's
// test tick is short here.
func TestDiamondRunsOnTheTickerAlone(t *testing.T) {
	s, runID, ref := openedDiamond(t)
	p := worker.New(s, nil, recordingExecutor(s, runID, ref, nil), clock.System(), 8)
	p.Tick = 30 * time.Millisecond

	if err := p.Run(context.Background(), runID, ref); err != nil {
		t.Fatalf("the bus-less pool did not drain the diamond: %v", err)
	}
	steps := readSteps(t, s, runID)
	for _, name := range []string{"extract", "transform", "load-warehouse", "load-cache", "notify"} {
		if steps[name].State != "succeeded" {
			t.Fatalf("step %s ended in %s without the bus, want succeeded", name, steps[name].State)
		}
	}
}
