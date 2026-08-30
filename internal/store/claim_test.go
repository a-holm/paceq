package store_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The claim predicate is the heart of M4-02: a step is claimable if and only
// if every frozen upstream has succeeded, past its retry gate, inside the
// run's parallel cap, and the run is still ours at the fencing epoch. Every
// test here drives the real store, never a model of it.

// dagDiamondSpec is the M4 reference diamond, in spec order. load-warehouse
// and load-cache both depend on transform, and notify on both, so the two
// loads are the independent leaves a parallel run must start together.
const dagDiamondSpec = `{"max_concurrent":1,"name":"diamond","schema":"paceq.job.v1","timeout_ms":3600000,` +
	`"steps":[` +
	`{"name":"extract","run":["/bin/true"],"shell":false},` +
	`{"name":"transform","needs":["extract"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-warehouse","needs":["transform"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load-cache","needs":["transform"],"run":["/bin/true"],"shell":false},` +
	`{"name":"notify","needs":["load-warehouse","load-cache"],"run":["/bin/true"],"shell":false}` +
	`]}`

// threeRootsSpec has three steps with no edges, all immediately claimable,
// under a declared budget of two, so a test can fill the cap and prove the
// gate. The budget comes from the spec, the way every run gets one.
const threeRootsSpec = `{"max_concurrent":1,"max_parallel":2,"name":"roots","schema":"paceq.job.v1",` +
	`"timeout_ms":3600000,` +
	`"steps":[` +
	`{"name":"one","run":["/bin/true"],"shell":false},` +
	`{"name":"two","run":["/bin/true"],"shell":false},` +
	`{"name":"three","run":["/bin/true"],"shell":false}` +
	`]}`

// claimDiamondSeed seeds the diamond job, materialises one manual run and
// claims the run, returning the run's id and the lease epoch that fenced the
// steps.
func claimDiamondSeed(t *testing.T, s *store.Store) (string, int64) {
	t.Helper()
	ctx := context.Background()
	aCanonicalJob(t, s, "diamond", dagDiamondSpec)
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "diamond"})
	if err != nil {
		t.Fatalf("materialise the diamond run: %v", err)
	}
	runID := out.Run.ID
	state, epoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil || state != "running" {
		t.Fatalf("claim the diamond run: state=%q err=%v", state, err)
	}
	return runID, epoch
}

// claim seed claims the next step of a run under the owner's fencing token.
func claimSeed(t *testing.T, s *store.Store, runID string, epoch int64) (*store.ClaimedStep, error) {
	t.Helper()
	return s.ClaimNextStep(context.Background(), runID, store.LeaseRef{Owner: testOwner, Epoch: epoch})
}

func succeedStep(t *testing.T, s *store.Store, runID, name string, epoch int64) {
	t.Helper()
	err := s.RecordStepOutcome(context.Background(), runID, name, store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
	}, store.LeaseRef{Owner: testOwner, Epoch: epoch})
	if err != nil {
		t.Fatalf("succeed %s of %s: %v", name, runID, err)
	}
}

// TestClaimAdmitsInTopologicalOrder: extract has no upstream, so it is first;
// transform waits on extract, so a claim while extract runs admits nothing.
func TestClaimAdmitsInTopologicalOrder(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	c1, err := claimSeed(t, s, runID, epoch)
	if err != nil || c1 == nil || c1.Name != "extract" {
		t.Fatalf("first claim = %v err=%v, want extract", c1, err)
	}
	if c2, err := claimSeed(t, s, runID, epoch); err != nil || c2 != nil {
		t.Fatalf("second claim = %v err=%v, want nothing while extract runs", c2, err)
	}
	succeedStep(t, s, runID, "extract", epoch)

	c3, err := claimSeed(t, s, runID, epoch)
	if err != nil || c3 == nil || c3.Name != "transform" {
		t.Fatalf("claim after extract succeeds = %v err=%v, want transform", c3, err)
	}
}

// TestDiamondBothLeavesBecomeClaimable: once transform succeeds, the two
// independent leaves are both claimable in deterministic index order, each by
// its own claim; they do not wait for each other, which is the very branch a
// parallel run benefits from.
func TestDiamondBothLeavesBecomeClaimable(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	c1, err := claimSeed(t, s, runID, epoch)
	if err != nil || c1 == nil || c1.Name != "extract" {
		t.Fatalf("claim extract = %v, %v", c1, err)
	}
	succeedStep(t, s, runID, "extract", epoch)
	c2, err := claimSeed(t, s, runID, epoch)
	if err != nil || c2 == nil || c2.Name != "transform" {
		t.Fatalf("claim transform = %v, %v", c2, err)
	}
	succeedStep(t, s, runID, "transform", epoch)

	c3, err := claimSeed(t, s, runID, epoch)
	if err != nil || c3 == nil || c3.Name != "load-warehouse" {
		t.Fatalf("claim the first leaf = %v, %v", c3, err)
	}
	c4, err := claimSeed(t, s, runID, epoch)
	if err != nil || c4 == nil || c4.Name != "load-cache" {
		t.Fatalf("claim the second leaf = %v, %v", c4, err)
	}

	// notify waits on both leaves, not on either alone.
	if c5, err := claimSeed(t, s, runID, epoch); err != nil || c5 != nil {
		t.Fatalf("notify admitted while a leaf still ran: %v, %v", c5, err)
	}
}

// TestUpstreamSkippedOrFailedIsNotSucceeded: the predicate requires succeeded,
// specifically, not merely "not pending". A step whose upstream is running,
// failed, skipped or still pending stays uncleaimable.
func TestUpstreamMustBeSucceededNotMerelyDone(t *testing.T) {
	for _, tc := range []struct {
		name  string
		drive func(t *testing.T, s *store.Store, runID string, epoch int64)
	}{
		// pending: extract has not started, so a claim takes extract itself
		// rather than transform; that very choice proves transform waits.
		{"pending", func(*testing.T, *store.Store, string, int64) {
			// nothing: extract stays pending and its low index wins.
		}},
		{"running", func(t *testing.T, s *store.Store, runID string, epoch int64) {
			c, err := claimSeed(t, s, runID, epoch)
			if err != nil || c == nil || c.Name != "extract" {
				t.Fatalf("claim extract: %v %v", c, err)
			}
		}},
		{"failed", func(t *testing.T, s *store.Store, runID string, epoch int64) {
			c, err := claimSeed(t, s, runID, epoch)
			if err != nil || c == nil || c.Name != "extract" {
				t.Fatalf("claim extract: %v %v", c, err)
			}
			if err := s.RecordStepOutcome(context.Background(), runID, "extract", store.StepOutcome{
				Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit, ExitCode: ptr(1),
			}, store.LeaseRef{Owner: testOwner, Epoch: epoch}); err != nil {
				t.Fatalf("fail extract: %v", err)
			}
		}},
		{"skipped", func(t *testing.T, s *store.Store, runID string, epoch int64) {
			if err := s.RecordStepOutcome(context.Background(), runID, "extract", store.StepOutcome{
				Event: "upstream_failed", ReasonCode: reason.STEPSkippedUpstreamFailed,
			}, store.LeaseRef{Owner: testOwner, Epoch: epoch}); err != nil {
				t.Fatalf("skip extract: %v", err)
			}
		}},
	} {
		t.Run("upstream_"+tc.name, func(t *testing.T) {
			s, _ := coreStore(t)
			runID, epoch := claimDiamondSeed(t, s)
			tc.drive(t, s, runID, epoch)

			// The next claim must never be transform, whose only upstream is
			// not succeeded. With extract pending or running the claim takes
			// extract or nothing; with it failed or skipped, nothing at all.
			c, err := claimSeed(t, s, runID, epoch)
			if err != nil {
				t.Fatalf("claim with extract %s: %v", tc.name, err)
			}
			if tc.name == "pending" {
				if c == nil || c.Name != "extract" {
					t.Fatalf("claim with extract pending = %v, want extract (transform must wait)", c)
				}
			} else if c != nil {
				t.Fatalf("claim with extract %s = %+v, want nothing (transform must wait)", tc.name, c)
			}
		})
	}
	// The control proves the shape is sound: with extract actually succeeded,
	// transform is claimed instead of something else.
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)
	c1, err := claimSeed(t, s, runID, epoch)
	if err != nil || c1 == nil || c1.Name != "extract" {
		t.Fatalf("claim extract: %v %v", c1, err)
	}
	succeedStep(t, s, runID, "extract", epoch)
	c2, err := claimSeed(t, s, runID, epoch)
	if err != nil || c2 == nil || c2.Name != "transform" {
		t.Fatalf("claim transform after extract succeeded: %v %v", c2, err)
	}
}

// TestClaimRefusesWhenTheRunWasAskedToStop: a requested cancellation closes
// the loop. The predicate admits nothing the moment cancel_requested_at is
// set, so no new step can start behind a stop nobody has observed yet.
func TestClaimRefusesWhenTheRunWasAskedToStop(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	if _, err := s.RequestCancel(context.Background(), runID, "test", "the run is unneeded"); err != nil {
		t.Fatalf("request the cancellation: %v", err)
	}
	c, err := claimSeed(t, s, runID, epoch)
	if err != nil {
		t.Fatalf("claim after the cancel request: %v", err)
	}
	if c != nil {
		t.Fatalf("a step was claimed behind a requested cancellation: %+v", c)
	}
}

// TestClaimRefusesOnAStaleFencingEpoch: the run lease is the claim's fence.
// A claim carrying a stale epoch admits nothing, because the WHERE names the
// epoch and the row holds another; the result is zero rows, exactly the shape
// the acceptance criterion binds (0 rows, discard the result).
func TestClaimRefusesOnAStaleFencingToken(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	// The real token claims; a forged later token, which a replaced executor
	// would hold, must not.
	c, err := s.ClaimNextStep(context.Background(), runID, store.LeaseRef{Owner: testOwner, Epoch: epoch})
	if err != nil || c == nil || c.Name != "extract" {
		t.Fatalf("claim at the true token = %v %v", c, err)
	}
	fake, err := s.ClaimNextStep(context.Background(), runID, store.LeaseRef{Owner: testOwner, Epoch: epoch + 1})
	if err != nil {
		t.Fatalf("claim at a stale token: %v", err)
	}
	if fake != nil {
		t.Fatalf("a step was claimed at a stale token: %+v", fake)
	}
}

// TestResultWriteRefusedOnALostLease: a running step whose lease moves on
// under it records nothing, because the verdict's CAS on the epoch misses.
// This is the discard path: the caller is told the write did not land and
// throws the result away, never overwriting the successor's row.
func TestResultWriteRefusedOnALostLease(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	c, err := s.ClaimNextStep(context.Background(), runID, store.LeaseRef{Owner: testOwner, Epoch: epoch})
	if err != nil || c == nil || c.Name != "extract" {
		t.Fatalf("claim extract: %v %v", c, err)
	}
	err = s.RecordStepOutcome(context.Background(), runID, "extract", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
	}, store.LeaseRef{Owner: testOwner, Epoch: epoch + 777})
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Fatalf("verdict after the lease moved on = %v, want ErrLeaseLost", err)
	}
}

// TestParallelCapIsEnforcedByTheClaim: the run row's max_parallel is the
// COUNT gate inside the claim, so the database, not the executor, is what
// keeps a run under the budget its job declared. Fill the cap and the next
// claim admits nothing; free a slot and it admits again.
func TestParallelCapIsEnforcedByTheClaim(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	aCanonicalJob(t, s, "roots", threeRootsSpec)
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "roots"})
	if err != nil {
		t.Fatalf("materialise the roots run: %v", err)
	}
	runID := out.Run.ID
	state, epoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil || state != "running" {
		t.Fatalf("claim the roots run: %v", err)
	}
	// two roots fit; the third hits the cap.
	if c, err := claimSeed(t, s, runID, epoch); err != nil || c == nil {
		t.Fatalf("claim under the cap = %v %v", c, err)
	}
	if c, err := claimSeed(t, s, runID, epoch); err != nil || c == nil {
		t.Fatalf("claim under the cap = %v %v", c, err)
	}
	if c, err := claimSeed(t, s, runID, epoch); err != nil || c != nil {
		t.Fatalf("a third step ran over the cap of two: %v %v", c, err)
	}

	// Finish one: the freed slot admits the third again, in index order.
	err = s.RecordStepOutcome(context.Background(), runID, "one", store.StepOutcome{
		Event: "step_succeeded", ReasonCode: reason.STEPSucceeded, ExitCode: ptr(0),
	}, store.LeaseRef{Owner: testOwner, Epoch: epoch})
	if err != nil {
		t.Fatalf("succeed one: %v", err)
	}
	c, err := claimSeed(t, s, runID, epoch)
	if err != nil || c == nil || c.Name != "three" {
		t.Fatalf("claim after a slot frees = %v %v, want three", c, err)
	}
}

// TestClaimAttributionRidesInTheEvent: the claim's step.started event names
// the actor and the epoch, so a later reader of run_events can say who ran the
// step without trusting a step row that records no owner.
func TestClaimWritesOneStartedEvent(t *testing.T) {
	s, _ := coreStore(t)
	runID, epoch := claimDiamondSeed(t, s)

	if c, err := claimSeed(t, s, runID, epoch); err != nil || c == nil || c.Name != "extract" {
		t.Fatalf("claim extract: %v %v", c, err)
	}
	found := 0
	events, err := s.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the events: %v", err)
	}
	for _, ev := range events {
		if ev.Kind == "step.started" && ev.StepName == "extract" {
			found++
			if ev.Actor != testOwner {
				t.Errorf("step.started actor = %q, want %q", ev.Actor, testOwner)
			}
			if ev.ToState != "running" || ev.FromState != "pending" {
				t.Errorf("step.started transition = %s->%s, want pending->running", ev.FromState, ev.ToState)
			}
		}
	}
	if found != 1 {
		t.Fatalf("step.started events for extract = %d, want exactly 1", found)
	}
}

// genRootStepsSpec builds a job whose steps are all independent roots under a
// budget wide enough for every one of them, so a test can flood a run with
// parallel claims.
func genRootStepsSpec(n int) string {
	var b strings.Builder
	fmt.Fprintf(&b, `{"max_concurrent":1,"max_parallel":%d,"name":"roots",`, n)
	b.WriteString(`"schema":"paceq.job.v1","timeout_ms":3600000,"steps":[`)
	for i := 0; i < n; i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(fmt.Sprintf(`{"name":"r%d","run":["/bin/true"],"shell":false}`, i))
	}
	b.WriteString(`]}`)
	return b.String()
}

// TestConcurrentClaimsPartitionTheStepsExactlyOnce: many goroutines race to
// claim the steps of one run over a real file. The single-writer claim
// serialises the reservations, so every step is claimed exactly once, none is
// skipped, and no SQLITE_BUSY escapes to a caller. This is the concurrency
// invariant the acceptance criterion names.
func TestConcurrentClaimsPartitionTheStepsExactlyOnce(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	const roots = 48
	aCanonicalJob(t, s, "roots", genRootStepsSpec(roots))
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "roots"})
	if err != nil {
		t.Fatalf("materialise the roots run: %v", err)
	}
	runID := out.Run.ID
	state, epoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: testOwner})
	if err != nil || state != "running" {
		t.Fatalf("claim the run: %v", err)
	}
	ref := store.LeaseRef{Owner: testOwner, Epoch: epoch}

	const workers = 32
	start := make(chan struct{})
	claimed := make(chan *store.ClaimedStep, roots)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				c, err := s.ClaimNextStep(ctx, runID, ref)
				if err != nil {
					errs <- err
					return
				}
				if c == nil {
					return
				}
				claimed <- c
			}
		}()
	}
	close(start)
	wg.Wait()
	close(claimed)
	close(errs)

	for err := range errs {
		t.Fatalf("a concurrent claim failed: %v", err)
	}

	seen := make(map[string]int, roots)
	for c := range claimed {
		seen[c.Name]++
	}
	if len(seen) != roots {
		t.Fatalf("claimed %d distinct steps, want %d", len(seen), roots)
	}
	for name, count := range seen {
		if count != 1 {
			t.Fatalf("step %s was claimed %d times, want exactly once", name, count)
		}
	}
}
