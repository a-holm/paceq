package store_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Issue #213: three writers land a step's verdict, and the row has to mean
// the same thing whichever one wrote it. The reaper is the writer that used
// to keep its own rules, and the spool committer is the one that used to
// drop the retry policy.

// A chain with no retry block anywhere, so every step is born with one
// attempt and the first failure closes the graph below it.
const reapChainSpec = `{"name":"reapchain","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"extract","run":["/bin/true"],"shell":false},` +
	`{"name":"transform","needs":["extract"],"run":["/bin/true"],"shell":false},` +
	`{"name":"load","needs":["transform"],"run":["/bin/true"],"shell":false}]}`

// A run whose first step carries a retry budget and whose second step needs
// nothing at all: the independent one is what a run-level close-out reaches
// with no failure anywhere above it.
const reapRootlessSpec = `{"name":"rootless","max_concurrent":1,"timeout_ms":3600000,` +
	`"schema":"paceq.job.v1","steps":[` +
	`{"name":"a","run":["/bin/true"],"shell":false,` +
	`"retry":{"max":3,"backoff":"fixed","initial_ms":1000,"max_delay_ms":30000,"jitter":"none"}},` +
	`{"name":"b","run":["/bin/true"],"shell":false},` +
	`{"name":"c","needs":["a"],"run":["/bin/true"],"shell":false}]}`

// outWaitTheLease moves the fake clock past any lease a test claimed with,
// plus the skew the reaper deliberately waits out on top.
func outWaitTheLease(clk interface{ Advance(time.Duration) }) {
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
}

// A. A step the reaper parks back at pending never reached a finish, so it
// must carry neither stamp. The number the unguarded writer left behind was
// the crash-detection latency, lease TTL plus skew plus the sweep's tick, and
// `paceq run show --json` reported it as the step's duration.
func TestAReapedStepBackAtPendingCarriesNoFinishStamp(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryableQueuedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", ref("doomed", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	outWaitTheLease(clk)

	if _, err := s.ReapExpiredRuns(ctx, store.ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != string(model.StepPending) {
		t.Fatalf("state = %s, want pending: the step has a budget left", step.State)
	}
	if !step.FinishedAt.IsZero() {
		t.Errorf("finished_at = %s on a pending step, want none", step.FinishedAt)
	}
	if step.DurationMS != 0 {
		t.Errorf("duration_ms = %d on a pending step, want none: that is how long the crash took to notice, not work",
			step.DurationMS)
	}
	assertFsckClean(t, ctx, s)
}

// The other half of the same rule, and the guard against over-correcting: a
// step the reaper closes terminally really did end, so both stamps stay and
// the duration is measured from its own start.
func TestAReapedStepClosedTerminallyKeepsItsStamps(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "reapchain", reapChainSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "extract", ref("doomed", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	startedAt := mustStep(t, ctx, s, runID, "extract").StartedAt
	outWaitTheLease(clk)
	reapedAt := clk.Now()

	if _, err := s.ReapExpiredRuns(ctx, store.ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "extract")
	if step.State != string(model.StepFailed) {
		t.Fatalf("state = %s, want failed: one attempt, and it is spent", step.State)
	}
	if !step.FinishedAt.Equal(reapedAt) {
		t.Errorf("finished_at = %s, want the reap instant %s", step.FinishedAt, reapedAt)
	}
	if want := reapedAt.Sub(startedAt).Milliseconds(); step.DurationMS != want {
		t.Errorf("duration_ms = %d, want %d measured from started_at", step.DurationMS, want)
	}
}

// B. The reaper lands a step on failed inside the requeue arm, and the
// closure of that failure belongs to the same transaction. Until it did,
// `paceq run show` advertised a queued run whose graph could never move:
// extract failed, transform and load still pending.
func TestTheReaperClosesTheDownstreamOfAStepItFails(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "reapchain", reapChainSpec)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "extract", ref("doomed", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	outWaitTheLease(clk)

	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	// The lost step spends the only attempt it had, so the closure ends the
	// whole graph and the run takes the verdict its steps aggregate to
	// rather than being offered to a holder with nothing to run.
	if len(reaped) != 1 || reaped[0].State != string(model.RunFailed) {
		t.Fatalf("the sweep answered %+v, want the run closed as failed", reaped)
	}

	extract := mustStep(t, ctx, s, runID, "extract")
	if extract.State != string(model.StepFailed) ||
		extract.ReasonCode != string(reason.STEPFailedExecutorLost) {
		t.Fatalf("extract came back %s/%q, want failed under %q",
			extract.State, extract.ReasonCode, reason.STEPFailedExecutorLost)
	}

	transform := mustStep(t, ctx, s, runID, "transform")
	if transform.State != string(model.StepSkipped) {
		t.Errorf("transform is %s, want skipped: it needs a step that failed", transform.State)
	}
	if transform.ReasonCode != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("transform reads %q, want %q", transform.ReasonCode, reason.STEPSkippedUpstreamFailed)
	}
	if !strings.Contains(transform.ReasonData, `"upstream":"extract"`) {
		t.Errorf("transform reason_data = %s, want the failed step named", transform.ReasonData)
	}

	load := mustStep(t, ctx, s, runID, "load")
	if load.State != string(model.StepSkipped) {
		t.Errorf("load is %s, want skipped", load.State)
	}
	if load.ReasonCode != string(reason.STEPSkippedUpstreamSkipped) {
		t.Errorf("load reads %q, want %q: its own upstream was skipped, not failed",
			load.ReasonCode, reason.STEPSkippedUpstreamSkipped)
	}
	// The failure and its closure are one write, so there is no commit at
	// which the graph disagrees with itself.
	assertFsckClean(t, ctx, s)
}

// C. The same failing attempt, committed from the spool instead of watched,
// takes the retry policy the run froze. Without it the step is runnable the
// millisecond the verdict lands, and a step failing against a service that is
// down re-fires at once for exactly the attempts a crash interrupted.
func TestASpoolCommittedFailureTakesThePolicyBackoff(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryingRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", ref("doomed", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	clk.Advance(time.Second)
	finishedAt := clk.Now()
	// The executor died between the child's exit and its verdict
	// transaction. The shim's file is the whole story of the attempt.
	outWaitTheLease(clk)

	exit := 1
	if err := s.CommitSpooledOutcome(ctx, store.SpoolOutcome{
		RunID:      runID,
		Step:       "build",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome: store.StepOutcome{
			Event:         "step_failed",
			ReasonCode:    reason.STEPFailedNonzeroExit,
			ExitCode:      &exit,
			FinishedAt:    finishedAt,
			OutcomeSource: "spool",
		},
	}); err != nil {
		t.Fatalf("CommitSpooledOutcome: %v", err)
	}

	step := mustStep(t, ctx, s, runID, "build")
	if step.State != string(model.StepPending) {
		t.Fatalf("state = %s, want pending: three retries are left", step.State)
	}
	due := finishedAt.Add(2 * time.Second)
	if !step.NextAttemptAt.Equal(due) {
		t.Errorf("next_attempt_at = %s, want %s: the frozen policy is fixed 2s with no jitter",
			step.NextAttemptAt, due)
	}
	if step.ReasonCode != string(reason.STEPRetryScheduled) {
		t.Errorf("reason_code = %q, want %q, the code the live path writes for the same exit",
			step.ReasonCode, reason.STEPRetryScheduled)
	}
	for _, key := range []string{`"backoff_ms":2000`, `"next_attempt_at":` + i64(due.UnixMilli())} {
		if !strings.Contains(step.ReasonData, key) {
			t.Errorf("reason_data = %s, want it to carry %s", step.ReasonData, key)
		}
	}
	// The rest of recovery: the dead lease goes, and the parked step waits
	// out its backoff in a database the sweep has nothing to say about.
	if _, err := s.ReapExpiredRuns(ctx, store.ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	assertFsckClean(t, ctx, s)
}

// D. What a run-level close-out reaches is a step with no failed ancestor:
// nothing it needs failed, the run simply ended around it. Calling that
// STEP_SKIPPED_UPSTREAM_FAILED is the same lie #205 took out of the engine's
// own sweep, under a different ending.
func TestTheReaperNamesARootlessSkipForWhatItIs(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aDagRun(t, s, "rootless", reapRootlessSpec)

	// Two crashes with a ceiling of one: the first is absorbed, the second
	// trips the quarantine and closes the run.
	for i := 0; i < 2; i++ {
		if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
			t.Fatalf("ClaimRun %d: %v", i, err)
		}
		if err := s.StartStep(ctx, runID, "a", ref("doomed", int64(2*i+1))); err != nil {
			t.Fatalf("StartStep %d: %v", i, err)
		}
		outWaitTheLease(clk)
		reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{MaxCrashCount: 1})
		if err != nil {
			t.Fatalf("ReapExpiredRuns %d: %v", i, err)
		}
		if len(reaped) != 1 {
			t.Fatalf("sweep %d answered %+v, want the run", i, reaped)
		}
		clk.Advance(store.DefaultRequeueBackoff)
	}

	run := mustGetRun(t, ctx, s, runID)
	if run.Run.State != string(model.RunFailed) || run.Run.ReasonCode != string(reason.RUNPoisoned) {
		t.Fatalf("the run is %s/%q, want failed under %q",
			run.Run.State, run.Run.ReasonCode, reason.RUNPoisoned)
	}

	// c hangs off the failed step, so it reads the upstream code and names
	// the step that failed.
	c := mustStep(t, ctx, s, runID, "c")
	if c.ReasonCode != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("c reads %q, want %q", c.ReasonCode, reason.STEPSkippedUpstreamFailed)
	}
	if !strings.Contains(c.ReasonData, `"upstream":"a"`) {
		t.Errorf("c reason_data = %s, want the failed step named", c.ReasonData)
	}

	// b needs nothing. Nothing upstream of it failed, because it has no
	// upstream at all.
	b := mustStep(t, ctx, s, runID, "b")
	if b.State != string(model.StepSkipped) {
		t.Fatalf("b is %s, want skipped: a terminal run has no open step", b.State)
	}
	if b.ReasonCode != string(reason.STEPSkippedRunAbandoned) {
		t.Errorf("b reads %q, want %q: it depends on nothing, so nothing it needs failed",
			b.ReasonCode, reason.STEPSkippedRunAbandoned)
	}
}

// assertFsckClean holds the sweep against a database one of the step writers
// just produced. I13 in particular now refuses a pending step with a finish
// stamp, so a writer that fabricates one is caught here as well as on the row.
func assertFsckClean(t *testing.T, ctx context.Context, s *store.Store) {
	t.Helper()
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %+v", violations)
	}
}
