package engine_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spool"
	"github.com/a-holm/paceq/internal/store"
)

// Recover's spool half (issue #39): what a dead attempt's shim wrote is what
// really happened, and recovery commits it instead of assuming the worst.
// The two tests here carry the two fates of a running step after a crash:
// a result on file, and nothing on file.

type recoveryWorld struct {
	t        *testing.T
	store    *store.Store
	eng      *engine.Engine
	clk      *clock.Fake
	spoolDir string
	runID    string
}

// newRecoveryWorld seeds one claimed run with one running step. retryMax is
// the step's retry budget: a row that has to prove a verdict did not buy a
// further attempt needs room for one.
func newRecoveryWorld(t *testing.T, retryMax int) *recoveryWorld {
	t.Helper()

	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	spoolDir := filepath.Join(dir, "spool", "attempts")
	if err := os.MkdirAll(filepath.Join(stateDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(spoolDir, 0o700); err != nil {
		t.Fatal(err)
	}

	clk := clock.NewFake(time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC))
	s, err := store.Open(context.Background(), filepath.Join(stateDir, "state.db"), store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	retry := ""
	if retryMax > 0 {
		retry = fmt.Sprintf(`,"retry":{"max":%d}`, retryMax)
	}
	spec := `{"schema":"paceq.job.v1","name":"spooled","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false` + retry + `}]}`
	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "spooled",
		SpecHash: "sha256:spooled",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "spooled", Actor: "test",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, _, err := s.ClaimRun(context.Background(), res.Run.ID,
		store.LeaseInput{Owner: "exec-doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(context.Background(), res.Run.ID, "only",
		store.LeaseRef{Owner: "exec-doomed", Epoch: 1}); err != nil {
		t.Fatalf("start: %v", err)
	}

	eng := &engine.Engine{
		Store:    s,
		StateDir: stateDir,
		LogRoot:  logsink.NewRoot(stateDir),
		Clock:    clk,
		Owner:    "recovery",
		SpoolDir: spoolDir,
	}
	return &recoveryWorld{t: t, store: s, eng: eng, clk: clk, spoolDir: spoolDir, runID: res.Run.ID}
}

func TestRecoverCommitsASpooledVerdict(t *testing.T) {
	w := newRecoveryWorld(t, 0)

	if err := spool.WriteResult(w.spoolDir, spool.Result{
		V:          spool.Version,
		RunID:      w.runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome:    spool.OutcomeSucceeded,
		ExitCode:   0,
		EndedAt:    w.clk.Now().UnixMilli(),
	}); err != nil {
		t.Fatalf("write the spool result: %v", err)
	}
	w.clk.Advance(2 * time.Minute) // the executor is dead; the lease lapsed

	state, err := w.eng.Recover(context.Background(), w.runID)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if state != "queued" {
		t.Fatalf("state = %s, want the run requeued", state)
	}
	detail, err := w.store.GetRun(context.Background(), w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Steps[0].State != "succeeded" {
		t.Fatalf("step state = %s, want succeeded", detail.Steps[0].State)
	}
	if detail.Steps[0].OutcomeSource != "spool" {
		t.Fatalf("outcome_source = %q, want spool", detail.Steps[0].OutcomeSource)
	}
	// The consumed file is gone: the attempt is settled, the letterbox is
	// empty.
	if _, err := os.Stat(filepath.Join(w.spoolDir,
		spool.FileName(w.runID, "only", 1))); !os.IsNotExist(err) {
		t.Fatalf("the consumed result survived recovery: %v", err)
	}
}

// Without a result file, recovery can only assume: STEP_FAILED_EXECUTOR_LOST,
// named as reconciled on the row.
func TestRecoverWithoutASpoolAssumes(t *testing.T) {
	w := newRecoveryWorld(t, 0)
	w.clk.Advance(2 * time.Minute)

	if _, err := w.eng.Recover(context.Background(), w.runID); err != nil {
		t.Fatalf("recover: %v", err)
	}
	detail, err := w.store.GetRun(context.Background(), w.runID)
	if err != nil {
		t.Fatal(err)
	}
	if detail.Steps[0].State != "failed" {
		t.Fatalf("step state = %s, want failed", detail.Steps[0].State)
	}
	if detail.Steps[0].ReasonCode != "STEP_FAILED_EXECUTOR_LOST" {
		t.Fatalf("reason = %s, want the executor-lost code", detail.Steps[0].ReasonCode)
	}
	if detail.Steps[0].OutcomeSource != "reconciled" {
		t.Fatalf("outcome_source = %q, want reconciled", detail.Steps[0].OutcomeSource)
	}
}

// The window the spool exists to close, walked with a cancellation in it
// (#204): an operator cancels a running step, the executor answers by
// killing the process group, and the executor dies before it can record the
// verdict it just read. What recovery commits from the file must be the
// verdict the live executor was holding, not a different story about the
// same signal.
func TestACancelledAttemptRecoversAsTheCancellationItWas(t *testing.T) {
	// Both retry budgets, because the wrong verdict costs something
	// different under each: with attempts to spare a failure buys one and
	// the cancelled work runs again, and with none left the step lands
	// failed, which outranks cancelled in the run's own fold (I10).
	for _, retryMax := range []int{3, 0} {
		t.Run(fmt.Sprintf("retry max %d", retryMax), func(t *testing.T) {
			recoverACancelledAttempt(t, retryMax)
		})
	}
}

func recoverACancelledAttempt(t *testing.T, retryMax int) {
	w := newRecoveryWorld(t, retryMax)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	live, err := runner.SpawnViaShim(ctx, runner.Spec{
		Argv:    []string{"/bin/sh", "-c", "sleep 60"},
		Timeout: 30 * time.Second,
		// Room for the shim to write its result before cmd.WaitDelay
		// gives up on it: this row is about what the file says, so the
		// row must not race the file into existence.
		KillGrace: 5 * time.Second,
		Ctx:       runner.RunContext{RunID: w.runID, Step: "only", Attempt: 1},
		// The operator's cancellation, delivered the instant the child
		// exists: the engine cancels the step's context and the process
		// group goes down with it.
		OnStart: func(int) { cancel() },
	}, runner.ShimTarget{
		Executable: os.Args[0],
		SpoolDir:   w.spoolDir,
		ClaimEpoch: 1,
	})
	if err != nil {
		t.Fatalf("spawn via shim: %v", err)
	}
	if live.Outcome != runner.Signalled {
		t.Fatalf("outcome = %v, want the signalled kill a cancellation produces", live.Outcome)
	}
	if live.ReasonData["spool"] == "missing" {
		t.Fatalf("the shim wrote no result, so this row proves nothing about the file: %v", live.ReasonData)
	}
	if live.ReasonData["cancelled"] != true {
		t.Fatalf("the live executor did not read this attempt as a cancellation: %v", live.ReasonData)
	}

	// The executor dies here, holding a verdict it never wrote down. The
	// result file is all that is left of the attempt.
	w.clk.Advance(2 * time.Minute)
	if _, err := w.eng.Recover(context.Background(), w.runID); err != nil {
		t.Fatalf("recover: %v", err)
	}

	detail, err := w.store.GetRun(context.Background(), w.runID)
	if err != nil {
		t.Fatal(err)
	}
	step := detail.Steps[0]
	if step.State != "cancelled" || step.ReasonCode != "STEP_CANCELLED" {
		t.Fatalf("the recovered step is %s/%s, want cancelled/STEP_CANCELLED",
			step.State, step.ReasonCode)
	}
	if step.Attempt != 1 {
		t.Fatalf("the cancelled attempt bought a retry: attempt %d of %d",
			step.Attempt, step.MaxAttempts)
	}
	if step.OutcomeSource != "spool" {
		t.Fatalf("outcome_source = %q, want spool", step.OutcomeSource)
	}
	if got := model.RunAggregate([]model.StepState{model.StepState(step.State)}, false); got != model.RunCancelled {
		t.Fatalf("the run folds to %s, want %s", got, model.RunCancelled)
	}
}
