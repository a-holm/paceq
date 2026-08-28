package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
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

func newRecoveryWorld(t *testing.T) *recoveryWorld {
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

	spec := `{"schema":"paceq.job.v1","name":"spooled","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
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
	w := newRecoveryWorld(t)

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
	w := newRecoveryWorld(t)
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
