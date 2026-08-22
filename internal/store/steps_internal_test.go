package store

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// internalStore is a migrated store for the in-package tests, which exist to
// reach the writer pool directly. The external tests in steps_test.go cover
// the behaviour; this one covers the guarantee that needs the handle.
func internalStore(t *testing.T) *Store {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("open store at %q: %v", path, err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func aJobInternal(t *testing.T, s *Store, name string) JobVersion {
	t.Helper()

	version, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:     name,
		Description: "the " + name + " job",
		SourcePath:  "jobs/" + name + ".yaml",
		SpecHash:    "sha256:" + name,
		SpecJSON:    `{"steps":[{"name":"build"}]}`,
	})
	if err != nil {
		t.Fatalf("record job %s: %v", name, err)
	}
	return version
}

// TestFinishStepRollsBackWholesale is the atomicity criterion for the log
// metadata: error_tail, log_path, log_bytes and log_truncated are written in
// the SAME transaction as the step's terminal transition. A failure between
// the two writes would leave a step whose verdict and its evidence disagree,
// and that drift is what this rule exists to prevent.
//
// The failure is injected with a trigger that aborts exactly the statement
// that writes log_path: the earliest moment the metadata write can fail on
// its own. The verdict may not survive it.
func TestFinishStepRollsBackWholesale(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)
	version := aJobInternal(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	if err := s.StartStep(ctx, run.ID, "extract", time.Now()); err != nil {
		t.Fatalf("start the step: %v", err)
	}

	// Abort the transaction from inside the metadata statement.
	_, err = s.w.ExecContext(ctx, `CREATE TRIGGER paceq_test_abort_on_log_metadata
AFTER UPDATE OF log_path ON steps
WHEN NEW.log_path IS NOT NULL AND OLD.log_path IS NULL
BEGIN SELECT RAISE(ABORT, 'injected failure after the terminal update'); END`)
	if err != nil {
		t.Fatalf("install the injection trigger: %v", err)
	}

	err = s.FinishStep(ctx, StepFinish{
		RunID:      run.ID,
		Step:       "extract",
		ToState:    "failed",
		ReasonCode: "STEP_FAILED_NONZERO_EXIT",
		FinishedAt: time.Now(),
		Log: StepLog{
			RelPath:   "2026-09-17/run/extract.1.ndjson",
			Bytes:     4096,
			Truncated: true,
			ErrorTail: "the tail",
		},
	})
	if err == nil {
		t.Fatal("FinishStep succeeded although the metadata write aborted")
	}
	if !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("error does not name the injected failure: %v", err)
	}

	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "running" {
		t.Fatalf("state = %q after the failed finish, want running: the verdict"+
			" must not outlive its log metadata", step.State)
	}
	if step.LogPath != "" || step.ErrorTail != "" || step.LogBytes != 0 || step.LogTruncated {
		t.Fatalf("log metadata survived the failed finish: %+v", step)
	}
}
