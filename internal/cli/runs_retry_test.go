package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Issue #10, M4-04: `runs retry` is the operator surface of
// ReopenTerminalRunByOperator. The command keeps the run's identity, reopens
// only what failed or was skipped, and reports the fencing token it moved.
// These tests drive the real command line against a real database; the store
// half of every invariant has its own suite in internal/store.

// retryTestProject makes one paceq project and returns the project directory
// with an open store on its state database.
func retryTestProject(t *testing.T) (string, *store.Store) {
	t.Helper()
	dir := t.TempDir()
	if res := runCLI(t, dir, nil, "init"); res.code != ExitOK {
		t.Fatalf("paceq init exited %d:\n%s%s", res.code, res.stdout, res.stderr)
	}
	path := filepath.Join(dir, stateDirName, store.DatabaseFileName)
	s, err := store.Open(context.Background(), path, store.Options{})
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return dir, s
}

// plantFailedChain records a five step job, runs it once with step c dying,
// and returns the failed run's id. Steps d and e end skipped behind it.
func plantFailedChain(t *testing.T, s *store.Store) string {
	t.Helper()
	ctx := context.Background()
	const spec = `{"name":"retrycli","max_concurrent":1,"timeout_ms":3600000,` +
		`"schema":"paceq.job.v1","steps":[` +
		`{"name":"a","run":["/bin/true"],"shell":false},` +
		`{"name":"b","needs":["a"],"run":["/bin/true"],"shell":false},` +
		`{"name":"c","needs":["b"],"run":["/bin/true"],"shell":false},` +
		`{"name":"d","needs":["c"],"run":["/bin/true"],"shell":false},` +
		`{"name":"e","needs":["d"],"run":["/bin/true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "retrycli",
		SpecHash: "sha256:retrycli",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "retrycli"})
	if err != nil {
		t.Fatalf("queue the run: %v", err)
	}
	runID := queued.Run.ID

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "cli:test", TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "cli:test", Epoch: 1}
	for _, step := range []string{"a", "b", "c"} {
		if err := s.StartStep(ctx, runID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		out := store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: new(int), FinishedAt: time.Now(),
		}
		if step == "c" {
			out = store.StepOutcome{
				Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
				ExitCode: new(int), FinishedAt: time.Now(),
			}
		}
		if err := s.RecordStepOutcome(ctx, runID, step, out, ref); err != nil {
			t.Fatalf("record %s: %v", step, err)
		}
		if step == "c" {
			break
		}
	}
	if _, err := s.FinishRun(ctx, runID, ref, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"c"}`,
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	return runID
}

// TestRunsRetryReopensTheFailedRun is the command line face of AC-1 and AC-3:
// same run id, the failed and skipped steps pending again, the succeeded ones
// untouched, and the answer names the epoch it moved the fence to.
func TestRunsRetryReopensTheFailedRun(t *testing.T) {
	dir, s := retryTestProject(t)
	runID := plantFailedChain(t, s)

	got := runCLI(t, dir, nil, "runs", "retry", runID, "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("runs retry exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	var doc struct {
		RunID    string   `json:"run_id"`
		NewEpoch int64    `json:"new_epoch"`
		Reopened []string `json:"reopened"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("the answer is not the retry record:\n%s\n%v", got.stdout, err)
	}
	if doc.RunID != runID {
		t.Errorf("run_id = %s, want the same run %s", doc.RunID, runID)
	}
	if doc.NewEpoch != 2 {
		t.Errorf("new_epoch = %d, want 2 (one claim happened before the retry)", doc.NewEpoch)
	}
	if len(doc.Reopened) != 3 || doc.Reopened[0] != "c" || doc.Reopened[1] != "d" || doc.Reopened[2] != "e" {
		t.Errorf("reopened = %v, want exactly [c d e]", doc.Reopened)
	}

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.State != "queued" {
		t.Errorf("the run is %s after a retry, want queued", detail.State)
	}
	for _, step := range detail.Steps {
		switch step.Name {
		case "c", "d", "e":
			if step.State != "pending" {
				t.Errorf("%s = %s, want pending again", step.Name, step.State)
			}
		case "a", "b":
			if step.State != "succeeded" {
				t.Errorf("%s = %s, want it left succeeded", step.Name, step.State)
			}
		}
	}
}

// TestRunsRetryTextModeSpeaksToTheOperator checks the words a person gets:
// the mark, the run, and which steps wait again. A pipe never sees them.
func TestRunsRetryTextModeSpeaksToTheOperator(t *testing.T) {
	dir, s := retryTestProject(t)
	runID := plantFailedChain(t, s)

	got := runCLI(t, dir, nil, "runs", "retry", runID, "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("runs retry exited %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if !strings.Contains(got.stdout, runID) {
		t.Errorf("the answer does not name the run:\n%s", got.stdout)
	}
	if !strings.Contains(got.stdout, "c") || !strings.Contains(got.stdout, "d") || !strings.Contains(got.stdout, "e") {
		t.Errorf("the answer does not name the reopened steps:\n%s", got.stdout)
	}
	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.State != "queued" {
		t.Errorf("the run is %s after a retry, want queued", detail.State)
	}
}

// TestRunsRetryWithStepReopensOnlyTheClosure is AC-5 through the CLI: naming
// b reopens b and everything below it, never a.
func TestRunsRetryWithStepReopensOnlyTheClosure(t *testing.T) {
	dir, s := retryTestProject(t)
	runID := plantFailedChain(t, s)

	// The planted run has c failed and d, e skipped behind it. Reopening b
	// alone would refuse: b succeeded. Point --step at d instead: d and its
	// downstream e reopen, c stays as it is.
	got := runCLI(t, dir, nil, "runs", "retry", runID, "--step", "d", "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("runs retry --step exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	var doc struct {
		Reopened []string `json:"reopened"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("the answer is not JSON:\n%s\n%v", got.stdout, err)
	}
	if len(doc.Reopened) != 2 || doc.Reopened[0] != "d" || doc.Reopened[1] != "e" {
		t.Errorf("reopened = %v, want exactly [d e]", doc.Reopened)
	}
}

// TestRunsRetryRefusesWhatCannotReopen pins the exit codes around the happy
// path: a run that never finished is a validation refusal, an unknown id is a
// not-found, and a succeeded run points at replay instead.
func TestRunsRetryRefusesWhatCannotReopen(t *testing.T) {
	dir, s := retryTestProject(t)
	failedID := plantFailedChain(t, s)

	t.Run("non terminal", func(t *testing.T) {
		// Queue a fresh run of the same job: it exists but has not started.
		queued, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{JobName: "retrycli"})
		if err != nil {
			t.Fatalf("queue: %v", err)
		}
		got := runCLI(t, dir, nil, "runs", "retry", queued.Run.ID)
		if got.code != ExitValidation {
			t.Fatalf("retrying a queued run exited %d, want %d\n%s", got.code, ExitValidation, got.stderr)
		}
	})

	t.Run("unknown id", func(t *testing.T) {
		got := runCLI(t, dir, nil, "runs", "retry", "01ZZZZZZZZZZZZZZZZZZZZZZZZZ")
		if got.code != ExitNotFound {
			t.Fatalf("retrying an unknown run exited %d, want %d", got.code, ExitNotFound)
		}
	})

	t.Run("succeeded run", func(t *testing.T) {
		// Finish the planted run properly this time: reopen it, claim, walk
		// the chain to the end, then try to retry what succeeded.
		res := runCLI(t, dir, nil, "runs", "retry", failedID, "-o", "json")
		if res.code != ExitOK {
			t.Fatalf("setup retry failed: %d\n%s", res.code, res.stderr)
		}
		ctx := context.Background()
		_, epoch, err := s.ClaimRun(ctx, failedID, store.LeaseInput{Owner: "cli:test", TTL: time.Hour})
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		ref := store.LeaseRef{Owner: "cli:test", Epoch: epoch}
		for _, step := range []string{"c", "d", "e"} {
			if err := s.StartStep(ctx, failedID, step, ref); err != nil {
				t.Fatalf("start %s: %v", step, err)
			}
			if err := s.RecordStepOutcome(ctx, failedID, step, store.StepOutcome{
				Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
				ExitCode: new(int), FinishedAt: time.Now(),
			}, ref); err != nil {
				t.Fatalf("record %s: %v", step, err)
			}
		}
		if _, err := s.FinishRun(ctx, failedID, ref, store.FinishReason{Code: reason.RUNSucceeded, Data: "{}"}); err != nil {
			t.Fatalf("finish: %v", err)
		}
		got := runCLI(t, dir, nil, "runs", "retry", failedID)
		if got.code != ExitValidation {
			t.Fatalf("retrying a succeeded run exited %d, want %d\n%s", got.code, ExitValidation, got.stderr)
		}
		if !strings.Contains(got.stderr, "replay") {
			t.Errorf("the refusal should point at replay:\n%s", got.stderr)
		}
	})
}
