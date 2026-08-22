package store_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// stepFixture makes one job with one step, queues a run of it and moves the
// step into running, which is the state every finish starts from.
func stepFixture(t *testing.T, s *store.Store, stepName string) store.Run {
	t.Helper()
	ctx := context.Background()
	version := aJob(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: stepName}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	if err := s.StartStep(ctx, run.ID, stepName, time.Now()); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	return run
}

func TestStartStepMovesAPendingStepToRunning(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	version := aJob(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}

	at := time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC)
	if err := s.StartStep(ctx, run.ID, "extract", at); err != nil {
		t.Fatalf("start: %v", err)
	}
	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "running" {
		t.Fatalf("state = %q, want running", step.State)
	}
	if step.Attempt != 1 {
		t.Fatalf("attempt = %d, want 1", step.Attempt)
	}
	if !step.StartedAt.Equal(at) {
		t.Fatalf("started_at = %v, want %v", step.StartedAt, at)
	}
}

// A step already running cannot start again: double starting would lose the
// first attempt's started_at and inflate the attempt counter.
func TestStartStepRefusesAStepThatIsNotPending(t *testing.T) {
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")
	if err := s.StartStep(context.Background(), run.ID, "extract", time.Now()); !errors.Is(err, store.ErrStepNotPending) {
		t.Fatalf("second start returned %v, want ErrStepNotPending", err)
	}
}

func TestFinishStepWritesVerdictAndLogMetadata(t *testing.T) {
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")
	ctx := context.Background()

	finished := time.Date(2026, 9, 17, 3, 4, 5, 0, time.UTC)
	err := s.FinishStep(ctx, store.StepFinish{
		RunID:       run.ID,
		Step:        "extract",
		ToState:     "failed",
		ReasonCode:  "STEP_FAILED_NONZERO_EXIT",
		ReasonText:  "exit status 1",
		Error:       "command failed",
		ExitCode:    1,
		HasExitCode: true,
		FinishedAt:  finished,
		Actor:       "engine",
		Log: store.StepLog{
			RelPath:   "2026-09-17/run/extract.1.ndjson",
			Bytes:     4096,
			Truncated: true,
			ErrorTail: "error: warehouse refused",
		},
	})
	if err != nil {
		t.Fatalf("finish: %v", err)
	}

	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "failed" || step.ReasonCode != "STEP_FAILED_NONZERO_EXIT" {
		t.Fatalf("verdict = %s/%s", step.State, step.ReasonCode)
	}
	if !step.HasExitCode || step.ExitCode != 1 {
		t.Fatalf("exit code = %d (has %v), want 1", step.ExitCode, step.HasExitCode)
	}
	// The log columns land with the verdict, not after it.
	if step.LogPath != "2026-09-17/run/extract.1.ndjson" {
		t.Fatalf("log_path = %q", step.LogPath)
	}
	if step.LogBytes != 4096 || !step.LogTruncated {
		t.Fatalf("log_bytes = %d, truncated = %v", step.LogBytes, step.LogTruncated)
	}
	if step.ErrorTail != "error: warehouse refused" {
		t.Fatalf("error_tail = %q", step.ErrorTail)
	}
	if step.FinishedAt != finished {
		t.Fatalf("finished_at = %v, want %v", step.FinishedAt, finished)
	}
	if step.DurationMS <= 0 {
		t.Fatalf("duration_ms = %d, want the started to finished span", step.DurationMS)
	}
}

func TestFinishStepWithoutTruncationWritesZero(t *testing.T) {
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")
	ctx := context.Background()
	err := s.FinishStep(ctx, store.StepFinish{
		RunID:      run.ID,
		Step:       "extract",
		ToState:    "succeeded",
		ReasonCode: "STEP_SUCCEEDED",
		FinishedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("finish: %v", err)
	}
	detail, _ := s.GetRun(ctx, run.ID)
	step := detail.Steps[0]
	if step.LogTruncated {
		t.Fatal("a clean log reports itself truncated")
	}
	if step.LogBytes != 0 || step.LogPath != "" {
		t.Fatalf("log metadata = %q/%d, want empty for a step without a sink", step.LogPath, step.LogBytes)
	}
}

func TestFinishStepRefusesAStepThatIsNotRunning(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	version := aJob(t, s, "nightly")
	run, _ := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	err := s.FinishStep(ctx, store.StepFinish{
		RunID:      run.ID,
		Step:       "extract",
		ToState:    "failed",
		ReasonCode: "STEP_FAILED_NONZERO_EXIT",
		FinishedAt: time.Now(),
	})
	if !errors.Is(err, store.ErrStepNotRunning) {
		t.Fatalf("finish returned %v, want ErrStepNotRunning", err)
	}
}

// A retry puts a failed step back to pending while attempts remain, and is
// refused once they are used up. This is the path a second attempt walks,
// which is what makes one file per attempt visible through the API.
func TestRetryStepSchedulesTheNextAttempt(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	version := aJob(t, s, "nightly")
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract", MaxAttempts: 2}},
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if err := s.StartStep(ctx, run.ID, "extract", time.Now()); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := s.FinishStep(ctx, store.StepFinish{
		RunID: run.ID, Step: "extract", ToState: "failed",
		ReasonCode: "STEP_FAILED_NONZERO_EXIT", FinishedAt: time.Now(),
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	next := time.Date(2026, 9, 17, 3, 5, 0, 0, time.UTC)
	if err := s.RetryStep(ctx, run.ID, "extract", next); err != nil {
		t.Fatalf("retry: %v", err)
	}
	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.Steps[0].State != "pending" || detail.Steps[0].Attempt != 1 {
		t.Fatalf("after retry: state %s attempt %d, want pending/1",
			detail.Steps[0].State, detail.Steps[0].Attempt)
	}
}

func TestRetryStepRefusesWhenAttemptsAreUsedUp(t *testing.T) {
	s, _ := coreStore(t)
	ctx := context.Background()
	version := aJob(t, s, "nightly")
	run, _ := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	_ = s.StartStep(ctx, run.ID, "extract", time.Now())
	_ = s.FinishStep(ctx, store.StepFinish{
		RunID: run.ID, Step: "extract", ToState: "failed",
		ReasonCode: "STEP_FAILED_NONZERO_EXIT", FinishedAt: time.Now(),
	})
	err := s.RetryStep(ctx, run.ID, "extract", time.Now())
	if !errors.Is(err, store.ErrNoRetryLeft) {
		t.Fatalf("retry returned %v, want ErrNoRetryLeft", err)
	}
}

func TestFinishStepRefusesAnUnknownTerminalState(t *testing.T) {
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")
	err := s.FinishStep(context.Background(), store.StepFinish{
		RunID:      run.ID,
		Step:       "extract",
		ToState:    "running",
		ReasonCode: "STEP_SUCCEEDED",
		FinishedAt: time.Now(),
	})
	if err == nil {
		t.Fatal("finishing into a non terminal state was accepted")
	}
}

// The acceptance criterion that sends people here: the error tail outlives
// the log file. Nothing in this read touches the filesystem.
func TestErrorTailOutlivesTheLogFile(t *testing.T) {
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")
	ctx := context.Background()
	if err := s.FinishStep(ctx, store.StepFinish{
		RunID:      run.ID,
		Step:       "extract",
		ToState:    "failed",
		ReasonCode: "STEP_FAILED_TIMEOUT",
		FinishedAt: time.Now(),
		Log: store.StepLog{
			RelPath:   "2026-09-17/run/extract.1.ndjson",
			Bytes:     99,
			Truncated: false,
			ErrorTail: "last words of the job",
		},
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if tail := detail.Steps[0].ErrorTail; tail != "last words of the job" {
		t.Fatalf("error_tail = %q, want the mirrored text", tail)
	}
}

func TestOpenReadOnlyServesReadsAndRefusesWrites(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	run := stepFixture(t, s, "extract")

	ro, err := store.OpenReadOnly(ctx, s.Path(), store.Options{})
	if err != nil {
		t.Fatalf("open read only: %v", err)
	}
	defer func() { _ = ro.Close() }()

	detail, err := ro.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("read through the read only store: %v", err)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("read only store saw %d steps", len(detail.Steps))
	}

	err = ro.AppendRunEvent(ctx, store.RunEvent{RunID: run.ID, At: time.Now(), Kind: "run.queued"})
	if err == nil {
		t.Fatal("the read only store accepted a write")
	}
	if !errors.Is(err, store.ErrReadOnly) {
		t.Fatalf("write error is %v, want ErrReadOnly", err)
	}
}
