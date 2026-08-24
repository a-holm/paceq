package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Issue #10, M4-04: `runs replay` is the second half of the operator pair.
// Retry continues one run in place; replay makes a NEW run beside it, frozen
// to the same job version the source ran, with steps that can be spared a
// second execution. The store half of every invariant has its own suite in
// internal/store; these tests pin what the command line promises.

type replayDoc struct {
	RunID    string   `json:"run_id"`
	ReplayOf string   `json:"replay_of"`
	Reused   []string `json:"reused"`
	Rerun    []string `json:"rerun"`
}

func mustParseReplay(t *testing.T, out string) replayDoc {
	t.Helper()
	var doc replayDoc
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("the answer is not the replay record:\n%s\n%v", out, err)
	}
	return doc
}

// TestRunsReplayMakesANewRunBesideTheOldOne is AC-6 through the CLI: a fresh
// ULID that answers for itself, replay_of naming the source, and the source
// left exactly as it was.
func TestRunsReplayMakesANewRunBesideTheOldOne(t *testing.T) {
	dir, s := retryTestProject(t)
	srcID := plantFailedChain(t, s)
	source, err := s.GetRun(context.Background(), srcID)
	if err != nil {
		t.Fatalf("read the source: %v", err)
	}

	got := runCLI(t, dir, nil, "runs", "replay", srcID, "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("runs replay exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	doc := mustParseReplay(t, got.stdout)
	if doc.RunID == "" || doc.RunID == srcID {
		t.Errorf("run_id = %q, want a new id beside %s", doc.RunID, srcID)
	}
	if doc.ReplayOf != srcID {
		t.Errorf("replay_of = %q, want the source %s", doc.ReplayOf, srcID)
	}
	if len(doc.Reused)+len(doc.Rerun) != 5 || len(doc.Rerun) != 5 {
		t.Errorf("reused = %v, rerun = %v, want nothing spared and all five rerun",
			doc.Reused, doc.Rerun)
	}

	replayed, err := s.GetRun(context.Background(), doc.RunID)
	if err != nil {
		t.Fatalf("read back the replay: %v", err)
	}
	if replayed.State != "queued" {
		t.Errorf("the replay is %s, want queued for the ordinary claim loop", replayed.State)
	}
	if replayed.JobVersionID != source.JobVersionID {
		t.Errorf("job_version_id = %s, want the source's frozen %s",
			replayed.JobVersionID, source.JobVersionID)
	}
	if replayed.RunKey != "" {
		t.Errorf("the replay carries run_key %q, want none (AC-13)", replayed.RunKey)
	}
	for _, step := range replayed.Steps {
		if step.State != "pending" {
			t.Errorf("step %s is %s, want pending in a full rerun", step.Name, step.State)
		}
	}
}

// TestRunsReplayWithFromSparesTheUpstream is AC-8 through the CLI: --from c
// spares a and b as born-succeeded references and reruns c, d, e.
func TestRunsReplayWithFromSparesTheUpstream(t *testing.T) {
	dir, s := retryTestProject(t)
	srcID := plantFailedChain(t, s)

	got := runCLI(t, dir, nil, "runs", "replay", srcID, "--from", "c", "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("runs replay --from exited %d, want %d\nstderr:\n%s", got.code, ExitOK, got.stderr)
	}
	doc := mustParseReplay(t, got.stdout)
	if len(doc.Reused) != 2 || doc.Reused[0] != "a" || doc.Reused[1] != "b" {
		t.Errorf("reused = %v, want exactly [a b]", doc.Reused)
	}
	if len(doc.Rerun) != 3 || doc.Rerun[0] != "c" || doc.Rerun[1] != "d" || doc.Rerun[2] != "e" {
		t.Errorf("rerun = %v, want exactly [c d e]", doc.Rerun)
	}

	replayed, err := s.GetRun(context.Background(), doc.RunID)
	if err != nil {
		t.Fatalf("read back the replay: %v", err)
	}
	for _, step := range replayed.Steps {
		switch step.Name {
		case "a", "b":
			if step.State != "succeeded" || step.ReasonCode != string(reason.STEPSkippedReplayReused) {
				t.Errorf("%s is %s/%s, want succeeded under STEP_SKIPPED_REPLAY_REUSED",
					step.Name, step.State, step.ReasonCode)
			}
		case "c":
			if step.State != "pending" {
				t.Errorf("%s is %s, want pending: --from spares the upstream, not the named step",
					step.Name, step.State)
			}
		}
	}
}

// TestRunsReplayWithFailedReusesEverySuccess is AC-9 through the CLI: every
// step that made it in the source is spared, whatever failed or was skipped
// behind it earns its outputs again.
func TestRunsReplayWithFailedReusesEverySuccess(t *testing.T) {
	dir, s := retryTestProject(t)
	srcID := plantFailedChain(t, s)

	got := runCLI(t, dir, nil, "runs", "replay", srcID, "--failed", "-o", "json")

	if got.code != ExitOK {
		t.Fatalf("runs replay --failed exited %d, want %d\nstderr:\n%s", got.code, ExitOK, got.stderr)
	}
	doc := mustParseReplay(t, got.stdout)
	if len(doc.Reused) != 2 || doc.Reused[0] != "a" || doc.Reused[1] != "b" {
		t.Errorf("reused = %v, want exactly [a b], the steps that succeeded", doc.Reused)
	}
	if len(doc.Rerun) != 3 || doc.Rerun[0] != "c" || doc.Rerun[1] != "d" || doc.Rerun[2] != "e" {
		t.Errorf("rerun = %v, want exactly [c d e]", doc.Rerun)
	}
}

// TestRunsReplayTextModeSpeaksToTheOperator checks the words a person gets:
// the source it read, the new run waiting in the queue, and which steps were
// spared. A pipe never sees them.
func TestRunsReplayTextModeSpeaksToTheOperator(t *testing.T) {
	dir, s := retryTestProject(t)
	runID := plantFailedChain(t, s)

	got := runCLI(t, dir, nil, "runs", "replay", runID, "--from", "c")

	if got.code != ExitOK {
		t.Fatalf("runs replay exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			got.code, ExitOK, got.stdout, got.stderr)
	}
	for _, want := range []string{runID, "a", "b"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the answer does not name %q:\n%s", want, got.stdout)
		}
	}
}

// TestRunsReplayRefusesWhatCannotReplay pins the refusals around the happy
// path: an unknown name, a run still going, two reuse rules at once, and a
// --from step the source never had.
func TestRunsReplayRefusesWhatCannotReplay(t *testing.T) {
	dir, s := retryTestProject(t)
	failedID := plantFailedChain(t, s)

	t.Run("unknown id", func(t *testing.T) {
		got := runCLI(t, dir, nil, "runs", "replay", "01ZZZZZZZZZZZZZZZZZZZZZZZZZ")
		if got.code != ExitNotFound {
			t.Fatalf("replaying an unknown run exited %d, want %d", got.code, ExitNotFound)
		}
	})

	t.Run("still running", func(t *testing.T) {
		queued, err := s.MaterializeManualTrigger(context.Background(),
			store.ManualTriggerInput{JobName: "retrycli"})
		if err != nil {
			t.Fatalf("queue: %v", err)
		}
		got := runCLI(t, dir, nil, "runs", "replay", queued.Run.ID)
		if got.code != ExitValidation {
			t.Fatalf("replaying a queued run exited %d, want %d\n%s",
				got.code, ExitValidation, got.stderr)
		}
	})

	t.Run("both reuse rules", func(t *testing.T) {
		got := runCLI(t, dir, nil, "runs", "replay", failedID, "--from", "c", "--failed")
		if got.code != ExitValidation {
			t.Fatalf("replaying with two reuse rules exited %d, want %d\n%s",
				got.code, ExitValidation, got.stderr)
		}
		if !strings.Contains(got.stderr, "--from") {
			t.Errorf("the refusal does not name the conflict:\n%s", got.stderr)
		}
	})

	t.Run("unknown step", func(t *testing.T) {
		got := runCLI(t, dir, nil, "runs", "replay", failedID, "--from", "nosuch")
		if got.code != ExitValidation {
			t.Fatalf("replaying --from nosuch exited %d, want %d\n%s",
				got.code, ExitValidation, got.stderr)
		}
	})
}
