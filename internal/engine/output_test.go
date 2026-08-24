package engine_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/reason"
)

// The publication half of #13 at engine level: the output file exists empty
// and 0600 before exec under the run's own directory, parsing happens after
// exit, a succeeded step's references reach artifacts, warnings ride beside
// the verdict without ever replacing it, and a failed step publishes nothing.

func TestExecuteRunCreatesTheOutputFileEmptyBeforeExec(t *testing.T) {
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"guard"}, []string{"require-empty-output"}, nil, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded: the guard found the file not empty, too open or missing", state)
	}

	detail, err := f.Store.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatal(err)
	}
	var published string
	for _, st := range detail.Steps {
		for _, a := range st.Artifacts {
			if a.Name == "output-path" {
				published = a.URI
			}
		}
	}
	wantDir := filepath.Join(f.StateDir, "runs", runID)
	if !strings.HasPrefix(published, wantDir) || !strings.HasSuffix(published, ".output.ndjson") {
		t.Errorf("$PACEQ_OUTPUT was %q, want a path under %s ending in .output.ndjson", published, wantDir)
	}
}

func TestExecuteRunPublishesASucceededStepsReferences(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"pub"}, []string{"write-output"}, nil, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	step := detail.Steps[0]
	if len(step.Artifacts) != 1 {
		t.Fatalf("published %+v, want one reference", step.Artifacts)
	}
	a := step.Artifacts[0]
	if a.Name != "out.txt" || a.URI != "file:///tmp/out.txt" ||
		a.SizeBytes == nil || *a.SizeBytes != 3 {
		t.Errorf("reference = %+v", a)
	}
	if !strings.Contains(step.ReasonData, `"emitted_params":{"rows":"42"}`) {
		t.Errorf("reason_data = %s, want the carried-forward params beside the verdict", step.ReasonData)
	}
}

func TestExecuteRunWarnsOnUnreadableOutputAndKeepsTheVerdict(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"broken"}, []string{"write-broken-output"}, nil, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s: an unreadable line must never fail a step that exited 0", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	step := detail.Steps[0]
	if step.ReasonCode != string(reason.STEPSucceeded) {
		t.Errorf("verdict = %s, want %s", step.ReasonCode, reason.STEPSucceeded)
	}
	if len(step.Artifacts) != 1 || step.Artifacts[0].Name != "kept" {
		t.Fatalf("artifacts = %+v, want only the line that parsed", step.Artifacts)
	}
	if !strings.Contains(step.ReasonData, string(reason.STEPOutputInvalid)) ||
		!strings.Contains(step.ReasonData, `"count":1`) {
		t.Errorf("reason_data = %s, want one aggregated STEP_OUTPUT_INVALID warning", step.ReasonData)
	}
}

func TestExecuteRunPublishesNothingFromAFailedStep(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"fails"}, []string{"write-then-exit 1"}, nil, 30_000)

	if state := f.mustFinish(t, runID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}
	rows, err := f.Store.RunsArtifacts(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Errorf("a failed step left %d artifact rows; failure never publishes", len(rows))
	}
}

func TestExecuteRunGivesALaterStepACollidingName(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// Both steps publish out.txt through write-output. The later step wins
	// the name and carries the collision warning.
	runID := f.aQueuedRun(t, []string{"first", "second"},
		[]string{"write-output", "write-output"},
		map[string]string{"second": "first"}, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded", state)
	}

	rows, err := f.Store.RunsArtifacts(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var seen bool
	for _, r := range rows {
		if r.Name == "out.txt" {
			seen = true
			if r.StepName != "second" {
				t.Errorf("out.txt is owned by %s, want the later step second", r.StepName)
			}
			if r.URI != "file:///tmp/out.txt" {
				t.Errorf("uri = %s", r.URI)
			}
		}
	}
	if !seen {
		t.Fatal("out.txt vanished from the run")
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range detail.Steps {
		if st.Name != "second" {
			continue
		}
		if !strings.Contains(st.ReasonData, string(reason.STEPSucceeded)) &&
			st.ReasonCode != string(reason.STEPSucceeded) {
			t.Errorf("winner verdict = %s, still want plain success", st.ReasonCode)
		}
		if !strings.Contains(st.ReasonData, string(reason.STEPOutputCollision)) ||
			!strings.Contains(st.ReasonData, `"loser":"first"`) {
			t.Errorf("reason_data = %s, want a collision warning naming the loser", st.ReasonData)
		}
	}
}
