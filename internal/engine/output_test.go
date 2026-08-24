package engine_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
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

// The injection half: the downstream step's environment carries the merged
// upstream closure, and only that.

func TestExecuteRunHandsUpstreamReferencesToTheDownstreamStep(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"produce", "consume"},
		[]string{"write-output", "publish-input-uri seen out.txt"},
		map[string]string{"consume": "produce"}, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded", state)
	}
	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range detail.Steps {
		if st.Name != "consume" {
			continue
		}
		for _, a := range st.Artifacts {
			if a.Name == "seen" && a.URI != "file:///tmp/out.txt" {
				t.Errorf("the downstream step saw uri %q, want the one produce published", a.URI)
			}
		}
		found := false
		for _, a := range st.Artifacts {
			if a.Name == "seen" {
				found = true
			}
		}
		if !found {
			t.Fatalf("consume published %+v, want the seen reference read from $PACEQ_INPUTS", st.Artifacts)
		}
	}
}

// A params key two upstream steps claimed resolves deterministically (the
// later verdict takes it), and the losing claim is logged as a warning on
// the consumer, not dropped: nothing about a step's inputs may disappear
// without a trace.
func TestExecuteRunLogsAWarningWhenUpstreamParamsCollide(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"first", "second", "consume"},
		[]string{"write-output", "write-output", "publish-input-uri seen out.txt"},
		map[string]string{"second": "first", "consume": "second"}, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded: a params-key collision is a warning, never a verdict", state)
	}

	events, err := f.Store.RunEvents(ctx, runID)
	if err != nil {
		t.Fatal(err)
	}
	var event *store.RunEvent
	for i := range events {
		if events[i].Kind == "step.inputs_collision" && events[i].StepName == "consume" {
			event = &events[i]
		}
	}
	if event == nil {
		t.Fatalf("no step.inputs_collision event on consume among %+v", events)
	}
	if event.ReasonCode != string(reason.STEPInputCollision) {
		t.Errorf("reason_code = %s, want %s", event.ReasonCode, reason.STEPInputCollision)
	}
	var detail struct {
		Collisions []struct {
			Name   string `json:"name"`
			Winner string `json:"winner"`
			Loser  string `json:"loser"`
		} `json:"collisions"`
	}
	if err := json.Unmarshal([]byte(event.DetailJSON), &detail); err != nil {
		t.Fatalf("event detail %s is not JSON: %v", event.DetailJSON, err)
	}
	if len(detail.Collisions) != 1 ||
		detail.Collisions[0].Name != "rows" ||
		detail.Collisions[0].Winner != "second" ||
		detail.Collisions[0].Loser != "first" {
		t.Errorf("collisions = %+v, want one naming rows, second over first", detail.Collisions)
	}
}

func TestASideStepStaysInvisibleToItsNeighboursInputs(t *testing.T) {
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"produce", "side", "consume"},
		[]string{
			"write-named-output mine /from-produce",
			"write-named-output theirs /from-side",
			"inputs-must-not-contain theirs",
		},
		map[string]string{"consume": "produce"}, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded: the side step leaked into $PACEQ_INPUTS", state)
	}
}

func TestExecuteRunSpillsOversizedInputsBesideTheAttempt(t *testing.T) {
	f := newFixture(t)
	// The producer's eight 24 KiB params push the merged document past
	// 128 KiB. The consumer proves the inline value is parked at null and
	// the file carries the producer's last emitted param.
	runID := f.aQueuedRun(t, []string{"produce", "consume"},
		[]string{
			"write-big-output 8",
			"inputs-file-holds-param blob7",
		},
		map[string]string{"consume": "produce"}, 30_000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded: the consumer never saw the spilled contract", state)
	}

	// The spill landed beside the attempt's other files and carries the
	// whole oversized params document that forced it.
	raw, err := os.ReadFile(filepath.Join(f.StateDir, "runs", runID, "consume.1.inputs.json"))
	if err != nil {
		t.Fatalf("read the spilled inputs of consume: %v", err)
	}
	if len(raw) <= engine.MaxInlineInputsBytes {
		t.Errorf("the spill holds %d bytes, want more than the %d inline bound",
			len(raw), engine.MaxInlineInputsBytes)
	}
	for _, want := range []string{`"blob0"`, `"blob7"`} {
		if !strings.Contains(string(raw), want) {
			t.Errorf("the spilled document is missing %s", want)
		}
	}
}
