package cli

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// fsck is the invariant sweep on the command line: the engine reads the
// state and every broken rule comes back as a finding.

func TestFsckOnASoundStateExitsZero(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	got := runCLI(t, dir, nil, "fsck")

	if got.code != ExitOK {
		t.Fatalf("fsck on a sound state = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	var doc struct {
		Violations []map[string]any `json:"violations"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("a pipe got no JSON document: %v\n%s", err, got.stdout)
	}
	if len(doc.Violations) != 0 {
		t.Errorf("a sound state reported %d violations: %v", len(doc.Violations), doc.Violations)
	}

	text := runCLI(t, dir, nil, "fsck", "-o", "text")
	if !strings.Contains(text.stdout, "sound") && text.code != ExitOK {
		t.Errorf("the text report neither says sound nor passes:\n%s", text.stdout)
	}
}

// TestFsckReportsViolationsAsFindings plants a real violation through the
// store's own API: two events whose states do not chain break invariant I15.
// The command must surface it and fail, never report a clean state when the
// engine found something.
func TestFsckReportsViolationsAsFindings(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      fixtureJobInput.JobName,
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	// The chain says queued -> running -> failed, but the second event
	// claims to start from queued again. Nothing that wrote this history
	// went through the machine's guards.
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID: run.ID, Kind: "run.started", FromState: "queued", ToState: "running",
	}); err != nil {
		t.Fatalf("append the first event: %v", err)
	}
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID: run.ID, Kind: "run.failed", FromState: "queued", ToState: "failed",
	}); err != nil {
		t.Fatalf("append the broken event: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A warning-class finding is reported but does not fail: the exit-code
	// contract is graded, and I15 (a history hole) is historic, not serving
	// breaking (M6-06). The finding is still listed, in every mode.
	got := runCLI(t, dir, nil, "fsck")
	if got.code != ExitOK {
		t.Fatalf("fsck failed on a warning-class finding, want exit 0 with the finding listed:\n%s%s", got.stdout, got.stderr)
	}
	var doc struct {
		Violations []struct {
			Check   string `json:"check"`
			Subject string `json:"subject"`
			Detail  string `json:"detail"`
		} `json:"violations"`
	}
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("the violations are not machine readable: %v\n%s", err, got.stdout)
	}
	found := false
	for _, v := range doc.Violations {
		if v.Check == "I15" && strings.Contains(v.Subject, run.ID) {
			found = true
		}
	}
	if !found {
		t.Errorf("findings = %v, want the I15 violation of run %s", doc.Violations, run.ID)
	}

	text := runCLI(t, dir, nil, "fsck", "-o", "text")
	for _, want := range []string{"I15"} {
		if !strings.Contains(text.stdout, want) {
			t.Errorf("the text findings do not name %q:\n%s", want, text.stdout)
		}
	}
}

// TestFsckFailsOnASeriousFinding plants I11, a fencing token that falls: the
// grade that does fail the command, because the state itself is wrong however
// well the machine is behaving. The events go through the public append, the
// same door every writer uses, and the tokens they carry are history no live
// writer could produce.
func TestFsckFailsOnASeriousFinding(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      fixtureJobInput.JobName,
		JobVersionID: version.ID,
		Origin:       "manual",
		Steps:        []store.NewStep{{Name: "extract"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID: run.ID, Kind: "run.started", FromState: "queued", ToState: "running",
		DetailJSON: `{"lease_epoch":5}`,
	}); err != nil {
		t.Fatalf("append the first token: %v", err)
	}
	if err := s.AppendRunEvent(ctx, store.RunEvent{
		RunID: run.ID, Kind: "run.failed", FromState: "running", ToState: "failed",
		DetailJSON: `{"lease_epoch":3}`,
	}); err != nil {
		t.Fatalf("append the falling token: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got := runCLI(t, dir, nil, "fsck")
	if got.code != ExitInternal {
		t.Fatalf("fsck exited %d over a falling token, want %d:\n%s%s",
			got.code, ExitInternal, got.stdout, got.stderr)
	}

	// Text mode names the severity and the remedy, so the finding can be
	// acted on from the report alone.
	text := runCLI(t, dir, nil, "fsck", "-o", "text")
	for _, want := range []string{"I11", "serious"} {
		if !strings.Contains(text.stdout, want) {
			t.Errorf("the text findings do not name %q:\n%s", want, text.stdout)
		}
	}
}

func TestFsckWithoutStateIsNotFound(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "fsck")

	if got.code != ExitNotFound {
		t.Errorf("fsck without a project exits %d, want %d", got.code, ExitNotFound)
	}
}
