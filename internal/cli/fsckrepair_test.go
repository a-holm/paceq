package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The repair flags and the graded exit contract, from the command line down.

// plantDuplicateRunKey returns a state directory holding two runs under one
// run_key: a critical finding (I3) that the dedup gate would have refused at
// the trigger, planted here through the manual create the gate does not cover.
func plantDuplicateRunKey(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, fixtureJobInput)
	if err != nil {
		t.Fatalf("record job: %v", err)
	}
	for range 2 {
		if _, err := s.CreateRunWithSteps(ctx, store.NewRun{
			JobName:      fixtureJobInput.JobName,
			JobVersionID: version.ID,
			Origin:       "manual",
			RunKey:       "shared",
			Steps:        []store.NewStep{{Name: "extract"}},
		}); err != nil {
			t.Fatalf("create the run: %v", err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// store.Open seeds the file 0644; a repair opens the ordinary way and
	// demands 0600, so the fixture meets the write path's own rule.
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if err := os.Chmod(dbPath, 0o600); err != nil {
		t.Fatalf("tighten the database mode: %v", err)
	}
	return dir
}

func TestFsckRepairDemandsConfirmationOnCriticals(t *testing.T) {
	dir := plantDuplicateRunKey(t)

	got := runCLI(t, dir, nil, "fsck", "--repair")
	if got.code != ExitInternal {
		t.Fatalf("an unconfirmed repair over a critical exited %d, want %d:\\n%s%s",
			got.code, ExitInternal, got.stdout, got.stderr)
	}
	for _, want := range []string{"--confirm"} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("the refusal never says %q:\\n%s", want, got.stderr)
		}
	}

	// Confirmed, the repair runs; the critical itself is unrepairable, so the
	// re-sweep reports it and the command still fails, now naming the
	// startup refusal that will follow.
	confirmed := runCLI(t, dir, nil, "fsck", "--repair", "--confirm")
	if confirmed.code != ExitInternal {
		t.Fatalf("a confirmed repair over an unrepairable critical exited %d:\\n%s%s",
			confirmed.code, confirmed.stdout, confirmed.stderr)
	}
	if !strings.Contains(confirmed.stderr, "startup will be refused") {
		t.Errorf("the critical report never names the startup refusal:\\n%s", confirmed.stderr)
	}
}

func TestFsckOnlyFlagFiltersTheReport(t *testing.T) {
	dir := plantDuplicateRunKey(t)

	got := runCLI(t, dir, nil, "fsck", "--json", "--only", "I3")
	if got.code != ExitInternal {
		t.Fatalf("fsck --only I3 exited %d, want %d:\\n%s%s", got.code, ExitInternal, got.stdout, got.stderr)
	}
	var doc fsckReport
	if err := json.Unmarshal([]byte(got.stdout), &doc); err != nil {
		t.Fatalf("the --json report is not the contract document: %v\\n%s", err, got.stdout)
	}
	if doc.Count != 1 || len(doc.Violations) != 1 || doc.Violations[0].Check != "I3" {
		t.Fatalf("the filtered report holds %+v", doc.Violations)
	}
	if doc.Violations[0].Severity != "critical" {
		t.Errorf("the I3 finding is graded %q", doc.Violations[0].Severity)
	}
}

func TestFsckJSONFlagPinsTheContract(t *testing.T) {
	dir := plantDuplicateRunKey(t)

	flagged := runCLI(t, dir, nil, "fsck", "--json")
	if flagged.code != ExitInternal {
		t.Fatalf("fsck --json exited %d:\\n%s%s", flagged.code, flagged.stdout, flagged.stderr)
	}
	if !strings.HasPrefix(strings.TrimSpace(flagged.stdout), "{") {
		t.Fatalf("fsck --json answered in text:\\n%s", flagged.stdout)
	}
	viaMode := runCLI(t, dir, nil, "fsck", "-o", "json")
	if flagged.stdout != viaMode.stdout {
		t.Fatalf("--json and -o json disagree:\\n--json:\\n%s\\n-o json:\\n%s",
			flagged.stdout, viaMode.stdout)
	}
	var doc fsckReport
	if err := json.Unmarshal([]byte(flagged.stdout), &doc); err != nil {
		t.Fatalf("the document does not parse: %v", err)
	}
	if doc.Critical == 0 || doc.Count != len(doc.Violations) {
		t.Fatalf("the report's own numbers disagree: %+v", doc)
	}
	for _, v := range doc.Violations {
		if v.Remedy == "" {
			t.Errorf("finding %s carries no remedy", v.Check)
		}
	}
}
