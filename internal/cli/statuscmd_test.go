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

// decodeStatusRows unwraps the status envelope a pipe gets, asserting the
// daemon-down fact that travels with it: the fixture has no daemon running.
func decodeStatusRows(t *testing.T, stdout string) []map[string]any {
	t.Helper()
	var envelope struct {
		DaemonUp *bool            `json:"daemon_up"`
		Jobs     []map[string]any `json:"jobs"`
	}
	if err := json.Unmarshal([]byte(stdout), &envelope); err != nil {
		t.Fatalf("decode the status envelope: %v\n%s", err, stdout)
	}
	if envelope.DaemonUp == nil || *envelope.DaemonUp {
		t.Fatalf("the status document does not say daemon_up false: %s", stdout)
	}
	return envelope.Jobs
}

// status is the per job view: one line per job, the newest run beside it.

func TestStatusShowsEveryJobsCurrentState(t *testing.T) {
	dir, _ := finishedRunsFixture(t)
	stateDir := filepath.Join(dir, stateDirName)

	// An idle job so the report also covers a job that never ran.
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(testOrigin))
	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "idle",
		SpecHash: "sha256:idle",
		SpecJSON: `{"steps":[{"name":"wait"}]}`,
	}); err != nil {
		t.Fatalf("record idle: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	stdout, readOut := terminalFile(t)
	stderr, readErr := pipeFile(t)
	env := Env{Stdout: stdout, Stderr: stderr, Dir: dir, Getenv: lookup(nil)}
	code := run(context.Background(), env, []string{"status"})
	_ = stdout.Close()
	_ = stderr.Close()
	if code != ExitOK {
		t.Fatalf("status at a terminal = %d\n%s", code, readErr())
	}
	table := readOut()

	for _, want := range []string{"nightly", "import", "idle", "succeeded", "failed"} {
		if !strings.Contains(table, want) {
			t.Errorf("the status table does not mention %q:\n%s", want, table)
		}
	}
	if !strings.Contains(table, "no runs") {
		t.Errorf("the idle job does not say it has no runs:\n%s", table)
	}

	piped := runCLI(t, dir, nil, "status")
	rows := decodeStatusRows(t, piped.stdout)
	if len(rows) != 3 {
		t.Fatalf("status listed %d jobs, want 3", len(rows))
	}
	byJob := map[string]map[string]any{}
	for _, row := range rows {
		name, _ := row["job"].(string)
		byJob[name] = row
	}
	if got, _ := byJob["nightly"]["state"].(string); got != "succeeded" {
		t.Errorf("nightly status = %v, want succeeded", byJob["nightly"]["state"])
	}
	if got, _ := byJob["import"]["reason_code"].(string); got != "RUN_FAILED_STEP" {
		t.Errorf("import reason = %v, want RUN_FAILED_STEP", byJob["import"]["reason_code"])
	}
	if idle := byJob["idle"]["run"]; idle != nil {
		t.Errorf("the idle job carries a run: %v", idle)
	}
}

func TestStatusOneJobOnly(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	got := runCLI(t, dir, nil, "status", "import")

	if got.code != ExitOK {
		t.Fatalf("status import = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	rows := decodeStatusRows(t, got.stdout)
	if len(rows) != 1 || rows[0]["job"] != "import" {
		t.Fatalf("rows = %v, want only import", rows)
	}

	missing := runCLI(t, dir, nil, "status", "nope")
	if missing.code != ExitNotFound {
		t.Errorf("an unknown job exits %d, want %d", missing.code, ExitNotFound)
	}
}
