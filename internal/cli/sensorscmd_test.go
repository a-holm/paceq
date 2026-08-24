package cli

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The sensors CLI unit tests drive the direct (no-daemon) write path and the
// dry-run bit-identity guarantee, both of which need a real store rather than
// the golden script's planted rows. pause/resume/reset/cursor set all fall
// back to flock + direct when there is no socket, which is exactly what these
// exercises cover.

// seedSensorCLI writes a sensor row plus its job directly, so the command
// under test has something to operate on without the apply seam (M3-01).
func seedSensorCLI(t *testing.T, dir, name, execJSON string) {
	t.Helper()
	stateDir := filepath.Join(dir, stateDirName)
	s, err := store.OpenState(t.Context(), stateDir, store.Options{})
	if err != nil {
		t.Fatalf("open state to seed: %v", err)
	}
	defer func() { _ = s.Close() }()
	if _, _, err := s.UpsertJobVersion(t.Context(), store.JobVersionInput{
		JobName: "polling-job", SpecHash: "sha256:seed",
		SpecJSON: `{"schema":"paceq.job.v1","name":"polling-job","steps":[{"name":"c","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := s.UpsertSensor(t.Context(), store.SensorSeedInput{
		Name: name, JobName: "polling-job", ExecJSON: execJSON,
	}); err != nil {
		t.Fatalf("seed sensor: %v", err)
	}
}

// hashDB returns the sha256 of the database file, for the bit-identity proof.
func hashDB(t *testing.T, dir string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, stateDirName, store.DatabaseFileName))
	if err != nil {
		t.Fatalf("read db: %v", err)
	}
	return hashBytes(data)
}

// hashBytes is the sha256 hex of a byte slice.
func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// openStoreRead opens the state database read-only for a CLI test.
func openStoreRead(t *testing.T, dir string) *store.Store {
	t.Helper()
	s, err := store.OpenReadOnly(t.Context(), filepath.Join(dir, stateDirName, store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("open read-only store: %v", err)
	}
	return s
}

func TestSensorsPauseResumeDirect(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d\n%s", got.code, got.stdout)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)

	got := runCLI(t, dir, nil, "sensors", "pause", "finder", "--reason", "deploying")
	if got.code != ExitOK {
		t.Fatalf("pause = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "paused") {
		t.Errorf("pause stdout = %q, want a paused confirmation", got.stdout)
	}

	got = runCLI(t, dir, nil, "sensors", "resume", "finder")
	if got.code != ExitOK {
		t.Fatalf("resume = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "resumed") {
		t.Errorf("resume stdout = %q, want a resumed confirmation", got.stdout)
	}
}

func TestSensorsPauseUnknownExit3(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)
	got := runCLI(t, dir, nil, "sensors", "pause", "finderz")
	if got.code != ExitNotFound {
		t.Fatalf("pause missing = %d, want %d\n%s", got.code, ExitNotFound, got.stderr)
	}
	if !strings.Contains(got.stderr, "did you mean") {
		t.Errorf("stderr = %q, want a did-you-mean hint", got.stderr)
	}
}

func TestSensorsResetWithoutConfirmationExit2(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)

	got := runCLI(t, dir, nil, "sensors", "reset", "finder", "--forget-run-keys")
	if got.code != ExitUsage {
		t.Fatalf("reset without --yes = %d, want %d\n%s", got.code, ExitUsage, got.stderr)
	}
	if !strings.Contains(got.stderr, "confirmation") {
		t.Errorf("stderr = %q, want a confirmation hint", got.stderr)
	}
}

func TestSensorsResetWithYesBumpsEpoch(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)

	got := runCLI(t, dir, nil, "sensors", "reset", "finder", "--yes")
	if got.code != ExitOK {
		t.Fatalf("reset --yes = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "reset") {
		t.Errorf("reset stdout = %q", got.stdout)
	}

	// The epoch must have moved. Read it back through the store.
	s := openStoreRead(t, dir)
	defer func() { _ = s.Close() }()
	row, err := s.GetSensor(t.Context(), "finder")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.DedupEpoch != 1 {
		t.Errorf("DedupEpoch after reset = %d, want 1", row.DedupEpoch)
	}
}

func TestSensorsCursorSetMovesCursorNotEpoch(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)

	got := runCLI(t, dir, nil, "sensors", "cursor", "set", "finder", "v9")
	if got.code != ExitOK {
		t.Fatalf("cursor set = %d\n%s", got.code, got.stdout+got.stderr)
	}

	s := openStoreRead(t, dir)
	defer func() { _ = s.Close() }()
	row, err := s.GetSensor(t.Context(), "finder")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.Cursor == nil || *row.Cursor != "v9" {
		t.Errorf("Cursor = %v, want &v9", row.Cursor)
	}
	if row.DedupEpoch != 0 {
		t.Errorf("DedupEpoch = %d after cursor set, want 0 (F4c: cursor move is not a reset)", row.DedupEpoch)
	}
}

// TestSensorsTestLeavesDBBitIdentical is the M3-06 bit-identity guarantee: a
// dry run reads real state but writes nothing, so the database file is the
// same bytes before and after.
func TestSensorsTestLeavesDBBitIdentical(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/echo","found"]`)

	before := hashDB(t, dir)
	got := runCLI(t, dir, nil, "sensors", "test", "finder", "-o", "json")
	if got.code != ExitOK {
		t.Fatalf("test = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "\"dry_run\":true") {
		t.Errorf("test stdout = %q, want dry_run true", got.stdout)
	}
	after := hashDB(t, dir)
	if before != after {
		t.Errorf("database changed under a dry run:\nbefore %s\nafter  %s", before, after)
	}
}

func TestSensorsTestPrintInputPipesOnlyJSON(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/echo","found"]`)

	got := runCLI(t, dir, nil, "sensors", "test", "finder", "--print-input")
	if got.code != ExitOK {
		t.Fatalf("test --print-input = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "\"sensor\":\"finder\"") {
		t.Errorf("print-input stdout = %q, want the contract JSON", got.stdout)
	}
	if !strings.Contains(got.stdout, "\"dry_run\":true") {
		t.Errorf("print-input stdout = %q, want dry_run true", got.stdout)
	}
}

func TestSensorsCursorGetUnknownExit3(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	got := runCLI(t, dir, nil, "sensors", "cursor", "get", "ghost")
	if got.code != ExitNotFound {
		t.Fatalf("cursor get ghost = %d, want %d\n%s", got.code, ExitNotFound, got.stderr)
	}
}

// TestSensorsTickDirectRunsAndCommits is the no-daemon tick path: the CLI
// runs the evaluation in this process and commits it atomically through the
// same store code the daemon uses. The sensor echoes its input back, so the
// run key is deterministic and the commit lands a triggered tick.
func TestSensorsTickDirectRunsAndCommits(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	// A sensor that prints a triggered contract with one run key.
	seedSensorCLI(t, dir, "finder", `["/bin/echo","{\"cursor\":\"next\",\"triggers\":[{\"run_key\":\"f1\"}]}"]`)

	got := runCLI(t, dir, nil, "sensors", "tick", "finder", "-o", "text")
	if got.code != ExitOK {
		t.Fatalf("tick = %d\n%s", got.code, got.stdout+got.stderr)
	}
	if !strings.Contains(got.stdout, "finished") {
		t.Errorf("tick stdout = %q, want a finished confirmation", got.stdout)
	}

	// A triggered tick must have advanced the cursor to "next".
	s := openStoreRead(t, dir)
	defer func() { _ = s.Close() }()
	row, err := s.GetSensor(t.Context(), "finder")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.Cursor == nil || *row.Cursor != "next" {
		t.Errorf("Cursor after tick = %v, want &next", row.Cursor)
	}
}

// TestSensorsTickPausedIsBusy proves a paused sensor refuses a forced tick
// with the busy exit code and a reason, never running the evaluation.
func TestSensorsTickPausedIsBusy(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	stateDir := filepath.Join(dir, stateDirName)
	s, err := store.OpenState(t.Context(), stateDir, store.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if _, _, err := s.UpsertJobVersion(t.Context(), store.JobVersionInput{
		JobName: "polling-job", SpecHash: "sha256:seed",
		SpecJSON: `{"schema":"paceq.job.v1","name":"polling-job","steps":[{"name":"c","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	if err := s.UpsertSensor(t.Context(), store.SensorSeedInput{
		Name: "finder", JobName: "polling-job", ExecJSON: `["/bin/true"]`, Paused: true,
	}); err != nil {
		t.Fatalf("seed paused sensor: %v", err)
	}
	_ = s.Close()

	got := runCLI(t, dir, nil, "sensors", "tick", "finder")
	if got.code != ExitBusy {
		t.Fatalf("tick paused = %d, want %d\n%s", got.code, ExitBusy, got.stderr)
	}
	if !strings.Contains(got.stderr, "paused") {
		t.Errorf("stderr = %q, want a paused explanation", got.stderr)
	}
}

// TestSensorsCompletionOffersNames proves the cobra completion for a sensor
// name lists the sensors in the state, and offers an empty completion when
// there is no state to read (the shell shows nothing rather than an error).
// It seeds a sensor and drives __complete, which is the exact code path the
// shell calls.
func TestSensorsCompletionOffersNames(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedSensorCLI(t, dir, "dropzone", `["/bin/true"]`)
	seedSensorCLI(t, dir, "watcher", `["/bin/true"]`)

	got := runCLI(t, dir, nil, "__complete", "sensors", "show", "")
	if got.code != ExitOK {
		t.Fatalf("__complete = %d\n%s", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, "dropzone") || !strings.Contains(got.stdout, "watcher") {
		t.Errorf("completion stdout = %q, want both sensor names", got.stdout)
	}
}

// TestSensorsCompletionEmptyWhenNoState proves the empty fallback: without a
// state directory the completion returns no candidates, not an error.
func TestSensorsCompletionEmptyWhenNoState(t *testing.T) {
	dir := t.TempDir()
	got := runCLI(t, dir, nil, "__complete", "sensors", "show", "")
	if got.code != ExitOK {
		t.Fatalf("__complete without state = %d\n%s", got.code, got.stderr)
	}
	if strings.Contains(got.stdout, "dropzone") {
		t.Errorf("completion offered a sensor with no state: %q", got.stdout)
	}
}
