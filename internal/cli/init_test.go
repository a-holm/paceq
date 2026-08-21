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

// TestInitCreatesAProjectThatWorks is 09 section 6.3: what init leaves behind
// has to be usable, not a skeleton with placeholders.
func TestInitCreatesAProjectThatWorks(t *testing.T) {
	dir := t.TempDir()

	got := runCLI(t, dir, nil, "init", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq init = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	for _, path := range []string{"paceq.yaml", "jobs/hello.yaml", ".gitignore", ".paceq/state.db"} {
		if _, err := os.Stat(filepath.Join(dir, path)); err != nil {
			t.Errorf("init did not create %s: %v", path, err)
		}
		if !strings.Contains(got.stdout, path) {
			t.Errorf("the report does not name %s:\n%s", path, got.stdout)
		}
	}
	if !strings.Contains(got.stdout, "Next steps:") {
		t.Errorf("the report has no next steps block:\n%s", got.stdout)
	}
	for _, want := range []string{"paceq doctor", "paceq run hello"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the next steps do not offer %q:\n%s", want, got.stdout)
		}
	}
	if got.stderr != "" {
		t.Errorf("init wrote to stderr:\n%s", got.stderr)
	}

	ignore := readFile(t, filepath.Join(dir, ".gitignore"))
	if !strings.Contains(ignore, ".paceq/") {
		t.Errorf(".gitignore does not exclude the state directory:\n%s", ignore)
	}
}

// TestExampleJobUsesAnArgvArray is 08 section 3.2: the example is what every
// user copies, so it has to show the form that never reaches a shell.
func TestExampleJobUsesAnArgvArray(t *testing.T) {
	dir := t.TempDir()

	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	job := readFile(t, filepath.Join(dir, "jobs", "hello.yaml"))
	line := jobRunLine(t, job)
	if !strings.HasPrefix(line, "[") || !strings.HasSuffix(line, "]") {
		t.Errorf("the example job runs a shell string rather than an argv array: %s\n%s", line, job)
	}
}

// jobRunLine is the value of the first run: key in the example job.
func jobRunLine(t *testing.T, job string) string {
	t.Helper()

	for _, line := range strings.Split(job, "\n") {
		trimmed := strings.TrimSpace(line)
		if after, ok := strings.CutPrefix(trimmed, "run:"); ok {
			return strings.TrimSpace(after)
		}
	}
	t.Fatalf("the example job has no run: key\n%s", job)
	return ""
}

// TestInitRefusesToRunTwice is the idempotence rule with the strongest evidence
// there is: the database file is byte for byte the one that was there before.
func TestInitRefusesToRunTwice(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}
	dbPath := filepath.Join(dir, ".paceq", store.DatabaseFileName)
	before := checksum(t, dbPath)

	got := runCLI(t, dir, nil, "init")

	if got.code != ExitUsage {
		t.Fatalf("a second paceq init = %d, want %d\n%s", got.code, ExitUsage, got.stderr)
	}
	if after := checksum(t, dbPath); after != before {
		t.Errorf("the database changed: %s before, %s after", before, after)
	}
	if !strings.Contains(got.stderr, "paceq.yaml") {
		t.Errorf("the refusal does not name what is already there:\n%s", got.stderr)
	}
	if !strings.Contains(got.stderr, "\n  ") {
		t.Errorf("the refusal has no indented next step:\n%s", got.stderr)
	}
}

// TestInitRefusesAWideStateDirectory is the fail closed check on state that
// already exists: paceq will not write into a directory other users can read,
// and says which path and which mode it needs.
func TestInitRefusesAWideStateDirectory(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, ".paceq")
	if err := os.Mkdir(state, 0o755); err != nil {
		t.Fatalf("create %s: %v", state, err)
	}

	got := runCLI(t, dir, nil, "init")

	if got.code != ExitValidation {
		t.Fatalf("init against a world readable state directory = %d, want %d\n%s",
			got.code, ExitValidation, got.stderr)
	}
	for _, want := range []string{state, "0755", "0700", "chmod"} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, got.stderr)
		}
	}
	if _, err := os.Stat(filepath.Join(state, store.DatabaseFileName)); err == nil {
		t.Error("init created a database in a directory it refused to use")
	}
}

// TestInitRefusesAWideDatabase covers the file rather than the directory. The
// refusal has to name the path and the mode required, because those are the two
// facts the fix needs.
func TestInitRefusesAWideDatabase(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}
	dbPath := filepath.Join(dir, ".paceq", store.DatabaseFileName)
	if err := os.Chmod(dbPath, 0o644); err != nil {
		t.Fatalf("chmod %s: %v", dbPath, err)
	}
	before := checksum(t, dbPath)

	got := runCLI(t, dir, nil, "init")

	if got.code != ExitValidation {
		t.Fatalf("init against a world readable database = %d, want %d\n%s",
			got.code, ExitValidation, got.stderr)
	}
	for _, want := range []string{dbPath, "0644", "0600", "chmod"} {
		if !strings.Contains(got.stderr, want) {
			t.Errorf("the refusal does not mention %q:\n%s", want, got.stderr)
		}
	}
	if after := checksum(t, dbPath); after != before {
		t.Errorf("the database changed: %s before, %s after", before, after)
	}
}

// TestInitIsBusyWhileAnotherProcessHoldsTheState is exit 6: the state is fine,
// somebody else has it.
func TestInitIsBusyWhileAnotherProcessHoldsTheState(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, ".paceq")
	if err := os.Mkdir(state, store.DirMode); err != nil {
		t.Fatalf("create %s: %v", state, err)
	}
	lock, err := store.AcquireStateLock(state)
	if err != nil {
		t.Fatalf("take the state lock: %v", err)
	}
	defer func() { _ = lock.Release() }()

	got := runCLI(t, dir, nil, "init")

	if got.code != ExitBusy {
		t.Fatalf("init against a held state directory = %d, want %d\n%s", got.code, ExitBusy, got.stderr)
	}
	if !strings.Contains(got.stderr, "PQ1001") {
		t.Errorf("the refusal does not carry the error code:\n%s", got.stderr)
	}
}

// TestInitJSONNamesWhatItCreated keeps the machine readable form useful for the
// installer scripts that will wrap init.
func TestInitJSONNamesWhatItCreated(t *testing.T) {
	dir := t.TempDir()

	got := runCLI(t, dir, nil, "init")

	if got.code != ExitOK {
		t.Fatalf("paceq init = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	doc := got.json(t)
	created, ok := doc["created"].([]any)
	if !ok || len(created) == 0 {
		t.Fatalf("the document lists nothing created: %v", doc)
	}
	if _, ok := doc["next_steps"].([]any); !ok {
		t.Errorf("the document has no next steps: %v", doc)
	}
}

// TestInitAgainstAnotherStateDirectory covers --db: the project files land here,
// the state lands where the flag says.
func TestInitAgainstAnotherStateDirectory(t *testing.T) {
	project := t.TempDir()
	elsewhere := filepath.Join(t.TempDir(), "state")
	dbPath := filepath.Join(elsewhere, store.DatabaseFileName)

	got := runCLI(t, project, nil, "init", "--db", dbPath)

	if got.code != ExitOK {
		t.Fatalf("paceq init --db = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Errorf("init did not create %s: %v", dbPath, err)
	}
	if _, err := os.Stat(filepath.Join(project, ".paceq")); err == nil {
		t.Error("init created the default state directory as well as the one it was given")
	}
}

// TestDatabaseFlagNamesADatabase. A state directory holds one database with one
// name, so a path that says otherwise is a usage error rather than a surprise
// two commands later.
func TestDatabaseFlagNamesADatabase(t *testing.T) {
	got := runCLI(t, t.TempDir(), nil, "doctor", "--db", "/tmp/paceq/other.db")

	if got.code != ExitUsage {
		t.Fatalf("--db with another file name = %d, want %d\n%s", got.code, ExitUsage, got.stderr)
	}
	if !strings.Contains(got.stderr, store.DatabaseFileName) {
		t.Errorf("the message does not say what the name has to be:\n%s", got.stderr)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(data)
}

func checksum(t *testing.T, path string) string {
	t.Helper()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
