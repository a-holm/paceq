package cli

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// TestInitThenDoctorIsClean is the end to end promise of M0: what init creates,
// doctor approves of. Both machine reading edges are planted, so the promise
// holds on a development machine that runs the sandboxless unit and on one
// where another paceq is running jobs: the checks it approves of are the ones
// about the state init created.
func TestInitThenDoctorIsClean(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	got := runCLIWithDoctorEdges(t, dir, sandboxedCLIStatus(), otherInstallationsJob(), "doctor")

	if got.code != ExitOK {
		t.Fatalf("paceq doctor = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	doc := got.json(t)
	if doc["status"] != "ok" {
		t.Errorf("status is %v, want ok:\n%s", doc["status"], got.stdout)
	}
	findings, ok := doc["findings"].([]any)
	if !ok || len(findings) == 0 {
		t.Fatalf("the report has no findings: %v", doc)
	}
	for _, item := range findings {
		finding, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("a finding is not an object: %v", item)
		}
		if finding["level"] != "ok" {
			t.Errorf("%v: %v", finding["title"], finding["detail"])
		}
	}
	if !strings.Contains(got.stdout, "another installation") {
		t.Errorf("the report never accounts for the other installation's job process:\n%s", got.stdout)
	}
}

// TestDoctorIgnoresARealForeignJobProcess is #189 against the real /proc walk,
// which is where the defect bit: a soak run in another worktree, with its own
// state directory, made this installation's report warn about processes it had
// never started and could never signal. The scan is machine wide on purpose;
// the attempt baselines decide what is ours, and a fresh init has none.
func TestDoctorIgnoresARealForeignJobProcess(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the job process scan walks /proc, which is a linux feature")
	}
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	// A job process of a second installation: it leads its own group, the
	// way the runner spawns jobs, and carries a run id no database here has.
	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "PACEQ_RUN_ID=01M17NNQ5Y3EXTKHX9TFCNBZ2J")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn the other installation's job: %v", err)
	}
	pid := cmd.Process.Pid
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() { _ = syscall.Kill(-pid, syscall.SIGKILL) })

	got := runCLIWithDoctorEdges(t, dir, sandboxedCLIStatus(), nil, "doctor")

	if got.code != ExitOK {
		t.Fatalf("paceq doctor = %d, want %d\n%s%s", got.code, ExitOK, got.stdout, got.stderr)
	}
	if doc := got.json(t); doc["status"] != "ok" {
		t.Errorf("status is %v, want ok:\n%s", doc["status"], got.stdout)
	}
	if strings.Contains(got.stdout, "orphaned job processes") {
		t.Errorf("a process this installation never started was called an orphan:\n%s", got.stdout)
	}
}

// TestDoctorReportsWhatTheIssueAsksFor keeps the report answering the questions
// an operator actually opens it for.
func TestDoctorReportsWhatTheIssueAsksFor(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	got := runCLI(t, dir, nil, "doctor", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq doctor = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	for _, want := range []string{
		"paceq", "state directory", "sandbox", "database", "journal mode", "schema version",
		"auto_vacuum", "write lock", "disk space", "time zone",
		filepath.Join(dir, ".paceq", store.DatabaseFileName),
	} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the report does not cover %q:\n%s", want, got.stdout)
		}
	}
}

// TestDoctorWarningsStillExitZero. A report that screams in a motd stops being
// read, so a warning is not a failure.
func TestDoctorWarningsStillExitZero(t *testing.T) {
	dir := t.TempDir()

	got := runCLI(t, dir, nil, "doctor")

	if got.code != ExitOK {
		t.Fatalf("doctor on an uninitialised directory = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if doc := got.json(t); doc["status"] != "warn" {
		t.Errorf("status is %v, want warn:\n%s", doc["status"], got.stdout)
	}
}

// TestDoctorFailsOnABrokenDatabase is the other half of the exit rule, and the
// case that must not panic: a file that is not a database at all.
func TestDoctorFailsOnABrokenDatabase(t *testing.T) {
	dir := t.TempDir()
	state := filepath.Join(dir, ".paceq")
	if err := os.Mkdir(state, store.DirMode); err != nil {
		t.Fatalf("create %s: %v", state, err)
	}
	dbPath := filepath.Join(state, store.DatabaseFileName)
	if err := os.WriteFile(dbPath, []byte("this is not a database"), store.DatabaseMode); err != nil {
		t.Fatalf("write %s: %v", dbPath, err)
	}

	got := runCLI(t, dir, nil, "doctor")

	if got.code != ExitInternal {
		t.Fatalf("doctor on a broken database = %d, want %d\n%s%s", got.code, ExitInternal, got.stdout, got.stderr)
	}
	doc := got.json(t)
	if doc["status"] != "fail" {
		t.Errorf("status is %v, want fail:\n%s", doc["status"], got.stdout)
	}
	if !strings.Contains(got.stderr, "\n  ") {
		t.Errorf("the failure has no indented next step:\n%s", got.stderr)
	}
}

// TestDoctorReadsAnotherStateDirectory covers --db on a read only command.
func TestDoctorReadsAnotherStateDirectory(t *testing.T) {
	project := t.TempDir()
	elsewhere := filepath.Join(t.TempDir(), "state")
	dbPath := filepath.Join(elsewhere, store.DatabaseFileName)
	if code := runCLI(t, project, nil, "init", "--db", dbPath).code; code != ExitOK {
		t.Fatalf("paceq init --db = %d, want %d", code, ExitOK)
	}

	got := runCLI(t, t.TempDir(), nil, "doctor", "--db", dbPath, "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("paceq doctor --db = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if !strings.Contains(got.stdout, dbPath) {
		t.Errorf("the report does not name the database it read:\n%s", got.stdout)
	}
}

// TestDoctorNamesTheHolderOfTheState. Another paceq running is the most common
// reason a report cannot read the database, and the least useful thing to be
// silent about.
func TestDoctorNamesTheHolderOfTheState(t *testing.T) {
	dir := t.TempDir()
	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}
	state := filepath.Join(dir, ".paceq")
	lock, err := store.AcquireStateLock(state)
	if err != nil {
		t.Fatalf("take the state lock: %v", err)
	}
	defer func() { _ = lock.Release() }()

	got := runCLI(t, dir, nil, "doctor", "-o", "text")

	if got.code != ExitOK {
		t.Fatalf("doctor while the state is held = %d, want %d\n%s", got.code, ExitOK, got.stderr)
	}
	if !strings.Contains(got.stdout, "write lock") || !strings.Contains(got.stdout, "held") {
		t.Errorf("the report does not say the lock is held:\n%s", got.stdout)
	}
}
