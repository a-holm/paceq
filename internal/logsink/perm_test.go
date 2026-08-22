package logsink

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

// TestCreateRefusesAWideDirectory is the fail closed rule: a log directory any
// other user can read is refused instead of written into. The refusal names the
// chmod that fixes it.
func TestCreateRefusesAWideDirectory(t *testing.T) {
	state := t.TempDir()
	root := NewRoot(state)
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	runDir := filepath.Dir(abs)
	if err := os.MkdirAll(runDir, 0o755); err != nil {
		t.Fatalf("prepare a wide run directory: %v", err)
	}

	f, err := root.Create(rel)
	if err == nil {
		_ = f.Close()
		t.Fatalf("a run directory with mode 0755 was accepted")
	}
	var perm *PermissionError
	if !errors.As(err, &perm) {
		t.Fatalf("error is %T (%v), want a PermissionError", err, err)
	}
	if perm.Got != 0o755 || perm.Path != runDir {
		t.Fatalf("refusal names mode %#o on %s, want 0755 on %s", perm.Got, perm.Path, runDir)
	}
	entries, err := os.ReadDir(runDir)
	if err != nil {
		t.Fatalf("read the refused directory: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("the refusal left %d files behind", len(entries))
	}
}

// The review warning this test answers: a directory that already existed is
// checked again, not trusted because MkdirAll did not have to create it.
func TestCreateReChecksADirectoryThatAlreadyExisted(t *testing.T) {
	state := t.TempDir()
	root := NewRoot(state)
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)

	// First open creates everything tight and succeeds.
	f, err := root.Create(rel)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	_ = f.Close()

	// Somebody widens the date shard afterwards. The next attempt has to
	// refuse rather than carry on inside it.
	dateDir := filepath.Join(root.Dir(), frozen.Format(dateLayout))
	if err := os.Chmod(dateDir, 0o750); err != nil {
		t.Fatalf("widen the date shard: %v", err)
	}
	if _, err := root.Create(root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 2)); err == nil {
		t.Fatal("a widened date shard was accepted on the second open")
	} else if !isPermission(err) {
		t.Fatalf("error is %T (%v), want a PermissionError", err, err)
	}
}

func TestCreateRefusesAnExistingWideFile(t *testing.T) {
	state := t.TempDir()
	root := NewRoot(state)
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		t.Fatalf("prepare the run directory: %v", err)
	}
	if err := os.WriteFile(abs, []byte("tampered\n"), 0o644); err != nil {
		t.Fatalf("prepare a wide log file: %v", err)
	}

	f, err := root.Create(rel)
	if err == nil {
		_ = f.Close()
		t.Fatal("a log file with mode 0644 was accepted")
	}
	if !isPermission(err) {
		t.Fatalf("error is %T (%v), want a PermissionError", err, err)
	}
	got, readErr := os.ReadFile(abs)
	if readErr != nil {
		t.Fatalf("read the refused file: %v", readErr)
	}
	if string(got) != "tampered\n" {
		t.Fatalf("the refusal changed the file: %q", got)
	}
}

// The modes are decided by the arguments to open, never by the umask. A
// permissive umask of 0022 would turn a careless 0666 into 0644 silently; the
// proof runs under exactly that umask and still finds 0700 and 0600 on disk.
func TestCreatedModesHoldUnderAWideUmask(t *testing.T) {
	old := syscall.Umask(0o022)
	t.Cleanup(func() { syscall.Umask(old) })

	state := t.TempDir()
	root := NewRoot(state)
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}

	f, err := root.Create(rel)
	if err != nil {
		t.Fatalf("create under umask 0022: %v", err)
	}
	_ = f.Close()

	info, err := os.Stat(abs)
	if err != nil {
		t.Fatalf("stat the log file: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("log file has mode %#o, want 0600 under umask 0022", got)
	}
	for _, dir := range []string{
		root.Dir(),
		filepath.Dir(abs),
		filepath.Dir(filepath.Dir(abs)),
	} {
		info, err := os.Stat(dir)
		if err != nil {
			t.Fatalf("stat %s: %v", dir, err)
		}
		if got := info.Mode().Perm(); got != 0o700 {
			t.Fatalf("%s has mode %#o, want 0700 under umask 0022", dir, got)
		}
	}
}

// isPermission reports whether err is the fail closed refusal.
func isPermission(err error) bool {
	var perm *PermissionError
	return errors.As(err, &perm)
}
