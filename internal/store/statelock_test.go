package store

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestStateLockRefusesASecondHolder is the whole point of the state lock: two
// writers against one state directory have to be impossible, and the second one
// has to learn why rather than block.
//
// Two Acquire calls in one process is a real second holder, not a shortcut:
// flock is owned by the open file description, so a second open of the same
// path conflicts exactly as another process would.
func TestStateLockRefusesASecondHolder(t *testing.T) {
	dir := stateDir(t)

	first, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("acquire the state lock: %v", err)
	}
	defer func() { _ = first.Release() }()

	second, err := AcquireStateLock(dir)
	if err == nil {
		_ = second.Release()
		t.Fatal("a second holder took the state lock, so two writers can run against one state directory")
	}

	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("error %v is not a *LockedError, so a caller cannot tell a held lock from a broken directory", err)
	}
	if want := filepath.Join(dir, lockFileName); locked.Path != want {
		t.Errorf("LockedError names %q, want %q", locked.Path, want)
	}
}

// TestStateLockIsReleasedAndRetakeable pins that Release hands the lock on and
// keeps the file. A deleted lock file would give the next process a fresh inode
// and therefore no lock at all.
func TestStateLockIsReleasedAndRetakeable(t *testing.T) {
	dir := stateDir(t)

	first, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("acquire the state lock: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("release the state lock: %v", err)
	}

	path := filepath.Join(dir, lockFileName)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("stat the lock file after release: %v", err)
	}

	second, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("retake the released state lock: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("release the state lock again: %v", err)
	}
}

// TestStateLockCreatesTheDirectoryPrivate covers the fail closed rule from the
// security plan: the state directory is 0700 and the lock file 0600, and paceq
// creates them that way itself rather than relying on the caller's umask.
func TestStateLockCreatesTheDirectoryPrivate(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")

	lock, err := AcquireStateLock(dir)
	if err != nil {
		t.Fatalf("acquire the state lock in a fresh directory: %v", err)
	}
	defer func() { _ = lock.Release() }()

	if got := statMode(t, dir); got != 0o700 {
		t.Errorf("state directory mode is %#o, want %#o", got, 0o700)
	}
	if got := statMode(t, filepath.Join(dir, lockFileName)); got != 0o600 {
		t.Errorf("lock file mode is %#o, want %#o", got, 0o600)
	}
}

// TestStateLockRefusesOpenPermissions is the other half of fail closed. Widening
// the mode back is not paceq's call: quietly fixing it would hide that the state
// was readable by other users, possibly for weeks.
func TestStateLockRefusesOpenPermissions(t *testing.T) {
	cases := []struct {
		name string
		mode os.FileMode
		file bool
	}{
		{name: "group readable directory", mode: 0o750},
		{name: "world readable directory", mode: 0o755},
		{name: "group readable lock file", mode: 0o640, file: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := stateDir(t)
			target := dir
			if c.file {
				lock, err := AcquireStateLock(dir)
				if err != nil {
					t.Fatalf("create the lock file: %v", err)
				}
				if err := lock.Release(); err != nil {
					t.Fatalf("release the state lock: %v", err)
				}
				target = filepath.Join(dir, lockFileName)
			}
			if err := os.Chmod(target, c.mode); err != nil {
				t.Fatalf("chmod %s: %v", target, err)
			}

			lock, err := AcquireStateLock(dir)
			if err == nil {
				_ = lock.Release()
				t.Fatalf("paceq started with %s at mode %#o", target, c.mode)
			}
			for _, want := range []string{target, modeText(c.file)} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q, so the operator cannot fix it", err, want)
				}
			}
		})
	}
}

// modeText is the mode a refusal has to name: the one paceq expects, not the one
// it found.
func modeText(file bool) string {
	if file {
		return "0600"
	}
	return "0700"
}

// stateDir is a temporary directory with the mode paceq requires. t.TempDir
// leaves the directory world readable, which the fail closed check refuses, so
// every test that wants a working state directory has to narrow it first.
func stateDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	return dir
}

func statMode(t *testing.T, path string) os.FileMode {
	t.Helper()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Mode().Perm()
}
