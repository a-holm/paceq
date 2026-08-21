//go:build unix

package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// TestMainSetsTheUmaskBeforeAnythingWrites measures the process umask through
// its only observable effect: a file created with wide permissions comes out
// narrow. Checking the mode of a file paceq creates would not do, because those
// are created with an explicit mode and would pass with no umask set at all.
func TestMainSetsTheUmaskBeforeAnythingWrites(t *testing.T) {
	dir := t.TempDir()
	restore := setUmaskForTest(t, 0o022)
	defer restore()

	stdout := captureStdout(t)
	code := Main(context.Background(), []string{"version", "-o", "json"})
	written := stdout()

	if code != ExitOK {
		t.Fatalf("paceq version = %d, want %d", code, ExitOK)
	}
	if !strings.Contains(written, "\"version\"") {
		t.Errorf("Main did not write the version to os.Stdout: %q", written)
	}

	probe := dir + "/probe"
	if err := os.WriteFile(probe, []byte("x"), 0o666); err != nil {
		t.Fatalf("write %s: %v", probe, err)
	}
	info, err := os.Stat(probe)
	if err != nil {
		t.Fatalf("stat %s: %v", probe, err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Errorf("a file created with mode 0666 came out %#o, want 0600: Main did not set umask 0077", got)
	}
}

// TestInitCreatesPrivateFiles is the fail closed rule from 08 section 3.9 on
// the artifacts themselves.
func TestInitCreatesPrivateFiles(t *testing.T) {
	dir := t.TempDir()
	restore := setUmaskForTest(t, 0o022)
	defer restore()

	if code := runCLI(t, dir, nil, "init").code; code != ExitOK {
		t.Fatalf("paceq init = %d, want %d", code, ExitOK)
	}

	cases := map[string]os.FileMode{
		".paceq":          store.DirMode,
		"jobs":            store.DirMode,
		".paceq/state.db": store.DatabaseMode,
		"paceq.yaml":      store.DatabaseMode,
		"jobs/hello.yaml": store.DatabaseMode,
		".gitignore":      store.DatabaseMode,
	}
	for path, want := range cases {
		info, err := os.Stat(filepath.Join(dir, path))
		if err != nil {
			t.Errorf("stat %s: %v", path, err)
			continue
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s has mode %#o, want %#o", path, got, want)
		}
	}
}

// setUmaskForTest replaces the process umask and gives back the function that
// restores it. The umask is process wide, so no test that uses it may run in
// parallel with another.
func setUmaskForTest(t *testing.T, mask int) func() {
	t.Helper()

	previous := syscall.Umask(mask)
	return func() { syscall.Umask(previous) }
}
