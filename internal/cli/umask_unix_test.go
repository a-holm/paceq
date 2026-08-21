//go:build unix

package cli

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// wideUmask is a mask that removes every permission bit, so no mode paceq asks
// for survives it. It is the probe that makes the ordering visible: a file
// created while it is still in force comes out 0000, whatever mode the caller
// passed, and a file created after Main replaced it comes out at paceq's own
// mode. Measuring with 0022 would prove nothing, because everything init
// creates is created with an explicit mode narrower than that.
const wideUmask = 0o777

// TestMainSetsTheUmaskBeforeAnythingWrites is 08 section 3.9 as an ordering
// rule: the umask is replaced before the first command runs, not somewhere on
// the way out. The evidence is every artifact of a real run, because each one
// carries the mask that was in force when the kernel created it.
func TestMainSetsTheUmaskBeforeAnythingWrites(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	// Redirected before the mask is widened: the capture file is the test's own
	// and has nothing to do with what Main creates.
	stdout := captureStdout(t)
	restore := setUmaskForTest(t, wideUmask)
	defer restore()

	code := Main(context.Background(), []string{"init"})
	written := stdout()

	if code != ExitOK {
		t.Fatalf("paceq init = %d, want %d: the run inherited a umask paceq should have replaced\n%s",
			code, ExitOK, written)
	}
	if !strings.Contains(written, `"created"`) {
		t.Errorf("Main did not write the report to os.Stdout: %q", written)
	}
	for path, want := range map[string]fs.FileMode{
		".paceq":                           store.DirMode,
		".paceq/paceq.lock":                store.DatabaseMode,
		".paceq/" + store.DatabaseFileName: store.DatabaseMode,
		"jobs":                             store.DirMode,
		"jobs/hello.yaml":                  store.DatabaseMode,
		"paceq.yaml":                       store.DatabaseMode,
		".gitignore":                       store.DatabaseMode,
	} {
		info, err := os.Stat(filepath.Join(dir, path))
		if err != nil {
			t.Errorf("stat %s: %v", path, err)
			continue
		}
		if got := info.Mode().Perm(); got != want {
			t.Errorf("%s has mode %#o, want %#o: it was created under the umask Main inherited (%#o)",
				path, got, want, wideUmask)
		}
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
