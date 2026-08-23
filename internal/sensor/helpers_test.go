//go:build unix

package sensor

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakecmd builds the runner package's testdata/fakecmd fixture and returns its
// path. The sensor modes live in that one shared fixture (M1-06), so every
// sensor test speaks the same deterministic vocabulary the runner tests do.
// Building happens once per test binary.
var (
	fakeCmdOnce sync.Once
	fakeCmdPath string
	fakeCmdErr  error
)

func fakecmd(t *testing.T) string {
	t.Helper()
	fakeCmdOnce.Do(func() {
		fakeCmdPath, fakeCmdErr = buildFakeCmd()
	})
	if fakeCmdErr != nil {
		t.Fatalf("build sensor fixture: %v", fakeCmdErr)
	}
	return fakeCmdPath
}

func buildFakeCmd() (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	dir, err := os.MkdirTemp("", "paceq-sensor-fakecmd-")
	if err != nil {
		return "", err
	}
	path := filepath.Join(dir, "fakecmd")
	build := exec.Command("go", "build", "-o", path, "./testdata/fakecmd")
	build.Dir = filepath.Join(root, "internal", "runner")
	build.Env = append(os.Environ(), "GOFLAGS=-buildvcs=false")
	if _, err := build.CombinedOutput(); err != nil {
		return "", err
	}
	return path, nil
}

// moduleRoot asks the go tool for the module directory, the robust way to
// find it under -trimpath. go test runs with the package directory as the
// working directory, so a bare `go list -m` resolves the module from here.
func moduleRoot() (string, error) {
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

// newTestEvaluator returns an evaluator with a small stdout cap and a short
// kill grace, so the overflow and timeout proofs stay fast without weakening
// what they assert.
func newTestEvaluator() *Evaluator {
	return NewEvaluator(Config{
		MaxStdout: 64 << 10, // 64 KiB
		KillGrace: 150 * time.Millisecond,
	}, nil)
}

// baseSpec returns a spec whose defaults right for most evaluator tests, with
// a short timeout so a hang proof finishes quickly.
func baseSpec(t *testing.T, argv ...string) Spec {
	t.Helper()
	return Spec{
		Name:        "dropzone",
		Job:         "import-file",
		Argv:        argv,
		Timeout:     10 * time.Second,
		MaxTriggers: 100,
	}
}

// evalBounded runs an evaluation inside a watchdog so a mutant that hangs
// turns a red test into a loud failure instead of a stalled gate.
func evalBounded(t *testing.T, bound time.Duration, ctx context.Context, ev *Evaluator, s Spec, in Input) Result {
	t.Helper()
	type outcome struct{ r Result }
	done := make(chan outcome, 1)
	go func() { done <- outcome{ev.Evaluate(ctx, s, in)} }()
	select {
	case o := <-done:
		return o.r
	case <-time.After(bound):
		t.Fatalf("Evaluate did not return within %s; termination is broken", bound)
		return Result{}
	}
}

// processWithCmdline reports whether any process on this host carries the
// given marker in its command line. It is the /proc-scan proof that a
// timeout killed the whole sensor process group, leader and grandchildren
// alike (05 section 11).
func processWithCmdline(marker string) bool {
	procs, err := filepath.Glob("/proc/[0-9]*/cmdline")
	if err != nil {
		return false
	}
	for _, p := range procs {
		raw, err := os.ReadFile(p)
		if err != nil {
			continue // the process exited mid scan
		}
		if strings.Contains(string(raw), marker) {
			return true
		}
	}
	return false
}

// waitForMarkerGone polls until no process carries the marker, failing loudly
// with what the scan still sees.
func waitForMarkerGone(t *testing.T, marker string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for processWithCmdline(marker) {
		if time.Now().After(deadline) {
			t.Fatalf("process group still has a member after %s: %q persists in /proc", within, marker)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
