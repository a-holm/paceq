//go:build unix

package notify

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The group-kill contract needs a real child that ignores SIGTERM plus a
// grandchild in the same group: exactly what the runner's fakecmd fixture
// provides. A timeout that killed only the leader would strand the
// grandchild; this test fails if either process outlives the escalation.
func TestExecNotifierTimeoutKillsWholeGroup(t *testing.T) {
	runUnixGroupKill(t)
}

func runUnixGroupKill(t *testing.T) {
	t.Helper()

	root := moduleRoot(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "fakecmd")
	build := exec.Command("go", "build", "-o", path, "./testdata/fakecmd")
	build.Dir = filepath.Join(root, "internal", "runner")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build fakecmd: %v\n%s", err, out)
	}

	marker := "paceq-notify-group-probe"
	e := &ExecNotifier{Name: "vakt", Argv: []string{path, "tree", "30s", marker}}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	sendErr := e.Send(ctx, aMsg())
	if sendErr == nil {
		t.Fatal("a timing-out notifier reported success")
	}
	if !strings.Contains(sendErr.Error(), "timed out") && !strings.Contains(sendErr.Error(), "group") &&
		!strings.Contains(sendErr.Error(), "killed") {
		t.Fatalf("the timeout error lost its meaning: %v", sendErr)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		live := procsWithArg(marker)
		if len(live) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d processes survived the whole-group kill: %v", len(live), live)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// procsWithArg lists /proc cmdlines carrying the marker.
func procsWithArg(marker string) []string {
	var found []string
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if !entry.IsDir() || !isNumeric(entry.Name()) {
			continue
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/cmdline")
		if err != nil {
			continue
		}
		cmdline := strings.ReplaceAll(string(raw), "\x00", " ")
		if strings.Contains(cmdline, marker) {
			found = append(found, strings.TrimSpace(cmdline))
		}
	}
	return found
}

func isNumeric(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return s != ""
}

func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the notify package")
		}
		dir = parent
	}
}
