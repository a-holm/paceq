package cli

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncedBuffer is safe to read while the command writes it.
type syncedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newServeProject makes a directory with applied state, which is all serve
// needs before its first tick.
func newServeProject(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	res := runCLI(t, dir, nil, "init")
	if res.code != ExitOK {
		t.Fatalf("paceq init failed:\n%s%s", res.stdout, res.stderr)
	}
	return dir
}

// TestSecondServeIsRefusedWithExitSix is acceptance criterion 2: two daemons
// cannot share one state directory, and the loser says so plainly and exits
// with the busy code.
func TestSecondServeIsRefusedWithExitSix(t *testing.T) {
	dir := newServeProject(t)

	var out syncedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		env := Env{
			Stdout: &bytes.Buffer{},
			Stderr: &out,
			Dir:    dir,
			Getenv: func(string) string { return "" },
		}
		done <- MainEnv(ctx, env, []string{"serve", "--jobs-dir", "jobs"})
	}()

	waitForOpenSession := func() {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if strings.Contains(out.String(), `"msg":"daemon ready"`) {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("the first serve never became ready:\n%s", out.String())
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	waitForOpenSession()

	second := runCLI(t, dir, nil, "serve")
	if second.code != ExitBusy {
		t.Fatalf("the second serve exited %d, want %d (busy)\nstderr:\n%s",
			second.code, ExitBusy, second.stderr)
	}
	if !strings.Contains(second.stderr, "already") {
		t.Errorf("the refusal says %q, want it to name the holder", second.stderr)
	}

	cancel()
	select {
	case code := <-done:
		if code != ExitOK {
			t.Fatalf("the first serve exited %d after cancellation, want %d", code, ExitOK)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the first serve did not stop within 15s of cancellation")
	}
}

// TestServeFlagsAreDeclared keeps the flag names the issue promises visible in
// the command's own help, where a script author looks for them.
func TestServeFlagsAreDeclared(t *testing.T) {
	res := runCLI(t, ".", nil, "serve", "--help")
	if res.code != ExitOK {
		t.Fatalf("serve --help exited %d", res.code)
	}
	for _, flag := range []string{
		"--jobs-dir", "--socket", "--workers", "--drain-timeout", "--no-notify-bus",
	} {
		if !strings.Contains(res.stdout, flag) {
			t.Errorf("serve --help does not mention %s", flag)
		}
	}
}

// TestServeRefusesArguments: the daemon takes flags, not positional words.
func TestServeRefusesArguments(t *testing.T) {
	res := runCLI(t, ".", nil, "serve", "now")
	if res.code != ExitUsage {
		t.Fatalf("serve now exited %d, want %d (usage)", res.code, ExitUsage)
	}
}
