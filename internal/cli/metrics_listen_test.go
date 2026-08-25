package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

// TestMetricsListenBeyondLoopbackIsRefusedWithExitTwo is binding test 8
// (#40): --metrics-listen with anything beyond loopback is refused before
// the daemon comes up, with exit 2 and an explanation that names the
// loopback alternative. A refusal that started a daemon anyway would be
// worse than one without metrics.
func TestMetricsListenBeyondLoopbackIsRefusedWithExitTwo(t *testing.T) {
	dir := newServeProject(t)

	for _, addr := range []string{"0.0.0.0:9753", "192.168.1.10:9753"} {
		res := runCLI(t, dir, nil, "serve", "--metrics-listen", addr)
		if res.code != ExitUsage {
			t.Errorf("serve --metrics-listen %s: exit %d, want %d (usage)",
				addr, res.code, ExitUsage)
		}
		if !strings.Contains(res.stderr, "loopback") {
			t.Errorf("serve --metrics-listen %s: refusal must explain loopback, got:\n%s",
				addr, res.stderr)
		}
	}
}

// TestMetricsListensAcceptLoopback keeps the refusal honest: a loopback
// address passes validation and reaches a running daemon, which the test
// then stops through its context - the same shape as every serve test.
func TestMetricsListensAcceptLoopback(t *testing.T) {
	dir := newServeProject(t)

	var out syncedBuffer
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		env := Env{
			Stdout: &bytes.Buffer{},
			Stderr: &out,
			Dir:    dir,
			Getenv: func(string) string { return "" },
		}
		done <- MainEnv(ctx, env, []string{
			"serve", "--jobs-dir", "jobs", "--metrics-listen", "127.0.0.1:0",
		})
	}()

	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && !strings.Contains(out.String(), "daemon ready") {
		time.Sleep(20 * time.Millisecond)
	}
	if !strings.Contains(out.String(), "daemon ready") {
		t.Fatalf("daemon with --metrics-listen 127.0.0.1:0 never became ready:\n%s", out.String())
	}
	cancel()

	select {
	case code := <-done:
		if code != ExitOK && code != ExitInterrupted {
			t.Errorf("a clean stop should not look like a failure: exit %d\n%s",
				code, out.String())
		}
	case <-time.After(60 * time.Second):
		t.Fatal("serve did not stop after its context was cancelled")
	}
}
