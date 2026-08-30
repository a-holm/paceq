package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/sockpath"
	"github.com/a-holm/paceq/internal/testutil"
)

// TestReadyLineNeverClaimsASocketTheDaemonDoesNotHave is the invariant #234
// exists for: an operator who reads socket:true and then cannot connect has
// been told a falsehood by the one process whose job is to report on itself.
// The two cases are the whole space - a socket that binds and one that cannot -
// and the second one is the defect: the daemon logged the bind failure, kept
// running, and announced socket:true from the configuration.
func TestReadyLineNeverClaimsASocketTheDaemonDoesNotHave(t *testing.T) {
	t.Run("a socket that binds", func(t *testing.T) {
		rec, logger := newRecLog()
		root := t.TempDir()
		socket := testutil.SocketPath(t)
		cfg := Config{
			StateDir:     filepath.Join(root, "state"),
			Version:      "test",
			JobsDir:      "jobs",
			Logger:       logger,
			Owner:        "serve:test",
			SocketPath:   socket,
			TickInterval: time.Hour,
		}
		if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
			t.Fatalf("create the state directory: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- Serve(ctx, cfg, clock.System()) }()
		waitForLogLine(t, rec, "daemon ready", 20*time.Second)

		ready := rec.named("daemon ready")
		if got := ready[0]["socket"]; got != true {
			t.Errorf("the ready line says socket=%v while the endpoints listen on %s", got, socket)
		}
		if _, err := os.Stat(socket); err != nil {
			t.Errorf("the ready line claims a socket that is not on disk: %v", err)
		}

		cancel()
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("a clean stop reported %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("Serve did not return within 20s of cancellation")
		}
	})

	t.Run("a socket that cannot bind", func(t *testing.T) {
		rec, logger := newRecLog()
		root := t.TempDir()
		socket := overLongSocketPath(t, root)
		cfg := Config{
			StateDir:     filepath.Join(root, "state"),
			Version:      "test",
			JobsDir:      "jobs",
			Logger:       logger,
			Owner:        "serve:test",
			SocketPath:   socket,
			TickInterval: time.Hour,
		}
		if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
			t.Fatalf("create the state directory: %v", err)
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errCh := make(chan error, 1)
		go func() { errCh <- Serve(ctx, cfg, clock.System()) }()

		var err error
		select {
		case err = <-errCh:
		case <-time.After(20 * time.Second):
			cancel()
			t.Fatalf("Serve kept running for 20s without the socket it was configured with; it logged:\n%s",
				rec.text())
		}
		if err == nil {
			t.Fatalf("Serve returned nil for a socket it never listened on; it logged:\n%s", rec.text())
		}
		for _, want := range []string{strconv.Itoa(len(socket)), strconv.Itoa(sockpath.MaxLen), socket} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal does not name %q: %v", want, err)
			}
		}
		if lines := rec.named("daemon ready"); len(lines) != 0 {
			t.Errorf("the daemon announced itself ready without its socket: %+v", lines)
		}
		if _, statErr := os.Stat(socket); statErr == nil {
			t.Errorf("a socket exists at %s, so the case proved nothing", socket)
		}
	})
}

// overLongSocketPath names a socket one byte past what the kernel takes, inside
// a directory that exists, so the refusal is about the length and nothing else.
func overLongSocketPath(t *testing.T, dir string) string {
	t.Helper()

	pad := sockpath.MaxLen + 1 - len(dir) - 1
	if pad < 1 {
		t.Skipf("TMPDIR gives %d bytes, so no path of %d bytes can be built under it",
			len(dir), sockpath.MaxLen+1)
	}
	path := filepath.Join(dir, strings.Repeat("s", pad))
	if len(path) != sockpath.MaxLen+1 {
		t.Fatalf("the padded path is %d bytes, want %d: %s", len(path), sockpath.MaxLen+1, path)
	}
	return path
}
