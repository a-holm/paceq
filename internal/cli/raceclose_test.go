package cli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/store"
)

// TestTheStartRaceAlwaysHasExactlyOneWriter is the M2-08 slice 6 proof: a
// daemon starting while a CLI write is in flight must end with the write
// done exactly once, by whichever transport won, never exit 6 from the
// start-up window and never two runs.
//
// The delay between "daemon decides to start" and "writer starts" walks a
// deterministic ladder across the iterations, so every interleaving of
// lock-take, migrate, listen and dial is visited. Everything waits on real
// events (the listener answering, the command finishing); there are no
// sleeps standing in for coordination.
func TestTheStartRaceAlwaysHasExactlyOneWriter(t *testing.T) {
	if testing.Short() {
		t.Skip("the ladder needs its full length")
	}
	const iterations = 200

	for i := range iterations {
		t.Run(fmt.Sprintf("iter-%03d", i), func(t *testing.T) {
			dir := newProjectWithState(t)
			applyJob(t, dir, quickJob)
			stateDir := filepath.Join(dir, ".paceq")
			sock := filepath.Join(dir, "race.sock")

			ctx, stopDaemon := context.WithCancel(context.Background())
			daemonErr := make(chan error, 1)
			go func(delay time.Duration) {
				time.Sleep(delay)
				cfg := daemon.Config{
					Version:    "test",
					StateDir:   stateDir,
					SocketPath: sock,
					// The daemon must actually execute what it accepts,
					// otherwise a socket-path write would wait for a run
					// that never starts. Twenty milliseconds keeps the
					// iterations quick.
					TickInterval:     20 * time.Millisecond,
					DisableNotifyBus: true,
					Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
				}
				daemonErr <- daemon.Serve(ctx, cfg, clock.System())
			}(time.Duration(i%7) * time.Millisecond)

			res := runCLI(t, dir, map[string]string{"PACEQ_SOCKET": sock}, "run", "quick")
			if res.code != ExitOK {
				t.Fatalf("iteration %d: the writer exited %d, want 0\nstderr:\n%s",
					i, res.code, res.stderr)
			}

			stopDaemon()
			select {
			case <-daemonErr:
			case <-time.After(15 * time.Second):
				t.Fatalf("iteration %d: the losing daemon did not stop within 15s", i)
			}

			st := openReadOnlyProject(t, dir)
			defer func() { _ = st.Close() }()
			rows, err := st.ListRuns(context.Background(), store.RunFilter{})
			if err != nil {
				t.Fatalf("iteration %d: list runs: %v", i, err)
			}
			if len(rows) != 1 {
				t.Fatalf("iteration %d: %d runs exist, want exactly 1", i, len(rows))
			}
			events, err := st.RunEvents(context.Background(), rows[0].ID)
			if err != nil || len(events) == 0 {
				t.Fatalf("iteration %d: no queued event (err=%v)", i, err)
			}
			actor := events[0].Actor
			if actor != "api" && !strings.HasPrefix(actor, "cli:") {
				t.Fatalf("iteration %d: actor %q is neither api nor cli", i, actor)
			}
		})
	}
}
