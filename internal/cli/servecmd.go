package cli

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/daemon"
)

// serveFlags are the knobs the serve command takes. They mirror the serve
// section of the configuration sketch; the config file itself arrives with
// the packaging milestone.
type serveFlags struct {
	jobsDir       string
	socket        string
	metricsListen string
	workers       int
	drainTimeout  time.Duration
	noNotifyBus   bool
}

func newServeCmd(env Env, g *globals) *cobra.Command {
	var f serveFlags
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the daemon: schedules, dispatch and execution in one process",
		Long: `Run the long lived process that owns this state directory.

One process holds the lock, runs every loop, and executes the work: the
scheduler, the dispatcher, the executor pool, the reaper, the janitor and
the health endpoints. Stopping it is safe at any moment: a SIGINT or
SIGTERM drains what is running, hands interrupted work back to the queue,
and leaves nothing behind. A second signal within that drain kills every
process group at once and exits 130.

While the daemon runs, no second paceq may write to the same state:
another serve, or any command that writes, is refused with exit 6.`,
		Example: `  paceq serve
    Serve the project in this directory.

  paceq serve --workers 4 --drain-timeout 1m
    Cap the executor pool and give running steps a minute to finish.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runServe(ctx, env, g, f)
		}),
	}
	cmd.Flags().StringVar(&f.jobsDir, "jobs-dir", "jobs", "directory the scheduler reads job files from")
	cmd.Flags().StringVar(&f.socket, "socket", "", "unix socket for the health endpoints (empty: disabled until M2-08)")
	cmd.Flags().StringVar(&f.metricsListen, "metrics-listen", "",
		"opt-in TCP bind for /metrics; loopback only, e.g. 127.0.0.1:9753 (default: unix socket only)")
	cmd.Flags().IntVar(&f.workers, "workers", 0, "runs executed at once (0: one per CPU)")
	cmd.Flags().DurationVar(&f.drainTimeout, "drain-timeout", 30*time.Second, "how long running steps may finish on a stop")
	cmd.Flags().BoolVar(&f.noNotifyBus, "no-notify-bus", false,
		"disable the wake-up bus and run on tickers alone (a test switch that must change nothing)")
	return cmd
}

func runServe(ctx context.Context, env Env, g *globals, f serveFlags) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}

	// The metrics TCP bind is opt-in and loopback only (#40). The refusal
	// is a usage error - exit 2 with the explanation - because the command
	// as written must never start: a daemon that came up without the
	// metrics surface its operator configured for is worse than one that
	// refused to start.
	if f.metricsListen != "" {
		if err := daemon.ValidateMetricsListen(f.metricsListen); err != nil {
			return usageError(
				fmt.Sprintf("--metrics-listen %s can only bind to loopback", f.metricsListen),
				err.Error(),
				"Use 127.0.0.1:<port> (or [::1]:<port>), or leave the flag out: /metrics then answers on the unix socket only.")
		}
	}

	// Signals reach the daemon twice: once through the process context,
	// which starts the graceful stop, and once through this channel, which
	// arms the hard stop for whoever insists a second time.
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigs)

	cfg := daemon.Config{
		Version:          buildinfo.Get().Version,
		StateDir:         stateDir,
		JobsDir:          f.jobsDir,
		SocketPath:       f.socket,
		MetricsListen:    f.metricsListen,
		Workers:          f.workers,
		DrainTimeout:     f.drainTimeout,
		DisableNotifyBus: f.noNotifyBus,
		Signals:          sigs,
		Logger:           slog.New(slog.NewJSONHandler(env.Stderr, nil)),
	}
	return daemon.Serve(ctx, cfg, clkOf(env))
}
