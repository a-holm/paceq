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
	"github.com/a-holm/paceq/internal/scheduler"
	"github.com/a-holm/paceq/internal/sockpath"
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
	shadow        bool
	observe       string
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
another serve, or any command that writes, is refused with exit 6.

The disk-guard watches the filesystem that holds the state every thirty
seconds. Under limits.disk_min_free_percent (default 10) or
limits.disk_min_free_bytes (default 1GiB), whichever binds first, the
daemon goes degraded: new runs are refused with reason code
RUN_REJECTED_DISK_LOW, running ones finish, ticks keep being decided, and
/readyz answers 503 until the disk recovers. Leaving degraded needs two
healthy checks in a row, so a burst of freed space cannot flap the daemon.
The log directory is capped at limits.log_max_bytes (default 10GiB): once
it is passed, the oldest date shards are removed first, and today's and
yesterday's are always kept. The database's write ahead log is watched at
limits.wal_warn_bytes (default 64MiB; four times that is the error level).
The four keys live in the limits section of config.yaml, the same file
that names the notifiers; the disk.low and wal.growth alerts go to the
notify_defaults on_failure targets.`,
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
	cmd.Flags().StringVar(&f.socket, "socket", "",
		"unix socket to listen on for client commands and health endpoints "+
			"(empty: no socket, so clients write directly; they need the same path in PACEQ_SOCKET or --socket)")
	cmd.Flags().StringVar(&f.metricsListen, "metrics-listen", "",
		"opt-in TCP bind for /metrics; loopback only, e.g. 127.0.0.1:9753 (default: unix socket only)")
	cmd.Flags().IntVar(&f.workers, "workers", 0, "runs executed at once (0: one per CPU)")
	cmd.Flags().DurationVar(&f.drainTimeout, "drain-timeout", 30*time.Second, "how long running steps may finish on a stop")
	cmd.Flags().BoolVar(&f.noNotifyBus, "no-notify-bus", false,
		"disable the wake-up bus and run on tickers alone (a test switch that must change nothing)")
	cmd.Flags().BoolVar(&f.shadow, "shadow", false,
		"shadow mode: plan and record every schedule, execute nothing (#32)")
	cmd.Flags().StringVar(&f.observe, "observe", "none",
		"with --shadow, where observed cron starts come from: none, journald or file=<path>")
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

	// Shadow observation setup fails here, on the command line, rather than
	// after migration inside the daemon: a typo in --observe must never
	// become a mystery about why serve refused to come up.
	if f.shadow {
		if _, err := scheduler.ParseObserveSpec(f.observe); err != nil {
			return usageError(fmt.Sprintf("--observe %s is not a usable source", f.observe), err.Error(),
				"Use none, journald, or file=/path/to/log.")
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
		Shadow:           f.shadow,
		Observe:          f.observe,
		Signals:          sigs,
		Logger:           slog.New(slog.NewJSONHandler(env.Stderr, nil)),
	}
	// A socket the kernel cannot name is refused before the daemon starts,
	// for the same reason --metrics-listen is: the length is knowable from
	// the string, and a daemon that came up without the health endpoints and
	// the write API its operator configured is worse than one that refused
	// to start (#234). The resolved path is what is measured, not the flag,
	// because a path derived from the state directory can exceed the limit
	// on its own. Go returns EINVAL for an over-long name before the bind
	// syscall runs, so an operator would otherwise read "invalid argument"
	// about a path that looks ordinary.
	if cfg.SocketPath != "" {
		if err := sockpath.Validate(cfg.SocketPath); err != nil {
			return usageError(err.Error(),
				fmt.Sprintf("sockaddr_un holds %d bytes including its terminator, so a longer name never reaches the kernel.",
					sockpath.MaxLen+1),
				"Give --socket a shorter path, or move the state directory closer to the root.")
		}
	}

	return daemon.Serve(ctx, cfg, clkOf(env))
}
