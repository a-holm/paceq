package daemon

import (
	"log/slog"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/janitor"
	"github.com/a-holm/paceq/internal/store"
)

// ExitHardStop is the exit code of a daemon that got a second stop request and
// answered it by killing every process group at once. It sits above the shell
// signal band, so "killed by insisting" can never be confused with the exit
// codes paceq hands out on its own (03 section 7.2).
const ExitHardStop = 130

// Deficits in the config fall back to these values. They are the keys from the
// serve section of the configuration sketch, with one place each to change.
const (
	defaultDrainTimeout   = 30 * time.Second
	defaultTickInterval   = 1 * time.Second
	defaultHeartbeatEvery = 10 * time.Second
	defaultReapEvery      = engine.DefaultReapInterval
	defaultReconcileEvery = 30 * time.Second
)

// Config is everything Serve needs beyond the state directory. The zero value
// runs with defaults: workers equal to the CPU count, a 30 second drain, and
// tickers at one second.
type Config struct {
	// Version lands in the session row and in /readyz, so an operator can
	// tell which binary has been running since when.
	Version string

	// StateDir holds the lock file, the database and the logs. It is the only
	// required field.
	StateDir string

	// JobsDir is where job files live. The scheduler reads it from M2-05 on;
	// until then it is accepted and carried.
	JobsDir string

	// ConfigDir is the directory paceq reads configuration files from.
	// Empty means /etc/paceq when running under systemd, or the working
	// directory otherwise.
	ConfigDir string

	// RuntimeDir is where transient runtime files live (the unix socket).
	// Empty means /run/paceq when running under systemd, falling back to
	// the state directory.
	RuntimeDir string

	// SocketPath enables the health endpoints over a unix socket when not
	// empty. Empty means disabled for now: the runtime directory contract
	// arrives with the systemd work, and nothing else should invent paths.
	SocketPath string

	// MetricsListen is the opt-in TCP bind for /metrics (#40). Empty means
	// the endpoint answers on the unix socket only, which is the default
	// the security plan fixes (08 section 6: no TCP listener in the MVP
	// without an explicit operator decision). When set, it is validated to
	// loopback before anything starts; anything else is refused with an
	// explanation, never bound.
	MetricsListen string

	// Workers is how many runs may execute at once. Zero means
	// runtime.NumCPU(). One dispatcher hands work out no matter what; this
	// caps the executors behind it.
	Workers int

	// DrainTimeout bounds phase two of the shutdown: how long executors may
	// keep their process groups while they finish. Zero means 30s.
	DrainTimeout time.Duration

	// KillGrace is the SIGTERM to SIGKILL gap inside every step's process
	// group during the drain. Zero leaves the runner's own default.
	KillGrace time.Duration

	// SensorMaxParallel is the global cap on concurrent sensor evaluations.
	// Zero means four.
	SensorMaxParallel int

	// TickInterval is the safety net period shared by the loops: how often a
	// loop looks at the world without being woken. Zero means 1s.
	TickInterval time.Duration

	// HeartbeatEvery is how often the session row's last_seen_at moves
	// forward. Zero means 10s.
	HeartbeatEvery time.Duration

	// LeaseTTL is the lease an executor claims per run. Zero means sixty
	// seconds, renewed every twenty by the daemon's renewal loop. Tests
	// shorten it; production should not.
	LeaseTTL time.Duration

	// RenewInterval overrides how often held leases renew. Zero means a
	// third of the ttl.
	RenewInterval time.Duration

	// ClockSkewAllowance is how long past the expiry the reaper waits before
	// it takes a run. Zero means ten seconds.
	ClockSkewAllowance time.Duration

	// RequeueBackoff is how long a reaped run waits before it is due again.
	// Zero means thirty seconds.
	RequeueBackoff time.Duration

	// ReapEvery is how often the reaper sweep looks for expired leases.
	// Zero means ten seconds.
	ReapEvery time.Duration

	// ReconcileEvery is the safety-net cadence for periodic reconciliation
	// (issue #62), riding under the reaper's role lease. Zero means thirty
	// seconds.
	ReconcileEvery time.Duration

	// MaxCrashCount is the poison quarantine line for runs that keep dying
	// with their executor. Zero means five.
	MaxCrashCount int

	// Owner names the claim holder in the database. Empty means serve:<pid>.
	Owner string

	// DisableNotifyBus is the --no-notify-bus switch. With it set the loops
	// run on their tickers alone, which is the standing proof that the bus is
	// an optimisation and never a dependency.
	DisableNotifyBus bool

	// Policies carries the retention configuration keys (issue #36). Zero
	// fields fall back to the shipped defaults, so a config that says
	// nothing keeps the documented horizons.
	Policies store.Policies

	// NightlyHour is the local hour the maintenance cycle aims for. Zero
	// means 03:00 (07 section 6.5). Tests move it to make a slot due.
	NightlyHour int

	// Signals carries copies of the process signals. When set, the daemon
	// watches it for the second stop request: two signals mean the operator
	// insists, and every process group gets SIGKILL before ExitHardStop. Nil
	// disables the watcher.
	Signals <-chan os.Signal

	// OnHardStop ends the process after the hard kill. Tests record the call;
	// production leaves it nil for os.Exit(ExitHardStop).
	OnHardStop func()

	// Logger receives the structured lines. JSON on stderr is what journald
	// wants (06 section 5); nil means slog's default.
	Logger *slog.Logger
}

// workerCount resolves Workers.
func (c Config) workerCount() int {
	if c.Workers > 0 {
		return c.Workers
	}
	return runtime.NumCPU()
}

func (c Config) drainTimeout() time.Duration {
	if c.DrainTimeout > 0 {
		return c.DrainTimeout
	}
	return defaultDrainTimeout
}

func (c Config) tickInterval() time.Duration {
	if c.TickInterval > 0 {
		return c.TickInterval
	}
	return defaultTickInterval
}

func (c Config) heartbeatEvery() time.Duration {
	if c.HeartbeatEvery > 0 {
		return c.HeartbeatEvery
	}
	return defaultHeartbeatEvery
}

func (c Config) owner() string {
	if c.Owner != "" {
		return c.Owner
	}
	return "serve:" + strconv.Itoa(os.Getpid())
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func (c Config) leaseTTL() time.Duration {
	if c.LeaseTTL > 0 {
		return c.LeaseTTL
	}
	return store.DefaultRunLeaseTTL
}

func (c Config) reapEvery() time.Duration {
	if c.ReapEvery > 0 {
		return c.ReapEvery
	}
	return defaultReapEvery
}

func (c Config) reconcileEvery() time.Duration {
	if c.ReconcileEvery > 0 {
		return c.ReconcileEvery
	}
	return defaultReconcileEvery
}

// killGrace returns the SIGTERM to SIGKILL gap used for sensor subprocess
// groups. Zero means the runner default, which the evaluator resolves.
func (c Config) killGrace() time.Duration { return c.KillGrace }

// nightlyHour resolves the local hour the maintenance cycle aims for
// (07 section 6.5 names 03:00). Zero means the shipped default.
func (c Config) nightlyHour() int {
	if c.NightlyHour > 0 {
		return c.NightlyHour
	}
	return janitor.NightlyHourDefault
}

// sensorMaxParallel resolves the sensor concurrency cap.
func (c Config) sensorMaxParallel() int {
	if c.SensorMaxParallel > 0 {
		return c.SensorMaxParallel
	}
	return 4
}
