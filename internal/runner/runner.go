package runner

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

const (
	// DefaultTimeout is the timeout a job gets when no layer above the runner
	// set one. A job without a deadline is a job that hangs forever one day.
	DefaultTimeout = time.Hour

	// MaxTimeout is the hard system cap. The validator refuses anything
	// larger; the runner repeats the check as a second line of defence.
	MaxTimeout = DefaultTimeout

	// DefaultKillGrace is how long a job may keep running after SIGTERM
	// before SIGKILL ends the whole group.
	DefaultKillGrace = 10 * time.Second
)

// RunContext is everything the runner knows about why this process exists. It
// becomes the PACEQ_ environment contract.
type RunContext struct {
	RunID        string
	Job          string
	Step         string
	Attempt      int
	RunKey       string
	Params       map[string]any
	ScheduledFor time.Time // zero means a manual run
}

// Spec is one process attempt. The zero value is not runnable: Argv, a
// positive Timeout within the cap and a Ctx are required.
type Spec struct {
	Argv       []string
	Shell      bool              // explicit opt-in: wrap in /bin/sh -c
	Workdir    string            // relative paths resolve inside the runner's working directory root
	Env        map[string]string // the job's own environment, highest user layer
	EnvFile    string            // KEY=VALUE file, mode 0600 exactly
	InheritEnv []string          // daemon variables to pass through, by name
	Timeout    time.Duration     // mandatory, capped at MaxTimeout
	KillGrace  time.Duration     // SIGTERM to SIGKILL gap; DefaultKillGrace when not positive
	Ctx        RunContext
	Clock      clock.Clock // nil means clock.System
	Stdout     io.Writer   // nil means /dev/null
	Stderr     io.Writer   // nil means /dev/null
	OutputPath string      // created before the command starts, handed over as PACEQ_OUTPUT

	// The upstream references of this step (#13). InputsJSON is the merged
	// closure in its frozen shape, handed over inline as PACEQ_INPUTS; an
	// empty string reads as the empty document {}. When the merged payload
	// crossed the spill bound, InputsJSON is the literal "null" and
	// InputsFile names the file that carries it instead, handed over as
	// PACEQ_INPUTS_FILE with PACEQ_INPUTS set to null so a jq pipeline
	// still reads.
	InputsJSON string
	InputsFile string

	// OnStart fires once per successful spawn, before Run returns, with the
	// child's pid. The engine persists a process baseline through it (issue
	// #62): pid plus /proc start ticks on file at spawn time is what later
	// lets the orphan sweep tell a surviving child of a dead executor from a
	// recycled pid. It is not called for a refused or failed spawn, and a
	// slow or failing callback delays the job's own bookkeeping by whatever
	// it takes; callers keep it cheap and non-fatal.
	OnStart func(pid int)
}

// ErrInvalidSpec wraps every refusal Run makes before touching a process.
// These are caller bugs: no attempt was made, no attempt is retryable, and the
// fix is in the spec, not the job.
var ErrInvalidSpec = errors.New("invalid runner spec")

func (s Spec) validate() error {
	if len(s.Argv) == 0 {
		return fmt.Errorf("%w: argv is empty", ErrInvalidSpec)
	}
	if !s.Shell && !strings.Contains(s.Argv[0], "/") {
		return fmt.Errorf("%w: argv[0] %q has no path separator; the runner does no PATH lookup", ErrInvalidSpec, s.Argv[0])
	}
	if s.Timeout <= 0 {
		return fmt.Errorf("%w: timeout must be positive, got %s", ErrInvalidSpec, s.Timeout)
	}
	if s.Timeout > MaxTimeout {
		return fmt.Errorf("%w: timeout %s exceeds the system cap of %s", ErrInvalidSpec, s.Timeout, MaxTimeout)
	}
	for k := range s.Env {
		if strings.HasPrefix(k, paceqPrefix) {
			return fmt.Errorf("%w: job env sets reserved key %s", ErrInvalidSpec, k)
		}
	}
	for _, k := range s.InheritEnv {
		if strings.HasPrefix(k, paceqPrefix) {
			return fmt.Errorf("%w: inherit_env lists reserved key %s", ErrInvalidSpec, k)
		}
	}
	return nil
}

// Run starts the command in its own process group and returns what happened.
//
// The error result is reserved for spec refusals (ErrInvalidSpec): the process
// was never attempted. Every operating system level failure, including a
// missing binary, comes back as a SpawnFailed Result with a nil error,
// because those are job outcomes a caller can log, explain and retry.
func Run(ctx context.Context, s Spec) (Result, error) {
	if err := s.validate(); err != nil {
		return Result{}, err
	}
	clk := s.Clock
	if clk == nil {
		clk = clock.System()
	}
	grace := s.KillGrace
	if grace <= 0 {
		grace = DefaultKillGrace
	}

	// Everything below can still fail before the process exists. Those
	// failures are SpawnFailed: nothing ran, so a retry is safe.
	workdir, err := resolveWorkdir(s.Workdir)
	if err != nil {
		return spawnFailure(err, "workdir")
	}
	env, err := buildEnv(s, workdir)
	if err != nil {
		return spawnFailure(err, "environment")
	}
	closeOutput, err := createOutput(workdir, s.OutputPath)
	if err != nil {
		return spawnFailure(err, "output")
	}
	defer closeOutput()

	argv := s.Argv
	if s.Shell {
		argv = append([]string{"/bin/sh", "-c"}, strings.Join(s.Argv, " "))
	}

	// CommandContext wires the parent context to Cancel: when a run is
	// cancelled from outside, the whole group gets the sequence. The deadline
	// itself is not a context feature; the clock timer in Run fires the same
	// escalation directly.
	//
	// Launching a configured argv is this package's entire purpose; that is
	// ground rule 1, user code never runs in-process. The spec validator has
	// already refused anything without an absolute or slash carrying argv[0],
	// so no PATH lookup is involved.
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...) // #nosec G204 - the configured job argv is the contract, see above
	cmd.Dir = workdir
	cmd.Env = env
	cmd.Stdout = s.Stdout // nil is /dev/null, which is what a log-less run wants
	cmd.Stderr = s.Stderr
	cmd.Stdin = nil // a job never inherits the daemon's stdin
	cmd.WaitDelay = grace
	cmd.SysProcAttr = sysProcAttr()

	esc := newEscalation(nil, grace, clk)
	defer esc.stop()
	// exec calls Cancel when the parent context ends. The deadline is not a
	// context feature: the clock timer below fires it, so a fake clock owns
	// every timing decision.
	cmd.Cancel = esc.fire

	var timedOut atomic.Bool
	timer := clk.NewTimer(s.Timeout)
	defer timer.Stop()

	releaseCoreLimit := zeroCoreLimit()
	startedAt := clk.Now().UnixMilli()

	if err := cmd.Start(); err != nil {
		releaseCoreLimit()
		return spawnFailure(err, "")
	}

	pgid := cmd.Process.Pid // the child led its own group since Setpgid
	esc.setGroup(pgid)
	releaseGroup := registerGroup(pgid)
	if s.OnStart != nil {
		s.OnStart(pgid)
	}

	// The deadline watcher starts only once the process exists; before that a
	// refusal or a spawn failure is the whole story. Run waits for it before
	// returning, so a caller counting goroutines finds nothing left behind.
	watcherDone := make(chan struct{})
	go func() {
		defer close(watcherDone)
		select {
		case <-timer.C:
			timedOut.Store(true)
			_ = esc.fire() // fire never fails; the error satisfies exec only
		case <-esc.done:
		}
	}()

	waitErr := cmd.Wait()
	finishedAt := clk.Now().UnixMilli()
	releaseGroup()
	releaseCoreLimit()
	esc.stop()
	<-watcherDone
	_ = waitErr // the classification reads the process state, not the error wrapper

	res := classify(cmd, timedOut.Load(), ctx.Err(), s.Timeout)
	res.Pgid = pgid
	res.StartedAt = startedAt
	res.FinishedAt = finishedAt
	return res, nil
}

// spawnFailure builds the Result for everything that fails before the command
// runs: the process never existed, so no exit status, no command side effects
// and a retry that is always safe.
func spawnFailure(cause error, stage string) (Result, error) {
	data := map[string]any{}
	var errno syscall.Errno
	if errors.As(cause, &errno) {
		data["errno"] = int(errno)
	}
	if stage != "" {
		data["stage"] = stage
	}
	return Result{
		Outcome:    SpawnFailed,
		ReasonCode: ReasonSpawn,
		ReasonData: data,
		Err:        cause,
	}, nil
}
