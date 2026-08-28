package runner

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The exec shim's argument surface (issue #39). The flags are parsed in this
// package so every entry point shares one parser: the hidden `paceq exec`
// command, and a test binary's TestMain, which answers `binary exec ...` the
// same way so a suite can drive the real exec chain through its own image.
//
// The surface is deliberately not a public contract: it is an implementation
// detail between two versions of the same binary. Everything after the first
// bare `--` is the job's argv, verbatim, flags and all.

// ParseExecArgs reads one shim invocation. The returned argv is everything
// after the first bare `--`; an error is a usage fact the caller renders.
func ParseExecArgs(args []string) (ShimConfig, []string, error) {
	var cfg ShimConfig
	cfg.WatchFD = -1
	cfg.BaseFD = -1

	need := func(name string, values []string, i *int) (string, error) {
		if *i+1 >= len(values) {
			return "", fmt.Errorf("%s needs a value", name)
		}
		*i++
		return values[*i], nil
	}

	var argv []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			argv = args[i+1:]
			break
		}
		if !strings.HasPrefix(arg, "--") {
			return cfg, nil, fmt.Errorf("unexpected argument %q before `--`", arg)
		}
		name, value := arg, ""
		hasValue := false
		if eq := strings.IndexByte(arg, '='); eq >= 0 {
			name, value, hasValue = arg[:eq], arg[eq+1:], true
		}
		take := func() (string, error) {
			if hasValue {
				return value, nil
			}
			return need(name, args, &i)
		}
		var err error
		switch name {
		case "--run-id":
			cfg.RunID, err = take()
		case "--step":
			cfg.Step, err = take()
		case "--attempt":
			cfg.Attempt, err = positiveInt(name, take)
		case "--claim-epoch":
			var n int64
			n, err = intValue(name, take)
			cfg.ClaimEpoch = n
		case "--spool-dir":
			cfg.SpoolDir, err = take()
		case "--workdir":
			cfg.Workdir, err = take()
		case "--timeout":
			var d time.Duration
			d, err = durationValue(name, take)
			cfg.Timeout = d
		case "--kill-grace":
			var d time.Duration
			d, err = durationValue(name, take)
			cfg.KillGrace = d
		case "--watch-fd":
			cfg.WatchFD, err = positiveInt(name, take)
		case "--base-fd":
			cfg.BaseFD, err = positiveInt(name, take)
		default:
			err = fmt.Errorf("unknown flag %s", name)
		}
		if err != nil {
			return cfg, nil, err
		}
	}

	if cfg.RunID == "" {
		return cfg, nil, errors.New("--run-id is required")
	}
	if cfg.Step == "" {
		return cfg, nil, errors.New("--step is required")
	}
	if cfg.Attempt <= 0 {
		return cfg, nil, errors.New("--attempt must be at least 1")
	}
	if cfg.SpoolDir == "" {
		return cfg, nil, errors.New("--spool-dir is required")
	}
	if cfg.Timeout <= 0 {
		return cfg, nil, errors.New("--timeout is required and must be positive: a step without a deadline is a hang one daemon death away from being forever")
	}
	return cfg, argv, nil
}

// ExecMain is the shim entry for a binary that may be exec'd as the shim
// without the cobra command tree in front of it — a test binary's TestMain
// dispatches here when its first argument is `exec`. The leading subcommand
// word is this entry's own and is stripped before the parse; the shipped
// command's cobra tree has already taken it off by the time RunE runs.
// clk is the clock the shim's stamps are read from; a test harness passes
// its frozen clock so its stamps agree with the daemon's, and production
// passes nil for the system clock. Parse failures print and exit 2 (the
// usage exit code), exactly what the command path produces. The returned
// code is the process exit code: the child's own, or 127 for a refused
// spawn.
func ExecMain(args []string, clk clock.Clock) int {
	if len(args) > 0 && args[0] == "exec" {
		args = args[1:]
	}
	cfg, argv, err := ParseExecArgs(args)
	if err != nil {
		fmt.Fprintf(os.Stderr, "paceq: exec: %v\n", err)
		return 2
	}
	if len(argv) == 0 {
		fmt.Fprintln(os.Stderr, "paceq: exec needs the command to run after `--`")
		return 2
	}
	cfg.Argv = argv
	cfg.Clock = clk
	// This process's own stdout and stderr are the log pipes the daemon
	// handed it; the job writes straight through them. The cobra command
	// path sets the same two from the CLI env.
	cfg.Stdout = os.Stdout
	cfg.Stderr = os.Stderr
	return ShimMain(context.Background(), cfg)
}

func positiveInt(name string, take func() (string, error)) (int, error) {
	raw, err := take()
	if err != nil {
		return 0, err
	}
	n, convErr := strconv.Atoi(raw)
	if convErr != nil || n < 1 {
		return 0, fmt.Errorf("%s wants a positive integer, got %q", name, raw)
	}
	return n, nil
}

func intValue(name string, take func() (string, error)) (int64, error) {
	raw, err := take()
	if err != nil {
		return 0, err
	}
	n, convErr := strconv.ParseInt(raw, 10, 64)
	if convErr != nil {
		return 0, fmt.Errorf("%s wants an integer, got %q", name, raw)
	}
	return n, nil
}

func durationValue(name string, take func() (string, error)) (time.Duration, error) {
	raw, err := take()
	if err != nil {
		return 0, err
	}
	d, convErr := time.ParseDuration(raw)
	if convErr != nil || d <= 0 {
		return 0, fmt.Errorf("%s wants a positive duration, got %q", name, raw)
	}
	return d, nil
}
