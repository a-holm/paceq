package cli

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/runner"
)

// The hidden `paceq exec` command (issue #39). This is the shim half of the
// daemon's process chain: the daemon launches its own image with this
// subcommand, and the subcommand launches the user's command beside it,
// watches the watchdog pipe, and writes the attempt's result durably to the
// spool before it exits.
//
// It is deliberately hidden and explicitly not a public contract: nothing
// about it is stable across releases except the spool file's format, which
// carries its own version field. A human who runs it by hand gets the same
// process guarantees minus the two pipe descriptors no hand can pass.
func newExecCmd(env Env, _ *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "exec -- <command> [args...]",
		Short:  "Run one step command as the daemon's shim (implementation detail)",
		Hidden: true,
		// The job's argv may contain anything, flags included; the parse
		// below owns every token after the command name.
		DisableFlagParsing: true,
		Args:               cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, argv, err := parseExecArgs(args)
			if err != nil {
				return err
			}
			if len(argv) == 0 {
				return usageError("exec needs the command to run after `--`",
					"paceq exec --run-id <id> --step <name> --attempt 1 --claim-epoch 1 --spool-dir <dir> --timeout 1h -- <command>")
			}
			cfg.Argv = argv
			cfg.Stdout = env.Stdout
			cfg.Stderr = env.Stderr
			code := runner.ShimMain(cmd.Context(), cfg)
			if code == 0 {
				return nil
			}
			// The child's exit code is the shim's exit code, and a job
			// that failed is not a paceq error message: the daemon
			// reads the code, the spool and the log, not this
			// process's stderr.
			return silentExit(code)
		},
	}
	return cmd
}

// parseExecArgs reads the shim's flags by hand. Cobra's flag machinery is
// bypassed on purpose: the user's command may carry `--` and flags of its
// own, and everything from the first bare `--` belongs to it verbatim.
func parseExecArgs(args []string) (runner.ShimConfig, []string, error) {
	var cfg runner.ShimConfig
	cfg.WatchFD = -1
	cfg.BaseFD = -1

	need := func(name string, values []string, i *int) (string, error) {
		if *i+1 >= len(values) {
			return "", usageError("exec: %s needs a value", name)
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
			return cfg, nil, usageError("exec: unexpected argument %q before `--`", arg)
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
			err = usageError("exec: unknown flag %s", name)
		}
		if err != nil {
			return cfg, nil, err
		}
	}

	if cfg.RunID == "" {
		return cfg, nil, usageError("exec: --run-id is required")
	}
	if cfg.Step == "" {
		return cfg, nil, usageError("exec: --step is required")
	}
	if cfg.Attempt <= 0 {
		return cfg, nil, usageError("exec: --attempt must be at least 1")
	}
	if cfg.SpoolDir == "" {
		return cfg, nil, usageError("exec: --spool-dir is required")
	}
	if cfg.Timeout <= 0 {
		return cfg, nil, usageError("exec: --timeout is required and must be positive",
			"a step without a deadline is a hang one daemon death away from being forever")
	}
	return cfg, argv, nil
}

func positiveInt(name string, take func() (string, error)) (int, error) {
	raw, err := take()
	if err != nil {
		return 0, err
	}
	n, convErr := strconv.Atoi(raw)
	if convErr != nil || n < 1 {
		return 0, usageError("exec: %s wants a positive integer, got %q", name, raw)
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
		return 0, usageError("exec: %s wants an integer, got %q", name, raw)
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
		return 0, usageError("exec: %s wants a positive duration, got %q", name, raw)
	}
	return d, nil
}

// silentExit is the exit-code passthrough: the returned error carries only a
// number, and renderError turns it into a process exit code without a word
// on stderr. A failed job is a fact, not a complaint.
func silentExit(code int) error {
	return &Error{code: code, silent: true}
}

// shimExecutable names this process's own image for the daemon side of the
// exec chain. Empty means the spawn path falls back to the direct runner,
// which is the honest degradation when the binary cannot be located (tests
// that never became a file, exotic launchers).
func shimExecutable() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exe
}
