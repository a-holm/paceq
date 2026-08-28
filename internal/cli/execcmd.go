package cli

import (
	"os"

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
// carries its own version field. The flag parsing lives in the runner
// package, which is also what a test binary's TestMain dispatches to, so a
// suite drives the same parser the shipped command runs.
func newExecCmd(env Env, _ *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:    "exec -- <command> [args...]",
		Short:  "Run one step command as the daemon's shim (implementation detail)",
		Hidden: true,
		// The job's argv may contain anything, flags included; the
		// runner's parser owns every token after the command name.
		DisableFlagParsing: true,
		Args:               cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, argv, err := runner.ParseExecArgs(args)
			if err != nil {
				return usageError("exec: %v", err.Error(),
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

// silentExit is the exit-code passthrough: the returned error carries only a
// number, and renderError turns it into a process exit code without a word
// on stderr. A failed job is a fact, not a complaint.
func silentExit(code int) error {
	return &Error{code: code, silent: true}
}

// shimExecutable names this process's own image for the daemon side of the
// exec chain. Empty means the spawn path falls back to the direct runner,
// which is the honest degradation when the binary cannot be located (tests
// that never became a file, exotic launchers). A test binary that answers
// `exec` through its TestMain is a full shim, so the same code path serves.
func shimExecutable() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	return exe
}
