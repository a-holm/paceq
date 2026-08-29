package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/store"
)

// Env is the process a command runs in. Every edge to the outside world goes
// through it, so a test can run the whole command line without a subprocess and
// without touching the developer's environment.
type Env struct {
	Stdout io.Writer
	Stderr io.Writer
	// Dir is the working directory. Relative paths resolve against it.
	Dir string
	// Getenv reads one environment variable.
	Getenv func(string) string
	// Clk is the clock commands run on. Nil means clock.System. A test
	// brings its own so a follow loop ticks when the test says so.
	Clk clock.Clock
	// Status reads the process sandbox for doctor. Nil reads the real
	// /proc/self/status. A test plants a status so the report answers on a
	// machine that sandboxes nothing.
	Status doctor.StatusReader
	// Procs lists the live job processes for doctor. Nil walks the real
	// /proc. A test plants a listing so the report answers whatever else on
	// the machine happens to carry PACEQ_RUN_ID.
	Procs doctor.ProcLister
}

// stateDirName is the state directory paceq creates inside a project.
const stateDirName = ".paceq"

// Main runs the command line and returns the process exit code.
func Main(ctx context.Context, args []string) int {
	return MainEnv(ctx, Env{Stdout: os.Stdout, Stderr: os.Stderr, Getenv: os.Getenv}, args)
}

// MainEnv runs the command line on an environment the caller built, and
// returns the process exit code. Main is the thin wrapper production uses;
// a test harness brings its own writers and clock through here, and both
// paths are the same code from this line on.
//
// The umask is the first thing that happens, before any command can create a
// file (08 section 3.9). Setting it later would leave whatever the first write
// created at the mode the environment happened to have.
func MainEnv(ctx context.Context, env Env, args []string) int {
	setUmask()

	if env.Dir == "" {
		dir, err := os.Getwd()
		if err != nil {
			// A working directory that cannot be read is not worth
			// refusing over: every path the commands use is resolved
			// against it, and a relative one still resolves the same
			// way the process would.
			dir = "."
		}
		env.Dir = dir
	}
	return run(ctx, env, args)
}

func run(ctx context.Context, env Env, args []string) int {
	root := newRoot(env)
	root.SetArgs(args)
	if err := root.ExecuteContext(ctx); err != nil {
		return renderError(env, classify(ctx, err))
	}
	return ExitOK
}

// globals are the flags every command shares.
type globals struct {
	output  string
	db      string
	quiet   bool
	verbose int
	noColor bool
}

const rootLong = `paceq runs scheduled jobs, sensors and small DAGs from one static binary.

Output follows the stream: a terminal gets human text, a pipe gets JSON.
PACEQ_OUTPUT picks a side for a caller that cannot pass flags, and -o
overrides everything. Data goes to stdout, progress and warnings to stderr.`

func newRoot(env Env) *cobra.Command {
	var g globals
	showVersion := false

	root := &cobra.Command{
		Use:   "paceq",
		Short: "Scheduled jobs, sensors and small DAGs, in one static binary",
		Long:  rootLong + "\n\n" + exitCodeHelp(),
		// paceq renders its own errors, with the three parts every message has,
		// and decides its own exit code from them.
		SilenceUsage:  true,
		SilenceErrors: true,
		Args:          cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case showVersion:
				// --version is what people type before they find the command.
				// It answers from the same code, so the two cannot drift.
				out, err := g.ui(env)
				if err != nil {
					return err
				}
				return writeVersion(out)
			case len(args) == 0:
				return cmd.Help()
			}
			return usageError(fmt.Sprintf("%q is not a paceq command", args[0]),
				"paceq --help  lists every command")
		},
	}
	root.SetOut(env.Stdout)
	root.SetErr(env.Stderr)
	root.CompletionOptions.DisableDefaultCmd = true
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		return usageError(err.Error(), cmd.CommandPath()+" --help  lists every flag")
	})

	flags := root.PersistentFlags()
	flags.StringVarP(&g.output, "output", "o", "", "text or json (default: PACEQ_OUTPUT, else text at a terminal, json in a pipe)")
	flags.StringVar(&g.db, "db", "", "state database to use (default: ./"+stateDirName+"/"+store.DatabaseFileName+")")
	flags.BoolVarP(&g.quiet, "quiet", "q", false, "only report what needs attention")
	flags.CountVarP(&g.verbose, "verbose", "v", "progress on stderr, repeatable: -v, -vv")
	flags.BoolVar(&g.noColor, "no-color", false, "no colour, whatever the terminal says (also NO_COLOR, CLICOLOR_FORCE)")
	root.Flags().BoolVar(&showVersion, "version", false, "same as paceq version")

	root.AddCommand(
		newVersionCmd(env, &g),
		newInitCmd(env, &g),
		newDoctorCmd(env, &g),
		newValidateCmd(env, &g),
		newApplyCmd(env, &g),
		newRunCmd(env, &g),
		newServeCmd(env, &g),
		newRunsCmd(env, &g),
		newExplainCmd(env, &g),
		newSchedulesCmd(env, &g),
		newSensorsCmd(env, &g),
		newShadowCmd(env, &g),
		newImportCmd(env, &g),
		newStatusCmd(env, &g),
		newFsckCmd(env, &g),
		newPruneCmd(env, &g),
		newDbCmd(env, &g),
		newExportCmd(env, &g),
		newErrorCmd(env, &g),
		newExecCmd(env, &g),
		newLogsCmd(env, &g),
		newLsCmd(env, &g),
		newNotificationsCmd(env, &g),
		newCutoverCmd(env, &g),
		newInstallServiceCmd(env, &g),
	)
	return root
}

// noArgs refuses positional arguments the way paceq refuses everything else:
// with the exit code for a wrong command line and a way forward.
func noArgs(cmd *cobra.Command, args []string) error {
	if len(args) == 0 {
		return nil
	}
	return usageError(fmt.Sprintf("%s takes no arguments, got %q", cmd.CommandPath(), strings.Join(args, " ")),
		cmd.CommandPath()+" --help  shows what it accepts")
}

// exactArgs is noArgs for a command that needs a fixed number of them.
func exactArgs(n int, what string) cobra.PositionalArgs {
	return func(cmd *cobra.Command, args []string) error {
		if len(args) == n {
			return nil
		}
		return usageError(fmt.Sprintf("%s takes %s, got %d arguments", cmd.CommandPath(), what, len(args)),
			cmd.CommandPath()+" --help  shows what it accepts")
	}
}

// runE wraps a command body so everything that leaves it is a paceq error with
// an exit code, and cobra's own errors stay distinguishable from ours.
func runE(env Env, g *globals, body func(ctx context.Context, out *ui) error) func(*cobra.Command, []string) error {
	return runArgsE(env, g, func(ctx context.Context, out *ui, _ []string) error {
		return body(ctx, out)
	})
}

func runArgsE(env Env, g *globals, body func(ctx context.Context, out *ui, args []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		out, err := g.ui(env)
		if err != nil {
			return err
		}
		if err := body(cmd.Context(), out, args); err != nil {
			return classify(cmd.Context(), err)
		}
		return nil
	}
}

// ui resolves the flags into the decisions a command renders with.
func (g *globals) ui(env Env) (*ui, error) {
	mode, err := g.mode(env)
	if err != nil {
		return nil, err
	}
	return &ui{
		out:     env.Stdout,
		err:     env.Stderr,
		mode:    mode,
		color:   g.useColor(env),
		symbols: symbols(unicodeOutput(env)),
		quiet:   g.quiet,
		verbose: g.verbose,
	}, nil
}

// mode is 03 section 7.1: a terminal gets text, everything else gets JSON.
// PACEQ_OUTPUT picks a side without a flag, which is how a caller on a pipe
// that cannot pass arguments pins the mode, and -o beats both of them.
func (g *globals) mode(env Env) (outputMode, error) {
	mode, said, err := chosenMode(g.output, "output format",
		"use -o text for people, or -o json for scripts")
	if said || err != nil {
		return mode, err
	}
	mode, said, err = chosenMode(env.Getenv("PACEQ_OUTPUT"), "PACEQ_OUTPUT",
		"set PACEQ_OUTPUT=text for people, or PACEQ_OUTPUT=json for scripts")
	if said || err != nil {
		return mode, err
	}
	if isTerminal(env.Stdout) {
		return modeText, nil
	}
	return modeJSON, nil
}

// chosenMode reads one explicit output selection. An empty value means
// nothing was said, and the caller falls through to the next way to decide;
// anything else must be a word paceq writes.
func chosenMode(value, source, next string) (outputMode, bool, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "text":
		return modeText, true, nil
	case "json":
		return modeJSON, true, nil
	case "":
		return modeText, false, nil
	default:
		return modeText, true, usageError(
			fmt.Sprintf("%s %q is not one paceq writes", source, value), next)
	}
}

// useColor follows the conventions a terminal user already has: the flag wins,
// then NO_COLOR, then CLICOLOR_FORCE, then whether stdout is a terminal.
func (g *globals) useColor(env Env) bool {
	switch {
	case g.noColor:
		return false
	case env.Getenv("NO_COLOR") != "":
		return false
	case env.Getenv("CLICOLOR_FORCE") != "":
		return true
	default:
		return isTerminal(env.Stdout)
	}
}

// stateDir is the directory holding the lock and the database. --db names the
// database file inside it, because a state directory holds exactly one.
func (g *globals) stateDir(env Env) (string, error) {
	if g.db == "" {
		return filepath.Join(env.Dir, stateDirName), nil
	}
	path := g.db
	if !filepath.IsAbs(path) {
		path = filepath.Join(env.Dir, path)
	}
	if filepath.Base(path) != store.DatabaseFileName {
		return "", usageError(
			fmt.Sprintf("--db %s does not name a paceq database: a state directory holds one database, called %s",
				g.db, store.DatabaseFileName),
			fmt.Sprintf("point it at the file: --db %s",
				filepath.Join(filepath.Dir(path), store.DatabaseFileName)))
	}
	return filepath.Dir(path), nil
}
