package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/store"
)

// fsck is the invariant sweep on the command line. The checks are SQL and
// they live in the store; the engine is the entry point, and the command is
// only the reporting around it.

type violationJSON struct {
	Check   string `json:"check"`
	Subject string `json:"subject"`
	Detail  string `json:"detail"`
}

type fsckReport struct {
	Count      int             `json:"count"`
	Violations []violationJSON `json:"violations"`
}

func newFsckCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "fsck",
		Short: "Check the state database against its invariants",
		Long: `Sweep the state for broken rules: a terminal run with open steps, a run
state its steps disagree with, timestamps that run backwards, a held run with
no reason, an event chain with a hole, a row that ended without a reason code.

The command reads only, so it is safe while another paceq works. It exits 0
when the state is sound and 1 when it is not, with every violation listed.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runFsck(ctx, env, g, out)
		}),
	}
}

func runFsck(ctx context.Context, env Env, g *globals, out *ui) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db",
		)
	}

	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	// The sweep is the engine's, not a copy of it: whatever the crash
	// harness plants, this command reports exactly what it reports.
	sweeper := &engine.Engine{Store: ro}
	violations, err := sweeper.Fsck(ctx)
	if err != nil {
		return internalError("could not sweep the state", err)
	}

	report := fsckReport{Count: len(violations), Violations: []violationJSON{}}
	for _, v := range violations {
		report.Violations = append(report.Violations, violationJSON{
			Check:   v.Check,
			Subject: v.Subject,
			Detail:  v.Detail,
		})
	}
	if out.mode == modeJSON {
		if err := out.json(report); err != nil {
			return err
		}
	} else if len(violations) == 0 {
		out.print("%s the state is sound: no violations", out.symbols.ok)
	} else {
		width := 0
		for _, v := range violations {
			width = max(width, len(v.Check))
		}
		for _, v := range violations {
			out.print("%s %s  %s", out.symbols.fail, pad(v.Check, width), v.Subject)
			out.print("%s %s", pad("", width+3), v.Detail)
		}
	}

	if len(violations) == 0 {
		return nil
	}
	return &Error{
		code: ExitInternal,
		what: fmt.Sprintf("the state has %s, listed above", count(len(violations), "violation")),
		next: []string{
			"stop writing to this state directory until the cause is found",
			"keep a copy of it: cp -a " + displayPath(env, stateDir) + " " + displayPath(env, stateDir) + ".damaged",
			"paceq doctor  checks the rest of the installation",
		},
	}
}
