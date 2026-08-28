package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// fsck is the invariant sweep on the command line. The checks are SQL and
// they live in the store; the command is only the reporting around them, plus
// the conservative repair (M6-06).
//
// Exit codes follow the contract in docs/reference/exit-codes.md, graded by
// severity: findings that are only warnings exit 0 (the state serves, the
// findings are cosmetic or historic), serious and critical findings exit 1,
// and a critical finding says so explicitly, because startup will be refused
// until it is gone.

type violationJSON struct {
	Check    string `json:"check"`
	Severity string `json:"severity"`
	Subject  string `json:"subject"`
	Detail   string `json:"detail"`
	Remedy   string `json:"remedy"`
}

type fsckReport struct {
	Count      int             `json:"count"`
	Warnings   int             `json:"warnings"`
	Critical   int             `json:"critical"`
	Violations []violationJSON `json:"violations"`
}

func newFsckCmd(env Env, g *globals) *cobra.Command {
	var (
		repair    bool
		confirm   bool
		forceJSON bool
		only      []string
	)
	cmd := &cobra.Command{
		Use:   "fsck",
		Short: "Check the state database against its invariants",
		Long: `Sweep the state for broken rules: a run running without a live lease, a
terminal run with open steps, a duplicate run key or tick slot, a cyclic
dependency graph, timestamps that run backwards, a held run with no reason,
an event chain with a hole, a row that ended without a reason code.

Every finding carries a severity and a remedy. Warnings are historic or
cosmetic: the command exits 0 with the findings listed. Serious and critical
findings mean the state itself is wrong, and the command exits 1. A critical
finding is a uniqueness rule or the dependency graph broken behind the code;
startup is refused while it stands.

--repair puts rows back where ordinary reconciliation would put them, and
writes what it did into the event history. It never deletes, never touches
run keys, and never repairs over a critical finding without --confirm.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			if forceJSON {
				out.mode = modeJSON
			}
			return runFsck(ctx, env, g, out, repair, confirm, only)
		}),
	}
	cmd.Flags().BoolVar(&repair, "repair", false,
		"repair the findings that are safely repairable")
	cmd.Flags().BoolVar(&confirm, "confirm", false,
		"confirm a repair while critical findings stand; required with --repair")
	cmd.Flags().StringArrayVar(&only, "only", nil,
		"sweep only these invariants, by name: I1, I3, reason, ...")
	cmd.Flags().BoolVar(&forceJSON, "json", false,
		"emit the JSON contract, the same document -o json pins")
	return cmd
}

func runFsck(ctx context.Context, env Env, g *globals, out *ui,
	repair, confirm bool, only []string,
) error {
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

	var violations []store.Violation
	if repair {
		// The repair needs the writer, so the state is opened the ordinary
		// way: a repair is a write, and the lock is what makes it safe.
		s, err := store.OpenState(ctx, stateDir, store.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = s.Close() }()
		outcomes, err := s.FsckRepair(ctx, only, confirm)
		if err != nil {
			var need *store.RepairConfirmError
			if errors.As(err, &need) {
				return repairConfirmError(need, env, stateDir)
			}
			return internalError("could not repair the state", err)
		}
		// Re-sweep after the repairs, so the report says what still stands
		// rather than what stood before them.
		violations, err = s.Fsck(ctx)
		if err != nil {
			return internalError("could not sweep the state", err)
		}
		if !out.quiet {
			for _, o := range outcomes {
				noun := count(o.Repaired, "row")
				out.print("%s %s  repaired %s", out.symbols.ok, pad(o.Invariant, 6), noun)
				if o.Skipped > 0 {
					out.print("%s %s  left %d for the operator", out.symbols.arrow, pad("", 6), o.Skipped)
				}
			}
		}
	} else {
		ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = ro.Close() }()
		violations, err = ro.Fsck(ctx)
		if err != nil {
			return internalError("could not sweep the state", err)
		}
	}

	violations = filterViolations(violations, only)
	report := fsckReport{Count: len(violations), Violations: []violationJSON{}}
	for _, v := range violations {
		switch v.Severity {
		case store.Critical:
			report.Critical++
		case store.Warning:
			report.Warnings++
		}
		report.Violations = append(report.Violations, violationJSON{
			Check:    v.Check,
			Severity: v.Severity.String(),
			Subject:  v.Subject,
			Detail:   v.Detail,
			Remedy:   v.Remedy,
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
			mark := out.symbols.fail
			if v.Severity == store.Warning {
				mark = out.symbols.warn
			}
			out.print("%s %s  %s  %s", mark, pad(v.Check, width), v.Severity, v.Subject)
			out.print("%s %s", pad("", width+3), v.Detail)
			out.print("%s %s %s", pad("", width+3), out.symbols.arrow, v.Remedy)
		}
	}

	// Exit codes, graded (03's table): warnings alone are exit 0. Anything
	// serious or critical is paceq reporting that the state is wrong.
	if report.Critical == 0 && report.Warnings == len(violations) {
		return nil
	}
	critical := report.Critical > 0
	next := []string{
		"stop writing to this state directory until the cause is found",
		"keep a copy of it: cp -a " + displayPath(env, stateDir) + " " + displayPath(env, stateDir) + ".damaged",
	}
	if critical {
		return &Error{
			code: ExitInternal,
			what: fmt.Sprintf("the state has %s and startup will be refused until they are gone",
				count(report.Critical, "critical violation")),
			next: append(next,
				"paceq fsck --json  preserves the full finding list before anything changes",
				"paceq fsck --repair --confirm  repairs what is safely repairable, after you confirm",
				"or restore the state from its last verified backup"),
		}
	}
	serious := len(violations) - report.Warnings
	return &Error{
		code: ExitInternal,
		what: fmt.Sprintf("the state has %s, listed above",
			count(serious, "serious violation")),
		next: append(next, "paceq fsck --repair  repairs the findings that are safely repairable"),
	}
}

// repairConfirmError is the refusal to repair over critical findings without
// --confirm. It is a usage-grade answer, not an internal error: the operator
// asked for a write the tool refuses to make quietly.
func repairConfirmError(need *store.RepairConfirmError, env Env, stateDir string) *Error {
	return &Error{
		code: ExitInternal,
		what: need.Error(),
		next: []string{
			"paceq fsck --json > /tmp/fsck.json  keep the finding list before anything changes",
			"read the findings: a critical violation means a hand edit or corruption",
			"paceq fsck --repair --confirm  repairs the rest, with the criticals standing",
			"or restore the state from its last verified backup: " + displayPath(env, stateDir),
		},
	}
}

// filterViolations applies the --only list to a sweep. Repair already scoped
// itself; this is the read-only path's filter, so a report and its repair
// speak the same names.
func filterViolations(violations []store.Violation, only []string) []store.Violation {
	if len(only) == 0 {
		return violations
	}
	out := make([]store.Violation, 0, len(violations))
	for _, v := range violations {
		for _, name := range only {
			if v.Check == name {
				out = append(out, v)
				break
			}
		}
	}
	return out
}
