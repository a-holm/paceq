package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/janitor"
	"github.com/a-holm/paceq/internal/store"
)

// prune is manual retention (06 section 9.4). The nightly daemon cycle runs
// the same machinery under the maintenance lease; the command exists so an
// operator can see the estimate first and run it without a daemon.
type pruneFlags struct {
	dryRun bool
}

func newPruneCmd(env Env, g *globals) *cobra.Command {
	var f pruneFlags
	cmd := &cobra.Command{
		Use:   "prune [--dry-run]",
		Short: "Apply the retention policies, or estimate them with --dry-run",
		Long: `Delete history past its horizon, never below the keep-minimums:
runs keep their newest 50 per job, ticks their newest 200 per source,
skipped ticks go in 7 days, dedup keys last 365. Log date shards go whole.

Deletion is batched: 200 rows per transaction with a 50 ms pause, so the
write lock is held for milliseconds at a time. A dry-run changes nothing
and prints what a real pass would delete.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runPrune(ctx, env, g, out, f)
		}),
	}
	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "show what would be deleted without deleting anything")
	return cmd
}

func runPrune(ctx context.Context, env Env, g *globals, out *ui, f pruneFlags) error {
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
	logRoot := filepath.Join(stateDir, "logs")

	if f.dryRun {
		ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
		if err != nil {
			return err
		}
		defer func() { _ = ro.Close() }()
		plans := janitor.New(janitor.Config{
			Store:   ro,
			Clock:   clkOf(env),
			LogRoot: logRoot,
		})
		plan, err := plans.PrunePlans(ctx)
		if err != nil {
			return internalError("could not estimate the retention pass", err)
		}
		renderPrunePlan(out, plan)
		return nil
	}

	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clkOf(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	executor := janitor.New(janitor.Config{
		Store:   s,
		Clock:   clkOf(env),
		LogRoot: logRoot,
	})
	res, err := executor.Prune(ctx)
	if err != nil {
		return internalError("the retention pass failed", err)
	}

	if out.mode == modeJSON {
		return out.json(res)
	}
	pol := executor.Policies()
	out.print("Retention applied")
	out.print("")
	printRuleLine(out, "runs", pol.RunsDays, pol.RunsKeepMin, "job", res.Deleted.Runs)
	printRuleLine(out, "ticks (skipped)", pol.TicksSkippedDays, 0, "", res.Deleted.SkippedTicks)
	printRuleLine(out, "ticks (other)", pol.TicksDays, pol.TicksKeepMin, "source", res.Deleted.Ticks)
	printRuleLine(out, "daemon_sessions", pol.SessionsDays, pol.SessionsKeepMin, "", res.Deleted.Sessions)
	printRuleLine(out, "run_keys", pol.RunKeysDays, 0, "", res.Deleted.RunKeys)
	out.print("  log shards           removed %d", len(res.LogShards))
	out.print("")
	out.print("Deleted %d rows in %d batches; vacuum freed %d pages.",
		res.Deleted.Total(), res.Batches, res.VacuumPagesReleased)
	for _, failure := range res.Failures {
		out.print("%s %s", out.symbols.fail, failure)
	}
	return nil
}

// printRuleLine renders one table's outcome in the same shape the dry-run
// uses, so an operator can compare the two side by side.
func printRuleLine(out *ui, name string, days, keepMin int, keepUnit string, deleted int64) {
	scope := fmt.Sprintf("%3d d", days)
	if days >= 365 {
		scope = fmt.Sprintf("%d d", days)
	}
	floor := ""
	if keepMin > 0 && keepUnit != "" {
		floor = fmt.Sprintf(", keep %d/%s", keepMin, keepUnit)
	} else if keepMin > 0 {
		floor = fmt.Sprintf(", keep %d", keepMin)
	}
	out.print("  %-20s %s%s     %d rows", name, scope, floor, deleted)
}

// renderPrunePlan shows the estimate. The layout mirrors the applied form on
// purpose: same rows, different headline.
func renderPrunePlan(out *ui, p janitor.Plan) {
	pol := store.DefaultPolicies()
	if out.mode == modeJSON {
		if err := out.json(p); err != nil {
			return
		}
		return
	}
	headline := "Retention plan"
	if len(p.LogShards) == 0 {
		out.print("%s (nothing to do: nothing is past its horizon)", headline)
	} else {
		out.print("%s", headline)
	}
	out.print("")
	printRuleLine(out, "runs", pol.RunsDays, pol.RunsKeepMin, "job", p.Runs)
	printRuleLine(out, "ticks (skipped)", pol.TicksSkippedDays, 0, "", p.SkippedTicks)
	printRuleLine(out, "ticks (other)", pol.TicksDays, pol.TicksKeepMin, "source", p.Ticks)
	printRuleLine(out, "daemon_sessions", pol.SessionsDays, pol.SessionsKeepMin, "", p.Sessions)
	printRuleLine(out, "run_keys", pol.RunKeysDays, 0, "", p.RunKeys)
	for _, shard := range p.LogShards {
		out.print("  %-20s gone        %s (%d bytes)", shard, "log shard", p.LogShardBytes[shard])
	}
	out.print("")
	out.print("Estimated: %d database rows, %d bytes of logs.", p.Total(), p.LogShardTotal())
	out.print("Run without --dry-run to apply.")
}
