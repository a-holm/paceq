package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// db compact is the guarded door to full VACUUM (07 section 6.3). It needs
// an exclusive lock and twice the disk space, so without the flag it refuses
// and says what to do instead.
func newDbCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "db",
		Short: "Direct database maintenance",
		Long: `Commands that operate on the state database file itself.
Daily upkeep belongs to pulseq prune, which batches everything.`,
	}
	var force bool
	compact := &cobra.Command{
		Use:   "compact",
		Short: "Shrink the state database with a full VACUUM",
		Long: `Rewrite the whole state database, returning every free page to the
filesystem. A full VACUUM needs an exclusive lock and roughly twice the
database in free disk space, and it blocks every writer until it finishes -
including the daemon, which will fail its writes for the duration.

The nightly maintenance already returns up to 2000 pages a night through
incremental vacuum, and pulseq prune runs the same pass on demand. Full
compaction is for after very large deletions only.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runDbCompact(ctx, env, g, out, force)
		}),
	}
	compact.Flags().BoolVar(&force, "i-know-this-blocks", false,
		"confirm you accept an exclusive lock and 2x disk usage for the duration")
	cmd.AddCommand(compact)
	return cmd
}

func runDbCompact(ctx context.Context, env Env, g *globals, out *ui, force bool) error {
	if !force {
		return usageError(
			"full VACUUM needs an exclusive lock and 2x the database in free disk space, and blocks every writer until it is done",
			"add --i-know-this-blocks if you are sure",
			"for daily upkeep, nothing is needed: pulseq prune already shrinks incrementally",
		)
	}
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clkOf(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	before, err := s.PageCount(ctx)
	if err != nil {
		return err
	}
	freelist, err := s.FreelistCount(ctx)
	if err != nil {
		return err
	}
	pageSize, _ := s.PageSize(ctx)
	if freelist == 0 {
		out.print("Nothing to compact: the database holds %d pages, none free.", before)
		return nil
	}
	if err := s.FullVacuum(ctx); err != nil {
		return internalError("the vacuum failed; the database is unchanged", err)
	}
	after, err := s.PageCount(ctx)
	if err != nil {
		return err
	}

	report := struct {
		PagesBefore int64 `json:"pages_before"`
		PagesAfter  int64 `json:"pages_after"`
		PagesFreed  int64 `json:"pages_freed"`
		PageSize    int64 `json:"page_size"`
	}{before, after, before - after, pageSize}
	if out.mode == modeJSON {
		return out.json(report)
	}
	out.print("Compacted: %d pages down to %d (%d freed, %d bytes each).",
		report.PagesBefore, report.PagesAfter, report.PagesFreed, report.PageSize)
	return nil
}
