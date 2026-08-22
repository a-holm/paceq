package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// runs is the noun. list is the only page it has in M1: show and cancel grow
// here in their own issues, beside the prefix lookup they share.

func newRunsCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "Inspect run history",
		Long: `Read the history of runs.

Without a subcommand this prints help. Every listing is newest first, walks
backwards with --before, and reads through the read only pool, so history is
there while another process writes new runs.`,
	}
	cmd.AddCommand(newRunsListCmd(env, g))
	return cmd
}

type runsListFlags struct {
	job    string
	states []string
	since  string
	limit  int
	before string
}

func newRunsListCmd(env Env, g *globals) *cobra.Command {
	var f runsListFlags
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List recent runs, newest first",
		Long: `One line per run: what ran, how it ended, when, and why.

The listing is a page, not the whole table: pass the last id of a page back
as --before to walk further into the past.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runRunsList(ctx, env, g, out, f)
		}),
	}
	cmd.Flags().StringVar(&f.job, "job", "", "only this job's runs")
	cmd.Flags().StringSliceVar(&f.states, "state", nil,
		"only runs in these states, comma separated (queued, running, succeeded, failed, cancelled)")
	cmd.Flags().StringVar(&f.since, "since", "", "only runs created within this long ago, such as 24h or 7d")
	cmd.Flags().IntVar(&f.limit, "limit", 0, "at most this many runs (default 50)")
	cmd.Flags().StringVar(&f.before, "before", "", "runs older than this id, for walking pages")
	return cmd
}

func runRunsList(ctx context.Context, env Env, g *globals, out *ui, f runsListFlags) error {
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
	for _, name := range f.states {
		if _, err := model.ParseRunState(name); err != nil {
			return usageError(
				fmt.Sprintf("%q is not a state a run can be in", name),
				"pick from: queued, running, succeeded, failed, cancelled",
			)
		}
	}
	var since time.Duration
	if f.since != "" {
		parsed, err := spec.ParseDuration(f.since)
		if err != nil {
			return usageError(
				fmt.Sprintf("--since %s cannot be read as a duration", f.since),
				"write a number and a unit, such as 30m, 24h or 7d",
			)
		}
		since = parsed
	}

	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	rows, err := ro.ListRuns(ctx, store.RunFilter{
		JobName: f.job,
		States:  f.states,
		Before:  f.before,
		Limit:   f.limit,
	})
	if err != nil {
		return internalError("could not read the run history", err)
	}

	// Recent is measured on the clock the command was given, never on the
	// wall behind its back.
	now := clkOf(env).Now()
	if since > 0 {
		cutoff := now.Add(-since)
		kept := rows[:0]
		for _, row := range rows {
			if !row.CreatedAt.Before(cutoff) {
				kept = append(kept, row)
			}
		}
		rows = kept
	}

	if out.mode == modeJSON {
		doc := make([]runListRow, 0, len(rows))
		for _, row := range rows {
			doc = append(doc, newListRow(row))
		}
		return out.json(doc)
	}
	if len(rows) == 0 {
		out.print("no runs yet: paceq run <job> makes the first one")
		return nil
	}

	widths := struct{ id, job, state, started int }{id: 2, job: 3, state: 5, started: 7}
	for _, row := range rows {
		widths.id = max(widths.id, len(row.ID))
		widths.job = max(widths.job, len(row.JobName))
		widths.state = max(widths.state, len(row.State))
		widths.started = max(widths.started, len(relAge(now, row.CreatedAt)))
	}
	for _, row := range rows {
		mark := out.symbols.ok
		switch model.RunState(row.State) {
		case model.RunFailed:
			mark = out.symbols.fail
		case model.RunCancelled:
			mark = out.symbols.warn
		case model.RunQueued, model.RunRunning:
			mark = out.symbols.arrow
		}
		out.print("%s %s  %s  %s  %s  %s",
			mark,
			pad(row.ID, widths.id),
			pad(row.JobName, widths.job),
			pad(row.State, widths.state),
			pad(relAge(now, row.CreatedAt), widths.started),
			runOutcome(row))
	}
	return nil
}

// runListRow is one listing entry as a script reads it.
type runListRow struct {
	ID         string `json:"id"`
	Job        string `json:"job"`
	Origin     string `json:"origin,omitempty"`
	State      string `json:"state"`
	ReasonCode string `json:"reason_code,omitempty"`
	CreatedAt  string `json:"created_at"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}

func newListRow(row store.RunSummary) runListRow {
	return runListRow{
		ID:         row.ID,
		Job:        row.JobName,
		Origin:     row.Origin,
		State:      row.State,
		ReasonCode: row.ReasonCode,
		CreatedAt:  rfc3339(row.CreatedAt),
		StartedAt:  rfc3339(row.StartedAt),
		FinishedAt: rfc3339(row.FinishedAt),
		DurationMS: millisBetween(row.StartedAt, row.FinishedAt),
	}
}

// runOutcome is the text column of the table: why the run ended, as words.
func runOutcome(row store.RunSummary) string {
	if row.ReasonCode == "" {
		return ""
	}
	if entry, ok := reason.Lookup(reason.Code(row.ReasonCode)); ok {
		return entry.Short
	}
	return row.ReasonCode
}

// relAge says how long ago something happened, roughly, on the clock the
// command was handed.
func relAge(now, then time.Time) string {
	if then.IsZero() {
		return ""
	}
	age := now.Sub(then)
	if age < 0 {
		age = 0
	}
	switch {
	case age < time.Minute:
		return fmt.Sprintf("%ds ago", int(age.Seconds()))
	case age < time.Hour:
		return fmt.Sprintf("%dm ago", int(age.Minutes()))
	case age < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(age.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(age.Hours()/24))
	}
}
