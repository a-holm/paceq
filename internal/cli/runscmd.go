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

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// runs is the noun. list walks the history backwards; show answers with one
// run by id or prefix, sharing the lookup with logs.

func newRunsCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "runs",
		Short: "Inspect run history",
		Long: `Read the history of runs.

Without a subcommand this prints help. Every listing is newest first, walks
backwards with --before, and reads through the read only pool, so history is
there while another process writes new runs.`,
	}
	cmd.AddCommand(newRunsListCmd(env, g), newRunsShowCmd(env, g), newRunsCancelCmd(env, g))
	return cmd
}

// runsCancelFlags carries the optional reason a cancellation records.
type runsCancelFlags struct {
	reason string
}

func newRunsCancelCmd(env Env, g *globals) *cobra.Command {
	var f runsCancelFlags
	cmd := &cobra.Command{
		Use:   "cancel <run>",
		Short: "Ask for one run to be stopped, durably",
		Long: `Record that this run should stop. The request lands in the database
before anything is killed: whoever holds the lease observes it and does
the stopping, so the history always agrees with what happened.

With the daemon up the request travels over its socket and the daemon's
own writer records it (actor api). With the daemon down the command takes
the state lock itself and writes directly (actor cli).`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsCancel(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.reason, "reason", "", "why the run should stop, kept beside the request")
	return cmd
}

// runRunsCancel resolves the dual-mode transport first, then records the
// request through whichever writer won.
func runRunsCancel(ctx context.Context, env Env, g *globals, out *ui, runArg string, f runsCancelFlags) error {
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
			"run the command inside the project directory, or pass --db")
	}

	plan := planWrite(ctx, env, g)
	if plan.err != nil {
		return plan.err
	}
	setting := resolveSocket(g, env, stateDir)

	var viaSocket bool
	switch {
	case plan.client != nil:
		viaSocket = true
		if err := plan.client.CancelRun(ctx, runArg, f.reason); err != nil {
			var wire *api.WireError
			if errors.As(err, &wire) {
				return wireFailure(env, wire)
			}
			return internalError("could not reach the daemon", err)
		}
		defer func() { _ = plan.client.Close() }()
	case plan.st != nil:
		if _, err := plan.st.RequestCancel(ctx, runArg, cliActor(), f.reason); err != nil {
			return cancelRefusal(ctx, plan.st, runArg, err)
		}
		defer func() { _ = plan.st.Close() }()
	default:
		return internalError("writer resolution produced neither transport", errors.New("empty plan"))
	}

	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()
	detail, err := ro.GetRun(ctx, runArg)
	if err != nil {
		return notFoundError(
			fmt.Sprintf("no run matches %q", runArg),
			"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
			"check the id: paceq explains it on every failure it reports")
	}
	if out.mode == modeJSON {
		down := noteDaemon(setting, env, out)
		var field *bool
		if probed := !setting.off && setting.path != ""; probed {
			field = daemonField(down, probed)
		}
		return writeRunRecordField(out, detail, field)
	}
	out.print("%s cancel requested for run %s  job %s  (%s)",
		out.symbols.ok, detail.ID, detail.JobName, map[bool]string{true: "via daemon", false: "directly"}[viaSocket])
	return nil
}

// cancelRefusal maps the direct path's store refusals for a cancel.
func cancelRefusal(ctx context.Context, st *store.Store, runArg string, err error) error {
	switch {
	case errors.Is(err, store.ErrRunNotFound):
		return notFoundError(
			fmt.Sprintf("no run matches %q", runArg),
			"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
			"check the id: paceq explains it on every failure it reports")
	case errors.Is(err, id.ErrInvalid):
		return notFoundError(
			fmt.Sprintf("no run matches %q", runArg),
			err.Error(),
			"an id is 26 characters from 0123456789ABCDEFGHJKMNPQRSTVWXYZ; any prefix of one works")
	default:
		return internalError("could not record the cancellation", err)
	}
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
		setting := resolveSocket(g, env, stateDir)
		down := noteDaemon(setting, env, out)
		if probed := !setting.off && setting.path != ""; probed {
			return out.json(runListEnvelope{DaemonUp: daemonField(down, probed), Runs: doc})
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

// runListEnvelope wraps the listing when a socket was explicitly named, for
// the same reason status wraps: the numbers should say where they came from.
type runListEnvelope struct {
	DaemonUp *bool        `json:"daemon_up,omitempty"`
	Runs     []runListRow `json:"runs"`
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

func newRunsShowCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "show <run>",
		Short: "Show one run: how it ended and how far the steps got",
		Long: `One run, whole: its steps in spec order, each with its state, its
exit code and where its log lives.

A run id may be shortened to any prefix that still names exactly one run.
The JSON form is the same record paceq run writes when a run ends, so one
jq program reads both.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsShow(ctx, env, g, out, args[0])
		}),
	}
}

// runRunsShow resolves the id or prefix and writes the record. The refusals
// are the ones logs gives for the same input, because two spellings of
// "this names no run" would be one more thing to learn.
func runRunsShow(ctx context.Context, env Env, g *globals, out *ui, runArg string) error {
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
			"run the command inside the project directory, or pass --db")
	}
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	detail, err := ro.GetRun(ctx, runArg)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrAmbiguousRunID):
			return notFoundError(
				fmt.Sprintf("%q does not name one run: the prefix matches more than one", runArg),
				ambiguousHint(err),
				"give more characters until the prefix names exactly one run",
			)
		case errors.Is(err, store.ErrRunNotFound):
			return notFoundError(
				fmt.Sprintf("no run matches %q", runArg),
				"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
				"check the id: paceq explains it on every failure it reports",
			)
		case errors.Is(err, id.ErrInvalid):
			// A name that cannot be an id at all names no run that can
			// exist, so it is a not-found with the reason spelled out
			// rather than an internal failure.
			return notFoundError(
				fmt.Sprintf("no run matches %q", runArg),
				err.Error(),
				"an id is 26 characters from 0123456789ABCDEFGHJKMNPQRSTVWXYZ; any prefix of one works",
			)
		default:
			return err
		}
	}

	if out.mode == modeJSON {
		setting := resolveSocket(g, env, stateDir)
		down := noteDaemon(setting, env, out)
		var field *bool
		if probed := !setting.off && setting.path != ""; probed {
			field = daemonField(down, probed)
		}
		return writeRunRecordField(out, detail, field)
	}
	mark := out.symbols.arrow
	switch model.RunState(detail.State) {
	case model.RunSucceeded:
		mark = out.symbols.ok
	case model.RunFailed:
		mark = out.symbols.fail
	case model.RunCancelled:
		mark = out.symbols.warn
	}
	out.print("%s run %s  job %s  %s", mark, detail.ID, detail.JobName, detail.State)
	if detail.ReasonCode != "" {
		out.print("  %s", outcomeText(detail.ReasonCode, detail.ReasonData))
	}
	for _, step := range detail.Steps {
		line := fmt.Sprintf("  %s  %s", step.Name, step.State)
		if step.ReasonCode != "" {
			line += "  " + outcomeText(step.ReasonCode, "")
		}
		out.print("%s", line)
	}
	return nil
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
