package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"

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
	cmd.AddCommand(newRunsListCmd(env, g), newRunsShowCmd(env, g), newRunsCancelCmd(env, g),
		newRunsRetryCmd(env, g), newRunsReplayCmd(env, g), newRunsArtifactsCmd(env, g))
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
		st := listState(row)
		widths.state = max(widths.state, len(st))
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
			pad(listState(row), widths.state),
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

	// The deferral facts (#68): a deferred run is queued but held
	// for the future, and a script reading --json sees why and what
	// blocked it.
	DeferReason string `json:"defer_reason,omitempty"`
	ReasonData  string `json:"reason_data,omitempty"`
	AvailableAt string `json:"available_at,omitempty"`
}

func newListRow(row store.RunSummary) runListRow {
	r := runListRow{
		ID:          row.ID,
		Job:         row.JobName,
		Origin:      row.Origin,
		State:       row.State,
		ReasonCode:  row.ReasonCode,
		CreatedAt:   rfc3339(row.CreatedAt),
		StartedAt:   rfc3339(row.StartedAt),
		FinishedAt:  rfc3339(row.FinishedAt),
		DurationMS:  millisBetween(row.StartedAt, row.FinishedAt),
		DeferReason: row.DeferReason,
		ReasonData:  row.ReasonData,
	}
	if !row.AvailableAt.IsZero() {
		r.AvailableAt = rfc3339(row.AvailableAt)
	}
	return r
}

// runOutcome is the text column of the table: why the run ended, as words.
func runOutcome(row store.RunSummary) string {
	if row.DeferReason == model.DeferReasonConcurrencyKey {
		if text := concDeferText(row.ReasonData); text != "" {
			return text
		}
	}
	if row.ReasonCode == "" {
		return ""
	}
	if entry, ok := reason.Lookup(reason.Code(row.ReasonCode)); ok {
		return entry.Short
	}
	return row.ReasonCode
}

// concDeferText turns a key deferral's reason_data into the words a human
// needs first: which key is wanted, and who holds it. Empty when the data
// does not name a key, so the ordinary reason text stands in its place.
func concDeferText(data string) string {
	key, blocking := concDeferFields(data)
	if key == "" {
		return ""
	}
	if blocking == "" {
		return "waiting for concurrency key " + key
	}
	return fmt.Sprintf("waiting for concurrency key %s (run %s holds it)", key, blocking)
}

// concDeferFields reads the wanted key and the holding run out of a key
// deferral's reason_data object.
func concDeferFields(data string) (key, blocking string) {
	var parsed struct {
		Key      string `json:"concurrency_key"`
		Blocking string `json:"blocking_run_id"`
	}
	if json.Unmarshal([]byte(data), &parsed) != nil {
		return "", ""
	}
	return parsed.Key, parsed.Blocking
}

// listState is the state column of the table, with the deferral facts folded in
// when the row is queued but held for the future: a human sees "deferred (reason)"
// where a machine sees the state alone.
func listState(row store.RunSummary) string {
	if row.DeferReason != "" {
		return "deferred"
	}
	return row.State
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
		return writeRunRecord(out, detail)
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
		// What the step published rides under its line: a name and the
		// uri it claimed. The references are history, so they read the
		// same however the run ended.
		for _, a := range step.Artifacts {
			out.print("      %s  %s", a.Name, a.URI)
		}
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

// --------------- cancel ---------------

func newRunsCancelCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "cancel <run>",
		Short: "Request cancellation of a run",
		Long: `Ask the daemon to stop a run. This is a request, not an immediate stop:
the executor observes the flag between steps and stops within roughly 20 seconds.

A queued run is cancelled immediately. A running run receives the request and
the executor honours it at its next step boundary. Repeated cancellations change
nothing: only the first request is recorded and its timestamp stays.`,
		Args: exactArgs(1, "one run id or prefix"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runRunsCancel(ctx, env, g, out, args[0])
		}),
	}
}

func runRunsCancel(ctx context.Context, env Env, g *globals, out *ui, runArg string) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}

	// Resolve the run first against a read-only store.
	ro, err := store.OpenReadOnly(ctx, filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		return err
	}
	detail, err := ro.GetRun(ctx, runArg)
	_ = ro.Close()
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
				runArg,
				"the ids of finished runs, shortest first: any prefix names a run as soon as it can",
				"check the id: paceq explains it on every failure it reports",
			)
		default:
			return err
		}
	}

	// Already terminal: nothing to cancel.
	terminal := map[string]bool{
		"succeeded": true, "failed": true, "cancelled": true,
	}
	if terminal[detail.State] {
		out.print("%s run %s is already %s", out.symbols.warn, detail.ID, detail.State)
		return nil
	}

	actor := cliActor()

	// Try through the daemon socket first.
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return socketRefusedError(err)
	}
	if socketPath != "" {
		if err := cancelViaSocket(ctx, socketPath, detail.ID, "manual"); err == nil {
			if detail.State == "queued" {
				out.print("%s cancellation of %s recorded; the run will not start", out.symbols.ok, detail.ID)
			} else {
				out.print("%s cancellation of %s requested; the owner stops within roughly 20 s", out.symbols.ok, detail.ID)
			}
			return nil
		} else if stop := stopOnRefusal(err); stop != nil {
			return stop
		}
		out.note(1, "daemon unreachable; writing directly to the database")
	} else {
		out.note(1, "no daemon socket; writing directly to the database")
	}
	if stop := daemonHoldsState(ctx, env, g, detail.ID+" was not cancelled"); stop != nil {
		return stop
	}

	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	cr, err := s.RequestCancel(ctx, detail.ID, actor, "manual")
	if err != nil {
		if errors.Is(err, store.ErrRunNotFound) {
			return notFoundError(
				fmt.Sprintf("no run matches %q", runArg),
				runArg,
				"check the id: paceq explains it on every failure it reports",
			)
		}
		return internalError("could not cancel the run", err)
	}

	if detail.State == "queued" {
		out.print("%s cancellation of %s recorded (requested at %s); the run will not start",
			out.symbols.ok, detail.ID, cr.CancelRequestedAt.UTC().Format("2006-01-02 15:04:05"))
	} else {
		out.print("%s cancellation of %s requested (requested at %s); the owner stops within roughly 20 s",
			out.symbols.ok, detail.ID, cr.CancelRequestedAt.UTC().Format("2006-01-02 15:04:05"))
	}
	return nil
}
