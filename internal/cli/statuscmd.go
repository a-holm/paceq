package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// status is the per job view: is every job well, and when did each one last
// do anything. It reads only, so it answers while another process runs.

func newStatusCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "status [job]",
		Short: "Show the latest outcome of every job",
		Long: `One line per job: its newest run, how it ended, when, and how far the
steps got. A job that has never run says so rather than being left out.

This is the plain M1 status: no next scheduled run and no freshness line,
because schedules arrive with M2.`,
		Args: cobra.MaximumNArgs(1),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runStatus(ctx, env, g, out, args)
		}),
	}
}

func runStatus(ctx context.Context, env Env, g *globals, out *ui, args []string) error {
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
	job := ""
	if len(args) == 1 {
		job = args[0]
	}

	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	rows, err := ro.JobLastRuns(ctx, job)
	if err != nil {
		return internalError("could not read the jobs", err)
	}
	if job != "" && len(rows) == 0 {
		return unknownJobError(ctx, ro, job)
	}

	now := clkOf(env).Now()
	if out.mode == modeJSON {
		doc := make([]statusRow, 0, len(rows))
		for _, row := range rows {
			doc = append(doc, newStatusRow(row))
		}
		setting := resolveSocket(g, env, stateDir)
		down := noteDaemon(setting, env, out)
		if probed := !setting.off && setting.path != ""; probed {
			return out.json(statusEnvelope{DaemonUp: daemonField(down, probed), Jobs: doc})
		}
		return out.json(doc)
	}

	widths := struct{ job, state int }{job: 3, state: 5}
	for _, row := range rows {
		widths.job = max(widths.job, len(row.JobName))
		widths.state = max(widths.state, len(row.State))
	}
	for _, row := range rows {
		if row.RunID == "" {
			out.print("%s  %s no runs yet", pad(row.JobName, widths.job), out.symbols.arrow)
			continue
		}
		mark := out.symbols.ok
		switch model.RunState(row.State) {
		case model.RunFailed:
			mark = out.symbols.fail
		case model.RunCancelled:
			mark = out.symbols.warn
		case model.RunQueued, model.RunRunning:
			mark = out.symbols.arrow
		}
		state := row.State
		if mark == out.symbols.fail || mark == out.symbols.warn {
			state += " " + shortReason(row.ReasonCode)
		}
		out.print("%s %s  %s  %s ago  %s  %d/%d",
			pad(row.JobName, widths.job),
			mark,
			pad(state, widths.state),
			relAge(now, row.CreatedAt),
			durationOf(row.StartedAt, row.FinishedAt),
			row.StepsDone, row.StepsTotal)
	}
	return nil
}

// statusRow flattens the job and its newest run into one object. A job that
// has never run carries its name alone.
type statusRow struct {
	Job        string `json:"job"`
	ID         string `json:"id,omitempty"`
	State      string `json:"state,omitempty"`
	ReasonCode string `json:"reason_code,omitempty"`
	CreatedAt  string `json:"created_at,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	StepsTotal int    `json:"steps_total,omitempty"`
	StepsDone  int    `json:"steps_done,omitempty"`
}

// statusEnvelope wraps the per job listing when a socket was explicitly
// named, so a script can see at a glance whether the numbers came from a
// live daemon or from whatever the last one left behind.
type statusEnvelope struct {
	DaemonUp *bool       `json:"daemon_up,omitempty"`
	Jobs     []statusRow `json:"jobs"`
}

func newStatusRow(row store.JobRunSummary) statusRow {
	out := statusRow{Job: row.JobName}
	if row.RunID == "" {
		return out
	}
	out.ID = row.RunID
	out.State = row.State
	out.ReasonCode = row.ReasonCode
	out.CreatedAt = rfc3339(row.CreatedAt)
	out.StartedAt = rfc3339(row.StartedAt)
	out.FinishedAt = rfc3339(row.FinishedAt)
	out.DurationMS = millisBetween(row.StartedAt, row.FinishedAt)
	out.StepsTotal = row.StepsTotal
	out.StepsDone = row.StepsDone
	return out
}

// shortReason is a reason code as a few words, for a table cell.
func shortReason(code string) string {
	if entry, ok := reason.Lookup(reason.Code(code)); ok {
		return entry.Short
	}
	return code
}
