package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/store"
)

// newSchedulesCmd is the noun group for schedule management.
func newSchedulesCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "schedules",
		Short: "Inspect and control schedules",
		Long: `Read and control the schedules the daemon fires on.

Schedules are the automatic firing rules for jobs: a cron expression, a timezone
and a catch-up policy decide when a run starts. Every subcommand reads from the
database directly; writes go through the daemon when it is running.`,
	}
	cmd.AddCommand(
		newSchedulesListCmd(env, g),
		newSchedulesShowCmd(env, g),
		newSchedulesPreviewCmd(env, g),
		newSchedulesPauseCmd(env, g),
		newSchedulesResumeCmd(env, g),
	)
	return cmd
}

// scheduleListRow is one list entry in JSON mode.
type scheduleListRow struct {
	ID         string `json:"id"`
	JobName    string `json:"job_name"`
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Expr       string `json:"expr"`
	Timezone   string `json:"timezone"`
	Paused     bool   `json:"paused"`
	LastTickAt string `json:"last_tick_at,omitempty"`
	NextTickAt string `json:"next_tick_at"`
	CreatedAt  string `json:"created_at,omitempty"`
	UpdatedAt  string `json:"updated_at,omitempty"`

	SpringForward string `json:"spring_forward,omitempty"`
	FallBack      string `json:"fall_back,omitempty"`
	Catchup       string `json:"catchup,omitempty"`
	CatchupLimit  int    `json:"catchup_limit,omitempty"`
	// SEAM (#68): overlap_policy omitted until the column lands.
}

func newScheduleListRow(row store.ScheduleRow) scheduleListRow {
	return scheduleListRow{
		ID:            row.ID,
		JobName:       row.JobName,
		Name:          row.Name,
		Kind:          row.Kind,
		Expr:          row.Expr,
		Timezone:      row.Timezone,
		Paused:        row.Paused,
		LastTickAt:    rfc3339Ptr(row.LastTickAt),
		NextTickAt:    rfc3339T(row.NextTickAt),
		CreatedAt:     rfc3339T(row.CreatedAt),
		UpdatedAt:     rfc3339T(row.UpdatedAt),
		SpringForward: row.SpringForward,
		FallBack:      row.FallBack,
		Catchup:       row.Catchup,
		CatchupLimit:  row.CatchupLimit,
	}
}

// rfc3339Ptr formats a *time.Time, returning "" for nil.
func rfc3339Ptr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// rfc3339T formats a time.Time, returning "" for zero.
func rfc3339T(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// ------------------- list -------------------

func newSchedulesListCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List every schedule, active first",
		Args:    noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runSchedulesList(ctx, env, g, out)
		}),
	}
}

func runSchedulesList(ctx context.Context, env Env, g *globals, out *ui) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	rows, err := ro.ListAllSchedules(ctx)
	if err != nil {
		return internalError("could not list schedules", err)
	}

	if out.mode == modeJSON {
		doc := make([]scheduleListRow, 0, len(rows))
		for _, row := range rows {
			doc = append(doc, newScheduleListRow(row))
		}
		return out.json(doc)
	}

	if len(rows) == 0 {
		out.print("no schedules yet: apply a job with a schedule to create one")
		return nil
	}

	// Column widths.
	wJob, wName, wExpr, wTZ := 3, 8, 4, 2
	for _, row := range rows {
		wJob = max(wJob, len(row.JobName))
		wName = max(wName, len(row.Name))
		wExpr = max(wExpr, len(row.Expr))
		wTZ = max(wTZ, len(row.Timezone))
	}
	for _, row := range rows {
		status := "active"
		mark := out.symbols.ok
		if row.Paused {
			status = "paused"
			mark = out.symbols.warn
		}
		out.print("%s %s  %s  %s  %s  %s",
			mark,
			pad(row.JobName, wJob),
			pad(row.Name, wName),
			pad(row.Expr, wExpr),
			pad(row.Timezone, wTZ),
			status,
		)
	}
	return nil
}

// ------------------- show -------------------

func newSchedulesShowCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "show <schedule>",
		Short: "Show one schedule with its policy and recent ticks",
		Long: `One schedule, whole: every policy field, the cursor, and the last
few ticks that fired or were skipped.

A schedule reference is job/schedule.name. When only one schedule has a given
name, the job prefix can be left out.`,
		Args: exactArgs(1, "one schedule reference"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSchedulesShow(ctx, env, g, out, args[0])
		}),
	}
}

func runSchedulesShow(ctx context.Context, env Env, g *globals, out *ui, ref string) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	sch, err := resolveScheduleRef(ctx, ro, ref)
	if err != nil {
		return err
	}

	ticks, err := ro.ScheduleTicks(ctx, sch.JobName, sch.Name)
	if err != nil {
		return internalError("could not read the ticks of "+ref, err)
	}

	if out.mode == modeJSON {
		type tickJSON struct {
			ScheduledFor string `json:"scheduled_for"`
			Outcome      string `json:"outcome"`
			ReasonCode   string `json:"reason_code,omitempty"`
			RepeatCount  int    `json:"repeat_count"`
			TriggerCount int    `json:"trigger_count"`
		}
		type scheduleShowJSON struct {
			ID              string     `json:"id"`
			JobName         string     `json:"job_name"`
			Name            string     `json:"name"`
			Kind            string     `json:"kind"`
			Expr            string     `json:"expr"`
			Timezone        string     `json:"timezone"`
			SpringForward   string     `json:"spring_forward"`
			FallBack        string     `json:"fall_back"`
			Catchup         string     `json:"catchup"`
			CatchupLimit    int        `json:"catchup_limit"`
			CatchupWindowMS int64      `json:"catchup_window_ms"`
			Paused          bool       `json:"paused"`
			LastTickAt      string     `json:"last_tick_at,omitempty"`
			NextTickAt      string     `json:"next_tick_at"`
			CreatedAt       string     `json:"created_at,omitempty"`
			UpdatedAt       string     `json:"updated_at,omitempty"`
			Ticks           []tickJSON `json:"ticks"`
		}
		doc := scheduleShowJSON{
			ID:              sch.ID,
			JobName:         sch.JobName,
			Name:            sch.Name,
			Kind:            sch.Kind,
			Expr:            sch.Expr,
			Timezone:        sch.Timezone,
			SpringForward:   sch.SpringForward,
			FallBack:        sch.FallBack,
			Catchup:         sch.Catchup,
			CatchupLimit:    sch.CatchupLimit,
			CatchupWindowMS: sch.CatchupWindowMS,
			Paused:          sch.Paused,
			LastTickAt:      rfc3339Ptr(sch.LastTickAt),
			NextTickAt:      rfc3339T(sch.NextTickAt),
			CreatedAt:       rfc3339T(sch.CreatedAt),
			UpdatedAt:       rfc3339T(sch.UpdatedAt),
			Ticks:           make([]tickJSON, 0, len(ticks)),
		}
		for _, t := range ticks {
			doc.Ticks = append(doc.Ticks, tickJSON{
				ScheduledFor: rfc3339T(t.ScheduledFor),
				Outcome:      t.Outcome,
				ReasonCode:   t.ReasonCode,
				RepeatCount:  t.RepeatCount,
				TriggerCount: t.TriggerCount,
			})
		}
		return out.json(doc)
	}

	status := "active"
	mark := out.symbols.ok
	if sch.Paused {
		status = "paused"
		mark = out.symbols.warn
	}
	out.print("%s schedule %s/%s  %s  %s %s",
		mark, sch.JobName, sch.Name, status, sch.Kind, sch.Expr)
	out.print("  timezone: %s  spring_forward: %s  fall_back: %s  catchup: %s (limit %d)",
		sch.Timezone, sch.SpringForward, sch.FallBack, sch.Catchup, sch.CatchupLimit)
	// SEAM (#68): overlap_policy column not yet available.
	if sch.LastTickAt != nil {
		out.print("  last tick: %s  next: %s",
			sch.LastTickAt.UTC().Format(cronTimeFmt),
			sch.NextTickAt.UTC().Format(cronTimeFmt))
	} else {
		out.print("  never fired  next: %s", sch.NextTickAt.UTC().Format(cronTimeFmt))
	}

	if len(ticks) > 0 {
		out.print("recent ticks:")
		shown := ticks
		if len(shown) > 20 {
			shown = ticks[len(ticks)-20:]
		}
		for _, t := range shown {
			out.print("  %s  %s  %s",
				t.ScheduledFor.UTC().Format(cronTimeFmt),
				pad(t.Outcome, 9),
				t.ReasonCode)
		}
	}
	return nil
}

// ------------------- preview -------------------

type previewFlags struct {
	from  string
	count int
}

func newSchedulesPreviewCmd(env Env, g *globals) *cobra.Command {
	var f previewFlags
	cmd := &cobra.Command{
		Use:   "preview <schedule>",
		Short: "Preview upcoming firings of a schedule",
		Long: `Show the next N firings a schedule would produce, with local time and
UTC side by side. Uses the same code path as the scheduler: the cron expression,
the timezone and the DST policy are all honoured exactly as the daemon would.

Preview has zero side effects: the database is bit-identical before and after.`,
		Args: exactArgs(1, "one schedule reference"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSchedulesPreview(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.from, "from", "now",
		"start the series at this moment (an RFC 3339 stamp, or 'now')")
	cmd.Flags().IntVar(&f.count, "n", 10,
		"show this many firings (1-100)")
	return cmd
}

func runSchedulesPreview(ctx context.Context, env Env, g *globals, out *ui, ref string, f previewFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	sch, err := resolveScheduleRef(ctx, ro, ref)
	if err != nil {
		return err
	}

	if f.count < 1 || f.count > 100 {
		return usageError(fmt.Sprintf("--n %d is outside 1-100", f.count),
			"pass a number between 1 and 100")
	}

	tz, err := cronx.LoadZone(sch.Timezone)
	if err != nil {
		return internalError(fmt.Sprintf("load timezone %s", sch.Timezone), err)
	}

	parsed, err := cronx.Parse(sch.Expr)
	if err != nil {
		return internalError(fmt.Sprintf("parse the cron expression %q", sch.Expr), err)
	}

	pol := policyOfRow(sch)

	cursor := clockForEnv(env).Now()
	if f.from != "" && f.from != "now" {
		t, err := parseTimeStamp(f.from)
		if err != nil {
			return usageError(fmt.Sprintf("--from %s is not a valid time stamp", f.from),
				"write an RFC 3339 stamp, such as 2026-03-29T00:00:00Z, or 'now'")
		}
		cursor = t
	}

	occs := make([]cronx.Occurrence, 0, f.count)
	for i := 0; i < f.count; i++ {
		occ, err := parsed.Next(cursor, tz, pol)
		if err != nil {
			if strings.Contains(err.Error(), "no more occurrences") {
				break
			}
			return internalError("could not compute the next occurrence", err)
		}
		occs = append(occs, occ)
		cursor = occ.At
	}

	if out.mode == modeJSON {
		type occJSON struct {
			Index      int    `json:"index"`
			Local      string `json:"local"`
			UTC        string `json:"utc"`
			Skipped    bool   `json:"skipped,omitempty"`
			SkipReason string `json:"skip_reason,omitempty"`
			SkipNote   string `json:"skip_note,omitempty"`
		}
		type previewJSON struct {
			JobName       string    `json:"job_name"`
			ScheduleName  string    `json:"schedule_name"`
			Expr          string    `json:"expr"`
			Timezone      string    `json:"timezone"`
			SpringForward string    `json:"spring_forward"`
			FallBack      string    `json:"fall_back"`
			From          string    `json:"from"`
			Occurrences   []occJSON `json:"occurrences"`
		}
		doc := previewJSON{
			JobName:       sch.JobName,
			ScheduleName:  sch.Name,
			Expr:          sch.Expr,
			Timezone:      sch.Timezone,
			SpringForward: sch.SpringForward,
			FallBack:      sch.FallBack,
			From:          rfc3339T(cursor),
			Occurrences:   make([]occJSON, 0, len(occs)),
		}
		for i, occ := range occs {
			entry := occJSON{
				Index:   i + 1,
				Local:   occ.At.In(tz).Format(cronTimeFmtWithOffset),
				UTC:     occ.At.UTC().Format(cronTimeFmt),
				Skipped: occ.Skipped,
			}
			if occ.Skipped {
				entry.SkipReason = occ.SkipReason
				entry.SkipNote = dstSkipNote(occ, sch)
			}
			doc.Occurrences = append(doc.Occurrences, entry)
		}
		return out.json(doc)
	}

	out.print("schedule %s/%s  %s  %s", sch.JobName, sch.Name, sch.Kind, sch.Expr)
	out.print("timezone: %s  spring_forward: %s  fall_back: %s",
		sch.Timezone, sch.SpringForward, sch.FallBack)
	out.print("")
	out.print("%4s  %-25s  %-25s", "#", "LOCAL", "UTC")
	for i, occ := range occs {
		local := occ.At.In(tz).Format(cronTimeFmtWithOffset)
		utc := occ.At.UTC().Format(cronTimeFmt)
		if occ.Skipped {
			reason := occ.SkipReason
			out.print("%3d.  %-25s  %-25s  %s %s", i+1, local, utc, out.symbols.warn, reason)
			if note := dstSkipNote(occ, sch); note != "" {
				out.print("      %s %s", out.symbols.arrow, note)
			}
		} else {
			out.print("%3d.  %-25s  %-25s", i+1, local, utc)
		}
	}
	return nil
}

// dstSkipNote returns a human-readable explanation for a DST-related skip.
func dstSkipNote(occ cronx.Occurrence, sch store.ScheduleRow) string {
	reason := occ.SkipReason
	policy := sch.SpringForward
	if strings.Contains(reason, "duplicate") || strings.Contains(reason, "ambiguous") {
		policy = sch.FallBack
	}
	switch {
	case strings.Contains(reason, "nonexistent"):
		return fmt.Sprintf("This hour does not exist (spring forward). Policy %q means it is skipped.", policy)
	case strings.Contains(reason, "duplicate") || strings.Contains(reason, "ambiguous"):
		return fmt.Sprintf("This hour appears twice (fall back). Policy %q picked one.", policy)
	case strings.Contains(reason, "catchup"):
		return "Skipped by catch-up policy."
	default:
		return ""
	}
}

// cronTimeFmt and cronTimeFmtWithOffset are the canonical renderings.
const (
	cronTimeFmt           = "2006-01-02 15:04:05"
	cronTimeFmtWithOffset = "2006-01-02 15:04:05 -0700"
)

// parseTimeStamp parses a timestamp in RFC 3339 format.
func parseTimeStamp(s string) (time.Time, error) {
	return time.Parse(time.RFC3339, s)
}

// ------------------- pause -------------------

func newSchedulesPauseCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "pause <schedule>",
		Short: "Pause a schedule: the next tick is skipped",
		Long: `Pause one schedule. The daemon stops materialising new runs for it
immediately. Already-queued runs are not affected; only new ticks are.

Paused schedules appear in the list with the paused marker. Resume brings them
back with the cursor recomputed from where it last was.`,
		Args: exactArgs(1, "one schedule reference"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSchedulesPause(ctx, env, g, out, args[0])
		}),
	}
}

func runSchedulesPause(ctx context.Context, env Env, g *globals, out *ui, ref string) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}

	// Resolve the schedule reference against a read-only store first.
	ro, err := store.OpenReadOnly(ctx, filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		return err
	}
	sch, err := resolveScheduleRef(ctx, ro, ref)
	_ = ro.Close()
	if err != nil {
		return err
	}

	// Try to pause through the daemon socket when it is running.
	socketPath, err := daemonSocket(stateDir)
	if err != nil {
		return socketRefusedError(err)
	}
	if socketPath != "" {
		if err := pauseViaSocket(ctx, socketPath, sch.JobName+"/"+sch.Name); err == nil {
			out.print("%s paused %s/%s", out.symbols.ok, sch.JobName, sch.Name)
			return nil
		} else if stop := stopOnRefusal(err); stop != nil {
			return stop
		}
		out.note(1, "daemon unreachable; writing directly to the database")
	} else {
		out.note(1, "no daemon socket; writing directly to the database")
	}

	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	_, err = s.PauseSchedule(ctx, sch.JobName, sch.Name)
	if err != nil {
		return internalError("could not pause "+ref, err)
	}

	out.print("%s paused %s/%s", out.symbols.ok, sch.JobName, sch.Name)
	return nil
}

// ------------------- resume -------------------

func newSchedulesResumeCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "resume <schedule>",
		Short: "Resume a paused schedule",
		Long: `Resume one paused schedule. The cursor is recomputed from where the
schedule last stopped, and the daemon picks it up on the next tick.

If the schedule was not paused nothing changes and the command still succeeds.`,
		Args: exactArgs(1, "one schedule reference"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSchedulesResume(ctx, env, g, out, args[0])
		}),
	}
}

func runSchedulesResume(ctx context.Context, env Env, g *globals, out *ui, ref string) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}

	// Resolve the schedule reference against a read-only store first.
	ro, err := store.OpenReadOnly(ctx, filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		return err
	}
	sch, err := resolveScheduleRef(ctx, ro, ref)
	_ = ro.Close()
	if err != nil {
		return err
	}

	// Already active.
	if !sch.Paused {
		out.print("%s %s/%s is already active", out.symbols.ok, sch.JobName, sch.Name)
		return nil
	}

	// Compute the next tick from the cursor.
	nextTickAt, err := computeNextTick(sch, clockForEnv(env))
	if err != nil {
		return internalError("could not compute the next tick for "+ref, err)
	}

	// Try to resume through the daemon socket.
	socketPath, err := daemonSocket(stateDir)
	if err != nil {
		return socketRefusedError(err)
	}
	if socketPath != "" {
		if err := resumeViaSocket(ctx, socketPath, sch.JobName+"/"+sch.Name); err == nil {
			out.print("%s resumed %s/%s", out.symbols.ok, sch.JobName, sch.Name)
			return nil
		} else if stop := stopOnRefusal(err); stop != nil {
			return stop
		}
		out.note(1, "daemon unreachable; writing directly to the database")
	} else {
		out.note(1, "no daemon socket; writing directly to the database")
	}

	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	_, err = s.ResumeSchedule(ctx, sch.JobName, sch.Name, nextTickAt)
	if err != nil {
		return internalError("could not resume "+ref, err)
	}

	out.print("%s resumed %s/%s", out.symbols.ok, sch.JobName, sch.Name)
	return nil
}

// ------------------- helpers -------------------

// openReadOnlyStore opens the state database for reading.
func openReadOnlyStore(ctx context.Context, env Env, g *globals) (*store.Store, error) {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, fs.ErrNotExist) {
		return nil, notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db",
		)
	}
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		return nil, err
	}
	return ro, nil
}

// scheduleRef is a parsed schedule reference.
type scheduleRef struct {
	Job  string
	Name string
}

// parseScheduleRef splits a reference into job and schedule name.
func parseScheduleRef(ref string) scheduleRef {
	parts := strings.SplitN(ref, "/", 2)
	if len(parts) == 2 {
		return scheduleRef{Job: parts[0], Name: parts[1]}
	}
	return scheduleRef{Name: parts[0]}
}

// resolveScheduleRef turns a reference into a schedule row.
func resolveScheduleRef(ctx context.Context, ro *store.Store, ref string) (store.ScheduleRow, error) {
	sr := parseScheduleRef(ref)
	if sr.Job != "" {
		row, err := ro.GetSchedule(ctx, sr.Job, sr.Name)
		if err != nil {
			if errors.Is(err, store.ErrNotFound) {
				return store.ScheduleRow{}, unknownScheduleError(ctx, ro, ref)
			}
			return store.ScheduleRow{}, err
		}
		return row, nil
	}

	// Bare name: search all schedules.
	all, err := ro.ListAllSchedules(ctx)
	if err != nil {
		return store.ScheduleRow{}, internalError("could not list schedules", err)
	}
	var matches []store.ScheduleRow
	for _, row := range all {
		if row.Name == sr.Name {
			matches = append(matches, row)
		}
	}
	switch len(matches) {
	case 0:
		return store.ScheduleRow{}, unknownScheduleError(ctx, ro, ref)
	case 1:
		return matches[0], nil
	default:
		names := make([]string, len(matches))
		for i, m := range matches {
			names[i] = m.JobName + "/" + m.Name
		}
		return store.ScheduleRow{}, notFoundError(
			fmt.Sprintf("%q matches %d schedules; qualify with the job name", sr.Name, len(matches)),
			ref,
			"write job/schedule to name one exactly: "+strings.Join(names, ", "),
		)
	}
}

// unknownScheduleError is the exit 3 refusal with a did-you-mean suggestion.
func unknownScheduleError(ctx context.Context, ro *store.Store, ref string) error {
	all, _ := ro.ListAllSchedules(ctx)
	next := []string{
		"paceq schedules list  shows every schedule",
		"write the reference as job/schedule.name",
	}
	var known []string
	for _, row := range all {
		known = append(known, row.Name, row.JobName+"/"+row.Name)
	}
	sr := parseScheduleRef(ref)
	if suggestion := diag.Suggest(sr.Name, known); suggestion != "" {
		next = append([]string{
			fmt.Sprintf("did you mean %q?", suggestion),
			"paceq schedules show " + suggestion + "  shows that schedule",
		}, next...)
	}
	return notFoundError(
		fmt.Sprintf("no schedule matches %q", ref),
		ref,
		next...,
	)
}

// computeNextTick returns the next fire-time after the cursor.
func computeNextTick(sch store.ScheduleRow, clk clock.Clock) (time.Time, error) {
	tz, err := cronx.LoadZone(sch.Timezone)
	if err != nil {
		return time.Time{}, err
	}
	parsed, err := cronx.Parse(sch.Expr)
	if err != nil {
		return time.Time{}, err
	}
	pol := policyOfRow(sch)

	cursor := clk.Now()
	if sch.LastTickAt != nil {
		cursor = *sch.LastTickAt
	}

	occ, err := parsed.Next(cursor, tz, pol)
	if err != nil {
		return clk.Now().Add(time.Hour), nil
	}
	return occ.At, nil
}

// policyOfRow maps the store's policy string columns onto cronx types.
func policyOfRow(sch store.ScheduleRow) cronx.Policy {
	var p cronx.Policy
	if sch.SpringForward == "shift" {
		p.SpringForward = cronx.Shift
	}
	if sch.FallBack == "both" {
		p.FallBack = cronx.Both
	}
	return p
}

// pauseViaSocket sends a POST /v1/schedules/{name}/pause to the daemon socket.
func pauseViaSocket(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/schedules/"+name+"/pause")
}

// resumeViaSocket sends a POST /v1/schedules/{name}/resume to the daemon socket.
func resumeViaSocket(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/schedules/"+name+"/resume")
}

// cancelViaSocket sends a POST /v1/runs/{id}/cancel to the daemon socket.
func cancelViaSocket(ctx context.Context, socketPath, runID, reason string) error {
	body := strings.NewReader(`{"reason":"` + reason + `"}`)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix"+"/v1/runs/"+runID+"/cancel", body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	return sendSocket(ctx, socketPath, req)
}

// clockForEnv returns the clock an Env carries, or the system clock.
func clockForEnv(env Env) clock.Clock {
	if env.Clk == nil {
		return clock.System()
	}
	return env.Clk
}
