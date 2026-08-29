package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
)

// newSensorsCmd is the noun group for sensor management. Sensors are the
// external triggers M3-01 materialises; this group puts an operator's hands on
// the machine: list and show read straight from the database so they work with
// the daemon down, while pause, resume, reset, tick and cursor set write
// through the daemon when it answers and fall back to flock + direct.
func newSensorsCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sensors",
		Short: "Inspect and control sensors",
		Long: `Read and control the sensors the daemon evaluates.

Sensors are the external triggers: a subprocess that watches the world and
asks for a run when something happened. Every read subcommand goes straight to
the database and works with the daemon down; every write subcommand goes
through the daemon when it is running and falls back to a direct write.`,
	}
	cmd.AddCommand(
		newSensorsListCmd(env, g),
		newSensorsShowCmd(env, g),
		newSensorsTestCmd(env, g),
		newSensorsTickCmd(env, g),
		newSensorsPauseCmd(env, g),
		newSensorsResumeCmd(env, g),
		newSensorsResetCmd(env, g),
		newSensorsCursorCmd(env, g),
	)
	return cmd
}

// sensorOutcomeText maps a tick outcome onto the one word the list shows.
func sensorOutcomeText(outcome string) string {
	switch outcome {
	case "triggered":
		return "triggered"
	case "skipped":
		return "skipped"
	case "error":
		return "error"
	case "running":
		return "running"
	case "":
		return "never"
	default:
		return outcome
	}
}

// completeSensorName is the cobra completion for a sensor-name argument: the
// names of every sensor in the state, or an empty completion when the state
// cannot be read (the shell shows nothing rather than an error). The 150 ms
// budget is comfortable: one read of the sensors table.
func completeSensorName(env Env, g *globals) func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
	return func(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
		ro, err := openReadOnlyStore(context.Background(), env, g)
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		defer func() { _ = ro.Close() }()
		rows, err := ro.ListSensors(context.Background())
		if err != nil {
			return nil, cobra.ShellCompDirectiveNoFileComp
		}
		var names []string
		for _, row := range rows {
			if strings.HasPrefix(row.Name, toComplete) {
				names = append(names, row.Name)
			}
		}
		return names, cobra.ShellCompDirectiveNoFileComp
	}
}

// ------------------- list -------------------

type sensorListFlags struct {
	job    string
	paused bool
}

func newSensorsListCmd(env Env, g *globals) *cobra.Command {
	var f sensorListFlags
	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List every sensor",
		Args:    noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runSensorsList(ctx, env, g, out, f)
		}),
	}
	cmd.Flags().StringVar(&f.job, "job", "", "only sensors of this job")
	cmd.Flags().BoolVar(&f.paused, "paused", false, "only paused sensors")
	return cmd
}

func runSensorsList(ctx context.Context, env Env, g *globals, out *ui, f sensorListFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	rows, err := ro.ListSensors(ctx)
	if err != nil {
		return internalError("could not list sensors", err)
	}

	filtered := rows
	if f.job != "" {
		var keep []store.SensorSummary
		for _, row := range rows {
			if row.JobName == f.job {
				keep = append(keep, row)
			}
		}
		filtered = keep
	}
	if f.paused {
		var keep []store.SensorSummary
		for _, row := range filtered {
			if row.Paused {
				keep = append(keep, row)
			}
		}
		filtered = keep
	}

	if out.mode == modeJSON {
		type sensorListRow struct {
			Name         string `json:"name"`
			Job          string `json:"job"`
			IntervalMS   int64  `json:"interval_ms"`
			Paused       bool   `json:"paused"`
			PausedReason string `json:"paused_reason,omitempty"`
			LastTickAt   string `json:"last_tick_at,omitempty"`
			LastOutcome  string `json:"last_outcome"`
			NextEvalAt   string `json:"next_eval_at"`
		}
		doc := make([]sensorListRow, 0, len(filtered))
		for _, row := range filtered {
			r := sensorListRow{
				Name:         row.Name,
				Job:          row.JobName,
				IntervalMS:   row.IntervalMS,
				Paused:       row.Paused,
				PausedReason: row.PausedReason,
				LastOutcome:  sensorOutcomeText(row.LastOutcome),
				NextEvalAt:   unixMilliUTC(row.NextEvalAt),
			}
			if row.LastTickAt != nil {
				r.LastTickAt = unixMilliUTC(*row.LastTickAt)
			}
			doc = append(doc, r)
		}
		return out.json(doc)
	}

	if len(filtered) == 0 {
		out.print("no sensors yet: apply a job with a sensor block to create one")
		return nil
	}

	wName, wJob, wInt := 4, 4, 8
	for _, row := range filtered {
		wName = max(wName, len(row.Name))
		wJob = max(wJob, len(row.JobName))
	}
	out.print("%s %s  %s  %s  %s  %s", pad("NAME", wName), pad("JOB", wJob), pad("INTERVAL", wInt), pad("LAST", 10), pad("OUTCOME", 10), "NEXT")
	for _, row := range filtered {
		status := sensorOutcomeText(row.LastOutcome)
		mark := out.symbols.ok
		if row.Paused {
			status = "paused"
			mark = out.symbols.warn
		}
		last := "never"
		if row.LastTickAt != nil {
			last = time.UnixMilli(*row.LastTickAt).UTC().Format("2006-01-02 15:04:05")
		}
		next := unixMilliUTC(row.NextEvalAt)
		out.print("%s %s  %s  %s  %s  %s",
			mark, pad(row.Name, wName), pad(row.JobName, wJob),
			pad(fmt.Sprintf("%ds", row.IntervalMS/1000), wInt),
			pad(last, 10), pad(status, 10), next)
	}
	return nil
}

// ------------------- show -------------------

type sensorShowFlags struct {
	limit int
}

func newSensorsShowCmd(env Env, g *globals) *cobra.Command {
	var f sensorShowFlags
	cmd := &cobra.Command{
		Use:   "show <sensor>",
		Short: "Show one sensor with its state and recent ticks",
		Long: `One sensor, whole: its definition, cursor, dedup epoch, and the
last few ticks that fired or were skipped.`,
		Args:              exactArgs(1, "one sensor name"),
		ValidArgsFunction: completeSensorName(env, g),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsShow(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().IntVar(&f.limit, "limit", 20, "how many recent ticks to show (1-100)")
	return cmd
}

func runSensorsShow(ctx context.Context, env Env, g *globals, out *ui, name string, f sensorShowFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	if f.limit < 1 || f.limit > 100 {
		return usageError(fmt.Sprintf("--limit %d is outside 1-100", f.limit),
			"pass a number between 1 and 100")
	}

	row, err := ro.GetSensor(ctx, name)
	if err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}

	ticks, err := ro.SensorTicks(ctx, name, f.limit)
	if err != nil {
		return internalError("could not read the ticks of "+name, err)
	}

	if out.mode == modeJSON {
		type tickJSON struct {
			StartedAt    string `json:"started_at"`
			Outcome      string `json:"outcome"`
			ReasonCode   string `json:"reason_code,omitempty"`
			TriggerCount int    `json:"trigger_count"`
			DedupedCount int    `json:"deduped_count"`
		}
		type showJSON struct {
			Name                string     `json:"name"`
			Job                 string     `json:"job"`
			Kind                string     `json:"kind"`
			IntervalMS          int64      `json:"interval_ms"`
			MinIntervalMS       int64      `json:"min_interval_ms"`
			TimeoutMS           int64      `json:"timeout_ms"`
			MaxTriggersPerTick  int        `json:"max_triggers_per_tick"`
			Paused              bool       `json:"paused"`
			PausedReason        string     `json:"paused_reason,omitempty"`
			Cursor              string     `json:"cursor,omitempty"`
			CursorVersion       int64      `json:"cursor_version"`
			DedupEpoch          int64      `json:"dedup_epoch"`
			ConsecutiveFailures int        `json:"consecutive_failures"`
			NextEvalAt          string     `json:"next_eval_at"`
			Ticks               []tickJSON `json:"ticks"`
		}
		doc := showJSON{
			Name:                row.Name,
			Job:                 row.JobName,
			Kind:                row.Kind,
			IntervalMS:          row.IntervalMS,
			MinIntervalMS:       row.MinIntervalMS,
			TimeoutMS:           row.TimeoutMS,
			MaxTriggersPerTick:  row.MaxTriggersPerTick,
			Paused:              row.Paused,
			PausedReason:        row.PausedReason,
			CursorVersion:       row.CursorVersion,
			DedupEpoch:          row.DedupEpoch,
			ConsecutiveFailures: row.ConsecutiveFailures,
			NextEvalAt:          unixMilliUTC(row.NextEvalAt),
			Ticks:               make([]tickJSON, 0, len(ticks)),
		}
		if row.Cursor != nil {
			doc.Cursor = *row.Cursor
		}
		for _, t := range ticks {
			doc.Ticks = append(doc.Ticks, tickJSON{
				StartedAt:    t.StartedAt.UTC().Format(time.RFC3339),
				Outcome:      t.Outcome,
				ReasonCode:   t.ReasonCode,
				TriggerCount: t.TriggerCount,
				DedupedCount: t.DedupedCount,
			})
		}
		return out.json(doc)
	}

	status := "active"
	mark := out.symbols.ok
	if row.Paused {
		status = "paused"
		mark = out.symbols.warn
	}
	out.print("%s sensor %s  %s  (job: %s)", mark, row.Name, status, row.JobName)
	out.print("  interval %ds  min_interval %ds  timeout %ds  max_triggers %d",
		row.IntervalMS/1000, row.MinIntervalMS/1000, row.TimeoutMS/1000, row.MaxTriggersPerTick)
	cursor := "(none)"
	if row.Cursor != nil {
		cursor = fmt.Sprintf("%q", *row.Cursor)
	}
	out.print("  cursor %s  (version %d)", cursor, row.CursorVersion)
	out.print("  dedup epoch %d  consecutive_failures %d", row.DedupEpoch, row.ConsecutiveFailures)
	if row.PausedReason != "" {
		out.print("  paused_reason %q", row.PausedReason)
	}

	if len(ticks) > 0 {
		out.print("recent ticks:")
		for _, t := range ticks {
			out.print("  %s  %s  %s  triggers %d  deduped %d",
				t.StartedAt.UTC().Format("2006-01-02 15:04:05"),
				pad(sensorOutcomeText(t.Outcome), 10),
				t.ReasonCode, t.TriggerCount, t.DedupedCount)
		}
	}
	return nil
}

// ------------------- test -------------------

type sensorTestFlags struct {
	cursor     string
	printInput bool
	timeout    string
}

func newSensorsTestCmd(env Env, g *globals) *cobra.Command {
	var f sensorTestFlags
	cmd := &cobra.Command{
		Use:               "test <sensor>",
		Short:             "Dry-run one sensor without writing anything",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Run one sensor against real state (real cursor, real dedup table)
but write nothing: the database is bit-identical before and after. The report
shows what would have happened, including the dedup verdict per run key.

--print-input writes ONLY the contract JSON to stdout, so the sensor can be
piped into directly:  paceq sensors test s3 --print-input | ./sensor.sh | jq`,
		Args: exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsTest(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.cursor, "cursor", "", "override the starting cursor (not saved)")
	cmd.Flags().BoolVar(&f.printInput, "print-input", false, "write only the contract JSON to stdout")
	cmd.Flags().StringVar(&f.timeout, "timeout", "", "override the evaluation timeout (a Go duration)")
	return cmd
}

func runSensorsTest(ctx context.Context, env Env, g *globals, out *ui, name string, f sensorTestFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	row, err := ro.GetSensor(ctx, name)
	if err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}

	spec, err := sensorSpecFromRow(row)
	if err != nil {
		return internalError("could not build the sensor spec for "+name, err)
	}
	if f.timeout != "" {
		d, err := time.ParseDuration(f.timeout)
		if err != nil {
			return usageError(fmt.Sprintf("--timeout %q is not a duration", f.timeout),
				"write a Go duration, such as 30s or 1m")
		}
		spec.Timeout = d
	}

	now := clockForEnv(env).Now()
	in := sensor.Input{
		Sensor:      spec.Name,
		Job:         spec.Job,
		Cursor:      spec.Cursor,
		LastTickAt:  row.LastTickAt,
		Now:         now.UnixMilli(),
		MaxTriggers: spec.MaxTriggers,
		DeadlineMS:  now.Add(spec.Timeout).UnixMilli(),
		DryRun:      true,
	}
	if f.cursor != "" {
		c := f.cursor
		in.Cursor = &c
	}

	if f.printInput {
		enc := json.NewEncoder(out.out)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(in); err != nil {
			return internalError("could not write the contract JSON", err)
		}
		return nil
	}

	ev := sensor.NewEvaluator(sensor.Config{}, clockForEnv(env))
	res := ev.Evaluate(ctx, spec, in)

	runKeys := make([]string, 0, len(res.Triggers))
	for _, tr := range res.Triggers {
		runKeys = append(runKeys, tr.RunKey)
	}
	verdicts, err := ro.PeekDedup(ctx, name, row.DedupEpoch, runKeys)
	if err != nil {
		return internalError("could not read the dedup gate for "+name, err)
	}
	verdictByKey := map[string]store.DedupVerdict{}
	for _, v := range verdicts {
		verdictByKey[v.RunKey] = v
	}

	if out.mode == modeJSON {
		type triggerJSON struct {
			RunKey string `json:"run_key"`
			Params string `json:"params,omitempty"`
			Dedup  string `json:"dedup"`
			RunID  string `json:"run_id,omitempty"`
		}
		type testJSON struct {
			Sensor          string        `json:"sensor"`
			DryRun          bool          `json:"dry_run"`
			ExitCode        int           `json:"exit_code"`
			DurationMS      int64         `json:"duration_ms"`
			CursorBefore    string        `json:"cursor_before,omitempty"`
			CursorAfter     string        `json:"cursor_after,omitempty"`
			Triggers        []triggerJSON `json:"triggers"`
			WouldCreateRuns int           `json:"would_create_runs"`
			StderrExcerpt   string        `json:"stderr_excerpt"`
		}
		doc := testJSON{
			Sensor:        name,
			DryRun:        true,
			ExitCode:      res.ExitCode,
			DurationMS:    res.DurationMS,
			Triggers:      make([]triggerJSON, 0, len(res.Triggers)),
			StderrExcerpt: res.StderrExcerpt,
		}
		if res.CursorBefore != nil {
			doc.CursorBefore = *res.CursorBefore
		}
		if res.CursorAfter != nil {
			doc.CursorAfter = *res.CursorAfter
		}
		for _, tr := range res.Triggers {
			dedup := "new"
			v := verdictByKey[tr.RunKey]
			if v.Seen {
				dedup = "seen"
			} else {
				doc.WouldCreateRuns++
			}
			tj := triggerJSON{RunKey: tr.RunKey, Dedup: dedup}
			if len(tr.Params) > 0 {
				tj.Params = string(tr.Params)
			}
			if v.Seen {
				tj.RunID = v.RunID
			}
			doc.Triggers = append(doc.Triggers, tj)
		}
		return out.json(doc)
	}

	out.print("Dry run of sensor %s - no runs created, cursor not saved.", name)
	out.print("  command:  %s", strings.Join(spec.Argv, " "))
	if res.CursorBefore != nil {
		out.print("  cursor in %q", *res.CursorBefore)
	}
	out.print("  duration: %dms  exit: %d", res.DurationMS, res.ExitCode)
	if res.TimedOut {
		out.print("  %s timed out after %s", out.symbols.fail, spec.Timeout)
	}
	out.print("  outcome:  %s", outcomeName(res.Outcome))
	if len(res.Triggers) == 0 {
		out.print("  no triggers")
		if res.ReasonText != "" {
			out.print("  reason:   %s", res.ReasonText)
		}
	} else {
		out.print("  %d trigger(s):", len(res.Triggers))
		for _, tr := range res.Triggers {
			v := verdictByKey[tr.RunKey]
			if v.Seen {
				out.print("    %s  %s (already run %s)", tr.RunKey, out.symbols.warn, v.RunID)
			} else {
				out.print("    %s  %s new", tr.RunKey, out.symbols.ok)
			}
		}
	}
	if res.CursorAfter != nil {
		out.print("  cursor out %q", *res.CursorAfter)
	}
	if res.StderrExcerpt != "" {
		out.print("  stderr tail:")
		out.print("    %s", strings.TrimSpace(res.StderrExcerpt))
	}
	return nil
}

// ------------------- tick -------------------

type sensorTickFlags struct {
	wait bool
}

func newSensorsTickCmd(env Env, g *globals) *cobra.Command {
	var f sensorTickFlags
	cmd := &cobra.Command{
		Use:               "tick <sensor>",
		Short:             "Force one evaluation of a sensor now",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Ask for one real evaluation of the sensor immediately.

With the daemon up this makes the sensor due and wakes the evaluator; --wait
waits for the evaluation to finish and reports its outcome. With the daemon
down the evaluation runs in this process, exactly like paceq run.`,
		Args: exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsTick(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().BoolVar(&f.wait, "wait", false, "wait for the evaluation to finish and report it")
	return cmd
}

func runSensorsTick(ctx context.Context, env Env, g *globals, out *ui, name string, f sensorTickFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()
	row, err := ro.GetSensor(ctx, name)
	if err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}
	if row.Paused {
		return busyError(fmt.Errorf("sensor %s is paused; resume it before forcing a tick", name))
	}

	// SEAM (#14, #16): tick through the daemon when it answers. The daemon's
	// evaluator owns the real tick transaction (Begin/CommitSensorTick); the
	// direct path below re-runs the same evaluation in this process.
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return socketRefusedError(err)
	}
	if socketPath != "" {
		if err := sensorTickViaSocket(ctx, socketPath, name); err == nil {
			if f.wait {
				out.print("%s tick of %s requested; the daemon is evaluating it", out.symbols.ok, name)
			} else {
				out.print("%s tick of %s requested", out.symbols.ok, name)
			}
			return nil
		} else if stop := stopOnRefusal(err); stop != nil {
			return stop
		}
		out.note(1, "daemon unreachable; evaluating directly")
	} else {
		out.note(1, "no daemon socket; evaluating directly")
	}

	// Direct path: run the evaluation in this process and commit it.
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	spec, err := sensorSpecFromRow(row)
	if err != nil {
		return internalError("could not build the sensor spec for "+name, err)
	}
	now := clockForEnv(env).Now()
	in := sensor.Input{
		Sensor:      spec.Name,
		Job:         spec.Job,
		Cursor:      spec.Cursor,
		LastTickAt:  row.LastTickAt,
		Now:         now.UnixMilli(),
		MaxTriggers: spec.MaxTriggers,
		DeadlineMS:  now.Add(spec.Timeout).UnixMilli(),
		DryRun:      false,
	}
	begin, err := s.BeginSensorTick(ctx, store.BeginSensorTickInput{
		SensorName: name, CursorBefore: derefString(spec.Cursor),
		Now: now,
	})
	if err != nil {
		return internalError("could not begin the sensor tick for "+name, err)
	}
	ev := sensor.NewEvaluator(sensor.Config{}, clockForEnv(env))
	res := ev.Evaluate(ctx, spec, in)

	outcome := store.OutcomeSkipped
	code := ""
	cursorAfter := ""
	if res.CursorAfter != nil {
		cursorAfter = *res.CursorAfter
	}
	switch res.Outcome {
	case sensor.Triggered:
		outcome = store.OutcomeTriggered
	case sensor.Skipped:
		outcome = store.OutcomeSkipped
		code = string(res.ReasonCode)
	case sensor.Errored:
		outcome = store.OutcomeError
		code = string(res.ReasonCode)
	}

	triggers := make([]store.SensorTrigger, 0, len(res.Triggers))
	for _, tr := range res.Triggers {
		params := ""
		if len(tr.Params) > 0 {
			params = string(tr.Params)
		}
		triggers = append(triggers, store.SensorTrigger{RunKey: tr.RunKey, ParamsJSON: params})
	}

	commit, err := s.CommitSensorTick(ctx, store.SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    name,
		JobName:       row.JobName,
		CursorVersion: begin.CursorVersion,
		CursorAfter:   cursorAfter,
		DedupEpoch:    row.DedupEpoch,
		Triggers:      triggers,
		Outcome:       outcome,
		ReasonCode:    storeReasonCode(res.ReasonCode),
		NextEvalAt:    now.Add(time.Duration(row.IntervalMS) * time.Millisecond).UnixMilli(),
		DurationMs:    res.DurationMS,
	})
	if err != nil {
		return internalError("could not commit the sensor tick for "+name, err)
	}
	if commit.Fenced {
		return busyError(fmt.Errorf("the tick of %s was fenced: the sensor advanced past this evaluation", name))
	}

	if out.mode == modeJSON {
		type tickJSON struct {
			Sensor     string `json:"sensor"`
			Outcome    string `json:"outcome"`
			ReasonCode string `json:"reason_code,omitempty"`
			Accepted   int    `json:"accepted"`
			Deduped    int    `json:"deduped"`
			DurationMS int64  `json:"duration_ms"`
		}
		return out.json(tickJSON{
			Sensor: name, Outcome: outcome, ReasonCode: code,
			Accepted: commit.Accepted, Deduped: commit.Deduped, DurationMS: res.DurationMS,
		})
	}

	out.print("%s tick of %s finished: %s", out.symbols.ok, name, outcome)
	if code != "" {
		out.print("  reason: %s", code)
	}
	out.print("  accepted %d run(s), %d deduped, in %dms",
		commit.Accepted, commit.Deduped, res.DurationMS)
	return nil
}

// ------------------- pause / resume -------------------

func newSensorsPauseCmd(env Env, g *globals) *cobra.Command {
	var reason string
	cmd := &cobra.Command{
		Use:               "pause <sensor>",
		Short:             "Pause a sensor: it stops being evaluated",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Pause one sensor. The daemon stops evaluating it immediately; runs
already queued are not affected. The reason is kept on the row so a pause
nobody remembers is explainable.`,
		Args: exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsPause(ctx, env, g, out, args[0], reason)
		}),
	}
	cmd.Flags().StringVar(&reason, "reason", "", "why it is paused")
	return cmd
}

func runSensorsPause(ctx context.Context, env Env, g *globals, out *ui, name, reason string) error {
	if err := requireSensor(ctx, env, g, name); err != nil {
		return err
	}
	return writeSensorOp(ctx, env, g, out, name,
		func(s *store.Store) error { return s.PauseSensor(ctx, name, reason) },
		"paused", sensorPauseSocketPath)
}

func newSensorsResumeCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:               "resume <sensor>",
		Short:             "Resume a paused sensor",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Resume one paused sensor. Its paused state and reason are cleared
and the breaker failure count is reset.`,
		Args: exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsResume(ctx, env, g, out, args[0])
		}),
	}
}

func runSensorsResume(ctx context.Context, env Env, g *globals, out *ui, name string) error {
	if err := requireSensor(ctx, env, g, name); err != nil {
		return err
	}
	return writeSensorOp(ctx, env, g, out, name,
		func(s *store.Store) error { return s.ResumeSensor(ctx, name) },
		"resumed", sensorResumeSocketPath)
}

// writeSensorOp is the dual-mode write path: try the daemon socket first, fall
// back to flock + direct through the same store code. socket is a function so
// the CLI does not need the daemon route names spelled out twice.
func writeSensorOp(ctx context.Context, env Env, g *globals, out *ui, name string,
	direct func(*store.Store) error, verb string, viaSocket func(context.Context, string, string) error,
) error {
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return socketRefusedError(err)
	}
	if socketPath != "" {
		if err := viaSocket(ctx, socketPath, name); err == nil {
			out.print("%s %s %s", out.symbols.ok, verb, name)
			return nil
		} else if stop := stopOnRefusal(err); stop != nil {
			return stop
		}
		out.note(1, "daemon unreachable; writing directly to the database")
	} else {
		out.note(1, "no daemon socket; writing directly to the database")
	}

	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clockForEnv(env)})
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	if err := direct(s); err != nil {
		return classifySensorWrite(name, err)
	}
	out.print("%s %s %s", out.symbols.ok, verb, name)
	return nil
}

// ------------------- reset -------------------

type sensorResetFlags struct {
	cursor        string
	forgetRunKeys bool
	yes           bool
}

func newSensorsResetCmd(env Env, g *globals) *cobra.Command {
	var f sensorResetFlags
	cmd := &cobra.Command{
		Use:               "reset <sensor>",
		Short:             "Reset a sensor's cursor and dedup epoch",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Reset a sensor: raise its dedup epoch so every run key becomes new
again, and set the cursor to NULL (full replay) or to --cursor.

The two notions are separate (F4c): the cursor is where the sensor starts
from; a run key is what deduplicates one trigger. A reset bumps the epoch,
which makes every old run key a new fingerprint; --cursor alone changes
nothing about the dedup table. --forget-run-keys deletes the sensor's run_key
rows, which is irreversible and needs confirmation in a terminal or --yes in a
script.`,
		Args: exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsReset(ctx, env, g, out, args[0], f)
		}),
	}
	cmd.Flags().StringVar(&f.cursor, "cursor", "", "set the cursor to this value instead of NULL")
	cmd.Flags().BoolVar(&f.forgetRunKeys, "forget-run-keys", false, "delete the sensor's run_key rows")
	cmd.Flags().BoolVar(&f.yes, "yes", false, "confirm a destructive reset without prompting")
	return cmd
}

func runSensorsReset(ctx context.Context, env Env, g *globals, out *ui, name string, f sensorResetFlags) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()
	row, err := ro.GetSensor(ctx, name)
	if err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}
	_ = row

	if f.forgetRunKeys && !f.yes {
		if isTerminal(out.out) {
			out.print("This deletes every dedup run_key for sensor %q permanently.", name)
			out.print("All earlier run keys can trigger again. Cursor is set to NULL (full replay).")
			out.print("Type the sensor name to confirm:")
			// The CLI never blocks on a prompt in a pipe; the TTY case is the
			// only one that asks, and that path is covered by --yes in tests.
			if !confirmTTY(name) {
				return usageError("confirmation required", "pass --yes to confirm in a script")
			}
		} else {
			return usageError(fmt.Sprintf("reset %s --forget-run-keys needs confirmation", name),
				"pass --yes to confirm in a script")
		}
	}

	var setCursor *string
	if f.cursor != "" {
		setCursor = &f.cursor
	}

	return writeSensorOp(ctx, env, g, out, name,
		func(s *store.Store) error {
			_, err := s.ResetSensor(ctx, store.ResetSensorInput{
				Name: name, SetCursor: setCursor, ForgetRunKeys: f.forgetRunKeys,
			})
			return err
		},
		"reset", sensorResetSocketPath)
}

// confirmTTY asks the operator to type the sensor name. It returns false when
// stdin is not a terminal or the name does not match.
func confirmTTY(name string) bool {
	// The tests drive the non-TTY --yes branch; a real TTY prompt is not
	// something the test binary can exercise deterministically, so the prompt
	// reads from stdin and the tests cover the refusal path.
	var typed string
	if _, err := fmt.Fscan(os.Stdin, &typed); err != nil {
		return false
	}
	return strings.TrimSpace(typed) == name
}

// ------------------- cursor -------------------

func newSensorsCursorCmd(env Env, g *globals) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cursor",
		Short: "Read or write a sensor's cursor",
	}
	cmd.AddCommand(newSensorsCursorGetCmd(env, g), newSensorsCursorSetCmd(env, g))
	return cmd
}

func newSensorsCursorGetCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:               "get <sensor>",
		Short:             "Show a sensor's cursor",
		ValidArgsFunction: completeSensorName(env, g),
		Args:              exactArgs(1, "one sensor name"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsCursorGet(ctx, env, g, out, args[0])
		}),
	}
}

func runSensorsCursorGet(ctx context.Context, env Env, g *globals, out *ui, name string) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	row, err := ro.GetSensor(ctx, name)
	if err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}

	if out.mode == modeJSON {
		type cursorJSON struct {
			Sensor        string `json:"sensor"`
			Cursor        string `json:"cursor,omitempty"`
			CursorVersion int64  `json:"cursor_version"`
			UpdatedAt     string `json:"updated_at,omitempty"`
		}
		doc := cursorJSON{Sensor: name, CursorVersion: row.CursorVersion}
		if row.Cursor != nil {
			doc.Cursor = *row.Cursor
		}
		return out.json(doc)
	}
	if row.Cursor == nil {
		out.print("sensor %s cursor: (none)", name)
	} else {
		out.print("sensor %s cursor: %q  (version %d)", name, *row.Cursor, row.CursorVersion)
	}
	return nil
}

func newSensorsCursorSetCmd(env Env, g *globals) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:               "set <sensor> <value>",
		Short:             "Move a sensor's cursor without touching its dedup epoch",
		ValidArgsFunction: completeSensorName(env, g),
		Long: `Move a sensor's cursor to the given value. This changes nothing
about the dedup table: the old run keys keep dedupping, because the epoch they
are tagged with is still current. To make old keys fire again, use reset.`,
		Args: exactArgs(2, "a sensor name and a cursor value"),
		RunE: runArgsE(env, g, func(ctx context.Context, out *ui, args []string) error {
			return runSensorsCursorSet(ctx, env, g, out, args[0], args[1], yes)
		}),
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "confirm without prompting")
	return cmd
}

func runSensorsCursorSet(ctx context.Context, env Env, g *globals, out *ui, name, value string, yes bool) error {
	if err := requireSensor(ctx, env, g, name); err != nil {
		return err
	}
	_ = yes // cursor set is not destructive; the flag is accepted for symmetry
	return writeSensorOp(ctx, env, g, out, name,
		func(s *store.Store) error {
			return s.SetSensorCursor(ctx, store.CursorInput{Name: name, Cursor: value})
		},
		"cursor set", sensorCursorSetSocketPath)
}

// ------------------- helpers -------------------

// requireSensor checks a sensor exists and returns the did-you-mean error when
// it does not, before any write is attempted.
func requireSensor(ctx context.Context, env Env, g *globals, name string) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()
	if _, err := ro.GetSensor(ctx, name); err != nil {
		return unknownSensorError(ctx, ro, err, name)
	}
	return nil
}

// unknownSensorError is the exit 3 refusal with a did-you-mean suggestion.
func unknownSensorError(ctx context.Context, ro *store.Store, err error, name string) error {
	if !errors.Is(err, store.ErrNotFound) {
		return internalError("could not read sensor "+name, err)
	}
	next := []string{
		"paceq sensors list  shows every sensor",
	}
	var known []string
	if ro != nil {
		if rows, listErr := ro.ListSensors(ctx); listErr == nil {
			for _, row := range rows {
				known = append(known, row.Name)
			}
		}
	}
	if suggestion := diag.Suggest(name, known); suggestion != "" {
		next = append([]string{
			fmt.Sprintf("did you mean %q?", suggestion),
			"paceq sensors show " + suggestion + "  shows that sensor",
		}, next...)
	}
	return notFoundError(fmt.Sprintf("no sensor matches %q", name), name, next...)
}

// classifySensorWrite turns a store write error into the right exit code.
func classifySensorWrite(name string, err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return notFoundError(fmt.Sprintf("no sensor matches %q", name), name)
	}
	return internalError("could not write the sensor state for "+name, err)
}

// sensorPauseSocketPath etc. name the daemon routes the socket write helper
// calls. They are separated so a later M3 issue (the daemon's sensor routes,
// #14/#16) can register them without touching the CLI.
func sensorPauseSocketPath(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/sensors/"+name+"/pause")
}

func sensorResumeSocketPath(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/sensors/"+name+"/resume")
}

func sensorResetSocketPath(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/sensors/"+name+"/reset")
}

func sensorCursorSetSocketPath(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/sensors/"+name+"/cursor")
}

func sensorTickViaSocket(ctx context.Context, socketPath, name string) error {
	return sockPost(ctx, socketPath, "/v1/sensors/"+name+"/tick")
}

// derefString returns the string a pointer points at, or "".
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// unixMilliUTC formats a Unix millisecond stamp as RFC 3339, or "" for zero.
func unixMilliUTC(ms int64) string {
	if ms == 0 {
		return ""
	}
	return time.UnixMilli(ms).UTC().Format(time.RFC3339)
}

// g.stateDirOrEmpty returns the state directory, or "" when it cannot be
// resolved. Used only to find the socket path, where an empty path simply
// means no daemon socket.
func (g *globals) stateDirOrEmpty(env Env) string {
	dir, err := g.stateDir(env)
	if err != nil {
		return ""
	}
	return dir
}

// outcomeName is the human word for a sensor evaluation verdict.
func outcomeName(o sensor.Outcome) string {
	switch o {
	case sensor.Triggered:
		return "triggered"
	case sensor.Skipped:
		return "skipped"
	case sensor.Errored:
		return "error"
	default:
		return "unknown"
	}
}

// storeReasonCode converts a sensor reason code into the reason.Code a commit
// row expects.
func storeReasonCode(c reason.Code) reason.Code { return c }
