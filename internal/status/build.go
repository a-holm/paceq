package status

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// Options are the knobs one build turns on: the clock tests freeze, and what
// the daemon looked like when the command was asked.
type Options struct {
	Clock    clock.Clock
	DaemonUp bool
}

// Build assembles the whole-project overview: one statement per fact, six
// statements in total, however many jobs the project holds. Every store call
// runs alone under its own deadline; nothing here opens a transaction or
// holds a read snapshot, because a held snapshot is what stops WAL
// checkpointing dead (07 section 6.4).
func Build(ctx context.Context, st *store.Store, opts Options) (*Report, error) {
	clk := opts.Clock
	if clk == nil {
		clk = clock.System()
	}
	now := clk.Now().UTC()

	rows, err := st.StatusJobs(ctx, "")
	if err != nil {
		return nil, err
	}
	nextTicks, err := st.StatusNextTicks(ctx)
	if err != nil {
		return nil, err
	}
	sensorIntervals, err := st.StatusSensorIntervals(ctx)
	if err != nil {
		return nil, err
	}
	stuck, err := st.StatusStuckCounts(ctx, now)
	if err != nil {
		return nil, err
	}
	sensorErrors, err := st.StatusSensorErrorCounts(ctx)
	if err != nil {
		return nil, err
	}

	rep := &Report{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   rfc3339(now),
		Jobs:          make([]Job, 0, len(rows)),
	}
	rep.Daemon = buildDaemon(ctx, st, opts)
	rep.Maintenance = buildMaintenance(ctx, st)

	for _, row := range rows {
		job := newJob(row, nextTicks[row.JobName], sensorIntervals[row.JobName], stuck[row.JobName], now)
		rep.Jobs = append(rep.Jobs, job)
	}
	sortJobs(rep.Jobs)

	rep.Summary = summarize(rep.Jobs, len(rows), now)
	rep.Summary.Queued, rep.Summary.Running, err = st.StatusStateCounts(ctx)
	if err != nil {
		return nil, err
	}
	rep.Summary.SensorsInError = total(sensorErrors)
	return rep, nil
}

// buildMaintenance reads the maintenance facts the janitor records in meta.
// A read failure degrades to an empty block rather than failing the whole
// report: the rest of the overview stays true without these lines.
func buildMaintenance(ctx context.Context, st *store.Store) Maint {
	get := func(key string) string {
		v, _, err := st.MetaValue(ctx, key)
		if err != nil {
			return ""
		}
		return v
	}
	m := Maint{
		LastAt: get(store.MetaKeyGCCycleLastAt),
		Status: get(store.MetaKeyGCCycleLastStatus),
		Error:  get(store.MetaKeyGCCycleLastError),
	}
	info, err := st.BackupStatus(ctx)
	if err == nil && info.HasBackup {
		m.LastBackup = info.LastAt.UTC().Format(time.RFC3339)
		m.BackupVerified = info.Verified()
	}
	return m
}

// buildDaemon fills the daemon block: up is the caller's answer to whether a
// daemon holds this state; since and version come from the daemon's own open
// session row, so they are absent exactly when there is nothing alive to
// describe.
func buildDaemon(ctx context.Context, st *store.Store, opts Options) Daemon {
	d := Daemon{Up: opts.DaemonUp}
	if !opts.DaemonUp {
		return d
	}
	version, since, found, err := st.StatusOpenSession(ctx)
	if err != nil || !found {
		return d
	}
	d.Version = version
	d.Since = rfc3339(since)
	return d
}

// newJob classifies one store row into the overview vocabulary.
func newJob(row store.StatusJobRow, nextTick int64, sensorInterval int64, stuckCount int, now time.Time) Job {
	job := Job{
		Name:             row.JobName,
		SensorIntervalMS: sensorInterval,
	}
	if nextTick > 0 {
		job.NextRunAt = rfc3339(time.UnixMilli(nextTick))
	}
	switch {
	case row.Paused:
		job.State = StatePaused
	case stuckCount > 0:
		job.State = StateStuck
	default:
		job.State = classifyOutcome(row, now)
	}
	if row.RunID != "" {
		job.LastRun = newLastRun(row)
	}
	job.Hint = HintFor(job.State, job.Name)
	return job
}

// classifyOutcome decides between ok, failed and idle from the newest
// finished run alone. A failure stays failed until something confirms
// recovery - a successful run after it, which reading only the newest
// finished run means is exactly "the newest finished run failed". Age does
// not soften the verdict: a job broken for a week with nothing green after
// it is precisely what an unattended monitor exists to keep shouting about,
// and retention is what eventually lets silence take over.
func classifyOutcome(row store.StatusJobRow, now time.Time) string {
	if row.RunID == "" {
		return StateIdle
	}
	if row.RunState == string(runFailed) {
		return StateFailed
	}
	return StateOK
}

// runFailed names the terminal state without importing internal/model for one
// word; the spelling is pinned by the schema CHECK.
const runFailed = "failed"

// sortJobs puts deviations first, then everything else, each alphabetically.
// The order is part of the promise: problems are at the top of one screen or
// they are missed.
func sortJobs(jobs []Job) {
	sort.SliceStable(jobs, func(i, j int) bool {
		a, b := deviationRank(jobs[i].State), deviationRank(jobs[j].State)
		if a != b {
			return a < b
		}
		return jobs[i].Name < jobs[j].Name
	})
}

// deviationRank is 0 for the states exit 5 reports, 1 for everything else.
func deviationRank(state string) int {
	switch state {
	case StateFailed, StateStuck, StateSLABreached:
		return 0
	}
	return 1
}

// IsDeviation reports whether a job state moves the command to exit 5. A
// paused job never does: an operator's deliberate pause is not a deviation,
// and a MOTD line that goes red forever because of one teaches people to
// ignore red.
func IsDeviation(state string) bool {
	return deviationRank(state) == 0
}

// HintFor is the runnable command behind a deviation - always a command,
// never prose (09 R12). An empty result means there is nothing to explain.
func HintFor(state, name string) string {
	if !IsDeviation(state) {
		return ""
	}
	return "paceq explain job " + name
}

func newLastRun(row store.StatusJobRow) *LastRun {
	out := &LastRun{
		ID:         row.RunID,
		StartedAt:  rfc3339(row.StartedAt),
		FinishedAt: rfc3339(row.FinishedAt),
		Outcome:    row.RunState,
		DurationMS: row.DurationMS,
		ReasonCode: row.ReasonCode,
	}
	if out.DurationMS <= 0 && !row.StartedAt.IsZero() && !row.FinishedAt.IsZero() {
		out.DurationMS = row.FinishedAt.Sub(row.StartedAt).Milliseconds()
	}
	return out
}

func summarize(jobs []Job, total int, now time.Time) Summary {
	s := Summary{Jobs: total}
	dayAgo := now.Add(-ConfirmWindow)
	for _, j := range jobs {
		if IsDeviation(j.State) {
			s.Deviations++
		}
		switch j.State {
		case StateSLABreached:
			s.SLABreached++
		case StateFailed:
			// failed_24h scopes to the day the monitoring contract
			// quotes; the job itself stays a deviation until a success
			// confirms recovery, however old the failure is.
			if j.LastRun != nil {
				when := parseStamp(j.LastRun.FinishedAt)
				if when.IsZero() || !when.Before(dayAgo) {
					s.Failed24h++
				}
			}
		}
	}
	return s
}

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// SubjectRef mirrors a resolved reference: which kind, and the full names it
// resolved to. The CLI converts its resolver's answer; this package stays
// free of any dependency on how references are parsed.
type SubjectRef struct {
	Kind     string // job | schedule | sensor | run
	Job      string
	Schedule string
	Sensor   string
	RunID    string
}

// BuildSubject assembles the status block for one reference: a handful of
// single-row reads, all keyed by name or id. Every miss should have been
// refused by the resolver before this runs; a subject that vanishes anyway
// comes back as an error rather than an empty block.
func BuildSubject(ctx context.Context, st *store.Store, ref SubjectRef, opts Options) (*RefReport, error) {
	clk := opts.Clock
	if clk == nil {
		clk = clock.System()
	}
	now := clk.Now().UTC()

	rep := &RefReport{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   rfc3339(now),
		Subject:       Subject(ref),
	}
	rep.Daemon = buildDaemon(ctx, st, opts)

	switch ref.Kind {
	case "job":
		return buildJobSubject(ctx, st, rep, ref.Job, now)
	case "schedule":
		return buildScheduleSubject(ctx, st, rep, ref.Job, ref.Schedule)
	case "sensor":
		return buildSensorSubject(ctx, st, rep, ref.Sensor)
	case "run":
		return buildRunSubject(ctx, st, rep, ref.RunID)
	default:
		return nil, fmt.Errorf("status: unknown subject kind %q", ref.Kind)
	}
}

// applyShadowFlags reads the persisted instance marker (meta) into the
// report. Every subject kind carries it: while a shadow serve runs, nothing
// in this state directory executes, whatever subject you asked about.
func applyShadowFlags(ctx context.Context, st *store.Store, rep *RefReport) {
	info, err := st.ShadowRuntime(ctx)
	if err == nil && info.Running {
		rep.InstanceShadow = true
	}
}

func buildJobSubject(ctx context.Context, st *store.Store, rep *RefReport, name string, now time.Time) (*RefReport, error) {
	view, err := st.Job(ctx, name)
	if err != nil {
		return nil, err
	}
	rows, err := st.StatusJobs(ctx, name)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 {
		return nil, fmt.Errorf("status: job %s read back %d rows, want 1", name, len(rows))
	}
	row := rows[0]

	nextTicks, err := st.StatusNextTicks(ctx)
	if err != nil {
		return nil, err
	}
	stuck, err := st.StatusStuckCounts(ctx, now)
	if err != nil {
		return nil, err
	}

	job := newJob(row, nextTicks[name], 0, stuck[name], now)
	rep.Paused = view.Paused
	rep.State = job.State
	rep.LastRun = job.LastRun
	rep.NextRunAt = job.NextRunAt
	rep.Hint = job.Hint
	if view.Paused {
		rep.State = StatePaused
		rep.Hint = ""
	}
	return rep, nil
}

func buildScheduleSubject(ctx context.Context, st *store.Store, rep *RefReport, jobName, name string) (*RefReport, error) {
	row, err := st.GetSchedule(ctx, jobName, name)
	if err != nil {
		return nil, err
	}
	applyShadowFlags(ctx, st, rep)
	rep.Schedule = &ScheduleFacts{
		Name:       row.Name,
		Kind:       row.Kind,
		Expr:       row.Expr,
		Timezone:   row.Timezone,
		NextTickAt: rfc3339(row.NextTickAt),
		Shadow:     row.Shadow,
	}
	if row.Shadow {
		rep.ScheduleShadow = true
	}
	if row.Paused {
		rep.State = StatePaused
		rep.Paused = true
	} else {
		rep.State = StateOK
	}
	return rep, nil
}

func buildSensorSubject(ctx context.Context, st *store.Store, rep *RefReport, name string) (*RefReport, error) {
	row, err := st.GetSensor(ctx, name)
	if err != nil {
		return nil, err
	}
	facts := &SensorFacts{
		IntervalMS:          row.IntervalMS,
		ConsecutiveFailures: row.ConsecutiveFailures,
		BreakerOpen:         row.ConsecutiveFailures >= sensorBreakerThreshold,
		LastOutcome:         row.LastOutcome,
		NextEvalAt:          rfc3339(time.UnixMilli(row.NextEvalAt)),
		PausedReason:        row.PausedReason,
	}
	rep.Sensor = facts
	switch {
	case row.Paused:
		rep.State = StatePaused
		rep.Paused = true
	case facts.ConsecutiveFailures > 0:
		rep.State = StateFailed
		rep.Hint = "paceq explain sensor " + name
	default:
		rep.State = StateOK
	}
	return rep, nil
}

// sensorBreakerThreshold is where the sensor runtime opens the breaker; the
// number lives with the evaluator, so status only borrows the observable
// consequence (failures at or past it mean the breaker tripped). Kept loose
// on purpose: BreakerOpen is a display hint over breaker_opened_at when the
// column carries a stamp, not a second opinion about the mechanism.
const sensorBreakerThreshold = 5

func buildRunSubject(ctx context.Context, st *store.Store, rep *RefReport, runID string) (*RefReport, error) {
	matches, err := st.ExplainRunsByPrefix(ctx, runID, 2)
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		return nil, store.ErrNotFound
	}
	run := matches[0]
	rep.Subject.RunID = run.ID
	duration := run.FinishedAt.Sub(run.StartedAt)
	if run.StartedAt.IsZero() || run.FinishedAt.IsZero() {
		duration = 0
	}
	rep.Run = &RunFacts{
		ID:         run.ID,
		JobName:    run.JobName,
		Origin:     run.Origin,
		State:      run.State,
		ReasonCode: run.ReasonCode,
		StartedAt:  rfc3339(run.StartedAt),
		FinishedAt: rfc3339(run.FinishedAt),
		DurationMS: duration.Milliseconds(),
	}
	switch run.State {
	case string(runFailed):
		rep.State = StateFailed
		rep.Hint = "paceq explain run " + run.ID
	case "succeeded":
		rep.State = StateOK
	default:
		// queued / running / cancelled: carry the run's own word. None of
		// them is a deviation, and renaming them here would only blur what
		// the runs table already says.
		rep.State = run.State
	}
	return rep, nil
}

// rfc3339 renders a stamp for the JSON documents. A missing stamp stays
// missing.
func rfc3339(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}
