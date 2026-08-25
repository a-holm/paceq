package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// The explain read path (M5-01). Everything here is a single query against
// the read pool: no transaction, no second statement that has to agree with
// the first, and no OFFSET. Pagination is by key (`id < cursor`), because an
// offset re-reads everything it skips while holding a read snapshot open,
// which is what starves WAL checkpointing (07 section 6.4).
//
// These methods exist so internal/explain never writes SQL: SQL lives in
// internal/store alone, and explain is a presentation layer over these rows.

// ExplainPageSize is how many rows one explain query fetches per page. The
// caller pages by passing the last id it saw as before.
const ExplainPageSize = 50

// ExplainSource names one tick producer: a schedule (whose ticks carry
// "job/name" as their source name) or a sensor (whose ticks carry the bare
// sensor name). The pairing matters: the two producers spell their source
// names differently in the ticks table, and a filter that ignores the kind
// can match the wrong rows.
type ExplainSource struct {
	Kind string // "schedule" or "sensor"
	Name string // source_name as stored on the tick row
}

// ExplainTick is one recorded evaluation as explain reads it back: what was
// due, when it ran, what it became, and why it produced nothing.
type ExplainTick struct {
	ID            string
	SourceKind    string
	SourceName    string
	ScheduledFor  time.Time // zero when the tick carries no fire-time
	StartedAt     time.Time
	LastStartedAt time.Time
	FinishedAt    time.Time // zero while still open or not yet closed
	DurationMS    int64
	HasDuration   bool
	RepeatCount   int
	Outcome       string
	ReasonCode    string
	ReasonText    string
	ReasonData    string
	TriggerCount  int
	DedupedCount  int
	CursorBefore  string
	CursorAfter   string
}

// ExplainTrigger is one trigger decision as explain reads it back.
type ExplainTrigger struct {
	ID         string
	TickID     string
	JobName    string
	RunKey     string
	CreatedAt  time.Time
	Outcome    string // accepted | deduped | rejected
	ReasonCode string
	ReasonText string
	RunID      string // the run an accepted or deduped trigger led to
}

// ExplainJobFacts are the job-level facts a report's summary needs.
type ExplainJobFacts struct {
	Paused        bool
	MaxConcurrent int
	Found         bool
}

// explainTickColumns is the projection every tick query shares.
const explainTickColumns = `t.id, t.source_kind, t.source_name, t.scheduled_for,
t.started_at, t.last_started_at, t.finished_at, t.duration_ms, t.repeat_count,
t.outcome, COALESCE(t.reason_code, ''), COALESCE(t.reason_text, ''),
COALESCE(t.reason_data, ''), t.trigger_count, t.deduped_count,
COALESCE(t.cursor_before, ''), COALESCE(t.cursor_after, '')`

func scanExplainTick(scan func(dest ...any) error) (ExplainTick, error) {
	var (
		t            ExplainTick
		scheduledFor sqlNullInt64
		startedAt    int64
		lastStarted  int64
		finishedAt   sqlNullInt64
		duration     sqlNullInt64
	)
	if err := scan(&t.ID, &t.SourceKind, &t.SourceName, &scheduledFor,
		&startedAt, &lastStarted, &finishedAt, &duration, &t.RepeatCount,
		&t.Outcome, &t.ReasonCode, &t.ReasonText, &t.ReasonData,
		&t.TriggerCount, &t.DedupedCount, &t.CursorBefore, &t.CursorAfter); err != nil {
		return ExplainTick{}, err
	}
	if scheduledFor.valid {
		t.ScheduledFor = time.UnixMilli(scheduledFor.value).UTC()
	}
	t.StartedAt = time.UnixMilli(startedAt).UTC()
	t.LastStartedAt = time.UnixMilli(lastStarted).UTC()
	if finishedAt.valid {
		t.FinishedAt = time.UnixMilli(finishedAt.value).UTC()
	}
	if duration.valid {
		t.DurationMS = duration.value
		t.HasDuration = true
	}
	return t, nil
}

// sqlNullInt64 is a local scan target so this file does not have to spread
// database/sql value types through its signatures.
type sqlNullInt64 struct {
	value int64
	valid bool
}

func (n *sqlNullInt64) Scan(src any) error {
	if src == nil {
		n.value, n.valid = 0, false
		return nil
	}
	v, ok := src.(int64)
	if !ok {
		return fmt.Errorf("explain: expected an integer column, got %T", src)
	}
	n.value, n.valid = v, true
	return nil
}

// explainTicksSQL builds the core tick query for n bound (source_kind,
// source_name) pairs. It is a function rather than an inline literal so the
// query-plan test runs EXPLAIN QUERY PLAN on exactly the text production
// executes.
func explainTicksSQL(n int) string {
	pairs := make([]string, n)
	for i := range pairs {
		pairs[i] = "(?, ?)"
	}
	// #nosec G202 - the concatenated segment adds only "(?, ?)" placeholder marks,
	// never user data; every value is parameter-bound via args.
	return `SELECT ` + explainTickColumns + `
FROM ticks t
WHERE (t.source_kind, t.source_name) IN (` + strings.Join(pairs, ", ") + `)
  AND t.started_at >= ?
  AND (? = '' OR t.id < ?)
ORDER BY t.id DESC
LIMIT ?`
}

// explainRunEventsSQL is one run's event chain, oldest event id first.
const explainRunEventsSQL = `SELECT e.run_id, COALESCE(e.step_name, ''),
e.at, e.kind, COALESCE(e.from_state, ''), COALESCE(e.to_state, ''),
COALESCE(e.reason_code, ''), e.actor, COALESCE(e.detail_json, '{}')
FROM run_events e WHERE e.run_id = ? ORDER BY e.id`

// explainTriggerColumns is the projection every trigger query shares.
const explainTriggerColumns = `g.id, g.tick_id, g.job_name, COALESCE(g.run_key, ''),
g.created_at, g.outcome, COALESCE(g.reason_code, ''), COALESCE(g.reason_text, ''),
COALESCE(g.run_id, '')`

// explainTriggersSQL builds the trigger lookup for n bound tick ids.
func explainTriggersSQL(n int) string {
	marks := strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
	// #nosec G202 - the concatenated segment adds only "?" placeholder marks,
	// never user data; every value is parameter-bound via args.
	return `SELECT ` + explainTriggerColumns + `
FROM triggers g WHERE g.tick_id IN (` + marks + `) ORDER BY g.id`
}

// explainRunsPrefixSQL is the prefix range scan that resolves a run reference.
const explainRunsPrefixSQL = `SELECT r.id, r.job_name, r.origin,
r.state, COALESCE(r.reason_code, ''), r.created_at, r.started_at, r.finished_at,
COALESCE(r.defer_reason, ''), COALESCE(r.reason_data, '')
FROM runs r WHERE r.id >= ? AND r.id < ? ORDER BY r.id LIMIT ?`

// explainOutagesSinceSQL reads the downtime overlapping the window.
const explainOutagesSinceSQL = `SELECT o.id, o.from_ts, o.to_ts, o.detected_at,
o.kind, COALESCE(o.prev_session, ''), o.missed_ticks
FROM outages o WHERE o.to_ts >= ? ORDER BY o.from_ts DESC`

// explainNewestRunSQL reads the newest run of a job, optionally one state.
func explainNewestRunSQL(withState bool) string {
	query := `SELECT r.id, r.job_name, r.origin, r.state, COALESCE(r.reason_code, ''),
r.created_at, r.started_at, r.finished_at, COALESCE(r.defer_reason, ''), ''
FROM runs r WHERE r.job_name = ?`
	if withState {
		query += ` AND r.state = ?`
	}
	return query + ` ORDER BY r.id DESC LIMIT 1`
}

// ExplainTicks reads one page of recorded evaluations for the named sources,
// newest id first, strictly older than before (empty before starts at the
// newest). The sources arrive as resolved kind/name pairs rather than as a
// subselect over schedules/sensors: the two kinds spell their tick source
// names differently, and binding them here keeps the plan a search on
// idx_ticks_source whatever mix the caller asks for.
func (s *Store) ExplainTicks(ctx context.Context, sources []ExplainSource, since time.Time, before string, limit int) ([]ExplainTick, error) {
	if len(sources) == 0 {
		return nil, nil
	}
	if limit <= 0 {
		limit = ExplainPageSize
	}
	// Row values in an IN list let SQLite drive idx_ticks_source
	// (source_kind, source_name, started_at DESC) once per pair. The marks
	// are concatenated; every value is parameter-bound via args.
	args := make([]any, 0, len(sources)*2+3)
	for _, src := range sources {
		args = append(args, src.Kind, src.Name)
	}
	args = append(args, since.UnixMilli(), before, before, limit)

	rows, err := s.r.QueryContext(ctx, explainTicksSQL(len(sources)), args...)
	if err != nil {
		return nil, fmt.Errorf("read ticks for explain: %w", err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ExplainTick, 0, limit)
	for rows.Next() {
		t, err := scanExplainTick(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan a tick for explain: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read ticks for explain: %w", err)
	}
	return out, nil
}

// explainTickByIDSQL reads one tick by its whole id.
const explainTickByIDSQL = `SELECT ` + explainTickColumns + `
FROM ticks t WHERE t.id = ?`

// ExplainTickByID reads one recorded evaluation by its whole id: the provenance
// line of a run report (which tick fired the trigger that queued this run).
func (s *Store) ExplainTickByID(ctx context.Context, tickID string) (ExplainTick, bool, error) {
	rows, err := s.r.QueryContext(ctx, explainTickByIDSQL, tickID)
	if err != nil {
		return ExplainTick{}, false, fmt.Errorf("read tick %s: %w", tickID, err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		tick, err := scanExplainTick(rows.Scan)
		if err != nil {
			return ExplainTick{}, false, fmt.Errorf("scan tick %s: %w", tickID, err)
		}
		return tick, true, nil
	}
	if err := rows.Err(); err != nil {
		return ExplainTick{}, false, fmt.Errorf("read tick %s: %w", tickID, err)
	}
	return ExplainTick{}, false, nil
}

// ExplainRunsByPrefix resolves a run id prefix the way git resolves an object:
// ids are ULIDs, so the prefix is a range scan on the primary key. Every match
// is returned up to limit, so the caller can name the whole candidate list
// when the prefix is ambiguous instead of the first two.
func (s *Store) ExplainRunsByPrefix(ctx context.Context, prefix string, limit int) ([]RunSummary, error) {
	span, err := id.PrefixRange(prefix)
	if err != nil {
		return nil, fmt.Errorf("resolve run %q: %w", prefix, err)
	}
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.r.QueryContext(ctx, explainRunsPrefixSQL,
		span.Lower, span.Upper, limit)
	if err != nil {
		return nil, fmt.Errorf("resolve run %q: %w", prefix, err)
	}
	defer func() { _ = rows.Close() }()

	var out []RunSummary
	for rows.Next() {
		var (
			r                 RunSummary
			created           int64
			started, finished sqlNullInt64
		)
		if err := rows.Scan(&r.ID, &r.JobName, &r.Origin, &r.State, &r.ReasonCode,
			&created, &started, &finished, &r.DeferReason, &r.ReasonData); err != nil {
			return nil, fmt.Errorf("scan a run for explain: %w", err)
		}
		r.CreatedAt = time.UnixMilli(created).UTC()
		if started.valid {
			r.StartedAt = time.UnixMilli(started.value).UTC()
		}
		if finished.valid {
			r.FinishedAt = time.UnixMilli(finished.value).UTC()
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve run %q: %w", prefix, err)
	}
	return out, nil
}

// ExplainRunEvents reads one run's event chain, oldest first. This is the
// read side of AppendRunEvent, and the query explain (M5-01) exists for.
func (s *Store) ExplainRunEvents(ctx context.Context, runID string) ([]RunEvent, error) {
	rows, err := s.r.QueryContext(ctx, explainRunEventsSQL, runID)
	if err != nil {
		return nil, fmt.Errorf("read the events of run %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]RunEvent, 0)
	for rows.Next() {
		var (
			e  RunEvent
			at int64
		)
		if err := rows.Scan(&e.RunID, &e.StepName, &at, &e.Kind, &e.FromState,
			&e.ToState, &e.ReasonCode, &e.Actor, &e.DetailJSON); err != nil {
			return nil, fmt.Errorf("scan an event of run %s: %w", runID, err)
		}
		e.At = time.UnixMilli(at).UTC()
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the events of run %s: %w", runID, err)
	}
	return out, nil
}

// ExplainTriggersByTicks reads the triggers a set of ticks produced, keyed by
// their tick id so the caller can hang them under the right timeline entry.
func (s *Store) ExplainTriggersByTicks(ctx context.Context, tickIDs []string) (map[string][]ExplainTrigger, error) {
	out := make(map[string][]ExplainTrigger)
	for _, chunk := range chunksOf(tickIDs, ExplainPageSize) {
		args := make([]any, len(chunk))
		for i, tickID := range chunk {
			args[i] = tickID
		}
		rows, err := s.r.QueryContext(ctx, explainTriggersSQL(len(chunk)), args...)
		if err != nil {
			return nil, fmt.Errorf("read the triggers of %d ticks: %w", len(chunk), err)
		}
		if err := collectTriggers(rows, out); err != nil {
			return nil, fmt.Errorf("read the triggers of %d ticks: %w", len(chunk), err)
		}
	}
	return out, nil
}

// ExplainTriggerByID reads one trigger by its whole id.
func (s *Store) ExplainTriggerByID(ctx context.Context, triggerID string) (ExplainTrigger, bool, error) {
	// The whole-id lookup reuses the tick-keyed projection through a one-row
	// read; the scan helper groups by tick id either way.
	rows, err := s.r.QueryContext(ctx, `SELECT `+explainTriggerColumns+`
FROM triggers g WHERE g.id = ?`, triggerID)
	if err != nil {
		return ExplainTrigger{}, false, fmt.Errorf("read trigger %s: %w", triggerID, err)
	}
	defer func() { _ = rows.Close() }()

	grouped := make(map[string][]ExplainTrigger)
	if err := collectTriggers(rows, grouped); err != nil {
		return ExplainTrigger{}, false, err
	}
	for _, list := range grouped {
		for _, t := range list {
			if t.ID == triggerID {
				return t, true, nil
			}
		}
	}
	return ExplainTrigger{}, false, nil
}

func collectTriggers(rows *sql.Rows, into map[string][]ExplainTrigger) error {
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			t         ExplainTrigger
			createdAt int64
		)
		if err := rows.Scan(&t.ID, &t.TickID, &t.JobName, &t.RunKey,
			&createdAt, &t.Outcome, &t.ReasonCode, &t.ReasonText, &t.RunID); err != nil {
			return fmt.Errorf("scan a trigger for explain: %w", err)
		}
		t.CreatedAt = time.UnixMilli(createdAt).UTC()
		into[t.TickID] = append(into[t.TickID], t)
	}
	return rows.Err()
}

// ExplainRun is one run as a timeline child needs it: identity, outcome,
// timing, and the trigger that caused it.
type ExplainRun struct {
	ID          string
	JobName     string
	Origin      string
	State       string
	ReasonCode  string
	ReasonText  string
	TriggerID   string
	CreatedAt   time.Time
	StartedAt   time.Time
	FinishedAt  time.Time
	DeferReason string
}

// explainRunsByJobSQL walks one job's runs newest first. The trigger id rides
// along so the report builder can join triggers to their runs in memory
// instead of asking the database for a direction it has no index for.
func explainRunsByJobSQL() string {
	return `SELECT r.id, r.job_name, r.origin, r.state,
COALESCE(r.reason_code, ''), COALESCE(r.reason_text, ''),
COALESCE(r.trigger_id, ''), r.created_at, r.started_at, r.finished_at,
COALESCE(r.defer_reason, '')
FROM runs r WHERE r.job_name = ? AND (? = '' OR r.id < ?)
ORDER BY r.id DESC LIMIT ?`
}

// ExplainRunsByJob reads one page of a job's runs, newest first, strictly
// older than before. The plan searches idx_runs_history; there is no OFFSET.
func (s *Store) ExplainRunsByJob(ctx context.Context, jobName, before string, limit int) ([]ExplainRun, error) {
	if limit <= 0 {
		limit = ExplainPageSize
	}
	rows, err := s.r.QueryContext(ctx, explainRunsByJobSQL(), jobName, before, before, limit)
	if err != nil {
		return nil, fmt.Errorf("read the runs of job %s: %w", jobName, err)
	}
	defer func() { _ = rows.Close() }()

	out := make([]ExplainRun, 0, limit)
	for rows.Next() {
		var (
			r                 ExplainRun
			created           int64
			started, finished sqlNullInt64
		)
		if err := rows.Scan(&r.ID, &r.JobName, &r.Origin, &r.State, &r.ReasonCode,
			&r.ReasonText, &r.TriggerID, &created, &started, &finished,
			&r.DeferReason); err != nil {
			return nil, fmt.Errorf("scan a run of job %s: %w", jobName, err)
		}
		r.CreatedAt = time.UnixMilli(created).UTC()
		if started.valid {
			r.StartedAt = time.UnixMilli(started.value).UTC()
		}
		if finished.valid {
			r.FinishedAt = time.UnixMilli(finished.value).UTC()
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the runs of job %s: %w", jobName, err)
	}
	return out, nil
}

// chunksOf splits ids into batches of at most size.
func chunksOf(ids []string, size int) [][]string {
	if len(ids) == 0 {
		return nil
	}
	var out [][]string
	for start := 0; start < len(ids); start += size {
		end := start + size
		if end > len(ids) {
			end = len(ids)
		}
		out = append(out, ids[start:end])
	}
	return out
}

// ExplainOutagesSince reads the downtime records that overlap the window,
// newest first. Outages is already the whole-table listing for tests; this is
// the windowed form a timeline renders.
func (s *Store) ExplainOutagesSince(ctx context.Context, since time.Time) ([]Outage, error) {
	rows, err := s.r.QueryContext(ctx, explainOutagesSinceSQL, since.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("read outages since %s: %w", since.Format(time.RFC3339), err)
	}
	defer func() { _ = rows.Close() }()

	var out []Outage
	for rows.Next() {
		var (
			o        Outage
			from, to int64
			detected int64
			missed   int
		)
		if err := rows.Scan(&o.ID, &from, &to, &detected, &o.Kind, &o.PrevSession, &missed); err != nil {
			return nil, fmt.Errorf("scan an outage: %w", err)
		}
		o.From = time.UnixMilli(from).UTC()
		o.To = time.UnixMilli(to).UTC()
		o.DetectedAt = time.UnixMilli(detected).UTC()
		o.MissedTicks = missed
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read outages since %s: %w", since.Format(time.RFC3339), err)
	}
	return out, nil
}

// ExplainJobFacts reads whether a job exists, whether it is paused, and its
// concurrency ceiling: the summary line of a job report.
func (s *Store) ExplainJobFacts(ctx context.Context, jobName string) (ExplainJobFacts, error) {
	var facts ExplainJobFacts
	err := s.r.QueryRowContext(ctx,
		`SELECT paused, max_concurrent FROM jobs WHERE name = ?`, jobName).
		Scan(&facts.Paused, &facts.MaxConcurrent)
	if errors.Is(err, sql.ErrNoRows) {
		return ExplainJobFacts{}, nil
	}
	if err != nil {
		return ExplainJobFacts{}, fmt.Errorf("read the facts of job %s: %w", jobName, err)
	}
	facts.Found = true
	return facts, nil
}

// ExplainNewestRun reads the newest run of a job, optionally restricted to one
// state (pass "" for any): the last outcome and last success of a summary.
func (s *Store) ExplainNewestRun(ctx context.Context, jobName, state string) (RunSummary, bool, error) {
	row := s.r.QueryRowContext(ctx, explainNewestRunSQL(state != ""), jobName, state)
	var (
		r                 RunSummary
		created           int64
		started, finished sqlNullInt64
	)
	err := row.Scan(&r.ID, &r.JobName, &r.Origin, &r.State, &r.ReasonCode,
		&created, &started, &finished, &r.DeferReason, &r.ReasonData)
	if errors.Is(err, sql.ErrNoRows) {
		return RunSummary{}, false, nil
	}
	if err != nil {
		return RunSummary{}, false, fmt.Errorf("read the newest run of job %s: %w", jobName, err)
	}
	r.CreatedAt = time.UnixMilli(created).UTC()
	if started.valid {
		r.StartedAt = time.UnixMilli(started.value).UTC()
	}
	if finished.valid {
		r.FinishedAt = time.UnixMilli(finished.value).UTC()
	}
	return r, true, nil
}

// ExplainActiveRuns counts a job's queued and running runs: what the
// concurrency line of a summary reports. The partial index
// ux_runs_conc_key's sibling idx_runs_active covers exactly this predicate.
func (s *Store) ExplainActiveRuns(ctx context.Context, jobName string) (int, error) {
	var n int
	err := s.r.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs
WHERE job_name = ? AND state IN ('queued', 'running')`, jobName).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("count the active runs of job %s: %w", jobName, err)
	}
	return n, nil
}

// ExplainNextScheduleTick reads when a job's schedules next come due (the
// earliest unpaused next_tick_at), or nothing when none is scheduled.
func (s *Store) ExplainNextScheduleTick(ctx context.Context, jobName string) (time.Time, bool, error) {
	var next sql.NullInt64
	err := s.r.QueryRowContext(ctx, `SELECT MIN(next_tick_at) FROM schedules
WHERE job_name = ? AND paused = 0`, jobName).Scan(&next)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && !next.Valid) {
		return time.Time{}, false, nil
	}
	if err != nil {
		return time.Time{}, false, fmt.Errorf("read the next tick of job %s: %w", jobName, err)
	}
	return time.UnixMilli(next.Int64).UTC(), true, nil
}
