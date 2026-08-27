package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// parseUnixMS parses a meta timestamp stored as a decimal string.
func parseUnixMS(v string) (time.Time, error) {
	ms, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}, errors.New("not a unix ms stamp")
	}
	return time.UnixMilli(ms).UTC(), nil
}

// Shadow mode (#32) replaces exactly one step of tick materialisation: where
// a real fire-time creates a trigger, a run key, a run and its steps, a
// shadow fire-time records that the decision chain reached "triggered" and
// stops. Nothing is executed - not even an echo. This file owns that branch
// and every read seam the shadow report and status surface stand on.
//
// Admission still runs, because it is part of what would have happened: a
// schedule whose job's concurrency ceiling is held at fire-time would have
// stood down, and the report's headline finding is exactly those
// TICK_SKIPPED_OVERLAP rows cron has no equivalent of. Two occupancy sources
// count inside the same transaction view:
//
//   - real occupancy: runs the rest of paceq actually queued or started
//     (manual runs of the same job, for instance);
//   - virtual occupancy: earlier shadow fire-times of the same job still
//     inside their estimated duration, so back-to-back cron shapes reproduce
//     without fabricating anything execution-shaped.
//
// The duration estimate comes from the job's own recent finished runs. A job
// with no history yet carries no estimate; virtual occupancy is then empty,
// which makes shadow optimistic rather than inventive, and the report says
// when it lacks ground to conclude.

const (
	// MetaShadowMode marks the state directory as served in shadow mode.
	// It is set on serve startup with --shadow and cleared on a normal
	// start, so `paceq status` from any process sees what instance is
	// running before it reads a single tick.
	MetaShadowMode = "shadow_mode"

	// MetaShadowSinceMs is when the current shadow period began, Unix ms.
	MetaShadowSinceMs = "shadow_since_ms"

	// MetaShadowObserve names the configured comparison source: journald,
	// file or none.
	MetaShadowObserve = "shadow_observe_source"
)

const (
	// ShadowEstimateRuns bounds how many recent finished runs feed the
	// duration average. Twenty keeps one weird outlier from owning forever
	// while still tracking a job whose runtime genuinely changed.
	ShadowEstimateRuns = 20

	// ShadowOccupancyCap is the longest span one earlier shadow fire-time
	// may occupy. An estimate above it is treated as this cap instead of
	// letting a bad average claim minutes forever.
	ShadowOccupancyCap = 24 * time.Hour
)

// shadowRunDurationMS returns the average duration of the job's most recent
// finished runs and how many runs backed it. Zero samples means no estimate:
// shadow occupancy then stays honest by counting nothing instead of guessing.
func shadowRunDurationMS(tx *sql.Tx, job string) (int64, int64, error) {
	const q = `SELECT COALESCE(AVG(duration_ms), 0), COUNT(*)
FROM (
    SELECT duration_ms FROM runs
     WHERE job_name = ? AND finished_at IS NOT NULL AND duration_ms IS NOT NULL
       AND state IN ('succeeded', 'failed')
     ORDER BY finished_at DESC
     LIMIT ?
)`
	var avgMS float64
	var n int64
	if err := tx.QueryRow(q, job, ShadowEstimateRuns).Scan(&avgMS, &n); err != nil {
		return 0, 0, fmt.Errorf("estimate the run duration of job %s: %w", job, err)
	}
	if n == 0 || avgMS <= 0 {
		return 0, n, nil
	}
	return int64(avgMS), n, nil
}

// shadowVirtualOccupancy counts earlier shadow fire-times of the same job
// that are still inside their estimated duration at this fire-time, plus the
// instant of the newest blocker for the reason text. The caller passes its
// own tick id so a just-claimed row cannot block itself, and the strictness
// keeps equal-instant siblings counted.
func shadowVirtualOccupancy(tx *sql.Tx, job string, durMS int64, fireAtMS int64, selfID string) (int64, int64, error) {
	const q = `SELECT COUNT(*), COALESCE(MAX(scheduled_for), 0)
FROM ticks
WHERE source_kind = 'schedule'
  AND source_name LIKE ?
  AND outcome = 'shadow_triggered'
  AND scheduled_for IS NOT NULL
  AND id <> ?
  AND scheduled_for > ?
  AND scheduled_for + ? > ?`
	pattern := job + "/%"
	var n int64
	var newest int64
	if err := tx.QueryRow(q, pattern, selfID,
		fireAtMS-int64(ShadowOccupancyCap/time.Millisecond),
		fireAtMS, durMS, fireAtMS).Scan(&n, &newest); err != nil {
		return 0, 0, fmt.Errorf("count the shadow occupancy of job %s: %w", job, err)
	}
	return n, newest, nil
}

// shadowJobLimit reads the ceiling the frozen definition imposes. The jobs
// row carries max_concurrent the same way the spec does; one missing row
// means the schedule points at nothing current and shadow must refuse like
// the real path refuses rather than silently admitting everything.
func shadowJobLimit(tx *sql.Tx, job string) (int, error) {
	var limit int
	err := tx.QueryRow(`SELECT max_concurrent FROM jobs WHERE name = ?`, job).Scan(&limit)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("simulating admission for %s: no such job: %w", job, ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("read the concurrency limit of job %s: %w", job, err)
	}
	if limit < 1 {
		limit = spec.DefaultMaxConcurrent
	}
	return limit, nil
}

// shadowObstacle bundles what the simulated admission found.
type shadowObstacle struct {
	held        int
	limit       int
	durMS       int64 // estimated duration behind the virtual count, 0 = unknown
	samples     int64
	virtualNew  int64  // unix ms of the newest virtual occupant, 0 = none
	blockingRun string // id of a real blocking run, if that is what holds
}

// shadowAdmissionTx decides whether the just-claimed shadow fire-time would
// have been admitted, entirely inside the caller's transaction.
func shadowAdmissionTx(tx *sql.Tx, in TickInput, fireAt time.Time, selfID string) (shadowObstacle, error) {
	limit, err := shadowJobLimit(tx, in.Schedule.JobName)
	if err != nil {
		return shadowObstacle{}, err
	}
	active, oldest, err := activeRunsForJobTx(tx, in.Schedule.JobName, fireAt.UnixMilli())
	if err != nil {
		return shadowObstacle{}, err
	}
	ob := shadowObstacle{held: active, limit: limit, blockingRun: oldest}
	durMS, samples, err := shadowRunDurationMS(tx, in.Schedule.JobName)
	if err != nil {
		return shadowObstacle{}, err
	}
	if durMS > 0 {
		capMS := int64(ShadowOccupancyCap / time.Millisecond)
		if durMS > capMS {
			durMS = capMS
		}
		ob.durMS = durMS
		ob.samples = samples
		n, newest, err := shadowVirtualOccupancy(tx, in.Schedule.JobName, durMS, fireAt.UnixMilli(), selfID)
		if err != nil {
			return shadowObstacle{}, err
		}
		ob.virtualNew = newest
		ob.held += int(n)
	}
	return ob, nil
}

// shadowWouldDeferDataJSON is the reason_data a deferred WOULD-carry under
// overlap: queue - the run materialises held into the future; there is no
// deferral record here, but the tick explains itself all the same.
func shadowWouldDeferDataJSON(ob shadowObstacle) string {
	data := fmt.Sprintf(`{"active":%d,"limit":%d,"shadow":true`, ob.held, ob.limit)
	if ob.durMS > 0 {
		data += fmt.Sprintf(`,"estimated_duration_ms":%d,"samples":%d`, ob.durMS, ob.samples)
	} else {
		data += `,"estimated_duration_ms":null`
	}
	if ob.blockingRun != "" {
		data += fmt.Sprintf(`,"blocking_run_id":%q`, ob.blockingRun)
	}
	return data + "}"
}

// resolveShadowOutcome takes a claimed would-trigger through simulated
// admission and adjusts the committed verdict. Called after the tick row
// already landed as 'shadow_triggered'; it may rewrite that row into the
// skipped stand-down the real path would have written - with the real reason
// code, never a shadow-specific invented one, so decisions stay comparable
// between branches.
func resolveShadowOutcomeTx(tx *sql.Tx, in TickInput, at int64,
	tickID, sourceName string, fireAt time.Time,
) (verdict string, code reason.Code, text string, err error) {
	ob, err := shadowAdmissionTx(tx, in, fireAt, tickID)
	if err != nil {
		return "", "", "", err
	}
	if ob.held < ob.limit {
		return OutcomeShadowTriggered, "", "", nil
	}
	switch overlapOf(in.Schedule.Overlap) {
	case "queue":
		// Under queue policy the run WOULD have materialised, deferred;
		// shadow keeps the marker and says why, but no slot was ever held.
		return OutcomeShadowTriggered, "",
			fmt.Sprintf("would defer: %d of %d slots of job %s are held",
				ob.held, ob.limit, in.Schedule.JobName), nil
	default:
		code := skipCodeFor(ob.held, ob.limit)
		text = fmt.Sprintf("the previous run of job %s is still going (%d of %d occupied)",
			in.Schedule.JobName, ob.held, ob.limit)
		if code == reason.TICKSkippedConcurrency && ob.blockingRun == "" && ob.virtualNew > 0 {
			text = fmt.Sprintf("the shadowed fire-time of %s is still going within its "+
				"estimated duration of %.1f s (%d of %d occupied)",
				time.UnixMilli(ob.virtualNew).UTC().Format(time.RFC3339),
				float64(ob.durMS)/1000, ob.held, ob.limit)
		}
		if err := markTickStoodDownTx(tx, tickID, at, code, text,
			shadowWouldDeferDataJSON(ob)); err != nil {
			return "", "", "", err
		}
		return OutcomeSkipped, code, text, nil
	}
}

// ---- writes and reads for observed cron behaviour -------------------------

// ShadowObservationSource enumerates where an observation came from.
const (
	ObsSourceJournald = "journald"
	ObsSourceFile     = "file"
	ObsSourceManual   = "manual"
)

// ShadowObservation is one cron start seen outside paceq.
type ShadowObservation struct {
	ObservedAt time.Time
	Source     string
	Raw        string
	Command    string
	CronUser   string
	// JobName is set when the command matched an imported job; matching is
	// best effort and stays unset otherwise.
	JobName string
	HasJob  bool
}

// InsertShadowObservation stores one observed start. The UNIQUE(source,
// observed_at, raw) key makes re-reading overlapping windows free: a line
// already recorded folds away silently. Returns whether this call inserted.
func (s *Store) InsertShadowObservation(ctx context.Context, o ShadowObservation) (bool, error) {
	res, err := s.w.ExecContext(ctx, `INSERT INTO shadow_observations
(observed_at, source, raw, command, cron_user, job_name)
VALUES (?, ?, ?, ?, ?, NULLIF(?, ''))
ON CONFLICT(source, observed_at, raw) DO NOTHING`,
		o.ObservedAt.UTC().UnixMilli(), o.Source, o.Raw, nullIfEmpty(o.Command),
		nullIfEmpty(o.CronUser), o.JobName)
	if err != nil {
		return false, fmt.Errorf("record a shadow observation: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("record a shadow observation: %w", err)
	}
	return n > 0, nil
}

// ListShadowObservations returns observations in [sinceMs, ...) ordered by
// time, optionally narrowed to one job (jobFilter "job/name"; empty is all).
func (s *Store) ListShadowObservations(ctx context.Context, sinceMs int64, jobFilter string) ([]ShadowObservation, error) {
	q := `SELECT observed_at, source, raw, command, cron_user, job_name
FROM shadow_observations WHERE observed_at >= ?`
	args := []any{sinceMs}
	if jobFilter != "" {
		job := strings.SplitN(jobFilter, "/", 2)[0]
		q += ` AND job_name = ?`
		args = append(args, job)
	}
	q += ` ORDER BY observed_at ASC, id ASC`

	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list shadow observations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ShadowObservation
	for rows.Next() {
		var o ShadowObservation
		var obsRaw, cmd, user, job sql.NullString
		var atMS int64
		if err := rows.Scan(&atMS, &o.Source, &obsRaw, &cmd, &user, &job); err != nil {
			return nil, fmt.Errorf("scan a shadow observation: %w", err)
		}
		o.ObservedAt = time.UnixMilli(atMS).UTC()
		o.Raw, o.Command, o.CronUser = obsRaw.String, cmd.String, user.String
		o.JobName, o.HasJob = job.String, job.Valid
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list shadow observations: %w", err)
	}
	return out, nil
}

// LatestShadowObservationStamp is the newest observation time for one source,
// used as a fetch watermark; zero means nothing stored yet.
func (s *Store) LatestShadowObservationStamp(ctx context.Context, source string) (time.Time, error) {
	var last sql.NullInt64
	err := s.r.QueryRowContext(ctx,
		`SELECT MAX(observed_at) FROM shadow_observations WHERE source = ?`,
		source).Scan(&last)
	if err != nil {
		return time.Time{}, fmt.Errorf("read the latest observation stamp: %w", err)
	}
	if !last.Valid {
		return time.Time{}, nil
	}
	return time.UnixMilli(last.Int64).UTC(), nil
}

// JobCommandRef pairs an exec step command with its job name.
type JobCommandRef struct {
	JobName string
	Command string
}

const jobCommandsSQL = `SELECT j.name, v.spec_json
FROM jobs j JOIN job_versions v ON v.id = j.current_version_id
ORDER BY j.name ASC`

// JobCommandsForMatching lists every command of every current job version,
// one entry per exec step. The observed-command matcher walks these to name
// the import a cron start belongs to. A version whose spec no longer parses
// is skipped rather than fatal: history matching must not break because one
// definition rotted.
func (s *Store) JobCommandsForMatching(ctx context.Context) ([]JobCommandRef, error) {
	rows, err := s.r.QueryContext(ctx, jobCommandsSQL)
	if err != nil {
		return nil, fmt.Errorf("list job commands: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []JobCommandRef
	for rows.Next() {
		var name, specJSON string
		if err := rows.Scan(&name, &specJSON); err != nil {
			return nil, fmt.Errorf("scan a job command row: %w", err)
		}
		job, err := spec.FromIR([]byte(specJSON))
		if err != nil || job == nil {
			continue
		}
		for _, step := range job.Steps {
			out = append(out, JobCommandRef{JobName: name, Command: strings.Join(step.Run, " ")})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list job commands: %w", err)
	}
	return out, nil
}

// ScheduleExpressionRow carries the planning side of one shadowed schedule,
// so the report can re-read its expression under different interpretations.
type ScheduleExpressionRow struct {
	SourceName string
	JobName    string
	Name       string
	Expr       string
	Timezone   string
	Catchup    string
}

const scheduleExpressionsSQL = `SELECT job_name || '/' || name AS src,
       job_name, name, expr, COALESCE(NULLIF(timezone,''),'UTC'), catchup
FROM schedules ORDER BY src ASC`

// ShadowScheduleExpressions lists every schedule's planning shape.
func (s *Store) ShadowScheduleExpressions(ctx context.Context) ([]ScheduleExpressionRow, error) {
	rows, err := s.r.QueryContext(ctx, scheduleExpressionsSQL)
	if err != nil {
		return nil, fmt.Errorf("list schedule expressions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ScheduleExpressionRow
	for rows.Next() {
		var r ScheduleExpressionRow
		if err := rows.Scan(&r.SourceName, &r.JobName, &r.Name, &r.Expr, &r.Timezone, &r.Catchup); err != nil {
			return nil, fmt.Errorf("scan a schedule expression: %w", err)
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list schedule expressions: %w", err)
	}
	return out, nil
}

// ---- reads for the shadow report ------------------------------------------

// ShadowAggregateRow groups ticks by schedule and verdict over one window.
// Everything the per-job diff needs flows through here; no report-level SQL
// lives outside the store.
type ShadowAggregateRow struct {
	// SourceName is the full source key: "<job>/<schedule>".
	SourceName string

	Outcome    string
	ReasonCode string

	Ticks   int64 // rows in the group
	Repeats int64 // repeat_count sum, so coalesced skips keep their weight

	FirstScheduled *time.Time
	LastScheduled  *time.Time
}

// shadowAggregatesSQL buckets schedule-kind ticks since the window opens.
// Filtering by --job narrows on the job prefix only.
const shadowAggregatesSQL = `SELECT source_name, outcome, COALESCE(reason_code, ''),
       COUNT(*), SUM(repeat_count),
       MIN(scheduled_for), MAX(scheduled_for)
FROM ticks
WHERE source_kind = 'schedule' AND scheduled_for IS NOT NULL AND scheduled_for >= ?
GROUP BY source_name, outcome, reason_code
ORDER BY source_name ASC, outcome ASC, reason_code ASC`

// ShadowAggregatesWindow returns the grouped verdicts for the report.
func (s *Store) ShadowAggregatesWindow(ctx context.Context, sinceMs int64) ([]ShadowAggregateRow, error) {
	rows, err := s.r.QueryContext(ctx, shadowAggregatesSQL, sinceMs)
	if err != nil {
		return nil, fmt.Errorf("aggregate shadow ticks: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ShadowAggregateRow
	for rows.Next() {
		var r ShadowAggregateRow
		var firstMS, lastMS sql.NullInt64
		if err := rows.Scan(&r.SourceName, &r.Outcome, &r.ReasonCode,
			&r.Ticks, &r.Repeats, &firstMS, &lastMS); err != nil {
			return nil, fmt.Errorf("scan shadow aggregates: %w", err)
		}
		if firstMS.Valid {
			t := time.UnixMilli(firstMS.Int64).UTC()
			r.FirstScheduled = &t
		}
		if lastMS.Valid {
			t := time.UnixMilli(lastMS.Int64).UTC()
			r.LastScheduled = &t
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("aggregate shadow ticks: %w", err)
	}
	return out, nil
}

// ShadowTickPoint is one would-run instant, the pairing side of the diff.
type ShadowTickPoint struct {
	SourceName   string
	ScheduledFor time.Time
}

// ShadowTriggeredPoints returns every would-run instant in the window,
// collapsed by source and instant (the UNIQUE gate guarantees they already
// are one row).
func (s *Store) ShadowTriggeredPoints(ctx context.Context, sinceMs int64, jobFilter string) ([]ShadowTickPoint, error) {
	q := `SELECT source_name, scheduled_for FROM ticks
WHERE source_kind = 'schedule' AND outcome = 'shadow_triggered'
  AND scheduled_for IS NOT NULL AND scheduled_for >= ?`
	args := []any{sinceMs}
	if jobFilter != "" {
		job := strings.SplitN(jobFilter, "/", 2)[0]
		q += ` AND source_name LIKE ?`
		args = append(args, job+"/%")
	}
	q += ` ORDER BY source_name ASC, scheduled_for ASC`

	rows, err := s.r.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list shadow trigger points: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []ShadowTickPoint
	for rows.Next() {
		var p ShadowTickPoint
		var atMS int64
		if err := rows.Scan(&p.SourceName, &atMS); err != nil {
			return nil, fmt.Errorf("scan a shadow trigger point: %w", err)
		}
		p.ScheduledFor = time.UnixMilli(atMS).UTC()
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list shadow trigger points: %w", err)
	}
	return out, nil
}

// ShadowEvidenceSummary is the thin overview `paceq shadow status` renders.
type ShadowEvidenceSummary struct {
	// TickCount counts every shadow_triggered evaluation including repeats.
	TickCount int64

	// FirstEvidence / LastEvidence bound the window of shadow activity by
	// write time (started_at), not fire time, so uptime answers honestly
	// even on schedules that have not fired yet.
	FirstEvidence *time.Time
	LastEvidence  *time.Time
}

// ShadowEvidence summarises how much shadow materialisation exists at all.
func (s *Store) ShadowEvidence(ctx context.Context) (ShadowEvidenceSummary, error) {
	q := `SELECT COUNT(*), SUM(repeat_count), MIN(started_at), MAX(started_at)
FROM ticks WHERE outcome = 'shadow_triggered'`
	var (
		counts, repeats sql.NullInt64
		first, last     sql.NullInt64
	)
	err := s.r.QueryRowContext(ctx, q).Scan(&counts, &repeats, &first, &last)
	if err != nil {
		return ShadowEvidenceSummary{}, fmt.Errorf("summarise shadow evidence: %w", err)
	}
	sum := ShadowEvidenceSummary{TickCount: repeats.Int64}
	if first.Valid {
		t := time.UnixMilli(first.Int64).UTC()
		sum.FirstEvidence = &t
	}
	if last.Valid {
		t := time.UnixMilli(last.Int64).UTC()
		sum.LastEvidence = &t
	}
	_ = counts
	return sum, nil
}

// ---- meta helpers: the instance-wide marker -------------------------------

// SetShadowRuntime persists what serve startup decided about shadow mode so
// commands in other processes can mark their output truthfully. A normal
// start clears both keys; --shadow sets them along with the chosen source.
func (s *Store) SetShadowRuntime(ctx context.Context, shadow bool, observeSource string) error {
	keys := map[string]string{}
	if shadow {
		now := s.clk.Now().UTC()
		keys[MetaShadowMode] = "1"
		keys[MetaShadowSinceMs] = fmt.Sprintf("%d", now.UnixMilli())
		keys[MetaShadowObserve] = observeSource
	} else {
		keys[MetaShadowMode] = ""
		keys[MetaShadowSinceMs] = ""
		keys[MetaShadowObserve] = ""
	}
	for _, k := range sortedKeys(keys) {
		v := keys[k]
		if v == "" {
			if _, err := s.w.ExecContext(ctx, `DELETE FROM meta WHERE key = ?`, k); err != nil {
				return fmt.Errorf("clear meta %s: %w", k, err)
			}
			continue
		}
		if _, err := s.w.ExecContext(ctx, `INSERT INTO meta (key, value) VALUES (?, ?)
ON CONFLICT(key) DO UPDATE SET value = excluded.value`, k, v); err != nil {
			return fmt.Errorf("write meta %s: %w", k, err)
		}
	}
	return nil
}

// ShadowRuntimeInfo is what `status` and friends read about the running (or
// last-running) shadow instance. Running reports the persisted flag; Since
// is when that flag was last raised.
type ShadowRuntimeInfo struct {
	Running bool
	Since   time.Time
	Observe string
}

// ShadowRuntime reads the persisted shadow markers.
func (s *Store) ShadowRuntime(ctx context.Context) (ShadowRuntimeInfo, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT key, value FROM meta WHERE key IN (?, ?, ?)`,
		MetaShadowMode, MetaShadowSinceMs, MetaShadowObserve)
	if err != nil {
		return ShadowRuntimeInfo{}, fmt.Errorf("read shadow runtime info: %w", err)
	}
	defer func() { _ = rows.Close() }()

	info := ShadowRuntimeInfo{}
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return ShadowRuntimeInfo{}, fmt.Errorf("scan shadow runtime info: %w", err)
		}
		switch k {
		case MetaShadowMode:
			info.Running = v == "1"
		case MetaShadowSinceMs:
			if ms, err := parseUnixMS(v); err == nil && !ms.IsZero() {
				info.Since = ms
			}
		case MetaShadowObserve:
			info.Observe = v
		}
	}
	if err := rows.Err(); err != nil {
		return ShadowRuntimeInfo{}, fmt.Errorf("scan shadow runtime info: %w", err)
	}
	return info, nil
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
