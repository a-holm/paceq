package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// JobRunSummary is what paceq status shows for one job: the job's newest run
// and how far its steps got. A job that has never run carries an empty RunID
// and state.
type JobRunSummary struct {
	JobName    string
	RunID      string
	State      string
	ReasonCode string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	StepsTotal int
	StepsDone  int

	// The deferral facts (#68), read straight off the newest run so status
	// can say "utsatt" with its reason without a second query.
	AvailableAt time.Time
	DeferReason string
	ReasonData  string
}

// JobLastRuns reads one row per job, with that job's newest run joined beside
// it. An empty jobName reads every job; a name narrows the answer to one. Jobs
// come back in name order, and a job without runs is still listed, because
// status is a per job report rather than a listing of runs.
//
// The newest run is decided by id alone. Ids are ULIDs, so id order is time
// order and the subquery stays a single index lookup per job.
func (s *Store) JobLastRuns(ctx context.Context, jobName string) ([]JobRunSummary, error) {
	query := `SELECT j.name,
	r.id, r.state, COALESCE(r.reason_code, ''),
	COALESCE(r.available_at, 0), COALESCE(r.defer_reason, ''), COALESCE(r.reason_data, ''),
	r.created_at, r.started_at, r.finished_at,
	(SELECT COUNT(*) FROM steps s2 WHERE s2.run_id = r.id),
	(SELECT COUNT(*) FROM steps s3 WHERE s3.run_id = r.id
		AND s3.state IN ('succeeded', 'failed', 'skipped', 'cancelled'))
FROM jobs j
LEFT JOIN runs r ON r.id =
	(SELECT r2.id FROM runs r2 WHERE r2.job_name = j.name ORDER BY r2.id DESC LIMIT 1)`
	args := []any{}
	if jobName != "" {
		query += "\nWHERE j.name = ?"
		args = append(args, jobName)
	}
	query += "\nORDER BY j.name"

	var out []JobRunSummary
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("read the last run of every job: %w", err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var (
				sum                 JobRunSummary
				runID, state, code  sql.NullString
				deferRed, reasonDt  sql.NullString
				availAt             int64
				createdAt           sql.NullInt64
				startedAt, finished sql.NullInt64
				total, done         int
			)
			if err := rows.Scan(&sum.JobName, &runID, &state, &code,
				&availAt, &deferRed, &reasonDt,
				&createdAt, &startedAt, &finished, &total, &done); err != nil {
				return fmt.Errorf("scan a job's last run: %w", err)
			}
			sum.RunID = runID.String
			sum.State = state.String
			sum.ReasonCode = code.String
			sum.DeferReason = deferRed.String
			sum.ReasonData = reasonDt.String
			if availAt > 0 {
				sum.AvailableAt = time.UnixMilli(availAt).UTC()
			}
			// A job without runs joins nothing, so every run column is
			// NULL, created_at included.
			if createdAt.Valid && createdAt.Int64 > 0 {
				sum.CreatedAt = time.UnixMilli(createdAt.Int64).UTC()
			}
			sum.StartedAt = nullableMillis(startedAt)
			sum.FinishedAt = nullableMillis(finished)
			sum.StepsTotal = total
			sum.StepsDone = done
			out = append(out, sum)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read the last run of every job: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// JobView is one job as `paceq jobs show` reads it back: the identity, the
// ceiling that admission control enforces, and where the job came from.
type JobView struct {
	Name          string
	Description   string
	MaxConcurrent int

	// Paused is the operator pause flag. A paused job still admits what a
	// person starts; it only stands its schedules down.
	Paused bool

	SourcePath string

	// CurrentVersion is the version number the job currently points at,
	// zero when no version has ever been applied to it.
	CurrentVersion int

	CreatedAt time.Time
	UpdatedAt time.Time
}

// Job reads one job back. An unknown name is ErrNotFound, the same answer
// every other single row read gives.
func (s *Store) Job(ctx context.Context, name string) (JobView, error) {
	var out JobView
	var description, source sql.NullString
	var version sql.NullInt64
	var createdAt, updatedAt int64
	err := s.r.QueryRowContext(ctx, `SELECT j.name, COALESCE(j.description, ''),
j.max_concurrent, j.paused, COALESCE(j.source_path, ''), v.version,
j.created_at, j.updated_at
FROM jobs j LEFT JOIN job_versions v ON v.id = j.current_version_id
WHERE j.name = ?`, name).Scan(&out.Name, &description, &out.MaxConcurrent,
		&out.Paused, &source, &version, &createdAt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return JobView{}, fmt.Errorf("read job %s: %w", name, ErrNotFound)
	}
	if err != nil {
		return JobView{}, fmt.Errorf("read job %s: %w", name, err)
	}
	out.Description = description.String
	out.SourcePath = source.String
	out.CurrentVersion = int(version.Int64)
	out.CreatedAt = time.UnixMilli(createdAt).UTC()
	out.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	return out, nil
}

// JobNames lists every job in name order. It is what a did you mean suggestion
// is drawn from when a command is given a job that does not exist.
func (s *Store) JobNames(ctx context.Context) ([]string, error) {
	var out []string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name FROM jobs ORDER BY name`)
		if err != nil {
			return fmt.Errorf("list the jobs: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return fmt.Errorf("list the jobs: %w", err)
			}
			out = append(out, name)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// nullableMillis turns a millisecond column that may be NULL into a time. An
// absent stamp is the zero time, never the epoch.
func nullableMillis(ms sql.NullInt64) time.Time {
	if !ms.Valid || ms.Int64 == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms.Int64).UTC()
}
