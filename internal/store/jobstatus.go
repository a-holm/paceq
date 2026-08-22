package store

import (
	"context"
	"database/sql"
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
				createdAt           sql.NullInt64
				startedAt, finished sql.NullInt64
				total, done         int
			)
			if err := rows.Scan(&sum.JobName, &runID, &state, &code,
				&createdAt, &startedAt, &finished, &total, &done); err != nil {
				return fmt.Errorf("scan a job's last run: %w", err)
			}
			sum.RunID = runID.String
			sum.State = state.String
			sum.ReasonCode = code.String
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
