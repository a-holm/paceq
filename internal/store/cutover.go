package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// CutoverEvent is one line a cutover or rollback changed, as the revision
// trail keeps it. The trail is the operator action's answer to the same
// question run_events answers for runs: what happened, to what, by whom,
// and when - written after the crontab itself is safe, because a trail
// that lies about an operation that failed is worse than a gap.
type CutoverEvent struct {
	ID         string
	Action     string // "cutover" or "rollback"
	JobName    string
	LineNumber int
	LineText   string
	Actor      string
	BackupPath string
	Forced     bool
	CreatedAt  time.Time
}

// RecordCutoverEvents writes one transaction's worth of cutover events.
// All rows commit together or none do: a cutover of five lines is five
// rows of one operation, and a trail with three of them would read as a
// partial change the crontab never had.
func (s *Store) RecordCutoverEvents(ctx context.Context, events []CutoverEvent) error {
	if len(events) == 0 {
		return nil
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for i := range events {
			e := &events[i]
			if e.ID == "" {
				id, err := id.New(e.CreatedAt)
				if err != nil {
					return fmt.Errorf("make a cutover event id: %w", err)
				}
				e.ID = id
			}
			forced := 0
			if e.Forced {
				forced = 1
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO cutover_events
(id, action, job_name, line_number, line_text, actor, backup_path, forced, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
				e.ID, e.Action, e.JobName, e.LineNumber, e.LineText,
				e.Actor, e.BackupPath, forced, e.CreatedAt.UnixMilli())
			if err != nil {
				return fmt.Errorf("record the cutover event for %s: %w", e.JobName, err)
			}
		}
		return nil
	})
}

// ListCutoverEvents reads the newest events, newest first. Status shows the
// recent ones so the trail is visible from the command line without SQLite;
// the limit bounds the query, not the trail, which retention owns like any
// other table.
func (s *Store) ListCutoverEvents(ctx context.Context, limit int) ([]CutoverEvent, error) {
	if limit <= 0 {
		limit = 10
	}
	var out []CutoverEvent
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT id, action, job_name, line_number,
line_text, actor, backup_path, forced, created_at
FROM cutover_events
ORDER BY created_at DESC, id DESC
LIMIT ?`, limit)
		if err != nil {
			return fmt.Errorf("read the cutover events: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var e CutoverEvent
			var forced int
			var at int64
			if err := rows.Scan(&e.ID, &e.Action, &e.JobName, &e.LineNumber,
				&e.LineText, &e.Actor, &e.BackupPath, &forced, &at); err != nil {
				return fmt.Errorf("scan a cutover event: %w", err)
			}
			e.Forced = forced == 1
			e.CreatedAt = time.UnixMilli(at).UTC()
			out = append(out, e)
		}
		return rows.Err()
	})
	return out, err
}

// JobSource is one job and the file it was loaded from, for the commands
// that need to walk back to a job's origin.
type JobSource struct {
	Name       string
	SourcePath string
}

// ListJobSources reads every job with the path its current version came
// from, in name order. A job applied by hand has an empty path: cutover
// cannot match such a job to a crontab line and must say so rather than
// guess, which is why the emptiness is data here and not a filter.
func (s *Store) ListJobSources(ctx context.Context) ([]JobSource, error) {
	var out []JobSource
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT name, COALESCE(source_path, '')
FROM jobs
ORDER BY name`)
		if err != nil {
			return fmt.Errorf("read the job sources: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var js JobSource
			if err := rows.Scan(&js.Name, &js.SourcePath); err != nil {
				return fmt.Errorf("scan a job source: %w", err)
			}
			out = append(out, js)
		}
		return rows.Err()
	})
	return out, err
}
