package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// Admission control (#68): the decision of whether a fire-time becomes a run
// now, stands down, or defers. The whole decision is one read, one branch and
// one write inside the transaction that already owns the tick, because the
// writer pool has one connection and the transaction is BEGIN IMMEDIATE: no
// other writer can commit between the read and the write, so the occupancy
// numbers are true when the branch runs. That is the single-writer win the
// design leans on, and it is why there is no SQL count gymnastics here.
//
// Two rules carry the whole feature:
//
//   - ACTIVE means running, or queued and due now. A deferred run (queued
//     with available_at ahead) is NOT active: it waits precisely for a slot,
//     and counting it would make the queue deadlock against itself.
//   - A deferred run always says why: available_at ahead of created_at
//     without a defer_reason is refused by the schema, and this file is one
//     of the two writers the CHECK guards (the reaper and the drain are the
//     others).

// DefaultDeferBackoff is how far a queued overlap defers its run. It is a
// policy number with one constraint: when the blocking run ends, the deferred
// run must already be due, so a release costs a wake rather than a wait. Half
// a second keeps every release inside the one second the acceptance criterion
// allows, with the whole backoff left as slack.
const DefaultDeferBackoff = 500 * time.Millisecond

// activeRunsForJobSQL counts a job's active runs and names the oldest one.
// The count is the admission number; the oldest active run is the one a skip
// points at, because that is the run the fire would have overlapped with.
// The partial index idx_runs_active (job_name) WHERE state IN ('queued',
// 'running') serves the scan; the query plan test pins it.
const activeRunsForJobSQL = `SELECT COUNT(*), COALESCE(MIN(id), '')
FROM runs
WHERE job_name = ?
  AND (state = 'running'
   OR (state = 'queued' AND available_at <= ?))`

// activeRunsForJobTx reads the occupancy of one job inside the caller's
// transaction. It takes the transaction, not the store, on purpose: an
// occupancy read that went through the read pool would be a decision made on
// numbers nobody was holding still.
func activeRunsForJobTx(tx *sql.Tx, job string, nowMilli int64) (int, string, error) {
	var n int
	var oldest string
	if err := tx.QueryRow(activeRunsForJobSQL, job, nowMilli).Scan(&n, &oldest); err != nil {
		return 0, "", fmt.Errorf("count the active runs of job %s: %w", job, err)
	}
	return n, oldest, nil
}

// ActiveRunsForJob reads the occupancy of one job. It exists for tests and
// for the fsck invariant; production decisions never call it, because a read
// outside the transaction that decides is exactly the shape this feature
// exists to avoid.
func (s *Store) ActiveRunsForJob(ctx context.Context, job string) (int, string, error) {
	now := s.clk.Now().UTC().UnixMilli()
	var n int
	var oldest string
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, activeRunsForJobSQL, job, now)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		if rows.Next() {
			if err := rows.Scan(&n, &oldest); err != nil {
				return err
			}
		}
		return rows.Err()
	})
	if err != nil {
		return 0, "", fmt.Errorf("count the active runs of job %s: %w", job, err)
	}
	return n, oldest, nil
}

// overlapOf normalises a schedule's overlap policy. An empty policy is skip:
// the schema default, and the value every row written before the column
// existed carries.
func overlapOf(overlap string) string {
	if overlap == "" {
		return "skip"
	}
	return overlap
}

// deferDataJSON builds the reason_data object a deferral or a skip records:
// which run held the slot, how many slots were held, and what the ceiling
// was. The difference between a useless explanation and a useful one is one
// id, so the id always rides along when there is one.
//
// scope is the promised key of both RUN_QUEUED_CONCURRENCY and
// TICK_SKIPPED_CONCURRENCY (#191), and it is a constant here because this
// writer knows exactly one ceiling: the job's max_concurrent. A key hold is a
// different decision under a different code, and there is no global ceiling
// to reach yet; either would arrive as another scope beside this one.
func deferDataJSON(blocking string, active, limit int) string {
	data := fmt.Sprintf(`{"active":%d,"limit":%d`, active, limit)
	if blocking != "" {
		data += fmt.Sprintf(`,"blocking_run_id":%q`, blocking)
	}
	return data + `,"scope":"job"}`
}

// skipCodeFor picks the reason code a stand-down records. One held slot
// against a limit of one is the classic overlap: the previous run is still
// going. Anything else is the ceiling itself being reached by several runs.
func skipCodeFor(active, limit int) reason.Code {
	if limit == 1 && active == 1 {
		return reason.TICKSkippedOverlap
	}
	return reason.TICKSkippedConcurrency
}

// deferReasonCode is the code a deferred run carries while it waits. The
// catalogue (#59) landed this code as RUN_QUEUED_CONCURRENCY; the issue
// sketch's RUN_QUEUED_CONCURRENCY_JOB names the same fact.
func deferReasonCode() reason.Code {
	return reason.RUNQueuedConcurrency
}

// deferReasonModel is the defer_reason word the schema CHECK and fsck I14
// look for on a queued run held into the future.
func deferReasonModel() string {
	return model.DeferReasonConcurrency
}

// activeLimitSQL is the I12 sweep (#68): the running rows of every job
// counted against its ceiling. The invariant governs what runs; queued
// backlog beside a full complement of running rows is the ordinary shape of
// a busy queue and never a violation.
const activeLimitSQL = `SELECT j.name, j.max_concurrent, COUNT(r.id)
FROM jobs j LEFT JOIN runs r ON r.job_name = j.name AND r.state = 'running'
GROUP BY j.name HAVING COUNT(r.id) > j.max_concurrent`

// ActiveRunViolations returns every job whose running rows outnumber its
// max_concurrent: the I12 invariant of 02 section 4.3. It is part of the
// full fsck sweep, and it is the check the startup quick subset calls as
// well once #62's reconciliation lands there - the call site is one line,
// because this method already reads exactly what that subset needs.
func (s *Store) ActiveRunViolations(ctx context.Context) ([]Violation, error) {
	rows, err := s.r.QueryContext(ctx, activeLimitSQL)
	if err != nil {
		return nil, fmt.Errorf("fsck I12: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []Violation
	for rows.Next() {
		var job string
		var limit int64
		var n int64
		if err := rows.Scan(&job, &limit, &n); err != nil {
			return nil, fmt.Errorf("fsck I12: %w", err)
		}
		out = append(out, Violation{
			Check:   "I12",
			Subject: "job " + job,
			Detail: fmt.Sprintf("%d runs are running against a ceiling of %d",
				n, limit),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("fsck I12: %w", err)
	}
	return out, nil
}
