package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/faults"
)

// Retention deletes history in bounded batches. The single write connection
// is the scarcest resource in the system, and one naive
// `DELETE FROM runs WHERE finished_at < x` can cascade to hundreds of
// thousands of child rows, which is exactly the "writing that holds the lock
// for seconds" the architecture forbids (07 section 6.1). Every delete here
// therefore touches at most PruneBatchLimit parent rows per transaction,
// ordered by id, and the caller loops until the batch comes back empty,
// pausing between batches so other writers get the lock back.
//
// The double criterion lives inside the queries, never as a clean-up pass:
// a row older than the horizon is only a candidate if the object it belongs
// to (a job, a tick source) has more recent history than the keep-minimum.
// A job that ran three times two years ago keeps all three runs, because
// those three are its newest fifty (06 section 9.4).
//
// Shape note, paid for with a measurement: the protection test is a scalar
// subquery around an ordered mini-select ("is this row inside the newest
// keepMin?"), not the more obvious `id NOT IN (SELECT ... LIMIT n)`. SQLite
// plans the correlated NOT-IN form as a LIST SUBQUERY whose per-row
// evaluation loses the index-order walk and sorts the object's whole history
// - 2.2 seconds per 500-row batch on a 200k-run database, which is the exact
// lock-hold catastrophe this file exists to prevent. The scalar form keeps
// the co-routine on idx_runs_job_finished / idx_ticks_source and stays flat
// at ~5 ms per selection from 25k to 200k rows. The same measurement killed
// two missing foreign-key indexes: runs.replay_of and triggers.run_id are
// ON DELETE SET NULL edges, and without indexes every deleted run scanned
// both tables (migration 0013).

// PruneBatchLimit caps every retention DELETE at this many parent rows per
// transaction. The plan sketch named 500 on the assumption it keeps each
// transaction in the tens of milliseconds; measured here (60k seeded runs,
// warm cache, modernc.org/sqlite), a 500-row batch with its cascade holds
// the write lock for p50 47.8 ms / p99 69 ms - over the 50 ms budget this
// issue exists to defend. Two hundred rows measures p50 ~19 ms / p99 ~29 ms,
// which leaves real headroom for scheduler noise and CPU contention, so the
// smaller number is what ships: the lock budget is the invariant, the batch
// size is only the mechanism.
const PruneBatchLimit = 200

// pruneResult carries what one batch deleted.
type pruneResult struct {
	Deleted int64
}

// runPruneBatch executes one bounded DELETE inside its own BEGIN IMMEDIATE
// transaction and reports how many rows went away. It exists so the fault
// point sits in exactly one place, between the statement and the commit of
// whichever batch the operator chose to crash.
func (s *Store) runPruneBatch(ctx context.Context, sqlText string, args ...any) (int64, error) {
	var n int64
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(ctx, sqlText, args...)
		if err != nil {
			return err
		}
		n, err = res.RowsAffected()
		return err
	})
	if err != nil {
		return 0, fmt.Errorf("retention batch: %w", err)
	}
	faults.Point("M5:prune:after_batch")
	return n, nil
}

// PruneRunsBatch deletes up to PruneBatchLimit terminal runs that finished
// before cutoff, except each job's newest keepMin finished runs, which are
// kept no matter how old they are. ON DELETE CASCADE removes the run's steps,
// step_deps, run_events and artifacts in the same transaction.
func (s *Store) PruneRunsBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error) {
	const q = `
DELETE FROM runs
 WHERE id IN (
   SELECT r.id
     FROM runs r
    WHERE r.finished_at IS NOT NULL
      AND r.finished_at < ?
      AND r.state IN ('succeeded', 'failed', 'cancelled')
      AND (
            SELECT count(*)
              FROM (
                SELECT r2.id
                  FROM runs r2
                 WHERE r2.job_name = r.job_name
                   AND r2.finished_at IS NOT NULL
                 ORDER BY r2.finished_at DESC, r2.id DESC
                 LIMIT ?
              ) prot
             WHERE prot.id = r.id
          ) = 0
    ORDER BY r.finished_at
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), keepMin, PruneBatchLimit)
}

// PruneSkippedTicksBatch deletes up to PruneBatchLimit skipped ticks older
// than cutoff. Skips are the highest volume, lowest value rows in the
// database once their coalescing window has passed (07 section 6.2), so they
// carry no keep-minimum: seven days of "nothing was due" is evidence enough.
// Deleting a tick cascades to its triggers.
func (s *Store) PruneSkippedTicksBatch(ctx context.Context, cutoff time.Time) (int64, error) {
	const q = `
DELETE FROM ticks
 WHERE id IN (
   SELECT id
     FROM ticks
    WHERE outcome = 'skipped'
      AND started_at < ?
    ORDER BY id
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), PruneBatchLimit)
}

// PruneTicksBatch deletes up to PruneBatchLimit non-skipped ticks older than
// cutoff, except each source's newest keepMin ticks. A source is the
// (source_kind, source_name) pair, so a quarterly schedule keeps its newest
// 200 evaluations even when every one of them is older than the horizon.
func (s *Store) PruneTicksBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error) {
	const q = `
DELETE FROM ticks
 WHERE id IN (
   SELECT t.id
     FROM ticks t
    WHERE t.outcome <> 'skipped'
      AND t.started_at < ?
      AND (
            SELECT count(*)
              FROM (
                SELECT t2.id
                  FROM ticks t2
                 WHERE t2.source_kind = t.source_kind
                   AND t2.source_name = t.source_name
                 ORDER BY t2.started_at DESC, t2.id DESC
                 LIMIT ?
              ) prot
             WHERE prot.id = t.id
          ) = 0
    ORDER BY t.started_at
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), keepMin, PruneBatchLimit)
}

// PruneRunKeysBatch deletes up to PruneBatchLimit dedup keys first seen
// before cutoff.
//
// This is deliberately the last table retention touches, and deleting here
// has a consequence an operator must be able to read somewhere other than
// this file: a run_key is what stops an old trigger from firing twice. Once
// the key is gone, the same trigger can fire again - a schedule replaying
// after a cursor reset, or a sensor whose dedup epoch moved. That is the
// documented price of keeping the table bounded (SYNTESE section 3.22), and
// the restore/runbook documentation owns repeating it in plain language.
func (s *Store) PruneRunKeysBatch(ctx context.Context, cutoff time.Time) (int64, error) {
	const q = `
DELETE FROM run_keys
 WHERE (source_id, epoch, run_key) IN (
   SELECT source_id, epoch, run_key
     FROM run_keys
    WHERE first_seen_at < ?
    ORDER BY source_id, epoch, run_key
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), PruneBatchLimit)
}

// PruneSessionsBatch deletes stopped daemon sessions older than cutoff,
// except the newest keepMin sessions overall and any session an outage row
// still points at. The outage reference has no ON DELETE action, so a session
// an outage cites simply survives until the outage does - outages are kept
// forever anyway (06 section 9.4).
func (s *Store) PruneSessionsBatch(ctx context.Context, cutoff time.Time, keepMin int) (int64, error) {
	const q = `
DELETE FROM daemon_sessions
 WHERE id IN (
   SELECT d.id
     FROM daemon_sessions d
    WHERE d.started_at < ?
      AND d.stopped_at IS NOT NULL
      AND d.id NOT IN (
            SELECT prev_session
              FROM outages
             WHERE prev_session IS NOT NULL
          )
      AND d.id NOT IN (
            SELECT d2.id
              FROM daemon_sessions d2
             ORDER BY d2.started_at DESC, d2.id DESC
             LIMIT ?
          )
    ORDER BY d.id
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), keepMin, PruneBatchLimit)
}

// RetentionPlan is the per-rule estimate `prune --dry-run` shows. Each count
// answers the same predicate the real delete would, including the
// keep-minimum protection, without the batch limit.
type RetentionPlan struct {
	Runs         int64 `json:"runs"`
	SkippedTicks int64 `json:"skipped_ticks"`
	Ticks        int64 `json:"ticks"`
	RunKeys      int64 `json:"run_keys"`
	Sessions     int64 `json:"daemon_sessions"`
}

// Total sums the database-side deletions the plan would perform.
func (p RetentionPlan) Total() int64 {
	return p.Runs + p.SkippedTicks + p.Ticks + p.RunKeys + p.Sessions
}

// EstimateRetention counts, per rule, the rows a full retention pass over
// this database would delete today. It reads through the read-only pool and
// writes nothing.
func (s *Store) EstimateRetention(ctx context.Context, p Policies, now time.Time) (RetentionPlan, error) {
	var plan RetentionPlan
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		counts := []struct {
			dst  *int64
			q    string
			args []any
		}{
			{
				&plan.Runs, `
SELECT count(*) FROM runs r
 WHERE r.finished_at IS NOT NULL AND r.finished_at < ?
   AND r.state IN ('succeeded', 'failed', 'cancelled')
   AND (
         SELECT count(*)
           FROM (
             SELECT r2.id FROM runs r2
              WHERE r2.job_name = r.job_name AND r2.finished_at IS NOT NULL
              ORDER BY r2.finished_at DESC, r2.id DESC
              LIMIT ?) prot
          WHERE prot.id = r.id
       ) = 0`,
				[]any{runsCutoff(now, p).UnixMilli(), p.RunsKeepMin},
			},
			{
				&plan.SkippedTicks, `
SELECT count(*) FROM ticks
 WHERE outcome = 'skipped' AND started_at < ?`,
				[]any{skippedTicksCutoff(now, p).UnixMilli()},
			},
			{
				&plan.Ticks, `
SELECT count(*) FROM ticks t
 WHERE t.outcome <> 'skipped' AND t.started_at < ?
   AND (
         SELECT count(*)
           FROM (
             SELECT t2.id FROM ticks t2
              WHERE t2.source_kind = t.source_kind AND t2.source_name = t.source_name
              ORDER BY t2.started_at DESC, t2.id DESC
              LIMIT ?) prot
          WHERE prot.id = t.id
       ) = 0`,
				[]any{ticksCutoff(now, p).UnixMilli(), p.TicksKeepMin},
			},
			{
				&plan.RunKeys, `
SELECT count(*) FROM run_keys WHERE first_seen_at < ?`,
				[]any{runKeysCutoff(now, p).UnixMilli()},
			},
			{
				&plan.Sessions, `
SELECT count(*) FROM daemon_sessions d
 WHERE d.started_at < ? AND d.stopped_at IS NOT NULL
   AND d.id NOT IN (SELECT prev_session FROM outages WHERE prev_session IS NOT NULL)
   AND d.id NOT IN (
         SELECT d2.id FROM daemon_sessions d2
          ORDER BY d2.started_at DESC, d2.id DESC
          LIMIT ?)`,
				[]any{sessionsCutoff(now, p).UnixMilli(), p.SessionsKeepMin},
			},
		}
		for _, c := range counts {
			if err := r.QueryRowContext(ctx, c.q, c.args...).Scan(c.dst); err != nil {
				return fmt.Errorf("estimate retention: %w", err)
			}
		}
		return nil
	})
	return plan, err
}
