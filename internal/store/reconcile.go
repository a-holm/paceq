package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/reason"
)

// The store half of startup reconciliation (issue #62). Three primitives live
// here and nowhere else, because SQL lives in internal/store and nowhere else:
//
//   - RecordOutage writes the row that answers "was the system up at all".
//     Its kind vocabulary is the schema's ('crash', 'clean', 'clock_jump',
//     'boot'), not the issue sketch's; the CHECK constraint is the law and
//     this file does not get a second opinion.
//   - RecordMissedTicks writes the synthetic evidence for a gap: one missed
//     tick per schedule slot nobody evaluated, each carrying
//     TICK_MISSED_DAEMON_DOWN and the id of its outages row. Inserts are
//     OR IGNORE against the tick slot uniqueness, so repeating a pass over
//     the same slots writes nothing the second time.
//   - FailHangingTicks closes evaluations a dead daemon left running with
//     TICK_ERROR_DAEMON_CRASHED. The cutoff keeps this session's own work
//     safe: only ticks that were already running when this process opened its
//     session can belong to a dead writer.

// OutageInput names the downtime an outage row explains.
type OutageInput struct {
	// From and To bound the unaccounted period. From is the last heartbeat of
	// the session that died; To is when the replacement started.
	From time.Time
	To   time.Time

	// Kind is the schema's word for what happened: 'crash' or 'boot' from
	// startup reconciliation, 'clock_jump' from jump detection.
	Kind string

	// PrevSession is the daemon_sessions row whose death opened the gap, or
	// empty when no session row survived to name (a first start after a wipe).
	PrevSession string
}

// Outage is one stored downtime record.
type Outage struct {
	ID          int64
	From        time.Time
	To          time.Time
	DetectedAt  time.Time
	Kind        string
	PrevSession string
	MissedTicks int
}

// RecordOutage stores the outage and returns its rowid. The detected_at stamp
// is this store's clock reading at write time, which is what bounds the SLO
// promise: an outage longer than the noise threshold exists within seconds of
// the startup that noticed it.
func (s *Store) RecordOutage(ctx context.Context, in OutageInput) (int64, error) {
	now := s.clk.Now().UTC()
	res, err := s.w.ExecContext(ctx, `INSERT INTO outages
(from_ts, to_ts, detected_at, kind, prev_session, missed_ticks)
VALUES (?, ?, ?, ?, ?, 0)`,
		in.From.UnixMilli(), in.To.UnixMilli(), now.UnixMilli(),
		in.Kind, nullIfEmpty(in.PrevSession))
	if err != nil {
		return 0, fmt.Errorf("record the outage: %w", err)
	}
	rowID, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("read the outage rowid back: %w", err)
	}
	return rowID, nil
}

// Outages lists every stored outage, oldest first. It is how tests and the
// explain surface read history back.
func (s *Store) Outages(ctx context.Context) ([]Outage, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT id, from_ts, to_ts, detected_at,
kind, COALESCE(prev_session, ''), missed_ticks
FROM outages ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list outages: %w", err)
	}
	defer rows.Close()

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
		return nil, fmt.Errorf("scan an outage: %w", err)
	}
	return out, rows.Close()
}

// MissedTick is one schedule slot inside a gap that nobody evaluated.
type MissedTick struct {
	SourceName   string
	ScheduledFor time.Time
}

// missedTickBatch caps how many synthetic ticks one transaction inserts. The
// perf promise on the sweep is that the write lock is never held long in one
// stretch; many small transactions keep a hundred-schedule gap from turning
// into one long hold.
const missedTickBatch = 256

// RecordMissedTicks writes the synthetic ticks for one outage and adds the
// count it actually inserted to the outage's total. Every insert is
// INSERT OR IGNORE: a slot that already has a tick row was really evaluated
// before the crash, and reconciliation never rewrites real history. That same
// ignore makes a repeated pass over the same slots a no-op, which is what
// makes the whole sweep safe to run twice.
//
// The work is chunked so a fourteen day gap across a hundred schedules cannot
// hold the write lock for one long stretch.
func (s *Store) RecordMissedTicks(ctx context.Context, sessionID string, outageID int64, ticks []MissedTick) (int, error) {
	inserted := 0
	for start := 0; start < len(ticks) || start == 0; start += missedTickBatch {
		end := start + missedTickBatch
		if end > len(ticks) {
			end = len(ticks)
		}
		chunk := ticks[start:end]
		n, err := s.recordMissedTickChunk(ctx, sessionID, outageID, chunk)
		if err != nil {
			return inserted, err
		}
		inserted += n
		if end >= len(ticks) {
			break
		}
	}
	return inserted, nil
}

func (s *Store) recordMissedTickChunk(ctx context.Context, sessionID string, outageID int64, ticks []MissedTick) (int, error) {
	inserted := 0
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		for _, mt := range ticks {
			at := mt.ScheduledFor.UTC()
			tickID, err := id.New(at)
			if err != nil {
				return fmt.Errorf("mint an id for a missed tick: %w", err)
			}
			res, err := tx.Exec(`INSERT OR IGNORE INTO ticks
(id, source_kind, source_name, scheduled_for, started_at, last_started_at,
 outcome, reason_code, reason_data, trigger_count, deduped_count, daemon_session_id)
VALUES (?, 'schedule', ?, ?, ?, ?, 'missed', ?, ?, 0, 0, ?)`,
				tickID, mt.SourceName, at.UnixMilli(), at.UnixMilli(), at.UnixMilli(),
				string(reason.TICKMissedDaemonDown),
				fmt.Sprintf(`{"outage_id":%d}`, outageID),
				sessionID)
			if err != nil {
				return fmt.Errorf("insert a missed tick for %s: %w", mt.SourceName, err)
			}
			written, err := res.RowsAffected()
			if err != nil {
				return fmt.Errorf("count the missed tick insert: %w", err)
			}
			inserted += int(written)
		}
		if inserted > 0 {
			if _, err := tx.Exec(`UPDATE outages SET missed_ticks = missed_ticks + ?
WHERE id = ?`, inserted, outageID); err != nil {
				return fmt.Errorf("add %d missed ticks to outage %d: %w", inserted, outageID, err)
			}
		}
		// The crash window inside the gap write: ticks counted, nothing
		// committed. A kill here leaves an outage row whose evidence is
		// partially written; the next pass completes it through
		// OutagesWithoutTicks and INSERT OR IGNORE (issue #62, W15).
		faults.Point("M2:reconcile:mid_gap_tx")
		return nil
	})
	if err != nil {
		return 0, err
	}
	return inserted, nil
}

// FailHangingTicks closes every tick still marked running whose evaluation
// started before the given instant. A running tick older than this process's
// session belongs to a daemon that died mid evaluation; nothing else could
// ever have written its verdict. Each closed tick records why: the code says
// the daemon crashed, not that the sensor failed.
//
// The returned slice names what closed, in no particular order, so a caller
// logs exactly as much as happened. An empty result means there was nothing to
// close, which is the common case for the periodic safety net.
func (s *Store) FailHangingTicks(ctx context.Context, startedBefore time.Time) ([]string, error) {
	cut := startedBefore.UTC().UnixMilli()
	var closed []string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(`UPDATE ticks
SET outcome = 'error',
	finished_at = ?,
	duration_ms = ? - started_at,
	reason_code = ?,
	reason_text = 'the daemon died while this evaluation was in flight'
WHERE outcome = 'running' AND started_at < ?
RETURNING source_kind || ':' || source_name`,
			s.clk.Now().UTC().UnixMilli(), s.clk.Now().UTC().UnixMilli(),
			string(reason.TICKErrorDaemonCrashed), cut)
		if err != nil {
			return fmt.Errorf("close hanging ticks: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			var who string
			if err := rows.Scan(&who); err != nil {
				return fmt.Errorf("scan a closed tick: %w", err)
			}
			closed = append(closed, who)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return closed, nil
}

// AttemptProcess is one attempt's recorded process identity: which pid led
// its process group, and what /proc said its start time was when it was
// spawned. The pair is what lets the orphan sweep tell a surviving child of
// a dead executor from a recycled pid that happens to carry the same number.
//
// Its write lives beside the other step transitions; this file only reads
// baselines back for the sweep.
type AttemptProcess struct {
	RunID      string
	Step       string
	PID        int
	StartTicks int64
}

// ActiveAttempts lists the baselines whose step and whose run are still
// running right now. A pid found on this list with matching start ticks is a
// legitimate live worker, and the orphan sweep leaves it alone.
func (s *Store) ActiveAttempts(ctx context.Context) ([]AttemptProcess, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT st.run_id, st.name, st.pid, st.pid_start_ticks
FROM steps st JOIN runs r ON r.id = st.run_id
WHERE st.pid IS NOT NULL AND st.state = 'running' AND r.state = 'running'
ORDER BY st.run_id, st.name`)
	return scanAttempts(rows, "active attempts", err)
}

// KnownAttempts lists every baseline ever recorded, whatever happened to the
// step afterwards. This is the sweep's history half: a process still carrying
// a dead attempt's PACEQ_RUN_ID is identified by a baseline that outlived the
// attempt.
func (s *Store) KnownAttempts(ctx context.Context) ([]AttemptProcess, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT run_id, name, pid, pid_start_ticks FROM steps
WHERE pid IS NOT NULL
ORDER BY run_id, name`)
	return scanAttempts(rows, "known attempts", err)
}

func scanAttempts(rows *sql.Rows, what string, err error) ([]AttemptProcess, error) {
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", what, err)
	}
	defer rows.Close()
	var out []AttemptProcess
	for rows.Next() {
		var (
			a     AttemptProcess
			pid   int64
			ticks int64
		)
		if err := rows.Scan(&a.RunID, &a.Step, &pid, &ticks); err != nil {
			return nil, fmt.Errorf("scan %s: %w", what, err)
		}
		a.PID = int(pid)
		a.StartTicks = ticks
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan %s: %w", what, err)
	}
	return out, rows.Close()
}

// GapSchedule is one enabled schedule as the gap walk needs it: enough to
// recompute where its fire times fell inside an outage.
type GapSchedule struct {
	JobName  string
	Name     string
	Expr     string
	Timezone string
}

// GapSchedules lists the schedules that were live to fire during a gap.
// Paused schedules are left out on purpose: their slots were not owed, and
// writing missed ticks beside them would turn an operator's pause into
// apparent downtime.
func (s *Store) GapSchedules(ctx context.Context) ([]GapSchedule, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT job_name, name, expr, timezone FROM schedules
WHERE paused = 0
ORDER BY job_name, name`)
	if err != nil {
		return nil, fmt.Errorf("list the schedules for the gap walk: %w", err)
	}
	defer rows.Close()
	var out []GapSchedule
	for rows.Next() {
		var g GapSchedule
		if err := rows.Scan(&g.JobName, &g.Name, &g.Expr, &g.Timezone); err != nil {
			return nil, fmt.Errorf("scan a schedule for the gap walk: %w", err)
		}
		out = append(out, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan a schedule for the gap walk: %w", err)
	}
	return out, rows.Close()
}

// OutagesWithoutTicks lists outage rows whose synthetic tick evidence has not
// been written yet (or legitimately holds zero slots). It is how reconciliation
// finishes what a crash mid-write left half explained: the row itself is the
// marker, and a later pass completes it.
func (s *Store) OutagesWithoutTicks(ctx context.Context) ([]Outage, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT id, from_ts, to_ts, detected_at,
kind, COALESCE(prev_session, ''), missed_ticks
FROM outages WHERE missed_ticks = 0 ORDER BY id`)
	if err != nil {
		return nil, fmt.Errorf("list unfinished outages: %w", err)
	}
	defer rows.Close()
	var out []Outage
	for rows.Next() {
		var (
			o        Outage
			from, to int64
			detected int64
			missed   int
		)
		if err := rows.Scan(&o.ID, &from, &to, &detected, &o.Kind, &o.PrevSession, &missed); err != nil {
			return nil, fmt.Errorf("scan an unfinished outage: %w", err)
		}
		o.From = time.UnixMilli(from).UTC()
		o.To = time.UnixMilli(to).UTC()
		o.DetectedAt = time.UnixMilli(detected).UTC()
		o.MissedTicks = missed
		out = append(out, o)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan an unfinished outage: %w", err)
	}
	return out, rows.Close()
}

// TickEvidence is one stored tick's explanation triple: what ran, what code
// explains it, and what data points at its cause.
type TickEvidence struct {
	SourceName string
	ReasonCode string
	ReasonData string
}

// MissedTickEvidence lists every synthetic missed tick. It is how tests and
// the explain surface read the gap's evidence back without touching SQL
// outside this package.
func (s *Store) MissedTickEvidence(ctx context.Context) ([]TickEvidence, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT source_name, COALESCE(reason_code, ''),
COALESCE(reason_data, '') FROM ticks WHERE outcome = 'missed' ORDER BY source_name`)
	if err != nil {
		return nil, fmt.Errorf("list the missed tick evidence: %w", err)
	}
	defer rows.Close()
	var out []TickEvidence
	for rows.Next() {
		var e TickEvidence
		if err := rows.Scan(&e.SourceName, &e.ReasonCode, &e.ReasonData); err != nil {
			return nil, fmt.Errorf("scan missed tick evidence: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Close()
}

// RunExists reports whether this database has ever heard of the run. The
// orphan sweep checks before it believes an environment variable: a foreign
// program that happens to carry PACEQ_RUN_ID must never be mistaken for one
// of ours.
func (s *Store) RunExists(ctx context.Context, runID string) (bool, error) {
	var one int
	err := s.r.QueryRowContext(ctx, `SELECT 1 FROM runs WHERE id = ?`, runID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up run %s: %w", runID, err)
	}
	return true, nil
}

// RecordOrphanKill writes the audit event that says this process group was
// signalled and why. A kill nobody can read about later is indistinguishable
// from sabotage, so the sweep's every shot lands here first.
//
// The event carries the run's current state on both ends of the transition,
// because a kill changes no state: it only explains a signal that was sent.
// Stamping the live state also keeps the event chain (I15) continuous even
// when another actor moved the run between the scan and this write.
func (s *Store) RecordOrphanKill(ctx context.Context, runID string, pid int) error {
	now := s.clk.Now().UTC()
	detail, err := json.Marshal(map[string]int{"pid": pid})
	if err != nil {
		return fmt.Errorf("encode the orphan kill detail: %w", err)
	}
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var state string
		if err := tx.QueryRow(`SELECT state FROM runs WHERE id = ?`, runID).Scan(&state); err != nil {
			return fmt.Errorf("read the state of run %s: %w", runID, err)
		}
		return appendRunEvent(tx, RunEvent{
			RunID:      runID,
			At:         now,
			Kind:       "run.orphan_killed",
			FromState:  state,
			ToState:    state,
			Actor:      "reconcile",
			DetailJSON: string(detail),
		})
	})
	if err != nil {
		return fmt.Errorf("record the orphan kill of pid %d on run %s: %w", pid, runID, err)
	}
	return nil
}

// OverrideBootIDForTest pins what the platform's boot id read reports from
// now on. Tests cannot edit /proc/sys/kernel/random/boot_id, and a changed
// boot id is the strongest evidence reconciliation has (issue #62), so the
// value itself is what gets replaced: call it before a start to stage one
// boot, and again with a different value to stage a machine restart. An
// empty value lifts the override.
//
// It exists for the startup reconciliation tests and their crash harness
// rows; production code has no reason to call it. It takes a value rather
// than a callback on purpose: the store's exported surface accepts no
// function values.
func (s *Store) OverrideBootIDForTest(boot string) {
	if boot == "" {
		s.bootOverride.Store(nil)
		return
	}
	s.bootOverride.Store(&boot)
}

// DumpForIdempotence renders every user table as ordered, comparable text,
// leaving out only the daemon session rows whose timestamps move on every
// start by design. Two calls bracketing a pass of OnStartup that wrote
// nothing come back equal, which is exactly the assertion AC7 and AC12 of
// issue #62 rest on: bit-identical after the first pass, untouched when
// healthy.
//
// The SQL lives here because all SQL lives here; callers get back plain
// strings they compare themselves.
func (s *Store) DumpForIdempotence(ctx context.Context) ([]string, error) {
	names, err := s.userTableNames(ctx)
	if err != nil {
		return nil, err
	}

	var out []string
	for _, name := range names {
		rows, err := s.r.QueryContext(ctx, `SELECT * FROM "`+name+`"`) // #nosec G202 - the name comes from sqlite_master itself (filtered above), never from user input; identifiers cannot be parameterised
		if err != nil {
			return nil, fmt.Errorf("dump table %s: %w", name, err)
		}
		cols, _ := rows.Columns()
		for rows.Next() {
			vals := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range vals {
				ptrs[i] = &vals[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan a row of %s: %w", name, err)
			}
			line := name
			for _, v := range vals {
				line += "\x1f" + renderDumpValue(v)
			}
			out = append(out, line)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("walk table %s: %w", name, err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close the walk of %s: %w", name, err)
		}
	}
	sort.Strings(out)
	return out, nil
}

// userTableNames lists the tables a dump covers: everything the migrations
// made, minus the session history whose timestamps are excluded from the
// idempotence promise by design. The engine's own bookkeeping tables are
// static once migrations finish and say nothing about reconciliation.
func (s *Store) userTableNames(ctx context.Context) ([]string, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT name FROM sqlite_master
WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
ORDER BY name`)
	if err != nil {
		return nil, fmt.Errorf("list the tables to dump: %w", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scan a table name: %w", err)
		}
		switch name {
		case "daemon_sessions", "schema_migrations", "schema_migration_lock":
			continue
		}
		out = append(out, name)
	}
	return out, rows.Err()
}

// renderDumpValue keeps NULL distinguishable from empty text and prints
// integers and floats in their natural forms, so two dumps differ only when
// the underlying values differ.
func renderDumpValue(v any) string {
	switch t := v.(type) {
	case nil:
		return "\x00"
	case []byte:
		return string(t)
	default:
		return fmt.Sprintf("%v", t)
	}
}
