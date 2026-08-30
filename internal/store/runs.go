package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/spec"
)

// Errors callers act on rather than report. Everything else that comes out of
// this file is a failure and reads as one.
var (
	// ErrRunNotFound is returned when no run has the id, or the id prefix.
	ErrRunNotFound = errors.New("no run has that id")

	// ErrAmbiguousRunID is returned when an id prefix matches more than one
	// run. The caller types more characters; nobody guesses on their behalf.
	ErrAmbiguousRunID = errors.New("the id prefix matches more than one run")

	// ErrConcurrencyKeyHeld is returned when an active run already holds the
	// concurrency key. It is an ordinary outcome, not a fault: the caller
	// records a skip with a reason code and moves on.
	ErrConcurrencyKeyHeld = errors.New("an active run already holds the concurrency key")
)

// listLimitDefault and listLimitMax bound a listing. Reads never paginate with
// OFFSET, so a caller walks a history by passing the last id back as Before,
// and the cap is what keeps one call from materialising the whole table.
const (
	listLimitDefault = 50
	listLimitMax     = 500
)

// State and origin are plain strings in the types below. internal/model owns
// the vocabulary and the transitions; the schema enforces the same values a
// second time through its CHECKs. Naming them here as well would give one rule
// three owners, and the third would be the one that drifts.

// NewRun is a run to materialise, with the steps it will run.
type NewRun struct {
	JobName      string
	JobVersionID string

	// TriggerID is the decision this run came out of, empty for a run nobody
	// triggered, such as a manual start.
	TriggerID string

	// Origin says which mechanism produced the run: schedule, sensor, manual,
	// retry, replay or backfill. The schema refuses anything else.
	Origin string

	// RunKey is the dedup key the trigger carried, empty when there is none.
	RunKey string

	// ConcurrencyKey caps how many runs sharing it may be active. Empty means
	// unlimited. A second active run on one key fails with
	// ErrConcurrencyKeyHeld, decided by the database and not by a read here.
	ConcurrencyKey string

	// ScheduledFor is the logical slot a schedule or backfill run belongs to,
	// zero for everything else. It is not when the run will start.
	ScheduledFor time.Time

	// AvailableAt is the earliest the run may be claimed. Zero means now. A
	// time in the future needs DeferReason: a run held back always says why.
	AvailableAt time.Time
	DeferReason string

	// ParamsJSON is the canonical parameter object, empty for none.
	ParamsJSON string

	// MaxAttempts is how many times the run may be attempted. Zero means one.
	MaxAttempts int

	// MaxParallel is how many steps of this run may be in flight at once, the
	// budget the claim predicate reads off the run row. Zero means the spec
	// default, which is what a job that declares nothing gets.
	MaxParallel int

	// ReplayOf is the run this one replays, empty when it replays nothing.
	ReplayOf string

	// Actor is who caused this, recorded on the queued event. Empty is system.
	Actor string

	// Steps are the run's steps in spec order, which is the order M1 runs them
	// in. Their dependency edges are frozen here and are never read from the
	// spec again.
	Steps []NewStep
}

// NewStep is one step of a run to materialise.
type NewStep struct {
	Name string

	// DependsOn are the steps that have to finish first. The names have to be
	// steps of the same run; nothing checks that here, because M1 runs steps in
	// order and M4-02 is what reads the edges.
	DependsOn []string

	// MaxAttempts is how many times the step may be attempted. Zero means one.
	MaxAttempts int
}

// Run is one attempt at one job version.
type Run struct {
	ID             string
	JobName        string
	JobVersionID   string
	TriggerID      string
	Origin         string
	RunKey         string
	State          string
	ConcurrencyKey string
	AvailableAt    time.Time
	DeferReason    string
	ScheduledFor   time.Time
	ParamsJSON     string
	Attempt        int
	MaxAttempts    int

	// LeaseOwner and LeaseEpoch say who is executing the run now. The epoch
	// is the fencing token: it rises on every claim and on every reap, so a
	// writer whose lease was taken can always be told apart from the current
	// holder by comparing numbers. Every result write proves its standing by
	// carrying both.
	LeaseOwner     string
	LeaseEpoch     int64
	LeaseExpiresAt time.Time

	// HeartbeatAt is the last renewal of the lease. It moves in one batched
	// transaction for every run an owner holds.
	HeartbeatAt time.Time

	// CrashCount is how many executors have died holding this run's lease.
	// Crossing the reaper's crash budget ends the run in the poison
	// quarantine instead of offering it to yet another executor.
	CrashCount int

	// ReplayOf is the run this one replays, empty when it replays nothing.
	// It is what ties a replay back to the attempt it was made from.
	ReplayOf string

	// The cancellation request, durable before anything is killed. Empty
	// time means nobody asked.
	CancelRequestedAt time.Time
	CancelRequestedBy string
	CancelReason      string

	ReasonCode string
	ReasonText string

	// ReasonData is the canonical detail object beside the reason code: for
	// a failed run, which step failed and why the run ended as it did.
	ReasonData string

	Error      string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time
	UpdatedAt  time.Time
}

// Step is one step of a run.
type Step struct {
	Name        string
	Index       int
	State       string
	Attempt     int
	MaxAttempts int

	// ExitCode is meaningful only when HasExitCode is set. A step killed by a
	// signal, or one that never started, has no exit code at all, and zero is a
	// perfectly ordinary success.
	ExitCode    int
	HasExitCode bool

	Signal     string
	StartedAt  time.Time
	FinishedAt time.Time
	DurationMS int64
	ReasonCode string
	ReasonText string
	ReasonData string
	Error      string
	LogPath    string

	// LogBytes, LogTruncated and ErrorTail are the log facts that live in
	// the database: how big the log grew, whether the quota cut it, and the
	// last few KiB of output. The tail is what explain shows after the log
	// file itself is gone.
	LogBytes     int64
	LogTruncated bool
	ErrorTail    string

	// OutcomeSource names where the verdict came from (#39): 'direct' when
	// the executor waited on the process itself, 'spool' when it was read
	// from the attempt's result spool after a crash, 'reconciled' when
	// recovery had no source and had to assume. Empty means the row was
	// written before the column existed, which reads as the direct era.
	OutcomeSource string

	// NextAttemptAt is when a pending step that failed with attempts left
	// becomes runnable again. M1 computes no retries, so nothing sets it
	// yet; the claim gate reads it all the same, because M1-09 will fill
	// it without touching a single reader.
	NextAttemptAt time.Time

	// Artifacts are the references the step published through its
	// $PACEQ_OUTPUT (#13). Only a succeeded step has any; they are filled
	// by GetRun and RunsArtifacts and never written here.
	Artifacts []Artifact
}

// RunDetail is a run with its steps, which is what showing one run needs.
// Events are not here: they are the explain query, and they are read on their
// own because most callers do not want them.
type RunDetail struct {
	Run
	Steps []Step
}

// RunSummary is one line of a listing.
type RunSummary struct {
	ID         string
	JobName    string
	Origin     string
	State      string
	ReasonCode string
	CreatedAt  time.Time
	StartedAt  time.Time
	FinishedAt time.Time

	// The deferral facts (#68): available_at decides whether a queued row
	// reads as deferred, defer_reason names why it waits, and reason_data
	// carries the blocking run id the display points at.
	AvailableAt time.Time
	DeferReason string
	ReasonData  string
}

// RunFilter narrows a listing. The zero filter lists the newest runs of every
// job.
type RunFilter struct {
	JobName string

	// States restricts the listing to these run states. Empty means all.
	States []string

	// Before is the keyset cursor: only runs whose id sorts below it. Ids are
	// ULIDs, so this walks the history backwards in time. Empty starts at the
	// newest run.
	Before string

	// Limit caps the page. Zero is 50 and anything above 500 is 500.
	Limit int
}

// RunEvent is one state transition, as explain will read it back.
type RunEvent struct {
	RunID string

	// StepName is set on an event about a step, empty on one about the run.
	StepName string

	// At is when it happened. Zero means now.
	At time.Time

	// Kind names the transition: run.queued, step.started, step.retry_scheduled
	// and so on.
	Kind       string
	FromState  string
	ToState    string
	ReasonCode string

	// Actor is who caused it: system, reaper, operator, cli:<uid>. Empty is
	// system.
	Actor string

	// DetailJSON is the canonical detail object. Empty is an empty object.
	DetailJSON string
}

// CreateRunWithSteps materialises a run: the run row, every step, the frozen
// dependency edges and the queued event, in one transaction. A crash anywhere
// in it leaves no rows at all, so there is no such thing as a run without its
// steps or a transition without its event.
//
// A concurrency key held by another active run returns ErrConcurrencyKeyHeld.
// The database decides that, through the partial unique index, and this method
// never reads the key first: a read followed by a write is a decision made on
// information that can already be out of date.
func (s *Store) CreateRunWithSteps(ctx context.Context, in NewRun) (Run, error) {
	now := s.clk.Now().UTC()
	runID, err := id.New(now)
	if err != nil {
		return Run{}, fmt.Errorf("mint a run id: %w", err)
	}

	run := Run{
		ID:             runID,
		JobName:        in.JobName,
		JobVersionID:   in.JobVersionID,
		TriggerID:      in.TriggerID,
		Origin:         in.Origin,
		RunKey:         in.RunKey,
		State:          "queued",
		ConcurrencyKey: in.ConcurrencyKey,
		AvailableAt:    in.AvailableAt,
		DeferReason:    in.DeferReason,
		ScheduledFor:   in.ScheduledFor,
		ParamsJSON:     in.ParamsJSON,
		MaxAttempts:    in.MaxAttempts,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if run.AvailableAt.IsZero() {
		run.AvailableAt = now
	}
	if run.ParamsJSON == "" {
		run.ParamsJSON = "{}"
	}
	if run.MaxAttempts == 0 {
		run.MaxAttempts = 1
	}
	maxParallel := in.MaxParallel
	if maxParallel < 1 {
		maxParallel = spec.DefaultMaxParallel
	}

	at := now.UnixMilli()
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		// DO NOTHING against the partial index rather than a caught error: the
		// conflict target names the index, so a conflict on anything else, such
		// as a repeated run id, still fails loudly.
		result, err := tx.Exec(`INSERT INTO runs
	(id, job_name, job_version_id, trigger_id, origin, run_key, state, available_at,
	 defer_reason, scheduled_for, params_json, concurrency_key, max_attempts, replay_of,
	 created_at, updated_at, max_parallel)
VALUES (?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (concurrency_key) WHERE concurrency_key IS NOT NULL
	AND state IN ('queued', 'running') DO NOTHING`,
			run.ID, run.JobName, run.JobVersionID, nullIfEmpty(run.TriggerID), run.Origin,
			nullIfEmpty(run.RunKey), run.AvailableAt.UnixMilli(), nullIfEmpty(run.DeferReason),
			nullTime(run.ScheduledFor), run.ParamsJSON, nullIfEmpty(run.ConcurrencyKey),
			run.MaxAttempts, nullIfEmpty(in.ReplayOf), at, at, maxParallel)
		if err != nil {
			return fmt.Errorf("create a run of job %s: %w", run.JobName, err)
		}
		written, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("create a run of job %s: %w", run.JobName, err)
		}
		if written == 0 {
			return fmt.Errorf("create a run of job %s: %w (%s)",
				run.JobName, ErrConcurrencyKeyHeld, run.ConcurrencyKey)
		}

		for i, step := range in.Steps {
			maxAttempts := step.MaxAttempts
			if maxAttempts == 0 {
				maxAttempts = 1
			}
			if _, err := tx.Exec(`INSERT INTO steps (run_id, name, idx, state, max_attempts)
VALUES (?, ?, ?, 'pending', ?)`, run.ID, step.Name, i, maxAttempts); err != nil {
				return fmt.Errorf("create step %s of run %s: %w", step.Name, run.ID, err)
			}
			for _, upstream := range step.DependsOn {
				if _, err := tx.Exec(`INSERT INTO step_deps (run_id, step_name, depends_on)
VALUES (?, ?, ?)`, run.ID, step.Name, upstream); err != nil {
					return fmt.Errorf("freeze the edge %s -> %s of run %s: %w",
						upstream, step.Name, run.ID, err)
				}
			}
		}

		return appendRunEvent(tx, RunEvent{
			RunID:      run.ID,
			At:         now,
			Kind:       "run.queued",
			ToState:    "queued",
			ReasonCode: run.DeferReason,
			Actor:      in.Actor,
		})
	})
	if err != nil {
		return Run{}, err
	}
	return run, nil
}

// AppendRunEvent records one transition on its own. Transitions that happen
// inside a write of their own carry their event in that same transaction
// instead, which is what keeps the history from disagreeing with the state.
func (s *Store) AppendRunEvent(ctx context.Context, e RunEvent) error {
	if e.At.IsZero() {
		e.At = s.clk.Now().UTC()
	}
	return s.withTx(ctx, func(tx *sql.Tx) error {
		return appendRunEvent(tx, e)
	})
}

// appendRunEvent writes one event inside a caller's transaction. It takes no
// context: a write transaction is not a place to call out of the process from,
// and the transaction already carries the caller's deadline.
func appendRunEvent(tx *sql.Tx, e RunEvent) error {
	actor := e.Actor
	if actor == "" {
		actor = "system"
	}
	detail := e.DetailJSON
	if detail == "" {
		detail = "{}"
	}
	_, err := tx.Exec(`INSERT INTO run_events
	(run_id, step_name, at, kind, from_state, to_state, reason_code, actor, detail_json)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.RunID, nullIfEmpty(e.StepName), e.At.UnixMilli(), e.Kind, nullIfEmpty(e.FromState),
		nullIfEmpty(e.ToState), nullIfEmpty(e.ReasonCode), actor, detail)
	if err != nil {
		return fmt.Errorf("record %s on run %s: %w", e.Kind, e.RunID, err)
	}
	return nil
}

// runColumns is the run projection, shared by the lookup and the read back, so
// the two cannot drift apart.
const runColumns = `id, job_name, job_version_id, trigger_id, origin, run_key, state,
	concurrency_key, available_at, defer_reason, scheduled_for, params_json, attempt,
	max_attempts, lease_owner, lease_epoch, lease_expires_at, heartbeat_at,
	cancel_requested_at, cancel_requested_by, cancel_reason, reason_code, reason_text,
	reason_data, error, crash_count, created_at, started_at, finished_at, updated_at,
	replay_of`

// GetRun reads one run and its steps. The argument is a whole id or any prefix
// of one, the way a git object is named: ids are ULIDs, so a prefix is a range
// scan on the primary key and not a scan of the table.
//
// A prefix matching more than one run is ErrAmbiguousRunID rather than the
// first match. Picking one would mean cancelling or replaying whichever run the
// database happened to order first.
//
// The run and its steps are two queries against the read pool, and the pool has
// many connections, so they are two snapshots and not one. A step that changes
// between them shows up as a step further along than the run says. That is the
// deliberate price of never opening an explicit read transaction: a read
// snapshot held open across a listing is what stops WAL checkpointing and grows
// the file without bound. Anything that has to see one instant reads it inside
// a write transaction instead.
func (s *Store) GetRun(ctx context.Context, idOrPrefix string) (RunDetail, error) {
	span, err := id.PrefixRange(idOrPrefix)
	if err != nil {
		return RunDetail{}, fmt.Errorf("look up run %q: %w", idOrPrefix, err)
	}

	var detail RunDetail
	err = s.withRead(ctx, func(ctx context.Context, r reader) error {
		// Two rows are read where one is wanted: the second is what tells an
		// ambiguous prefix from a unique one, and LIMIT 2 stops there.
		rows, err := r.QueryContext(ctx, `SELECT `+runColumns+`
FROM runs WHERE id >= ? AND id < ? ORDER BY id LIMIT 2`, span.Lower, span.Upper)
		if err != nil {
			return fmt.Errorf("look up run %q: %w", idOrPrefix, err)
		}
		found, err := scanRuns(rows)
		if err != nil {
			return fmt.Errorf("look up run %q: %w", idOrPrefix, err)
		}
		switch len(found) {
		case 0:
			return fmt.Errorf("look up run %q: %w", idOrPrefix, ErrRunNotFound)
		case 1:
		default:
			return fmt.Errorf("look up run %q: %w (%s and %s)",
				idOrPrefix, ErrAmbiguousRunID, found[0].ID, found[1].ID)
		}
		detail.Run = found[0]

		detail.Steps, err = readSteps(ctx, r, detail.Run.ID)
		if err != nil {
			return err
		}
		if err := attachArtifacts(ctx, r, detail.Run.ID, detail.Steps); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return RunDetail{}, err
	}
	return detail, nil
}

// ListRuns is one page of run history, newest first. Ids are ULIDs, so id order
// is time order and the cursor is the last id of the previous page.
//
// There is no OFFSET anywhere in this package. An offset re-reads everything it
// skips, and a listing that holds a read snapshot open while it does so is what
// starves WAL checkpointing and grows the file without bound.
func (s *Store) ListRuns(ctx context.Context, f RunFilter) ([]RunSummary, error) {
	where := make([]string, 0, 3)
	args := make([]any, 0, len(f.States)+3)
	if f.JobName != "" {
		where = append(where, "job_name = ?")
		args = append(args, f.JobName)
	}
	if len(f.States) > 0 {
		where = append(where, "state IN ("+strings.TrimSuffix(strings.Repeat("?, ", len(f.States)), ", ")+")")
		for _, state := range f.States {
			args = append(args, state)
		}
	}
	if f.Before != "" {
		where = append(where, "id < ?")
		args = append(args, f.Before)
	}

	limit := f.Limit
	if limit <= 0 {
		limit = listLimitDefault
	}
	if limit > listLimitMax {
		limit = listLimitMax
	}
	args = append(args, limit)

	query := `SELECT id, job_name, origin, state, reason_code,
available_at, defer_reason, reason_data, created_at, started_at, finished_at
FROM runs`
	if len(where) > 0 {
		query += "\nWHERE " + strings.Join(where, " AND ")
	}
	query += "\nORDER BY id DESC LIMIT ?"

	var out []RunSummary
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var (
				sum        RunSummary
				reason     sql.NullString
				deferRed   sql.NullString
				reasonData sql.NullString
				createdAt  int64
				startedAt  sql.NullInt64
				finishedAt sql.NullInt64
				availAt    int64
			)
			if err := rows.Scan(&sum.ID, &sum.JobName, &sum.Origin, &sum.State, &reason,
				&availAt, &deferRed, &reasonData,
				&createdAt, &startedAt, &finishedAt); err != nil {
				return fmt.Errorf("scan a run: %w", err)
			}
			sum.ReasonCode = reason.String
			sum.DeferReason = deferRed.String
			sum.ReasonData = reasonData.String
			sum.AvailableAt = time.UnixMilli(availAt).UTC()
			sum.CreatedAt = time.UnixMilli(createdAt).UTC()
			sum.StartedAt = timeOrZero(startedAt)
			sum.FinishedAt = timeOrZero(finishedAt)
			out = append(out, sum)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("list runs: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// scanRuns reads a run projection in the order runColumns names. It closes the
// rows: an unread result set on the single write connection deadlocks the
// process against itself, and the read pool deserves the same discipline.
func scanRuns(rows *sql.Rows) ([]Run, error) {
	defer func() { _ = rows.Close() }()

	var out []Run
	for rows.Next() {
		var (
			run                                  Run
			trigger, runKey, concurrency, params sql.NullString
			deferReason, reasonCode, reasonText  sql.NullString
			reasonData                           sql.NullString
			failure                              sql.NullString
			leaseOwner, cancelBy, cancelReason   sql.NullString
			scheduledFor, startedAt, finishedAt  sql.NullInt64
			leaseExpiresAt, cancelRequestedAt    sql.NullInt64
			heartbeatAt                          sql.NullInt64
			availableAt, createdAt, updatedAt    int64
			leaseEpoch, crashCount               int64
			replayOf                             sql.NullString
		)
		if err := rows.Scan(&run.ID, &run.JobName, &run.JobVersionID, &trigger, &run.Origin,
			&runKey, &run.State, &concurrency, &availableAt, &deferReason, &scheduledFor, &params,
			&run.Attempt, &run.MaxAttempts, &leaseOwner, &leaseEpoch, &leaseExpiresAt,
			&heartbeatAt, &cancelRequestedAt, &cancelBy, &cancelReason, &reasonCode, &reasonText,
			&reasonData, &failure, &crashCount, &createdAt, &startedAt, &finishedAt, &updatedAt,
			&replayOf); err != nil {
			return nil, err
		}
		run.ReplayOf = replayOf.String
		run.TriggerID = trigger.String
		run.RunKey = runKey.String
		run.ConcurrencyKey = concurrency.String
		run.DeferReason = deferReason.String
		run.ParamsJSON = params.String
		run.ReasonCode = reasonCode.String
		run.ReasonText = reasonText.String
		run.ReasonData = reasonData.String
		run.Error = failure.String
		run.LeaseOwner = leaseOwner.String
		run.LeaseEpoch = leaseEpoch
		run.LeaseExpiresAt = timeOrZero(leaseExpiresAt)
		run.HeartbeatAt = timeOrZero(heartbeatAt)
		run.CrashCount = int(crashCount)
		if cancelRequestedAt.Valid {
			run.CancelRequestedAt = time.UnixMilli(cancelRequestedAt.Int64).UTC()
		}
		run.CancelRequestedBy = cancelBy.String
		run.CancelReason = cancelReason.String
		run.AvailableAt = time.UnixMilli(availableAt).UTC()
		run.ScheduledFor = timeOrZero(scheduledFor)
		run.CreatedAt = time.UnixMilli(createdAt).UTC()
		run.StartedAt = timeOrZero(startedAt)
		run.FinishedAt = timeOrZero(finishedAt)
		run.UpdatedAt = time.UnixMilli(updatedAt).UTC()
		out = append(out, run)
	}
	return out, rows.Err()
}

// readSteps is the steps of one run in spec order.
func readSteps(ctx context.Context, r reader, runID string) ([]Step, error) {
	rows, err := r.QueryContext(ctx, `SELECT name, idx, state, attempt, max_attempts, exit_code,
signal, started_at, finished_at, duration_ms, reason_code, reason_text, reason_data, error,
log_path, log_bytes, log_truncated, error_tail, next_attempt_at, outcome_source
FROM steps WHERE run_id = ? ORDER BY idx`, runID)
	if err != nil {
		return nil, fmt.Errorf("read the steps of run %s: %w", runID, err)
	}
	return scanSteps(rows)
}

// scanSteps reads the steps projection, shared by every step read in the
// package so the columns cannot drift between the read pool and a write
// transaction. It closes the rows: an unread result set on the single write
// connection deadlocks the process against itself.
func scanSteps(rows *sql.Rows) ([]Step, error) {
	defer func() { _ = rows.Close() }()

	var out []Step
	for rows.Next() {
		var (
			step                      Step
			exitCode                  sql.NullInt64
			signal, reasonCode        sql.NullString
			reasonText                sql.NullString
			failure, logs, outcomeSrc sql.NullString
			reasonData                sql.NullString
			startedAt, finishedAt     sql.NullInt64
			durationMS                sql.NullInt64
			nextAttemptAt             sql.NullInt64
		)
		errorTail := sql.NullString{}
		if err := rows.Scan(&step.Name, &step.Index, &step.State, &step.Attempt, &step.MaxAttempts,
			&exitCode, &signal, &startedAt, &finishedAt, &durationMS, &reasonCode, &reasonText,
			&reasonData, &failure, &logs, &step.LogBytes, &step.LogTruncated, &errorTail,
			&nextAttemptAt, &outcomeSrc); err != nil {
			return nil, fmt.Errorf("scan a step: %w", err)
		}
		step.ExitCode = int(exitCode.Int64)
		step.HasExitCode = exitCode.Valid
		step.Signal = signal.String
		step.StartedAt = timeOrZero(startedAt)
		step.FinishedAt = timeOrZero(finishedAt)
		step.DurationMS = durationMS.Int64
		step.ReasonCode = reasonCode.String
		step.ReasonText = reasonText.String
		step.ReasonData = reasonData.String
		step.Error = failure.String
		step.LogPath = logs.String
		step.ErrorTail = errorTail.String
		step.NextAttemptAt = timeOrZero(nextAttemptAt)
		step.OutcomeSource = outcomeSrc.String
		out = append(out, step)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan a step: %w", err)
	}
	return out, nil
}

// nullTime keeps an unset time out of the database as NULL. A zero time is not
// a time anything happened at, and stored as 1970 it would sort as one.
func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UnixMilli()
}

// timeOrZero is the other direction: an absent timestamp is the zero time, not
// the epoch.
func timeOrZero(ms sql.NullInt64) time.Time {
	if !ms.Valid {
		return time.Time{}
	}
	return time.UnixMilli(ms.Int64).UTC()
}
