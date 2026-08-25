package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/faults"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The run lease. Ownership of a run is explicit and time bounded: one
// statement claims, one transaction renews every run the owner holds at once,
// the reaper takes what expired and hands it to a new holder, and every write
// from a holder carries the fencing token so a frozen worker can never
// overwrite its successor's verdict (issue #60).
//
// The timing constants are the ones the issue fixes. The ttl is renewed every
// tick a third its length, so two lost renewals are tolerated before
// leadership of a run is even in question. The skew allowance is what keeps
// the reaper late on purpose: the owner measures its budget on a monotonic
// clock and gives up early; the reaper waits out the wall clock plus skew and
// takes over sent.
const (
	// DefaultRunLeaseTTL is how long a claim lasts. Sixty seconds, renewed
	// every twenty.
	DefaultRunLeaseTTL = 60 * time.Second

	// DefaultClockSkewAllowance is how long past the expiry the reaper waits
	// before it takes a run. Ten seconds.
	DefaultClockSkewAllowance = 10 * time.Second

	// DefaultRequeueBackoff is how long a reaped run waits before it is due
	// again. The crash already cost a ttl; the backoff keeps a job that kills
	// its executor from burning the queue the moment it is requeued.
	DefaultRequeueBackoff = 30 * time.Second

	// DefaultMaxCrashCount is the poison quarantine line (02 section 5.7):
	// once a run has outlived this many executors, the next reap fails it for
	// good instead of requeueing it.
	DefaultMaxCrashCount = 5

	// reapLimit caps one sweep. The dispatcher re-reads on every wake, so a
	// backlog longer than this is processed over ticks instead of materialised
	// in one slice.
	reapLimit = 500
)

// ErrLeaseLost is what every result write answers when the writer's lease has
// gone: another holder claimed after a reap, or the row moved on. A caller
// that sees it must discard its result and stop working on the run; the
// engine turns that into a run.result_discarded event and kills its process
// group.
var ErrLeaseLost = errors.New("the run lease was lost")

// LeaseRef is who claims to hold a run's lease, and at which fencing token.
// Every result write proves its standing by carrying one.
//
// The zero LeaseRef means the system itself, writing against an executor that
// is already gone: recovery closing a dead attempt's steps. That path is real,
// and it is gated the other way round: the store refuses it while the lease is
// still live, because a live lease means someone else is driving.
type LeaseRef struct {
	Owner string
	Epoch int64
}

// held reports whether the ref speaks for a holder rather than for recovery.
func (r LeaseRef) held() bool { return r.Owner != "" }

// ClaimSpec says who claims and how much.
type ClaimSpec struct {
	// Owner is the executor name every later write must come from.
	Owner string

	// TTL is how long each claim lasts. Zero means DefaultRunLeaseTTL.
	TTL time.Duration

	// Limit caps the batch. Zero means one.
	Limit int

	// Only restricts the claim to these ids. An executor claiming the exact
	// run it was handed uses this; the empty slice claims whatever is due.
	Only []string
}

// ClaimedRun is one run this process just took: everything ExecuteRun needs to
// drive it, with the fencing token first among equals.
type ClaimedRun struct {
	ID           string
	JobName      string
	JobVersionID string
	RunKey       string
	Attempt      int
	LeaseEpoch   int64
	ParamsJSON   string
}

// claimRunsSQL flips the chosen rows to running: one named owner, each epoch
// up by exactly one, fencing tokens coming back with the rows. The candidate
// selection is no longer inside this statement (#68): the claim now decides
// per job under the job's ceiling, and a quota decision cannot live in a
// LIMIT clause. The choice happens in claimTx, in the same BEGIN IMMEDIATE
// transaction as this update, which is what keeps the window between choosing
// and taking shut.
//
// The predicate rides the partial claim index (available_at WHERE state =
// 'queued'), which the query plan test pins.
const claimRunsSQL = `UPDATE runs SET
	state = 'running',
	lease_owner = ?1,
	lease_epoch = lease_epoch + 1,
	lease_expires_at = ?3,
	heartbeat_at = ?2,
	started_at = COALESCE(started_at, ?2),
	updated_at = ?2
WHERE id IN (?4)
RETURNING id, job_name, job_version_id, run_key, attempt, lease_epoch, params_json`

// claimCandidatesSQL reads the due queue in claim order with each job's
// ceiling beside it. It is the read half of the claim's read-decide-write,
// served by idx_runs_claim exactly as the old embedded subselect was; the
// query plan test pins this statement too. The Only clause slots in ahead of
// the order, which is where a predicate belongs.
//
// Rows whose defer_reason is "concurrency_key" (#17, model's
// DeferReasonConcurrencyKey) never appear here. A keyless deferral holds no
// key and must not be started by the ordinary queue path; its way out is the
// keyed start in concurrency_key.go, and nowhere else.
const claimCandidatesSQL = `SELECT r.id, r.job_name, j.max_concurrent
FROM runs r
JOIN jobs j ON j.name = r.job_name
WHERE r.state = 'queued'
	AND r.available_at <= ?
	AND r.cancel_requested_at IS NULL
	AND (r.defer_reason IS NULL OR r.defer_reason <> 'concurrency_key')
%s
ORDER BY r.scheduled_for, r.created_at, r.id`

// closeCancelledQueuedSQL closes queued runs whose cancellation was requested
// before anyone claimed them. There is no process group to kill, which makes
// this the cheapest cancellation in the system. It runs in the claim
// transaction, so a run asked to stop can never slip through a claim cycle
// and start anyway.
//
// nolint:fencing: queued rows hold no lease, so there is no token to fence
// with; the cancel request itself is the authority this statement acts on.
const closeCancelledQueuedSQL = `UPDATE runs SET
	state = 'cancelled',
	finished_at = ?1,
	reason_code = ?2,
	defer_reason = NULL,
	updated_at = ?1
WHERE id IN (
	SELECT id FROM runs
	WHERE state = 'queued' AND cancel_requested_at IS NOT NULL
	LIMIT ?3
)
RETURNING id, lease_epoch, cancel_requested_by`

// ClaimRuns claims up to Limit due runs for Owner in one transaction: the
// cancel sweep for queued runs asked to stop, then the claim statement, then
// one run.started event per claimed row. Everything lands together or not at
// all, which is why a crash mid-claim leaves every run exactly as it was.
//
// Runs that are not due yet, already claimed, or asked to stop come back some
// other cycle; the result only names what this call took.
func (s *Store) ClaimRuns(ctx context.Context, spec ClaimSpec) ([]ClaimedRun, error) {
	if spec.Owner == "" {
		return nil, errors.New("claim runs: no owner was named")
	}
	ttl := spec.TTL
	if ttl <= 0 {
		ttl = DefaultRunLeaseTTL
	}
	limit := spec.Limit
	if limit <= 0 {
		limit = 1
	}
	now := s.clk.Now().UTC()

	var out []ClaimedRun
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		if err := closeCancelledQueuedTx(tx, now); err != nil {
			return err
		}
		return claimTx(tx, spec, now, now.Add(ttl), limit, &out)
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// claimTx is the claim's read-decide-write, all inside one BEGIN IMMEDIATE
// transaction: read the due queue with each job's ceiling, count the running
// rows per job, choose candidates in order while the ceiling has room, then
// flip exactly the chosen rows. The writer pool has one connection and the
// transaction takes the write lock first, so nothing can commit between the
// count and the flip; the decision cannot be acting on numbers that moved.
//
// The running count is the baseline on purpose: counting a candidate against
// its own job's ceiling would deadlock an empty machine (one due run, limit
// one, and the count already full). A deferred run whose available_at has
// arrived therefore claims like any other queued row, but only into a slot.
func claimTx(tx *sql.Tx, spec ClaimSpec, now, expires time.Time, limit int, out *[]ClaimedRun) error {
	ids, err := chooseClaimableTx(tx, spec, now, limit)
	if err != nil {
		return err
	}

	if len(ids) > 0 {
		query := strings.Replace(claimRunsSQL,
			"IN (?4)", "IN ("+idFilter(ids)+")", 1)
		args := []any{spec.Owner, now.UnixMilli(), expires.UnixMilli()}
		for _, id := range ids {
			args = append(args, id)
		}
		rows, err := tx.Query(query, args...)
		if err != nil {
			return fmt.Errorf("claim runs for %s: %w", spec.Owner, err)
		}

		for rows.Next() {
			var cl ClaimedRun
			var runKey sql.NullString
			if err := rows.Scan(&cl.ID, &cl.JobName, &cl.JobVersionID, &runKey,
				&cl.Attempt, &cl.LeaseEpoch, &cl.ParamsJSON); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan a claimed run: %w", err)
			}
			cl.RunKey = runKey.String
			*out = append(*out, cl)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan a claimed run: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close the claimed rows: %w", err)
		}
	}

	// Key deferrals come last, within whatever budget the ordinary queue
	// left over (#17): they already waited a backoff, and the order stays
	// deterministic for goldens and crash replays.
	if remaining := limit - len(*out); remaining > 0 {
		if err := claimKeyDeferredTx(tx, spec, now, expires, remaining, out); err != nil {
			return err
		}
	}

	// The claim statement has executed but nothing is committed, which is
	// exactly the window the crash harness kills in (#75): a process lost
	// here rolls back to queued and loses nothing.
	faults.Point("M1:transition:after_update:claim")

	for _, cl := range *out {
		if err := appendRunEvent(tx, RunEvent{
			RunID:      cl.ID,
			At:         now,
			Kind:       "run.started",
			FromState:  string(model.RunQueued),
			ToState:    string(model.RunRunning),
			Actor:      spec.Owner,
			DetailJSON: epochDetail(cl.LeaseEpoch),
		}); err != nil {
			return err
		}
	}
	return nil
}

// chooseClaimableTx walks the due queue in claim order and keeps every run
// whose job still has room under max_concurrent. The overall batch limit
// caps the walk; the per job ceilings cap what may start. Both are applied
// before anything is written, so a refusal costs a read and never a rollback.
func chooseClaimableTx(tx *sql.Tx, spec ClaimSpec, now time.Time, limit int) ([]string, error) {
	onlyClause := ""
	if len(spec.Only) > 0 {
		onlyClause = "AND r.id IN (" + strings.TrimSuffix(strings.Repeat("?, ", len(spec.Only)), ", ") + ")"
	}
	query := fmt.Sprintf(claimCandidatesSQL, onlyClause)
	args := []any{now.UnixMilli()}
	for _, id := range spec.Only {
		args = append(args, id)
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("read the claim candidates for %s: %w", spec.Owner, err)
	}
	type candidate struct {
		id    string
		job   string
		limit int
	}
	var cands []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.id, &c.job, &c.limit); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan a claim candidate for %s: %w", spec.Owner, err)
		}
		cands = append(cands, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("read the claim candidates for %s: %w", spec.Owner, err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close the claim candidates of %s: %w", spec.Owner, err)
	}

	held, err := runningCountsTx(tx)
	if err != nil {
		return nil, err
	}
	chosen := make([]string, 0, min(len(cands), limit))
	for _, c := range cands {
		if len(chosen) == limit {
			break
		}
		if c.limit < 1 {
			c.limit = 1
		}
		if held[c.job] >= c.limit {
			continue
		}
		held[c.job]++
		chosen = append(chosen, c.id)
	}
	return chosen, nil
}

// runningCountsTx counts the running rows per job inside the caller's
// transaction. It is the claim side's occupancy baseline; see claimTx for why
// it counts only running rows.
const runningCountsSQL = `SELECT job_name, COUNT(*) FROM runs
WHERE state = 'running'
GROUP BY job_name`

func runningCountsTx(tx *sql.Tx) (map[string]int, error) {
	rows, err := tx.Query(runningCountsSQL)
	if err != nil {
		return nil, fmt.Errorf("count the running runs: %w", err)
	}
	defer func() { _ = rows.Close() }()
	counts := map[string]int{}
	for rows.Next() {
		var job string
		var n int
		if err := rows.Scan(&job, &n); err != nil {
			return nil, fmt.Errorf("scan a running count: %w", err)
		}
		counts[job] = n
	}
	return counts, rows.Err()
}

// idFilter builds the explicit id membership test the claim update runs
// against, one placeholder per chosen id. The placeholders are numbered ?4
// onward, after the three fixed parameters.
func idFilter(ids []string) string {
	named := make([]string, len(ids))
	for i := range ids {
		named[i] = fmt.Sprintf("?%d", i+4)
	}
	return strings.Join(named, ", ")
}

func closeCancelledQueuedTx(tx *sql.Tx, now time.Time) error {
	rows, err := tx.Query(closeCancelledQueuedSQL,
		now.UnixMilli(), string(reason.RUNCancelledManual), claimableLimit)
	if err != nil {
		return fmt.Errorf("close queued cancelled runs: %w", err)
	}
	type stopped struct {
		id    string
		epoch int64
		by    sql.NullString
	}
	var closed []stopped
	for rows.Next() {
		var c stopped
		if err := rows.Scan(&c.id, &c.epoch, &c.by); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan a closed cancellation: %w", err)
		}
		closed = append(closed, c)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("scan a closed cancellation: %w", err)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close the cancellations: %w", err)
	}
	for _, c := range closed {
		actor := c.by.String
		if actor == "" {
			actor = "system"
		}
		// A cancelled run carries no open steps past this transaction (I2):
		// its pending steps leave through their own events, the same way any
		// step leaves pending when its run ends without it.
		if err := skipPendingStepsTx(tx, c.id, now, string(reason.STEPCancelled)); err != nil {
			return err
		}
		if err := appendRunEvent(tx, RunEvent{
			RunID:      c.id,
			At:         now,
			Kind:       "run.cancelled",
			FromState:  string(model.RunQueued),
			ToState:    string(model.RunCancelled),
			ReasonCode: string(reason.RUNCancelledManual),
			Actor:      actor,
			DetailJSON: epochDetail(c.epoch),
		}); err != nil {
			return err
		}
	}
	return nil
}

// ClaimRun claims exactly this run for the caller. It is the single run form
// of ClaimRuns and shares its one statement.
//
// The returned state is "running" with the new fencing token, or "cancelled"
// when a cancellation was waiting before the run ever started: the sweep in
// the same transaction closed it, and there was never a lease to hand out.
// Anything else is ErrNotClaimable.
func (s *Store) ClaimRun(ctx context.Context, runID string, in LeaseInput) (string, int64, error) {
	if in.TTL <= 0 {
		in.TTL = DefaultRunLeaseTTL
	}
	got, err := s.ClaimRuns(ctx, ClaimSpec{
		Owner: in.Owner,
		TTL:   in.TTL,
		Only:  []string{runID},
	})
	if err != nil {
		return "", 0, err
	}
	if len(got) == 1 {
		return string(model.RunRunning), got[0].LeaseEpoch, nil
	}

	// Not claimed. Either somebody beat us to it, or a cancel request was
	// waiting and the sweep closed the run in this very transaction. Read the
	// row to say which, without writing anything.
	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		return "", 0, fmt.Errorf("claim run %s: %w", runID, err)
	}
	switch model.RunState(detail.Run.State) {
	case model.RunCancelled:
		return string(model.RunCancelled), detail.Run.LeaseEpoch, nil
	default:
		return "", 0, fmt.Errorf("claim run %s: %w (%s)", runID, ErrNotClaimable, detail.Run.State)
	}
}

// LeaseRenewal is one held run's answer to the heartbeat: the token the owner
// still holds, and any cancellation request that arrived since. The answer is
// the whole two way channel (11 section 4.3): cancellation needs no push, no
// extra polling and no second mechanism, because every renewal carries it.
type LeaseRenewal struct {
	ID                string
	LeaseEpoch        int64
	CancelRequestedAt time.Time
	CancelRequestedBy string
}

// renewRunLeasesSQL moves the deadline and the heartbeat stamp of every run
// the owner still holds, in one statement, and reports each row's token and
// cancel request back. Rows that have left the owner do not appear: silence
// about a run is the answer that says the lease is gone.
//
// nolint:fencing: the renewal keys on lease_owner, which is itself the
// ownership proof; the fencing token deliberately does not move here, or
// every renewal would fence its own holder out.
const renewRunLeasesSQL = `UPDATE runs SET
	heartbeat_at = ?1,
	lease_expires_at = ?2,
	updated_at = ?1
WHERE lease_owner = ?3 AND state = 'running'
RETURNING id, lease_epoch, cancel_requested_at, cancel_requested_by`

// RenewRunLeases is the batch heartbeat: one transaction for all the runs the
// owner holds, no matter how many there are, so the write cost of staying
// alive does not scale with the workload. Callers diff the answer against
// what they believe they hold; a run missing from the answer, or answering
// with a different token, is a lease lost and must be self-fenced.
func (s *Store) RenewRunLeases(ctx context.Context, owner string, ttl time.Duration) ([]LeaseRenewal, error) {
	if owner == "" {
		return nil, errors.New("renew run leases: no owner was named")
	}
	if ttl <= 0 {
		ttl = DefaultRunLeaseTTL
	}
	now := s.clk.Now().UTC()

	var out []LeaseRenewal
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query(renewRunLeasesSQL, now.UnixMilli(), now.Add(ttl).UnixMilli(), owner)
		if err != nil {
			return fmt.Errorf("renew the run leases of %s: %w", owner, err)
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var (
				r       LeaseRenewal
				cancel  sql.NullInt64
				cancelB sql.NullString
			)
			if err := rows.Scan(&r.ID, &r.LeaseEpoch, &cancel, &cancelB); err != nil {
				return fmt.Errorf("scan a renewal: %w", err)
			}
			r.CancelRequestedAt = timeOrZero(cancel)
			r.CancelRequestedBy = cancelB.String
			out = append(out, r)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ReapOptions tunes one sweep. Zero fields mean the defaults above.
type ReapOptions struct {
	// Skew is the wall clock allowance past the expiry before a lease counts
	// as dead. Zero means DefaultClockSkewAllowance.
	Skew time.Duration

	// Backoff is how long a requeued run waits before it is due again.
	Backoff time.Duration

	// MaxCrashCount is the poison line. Zero means DefaultMaxCrashCount.
	MaxCrashCount int

	// Limit caps the sweep. Zero means reapLimit.
	Limit int

	// IgnoreLease widens the sweep from "leases that expired" to "every
	// running run". It exists for startup reconciliation after a machine
	// restart (#62): a changed boot id proves every child process dead, so
	// lease_expires_at has stopped being evidence about anything and waiting
	// it out would be pure delay. Nothing else may set it. The decision arms
	// are the same either way; only candidate selection changes, and the
	// fencing token still rises in every arm.
	IgnoreLease bool
}

// ReapedRun is one run the reaper took, with where it went.
type ReapedRun struct {
	ID         string
	State      string
	ReasonCode string
	CrashCount int
	Attempt    int
	LeaseEpoch int64
}

// reapCandidatesSQL names the running rows whose lease died past the skew
// allowance. The partial reaper index (lease_expires_at WHERE state =
// 'running') is what keeps this a search and not a scan; the query plan test
// pins that.
const reapCandidatesSQL = `SELECT id FROM runs
WHERE state = 'running'
	AND lease_expires_at IS NOT NULL
	AND lease_expires_at <= ?1
ORDER BY lease_expires_at
LIMIT ?2`

// reapAllRunningSQL is the boot sweep's candidate list: every running row, no
// lease predicate at all. A changed boot id is stronger evidence than any
// expiry timestamp (issue #62), and the partial index on running state keeps
// the scan bounded to exactly the rows the sweep means to take.
const reapAllRunningSQL = `SELECT id FROM runs
WHERE state = 'running'
ORDER BY lease_expires_at
LIMIT ?1`

// ReapExpiredRuns takes every run whose lease expired past the skew allowance
// and decides its fate in the one transaction: a cancellation request on a
// dead owner completes as cancelled, a run past its crash budget fails into
// the poison quarantine, a run with no attempt budget left fails reconciled,
// and everything else is requeued with a backoff for a new holder. The
// fencing token rises in every arm, including the failures: a late zombie
// must not overwrite a verdict someone else owns, whatever the verdict was.
//
// Each decision writes its own run_events row, by the reaper, in the same
// transaction as the state change (G10).
func (s *Store) ReapExpiredRuns(ctx context.Context, opt ReapOptions) ([]ReapedRun, error) {
	skew := opt.Skew
	if skew <= 0 {
		skew = DefaultClockSkewAllowance
	}
	backoff := opt.Backoff
	if backoff <= 0 {
		backoff = DefaultRequeueBackoff
	}
	maxCrash := opt.MaxCrashCount
	if maxCrash <= 0 {
		maxCrash = DefaultMaxCrashCount
	}
	limit := opt.Limit
	if limit <= 0 {
		limit = reapLimit
	}
	now := s.clk.Now().UTC()
	cut := now.Add(-skew).UnixMilli()

	var out []ReapedRun
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// Candidate selection is the only place the two sweeps differ. The
		// ordinary reaper trusts lease expiry plus skew; the boot sweep
		// trusts nothing but the running state, because its caller holds
		// boot-changed proof that every holder died with the machine.
		query := reapCandidatesSQL
		args := []any{cut, limit}
		if opt.IgnoreLease {
			query = reapAllRunningSQL
			args = []any{limit}
		}
		rows, err := tx.Query(query, args...)
		if err != nil {
			return fmt.Errorf("find expired run leases: %w", err)
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("scan an expired run: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("scan an expired run: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close the expired rows: %w", err)
		}

		for _, id := range ids {
			reaped, err := reapOneTx(tx, id, now, backoff, maxCrash)
			if err != nil {
				return err
			}
			out = append(out, reaped)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Every reaped run was a lease a dead holder kept: one hook call per
	// sweep, after the transaction, so a refused reclaim counts nowhere (#40).
	s.observeLeaseReclaims(len(out))
	return out, nil
}

// reapOneTx takes one expired run through its decision and its writes. The
// caller has already established, in this same transaction, that the lease is
// dead past the skew; nothing here can race a live holder.
func reapOneTx(tx *sql.Tx, runID string, now time.Time, backoff time.Duration, maxCrash int) (ReapedRun, error) {
	run, err := readRunTx(tx, runID)
	if err != nil {
		return ReapedRun{}, err
	}
	if run.State != string(model.RunRunning) {
		return ReapedRun{}, fmt.Errorf("reap run %s: it is %s, not running", runID, run.State)
	}

	// The decision, in the order the issue fixes: an operator's cancel beats
	// everything, then the poison quarantine, then the spent attempt budget,
	// then the ordinary requeue.
	switch {
	case !run.CancelRequestedAt.IsZero():
		return reapToCancelledTx(tx, run, now)
	case run.CrashCount+1 > maxCrash:
		return reapToFailedTx(tx, run, now, reason.RUNPoisoned, "run.poisoned")
	case run.Attempt >= run.MaxAttempts:
		return reapToFailedTx(tx, run, now, reason.RUNOrphanedReconciled, "run.failed")
	default:
		return reapToQueuedTx(tx, run, now, backoff)
	}
}

// reapToQueuedTx puts the run back in the queue for a new holder: the epoch
// rises, the crash counts, the dead attempt's running steps go back through
// the machine as lost, and the run waits out the backoff before it is due.
func reapToQueuedTx(tx *sql.Tx, run Run, now time.Time, backoff time.Duration) (ReapedRun, error) {
	state, _, err := model.NextRunState(model.RunRunning, model.EvLeaseExpired, model.Guards{
		LeaseValid:      false,
		CrashBudgetLeft: true,
	})
	if err != nil {
		return ReapedRun{}, fmt.Errorf("reap run %s: %w", run.ID, err)
	}
	if state != model.RunQueued {
		return ReapedRun{}, fmt.Errorf("reap run %s: the machine sent it to %s", run.ID, state)
	}
	if err := failRunningStepsTx(tx, run.ID, now, false); err != nil {
		return ReapedRun{}, err
	}
	epoch := run.LeaseEpoch + 1
	due := now.Add(backoff)
	if _, err := tx.Exec(`UPDATE runs SET
		state = 'queued',
		lease_owner = NULL,
		lease_expires_at = NULL,
		lease_epoch = lease_epoch + 1,
		crash_count = crash_count + 1,
		defer_reason = ?,
		available_at = ?,
		error = COALESCE(error, 'lease expired'),
		updated_at = ?
		WHERE id = ? AND state = 'running' AND lease_epoch = ?`,
		model.DeferReasonAfterCrash, due.UnixMilli(), now.UnixMilli(), run.ID, run.LeaseEpoch); err != nil {
		return ReapedRun{}, fmt.Errorf("requeue run %s: %w", run.ID, err)
	}
	if err := appendRunEvent(tx, RunEvent{
		RunID:      run.ID,
		At:         now,
		Kind:       "run.requeued",
		FromState:  string(model.RunRunning),
		ToState:    string(state),
		Actor:      "reaper",
		DetailJSON: epochDetail(epoch),
	}); err != nil {
		return ReapedRun{}, err
	}
	return ReapedRun{
		ID:         run.ID,
		State:      string(state),
		CrashCount: run.CrashCount + 1,
		Attempt:    run.Attempt,
		LeaseEpoch: epoch,
	}, nil
}

// reapToFailedTx closes the run as failed under either failure code. The
// steps all leave terminal (I2: a terminal run has no open step), the verdict
// is stamped, and the token still rises.
func reapToFailedTx(tx *sql.Tx, run Run, now time.Time, code reason.Code, kind string) (ReapedRun, error) {
	state, effects, err := model.NextRunState(model.RunRunning, model.EvLeaseExpired, model.Guards{
		LeaseValid:      false,
		CrashBudgetLeft: false,
		ReasonCode:      string(code),
	})
	if err != nil {
		return ReapedRun{}, fmt.Errorf("reap run %s: %w", run.ID, err)
	}
	if state != model.RunFailed {
		return ReapedRun{}, fmt.Errorf("reap run %s: the machine sent it to %s", run.ID, state)
	}
	if err := failRunningStepsTx(tx, run.ID, now, true); err != nil {
		return ReapedRun{}, err
	}
	if err := skipPendingStepsTx(tx, run.ID, now, string(reason.STEPSkippedUpstreamFailed)); err != nil {
		return ReapedRun{}, err
	}
	epoch := run.LeaseEpoch + 1
	if _, err := tx.Exec(`UPDATE runs SET
		state = 'failed',
		reason_code = ?,
		finished_at = ?,
		duration_ms = CASE WHEN started_at IS NULL THEN NULL ELSE ? - started_at END,
		lease_owner = NULL,
		lease_expires_at = NULL,
		lease_epoch = lease_epoch + 1,
		crash_count = crash_count + 1,
		error = COALESCE(error, 'lease expired'),
		updated_at = ?
		WHERE id = ? AND state = 'running' AND lease_epoch = ?`,
		string(code), now.UnixMilli(), now.UnixMilli(), now.UnixMilli(), run.ID, run.LeaseEpoch); err != nil {
		return ReapedRun{}, fmt.Errorf("fail run %s: %w", run.ID, err)
	}
	_ = effects
	if err := appendRunEvent(tx, RunEvent{
		RunID:      run.ID,
		At:         now,
		Kind:       kind,
		FromState:  string(model.RunRunning),
		ToState:    string(model.RunFailed),
		ReasonCode: string(code),
		Actor:      "reaper",
		DetailJSON: epochDetail(epoch),
	}); err != nil {
		return ReapedRun{}, err
	}
	return ReapedRun{
		ID:         run.ID,
		State:      string(model.RunFailed),
		ReasonCode: string(code),
		CrashCount: run.CrashCount + 1,
		Attempt:    run.Attempt,
		LeaseEpoch: epoch,
	}, nil
}

// reapToCancelledTx completes a cancellation whose requester outlived the
// owner: the request was durable, the owner is gone, and the run ends
// cancelled instead of being offered to a new holder.
func reapToCancelledTx(tx *sql.Tx, run Run, now time.Time) (ReapedRun, error) {
	if err := cancelRunningStepsTx(tx, run.ID, now); err != nil {
		return ReapedRun{}, err
	}
	if err := skipPendingStepsTx(tx, run.ID, now, string(reason.STEPCancelled)); err != nil {
		return ReapedRun{}, err
	}
	epoch := run.LeaseEpoch + 1
	if _, err := tx.Exec(`UPDATE runs SET
		state = 'cancelled',
		reason_code = ?,
		finished_at = ?,
		duration_ms = CASE WHEN started_at IS NULL THEN NULL ELSE ? - started_at END,
		lease_owner = NULL,
		lease_expires_at = NULL,
		lease_epoch = lease_epoch + 1,
		crash_count = crash_count + 1,
		updated_at = ?
		WHERE id = ? AND state = 'running' AND lease_epoch = ?`,
		string(reason.RUNCancelledManual), now.UnixMilli(), now.UnixMilli(),
		now.UnixMilli(), run.ID, run.LeaseEpoch); err != nil {
		return ReapedRun{}, fmt.Errorf("cancel run %s: %w", run.ID, err)
	}
	if err := appendRunEvent(tx, RunEvent{
		RunID:      run.ID,
		At:         now,
		Kind:       "run.cancelled",
		FromState:  string(model.RunRunning),
		ToState:    string(model.RunCancelled),
		ReasonCode: string(reason.RUNCancelledManual),
		Actor:      "reaper",
		DetailJSON: epochDetail(epoch),
	}); err != nil {
		return ReapedRun{}, err
	}
	return ReapedRun{
		ID:         run.ID,
		State:      string(model.RunCancelled),
		ReasonCode: string(reason.RUNCancelledManual),
		CrashCount: run.CrashCount + 1,
		Attempt:    run.Attempt,
		LeaseEpoch: epoch,
	}, nil
}

// failRunningStepsTx closes every running step of a run the reaper is taking.
// With a budget left the lost attempt goes back to pending for its next try
// (parked at once, the same answer recovery gives); with terminal set, or no
// budget left, the step fails under STEP_FAILED_EXECUTOR_LOST: the verdict
// was lost with the executor, and recording exactly that beats inventing one.
func failRunningStepsTx(tx *sql.Tx, runID string, now time.Time, terminal bool) error {
	steps, err := readStepsTx(tx, runID)
	if err != nil {
		return err
	}
	for _, step := range steps {
		if model.StepState(step.State) != model.StepRunning {
			continue
		}
		left := !terminal && step.Attempt < step.MaxAttempts
		state, effects, err := model.NextStepState(model.StepRunning, model.EvStepFailed, model.Guards{
			ReasonCode:   string(reason.STEPFailedExecutorLost),
			AttemptsLeft: left,
		})
		if err != nil {
			return fmt.Errorf("close the lost step %s of run %s: %w", step.Name, runID, err)
		}
		var nextAttempt any
		if state == model.StepPending {
			nextAttempt = now.UnixMilli()
		}
		// nolint:fencing: the sweep transaction already proved this run's
		// lease expired past the skew; steps carry no token of their own.
		if _, err := tx.Exec(`UPDATE steps SET
			state = ?, reason_code = ?, reason_data = '{}',
			finished_at = ?,
			duration_ms = CASE WHEN started_at IS NULL THEN NULL ELSE ? - started_at END,
			next_attempt_at = ?
			WHERE run_id = ? AND name = ? AND state = 'running'`,
			string(state), string(reason.STEPFailedExecutorLost), now.UnixMilli(),
			now.UnixMilli(), nextAttempt, runID, step.Name); err != nil {
			return fmt.Errorf("close the lost step %s of run %s: %w", step.Name, runID, err)
		}
		if err := appendRunEvent(tx, RunEvent{
			RunID:      runID,
			StepName:   step.Name,
			At:         now,
			Kind:       emitKind(effects),
			FromState:  string(model.StepRunning),
			ToState:    string(state),
			ReasonCode: string(reason.STEPFailedExecutorLost),
			Actor:      "reaper",
		}); err != nil {
			return err
		}
	}
	return nil
}

// cancelRunningStepsTx marks every running step of a dying run cancelled: the
// kill already happened, or the process is gone, and the history should say
// the steps ended by cancellation rather than pretend they never ran.
func cancelRunningStepsTx(tx *sql.Tx, runID string, now time.Time) error {
	steps, err := readStepsTx(tx, runID)
	if err != nil {
		return err
	}
	for _, step := range steps {
		if model.StepState(step.State) != model.StepRunning {
			continue
		}
		state, effects, err := model.NextStepState(model.StepRunning, model.EvCancelObserved, model.Guards{
			ReasonCode: string(reason.STEPCancelled),
		})
		if err != nil {
			return fmt.Errorf("cancel the step %s of run %s: %w", step.Name, runID, err)
		}
		// nolint:fencing: the same sweep transaction proved this run's lease
		// dead before deciding the cancel; steps carry no token of their own.
		if _, err := tx.Exec(`UPDATE steps SET
			state = 'cancelled', reason_code = ?, reason_data = '{}',
			finished_at = ?,
			duration_ms = CASE WHEN started_at IS NULL THEN NULL ELSE ? - started_at END
			WHERE run_id = ? AND name = ? AND state = 'running'`,
			string(reason.STEPCancelled), now.UnixMilli(), now.UnixMilli(), runID, step.Name); err != nil {
			return fmt.Errorf("cancel the step %s of run %s: %w", step.Name, runID, err)
		}
		if err := appendRunEvent(tx, RunEvent{
			RunID:      runID,
			StepName:   step.Name,
			At:         now,
			Kind:       emitKind(effects),
			FromState:  string(model.StepRunning),
			ToState:    string(state),
			ReasonCode: string(reason.STEPCancelled),
			Actor:      "reaper",
		}); err != nil {
			return err
		}
	}
	return nil
}

// checkLeaseTx is the fence every holder write crosses, inside the caller's
// transaction: the run must still be running and still belong to the writer
// at the writer's token. A ref with no owner speaks for recovery, and that
// path demands the opposite proof: the lease must already be dead, or someone
// alive is driving and the system has no business touching the row.
//
// The check sits inside the same BEGIN IMMEDIATE transaction as the write it
// guards. SQLite serialises writers, so there is no window between this
// answer and the write landing; checking outside would be a TOCTOU hole.
func checkLeaseTx(tx *sql.Tx, runID string, ref LeaseRef, now time.Time) error {
	run, err := readRunTx(tx, runID)
	if err != nil {
		return err
	}
	if run.State != string(model.RunRunning) {
		return fmt.Errorf("write run %s: %w (it is %s)", runID, ErrLeaseLost, run.State)
	}
	if ref.held() {
		if run.LeaseOwner != ref.Owner || run.LeaseEpoch != ref.Epoch {
			return fmt.Errorf("write run %s: %w (held by %q at epoch %d)",
				runID, ErrLeaseLost, run.LeaseOwner, run.LeaseEpoch)
		}
		return nil
	}
	if run.LeaseExpiresAt.IsZero() || run.LeaseExpiresAt.After(now) {
		return fmt.Errorf("write run %s: %w (the lease of %q is still live)",
			runID, ErrLeaseLost, run.LeaseOwner)
	}
	return nil
}

// epochDetail renders the fencing token into an event's detail object, so the
// history of every run-level transition records the token it moved under.
// The I11 sweep reads these back and checks the sequence never falls.
func epochDetail(epoch int64) string {
	return fmt.Sprintf(`{"lease_epoch":%d}`, epoch)
}

// mergeEpochDetail folds the fencing token into an existing detail object,
// keeping whatever facts the caller already recorded beside it.
func mergeEpochDetail(baseJSON string, epoch int64) string {
	m := map[string]any{}
	if err := json.Unmarshal([]byte(baseJSON), &m); err != nil {
		m = map[string]any{}
	}
	m["lease_epoch"] = epoch
	b, err := json.Marshal(m)
	if err != nil {
		return epochDetail(epoch)
	}
	return string(b)
}
