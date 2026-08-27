package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/a-holm/paceq/internal/model"
)

// OutboxMsg is one claimed notification on its way to its notifier. It
// carries the window facts the dispatcher merges into the wire payload: an
// event that was collapsed into this delivery reports how many similar
// events it swallowed and when the window opened.
type OutboxMsg struct {
	ID       int64
	Topic    string
	Subject  string
	Target   string
	Payload  string
	Attempts int

	// Suppressed is how many later events in the same group collapsed into
	// this delivery ("+49 lignende"). Zero when nothing grouped here.
	Suppressed int64
	// WindowOpenedAt is when the group's delivery window opened; zero for
	// ungrouped messages.
	WindowOpenedAt time.Time
}

// ErrNotificationNotFound is what reads and writes return when an outbox id
// does not name a row. The CLI turns it into exit 3.
var ErrNotificationNotFound = errors.New("no such notification")

// ErrNotificationDelivered is what RetryOutbox refuses with: a delivered row
// is history, and history does not resend itself.
var ErrNotificationDelivered = errors.New("delivered")

// claimDueSQL selects the ids to hand out: due by available_at, not yet
// delivered, never given up on, oldest claim first. The partial pending index
// serves it whole.
const claimDueSQL = `
SELECT id FROM outbox
 WHERE delivered_at IS NULL AND failed_at IS NULL AND available_at <= ?
 ORDER BY available_at, id
 LIMIT ?`

// claimUpdateSQL takes the rows in one IMMEDIATE transaction: attempts rise
// before anything leaves the building, so a crash mid-delivery cannot make a
// row invisible forever - after visibility elapses it comes back and gets
// delivered again. That repeat is the documented at-least-once edge.
const claimUpdateSQL = `
UPDATE outbox SET attempts = attempts + 1, available_at = ?
 WHERE id = ? AND delivered_at IS NULL AND failed_at IS NULL
 RETURNING id, topic, subject, target, payload, attempts`

// windowFactsSQL pulls the throttle bookkeeping of already-inserted openers.
const windowFactsSQL = `
SELECT opener_id, suppressed, opened_at FROM outbox_windows WHERE opener_id IN `

// ClaimOutbox marks up to limit due rows as being delivered right now and
// returns them. The update commits first; sending happens strictly outside,
// which is why a claimed row that crashes stays bounded-visible rather than
// lost: once visibility elapses the row is due again and claims anew.
func (s *Store) ClaimOutbox(ctx context.Context, limit int, now time.Time, visibility time.Duration) ([]OutboxMsg, error) {
	if limit <= 0 {
		limit = 1
	}
	nowMs := now.UTC().UnixMilli()
	var msgs []OutboxMsg
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		type due struct {
			id int64
		}
		rows, err := tx.QueryContext(ctx, claimDueSQL, nowMs, limit)
		if err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}
		var dues []due
		for rows.Next() {
			var d due
			if err := rows.Scan(&d.id); err != nil {
				_ = rows.Close()
				return fmt.Errorf("claim outbox: %w", err)
			}
			dues = append(dues, d)
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return fmt.Errorf("claim outbox: %w", err)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("claim outbox: %w", err)
		}
		if len(dues) == 0 {
			return nil // Guard before the fallback SELECT: empty means done.
		}
		ids := "(" + strings.TrimSuffix(strings.Repeat("?,", len(dues)), ",") + ")"
		args := make([]any, 0, len(dues))
		for _, d := range dues {
			args = append(args, d.id)
		}

		deliverAtMs := nowMs + visibility.Milliseconds()
		fetched := make(map[int64]OutboxMsg, len(dues))
		order := make([]int64, 0, len(dues))
		for _, d := range dues {
			row := tx.QueryRowContext(ctx, claimUpdateSQL, deliverAtMs, d.id)
			var m OutboxMsg
			scanErr := row.Scan(&m.ID, &m.Topic, &m.Subject, &m.Target, &m.Payload, &m.Attempts)
			if errors.Is(scanErr, sql.ErrNoRows) {
				continue // Someone else's transaction settled it; next claim sees the truth.
			}
			if scanErr != nil {
				return fmt.Errorf("claim outbox row %d: %w", d.id, scanErr)
			}
			fetched[m.ID] = m
			order = append(order, m.ID)
		}

		wrows, werr := tx.QueryContext(ctx, windowFactsSQL+ids, args...)
		if werr != nil {
			return fmt.Errorf("read outbox windows: %w", werr)
		}
		defer func() { _ = wrows.Close() }()
		for wrows.Next() {
			var opener, suppressed, opened int64
			if err := wrows.Scan(&opener, &suppressed, &opened); err != nil {
				return fmt.Errorf("scan outbox windows: %w", err)
			}
			m, ok := fetched[opener]
			if !ok {
				continue
			}
			m.Suppressed = suppressed
			m.WindowOpenedAt = time.UnixMilli(opened).UTC()
			fetched[opener] = m
		}
		if err := wrows.Err(); err != nil {
			return fmt.Errorf("read outbox windows: %w", err)
		}

		msgs = msgs[:0]
		for _, id := range order {
			msgs = append(msgs, fetched[id])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return msgs, nil
}

// MarkOutboxDelivered closes a delivery. The row stays forever as history:
// the audit question "did we notify?" points at this timestamp.
func (s *Store) MarkOutboxDelivered(ctx context.Context, id int64, now time.Time) error {
	res, err := s.w.ExecContext(ctx,
		`UPDATE outbox SET delivered_at = ?, last_error = NULL WHERE id = ?`,
		now.UTC().UnixMilli(), id)
	return confirmWrite(res, err, "deliver outbox row")
}

// RescheduleOutbox puts a failed attempt back into rotation after backoff,
// recording the error that caused it.
func (s *Store) RescheduleOutbox(ctx context.Context, id int64, availableAt time.Time, errMsg string) error {
	res, err := s.w.ExecContext(ctx,
		`UPDATE outbox SET available_at = ?, last_error = ?
		  WHERE id = ? AND delivered_at IS NULL AND failed_at IS NULL`,
		availableAt.UTC().UnixMilli(), errMsg, id)
	return confirmWrite(res, err, "reschedule outbox row")
}

// MarkOutboxFailed gives up permanently: failed_at seals the row into the
// pending index's blind spot but keeps every fact on disk. Nothing ever
// deletes a failed notification silently; only retention horizons or an
// operator's retry move it afterwards.
func (s *Store) MarkOutboxFailed(ctx context.Context, id int64, now time.Time, errMsg string) error {
	res, err := s.w.ExecContext(ctx,
		`UPDATE outbox SET failed_at = ?, last_error = ?
		  WHERE id = ? AND delivered_at IS NULL`,
		now.UTC().UnixMilli(), errMsg, id)
	return confirmWrite(res, err, "fail outbox row")
}

// confirmWrite turns zero affected rows into NotFound rather than silent
// success, for the single-row outbox updates above.
func confirmWrite(res sql.Result, err error, what string) error {
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: %w", what, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: %w", what, ErrNotificationNotFound)
	}
	return nil
}

// insertNotificationsTx writes the prepared notifications inside the caller's
// IMMEDIATE transaction. Dedup first (a UNIQUE index enforces one row per
// event), then the throttle decision against the windows table, both under
// the same write lock: suppression accounting can never drift from delivery.
//
// The engine prepares everything; nothing here re-renders payload bytes.
func insertNotificationsTx(tx *sql.Tx, notes []model.Notification) error {
	const probe = `SELECT 1 FROM outbox WHERE dedup_key = ?`
	const ins = `
INSERT INTO outbox(topic, subject, target, payload, dedup_key, created_at, available_at)
VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(dedup_key) DO NOTHING`
	const openWin = `
INSERT INTO outbox_windows(topic, target, group_key, opener_id, opened_at, suppressed)
VALUES (?, ?, ?, ?, ?, 0)
ON CONFLICT(topic, target, group_key) DO UPDATE SET
  opener_id = excluded.opener_id,
  opened_at = excluded.opened_at,
  suppressed = 0`
	const countSuppressed = `
UPDATE outbox_windows SET suppressed = suppressed + 1
 WHERE topic = ? AND target = ? AND group_key = ?`

	for _, n := range notes {
		if n.Topic == "" || n.Subject == "" || n.Target == "" {
			return fmt.Errorf("notification without topic/subject/target: %+v", redactNote(n))
		}
		dedupKey := n.DedupKey
		if dedupKey == "" {
			return fmt.Errorf("notification without dedup key: %+v", redactNote(n))
		}

		var exists any
		err := tx.QueryRow(probe, dedupKey).Scan(&exists)
		if err == nil {
			continue // Same logical event already recorded: dedup wins.
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("probe outbox dedup key: %w", err)
		}

		if n.Throttle > 0 {
			opened, suppressed, ok, werr := readWindowTx(tx, n.Topic, n.Target, n.GroupKey)
			if werr != nil {
				return werr
			}
			inWindow := ok && n.CreatedAt.Sub(time.UnixMilli(opened)) < n.Throttle
			if inWindow {
				if _, uerr := tx.Exec(countSuppressed,
					n.Topic, n.Target, n.GroupKey); uerr != nil {
					return fmt.Errorf("count suppressed notification: %w", uerr)
				}
				_ = suppressed
				continue
			}
		}

		res, ierr := tx.Exec(ins,
			n.Topic, n.Subject, n.Target, n.Payload, n.DedupKey,
			n.CreatedAt.UTC().UnixMilli(), n.AvailableAt.UTC().UnixMilli())
		if ierr != nil {
			return fmt.Errorf("insert outbox row: %w", ierr)
		}
		inserted, rerr := res.RowsAffected()
		if rerr != nil {
			return fmt.Errorf("insert outbox row: %w", rerr)
		}
		if inserted == 0 {
			continue // A concurrent-looking dedup hit; same branch as above.
		}

		if n.Throttle > 0 {
			id, ger := res.LastInsertId()
			if ger != nil {
				return fmt.Errorf("insert outbox row: %w", ger)
			}
			if _, uerr := tx.Exec(openWin,
				n.Topic, n.Target, n.GroupKey, id,
				n.CreatedAt.UTC().UnixMilli()); uerr != nil {
				return fmt.Errorf("open outbox window: %w", uerr)
			}
		}
	}
	return nil
}

// readWindowTx reads one throttle bucket. ok is false when no window exists.
func readWindowTx(tx *sql.Tx, topic, target, group string) (openedAt, suppressed int64, ok bool, err error) {
	row := tx.QueryRow(`SELECT opened_at, suppressed FROM outbox_windows
		WHERE topic = ? AND target = ? AND group_key = ?`, topic, target, group)
	switch serr := row.Scan(&openedAt, &suppressed); {
	case errors.Is(serr, sql.ErrNoRows):
		return 0, 0, false, nil
	case serr != nil:
		return 0, 0, false, fmt.Errorf("read outbox window: %w", serr)
	default:
		return openedAt, suppressed, true, nil
	}
}

// redactNote keeps the error path honest about identity without echoing a
// payload full of job output into logs.
func redactNote(n model.Notification) map[string]string {
	return map[string]string{"topic": n.Topic, "subject": n.Subject, "target": n.Target}
}

// NotificationSummary is the CLI-facing view of one outbox row (#29): stable
// fields, RFC-timestamped facts, the payload verbatim.
type NotificationSummary struct {
	ID        int64      `json:"id"`
	Topic     string     `json:"topic"`
	Subject   string     `json:"subject"`
	Target    string     `json:"target"`
	State     string     `json:"state"` // pending | delivered | failed
	CreatedAt time.Time  `json:"created_at"`
	Delivered *time.Time `json:"delivered_at,omitempty"`
	Failed    *time.Time `json:"failed_at,omitempty"`
	Attempts  int        `json:"attempts"`
	LastError string     `json:"last_error,omitempty"`
	Payload   string     `json:"payload"`
}

// State derives from the timestamps exactly like every other reader here:
// there is no state column to disagree with.
func rowState(delivered, failed sql.NullInt64) string {
	switch {
	case failed.Valid:
		return "failed"
	case delivered.Valid:
		return "delivered"
	default:
		return "pending"
	}
}

const notificationColumns = `id, topic, subject, target, payload, created_at,
	attempts, delivered_at, failed_at, last_error`

// scanNotification scans one row in notificationColumns order.
func scanNotification(scan func(...any) error) (NotificationSummary, error) {
	var (
		n                 NotificationSummary
		created           sql.NullInt64
		delivered, failed sql.NullInt64
		lastError         sql.NullString
	)
	if err := scan(&n.ID, &n.Topic, &n.Subject, &n.Target, &n.Payload, &created,
		&n.Attempts, &delivered, &failed, &lastError); err != nil {
		return n, err
	}
	n.CreatedAt = time.UnixMilli(created.Int64).UTC()
	n.State = rowState(delivered, failed)
	if delivered.Valid {
		t := time.UnixMilli(delivered.Int64).UTC()
		n.Delivered = &t
	}
	if failed.Valid {
		t := time.UnixMilli(failed.Int64).UTC()
		n.Failed = &t
	}
	n.LastError = lastError.String
	return n, nil
}

// ListNotifications answers `pulseq notifications list`. Newest first, the
// way an audit reads. All filters are optional; an empty filter lists
// everything still on disk.
type NotificationFilter struct {
	Since   time.Time // only rows created at or after this instant
	State   string    // "", "pending", "delivered" or "failed"
	Subject string    // exact job/sensor match
	Limit   int       // capped hard below; zero means DefaultListLimit
}

// DefaultListLimit bounds one listing. An unbounded audit query on a busy
// outbox would be a read-side outage waiting to happen.
const DefaultListLimit = 200

// MaxListLimit is the explicit ceiling even a larger request cannot pass.
const MaxListLimit = 10_000

func (f NotificationFilter) clamp() NotificationFilter {
	out := f
	switch {
	case out.Limit <= 0:
		out.Limit = DefaultListLimit
	case out.Limit > MaxListLimit:
		out.Limit = MaxListLimit
	}
	return out
}

var listStates = map[string]bool{"": true, "pending": true, "delivered": true, "failed": true}

// ValidListState reports whether f asked for a state the contract names.
func ValidListState(state string) bool { return listStates[state] }

// ListNotifications implements the read half of the notifications noun group.
func (s *Store) ListNotifications(ctx context.Context, f NotificationFilter) ([]NotificationSummary, error) {
	f = f.clamp()
	if !ValidListState(f.State) {
		return nil, fmt.Errorf("state %q is not one of pending, delivered, failed", f.State)
	}
	where := []string{"1=1"}
	var args []any
	if !f.Since.IsZero() {
		where = append(where, "created_at >= ?")
		args = append(args, f.Since.UTC().UnixMilli())
	}
	if f.State == "pending" {
		where = append(where, "delivered_at IS NULL AND failed_at IS NULL")
	} else if f.State != "" {
		col := "delivered_at"
		if f.State == "failed" {
			col = "failed_at"
		}
		where = append(where, col+" IS NOT NULL") // identifier built from a closed vocabulary, never user data #nosec G202
	}
	if f.Subject != "" {
		where = append(where, "subject = ?")
		args = append(args, f.Subject)
	}

	q := `SELECT ` + notificationColumns + ` FROM outbox WHERE ` +
		strings.Join(where, " AND ") +
		` ORDER BY created_at DESC, id DESC LIMIT ?` // identifiers are constants above #nosec G202
	args = append(args, f.Limit)

	var out []NotificationSummary
	err := s.withRead(ctx, func(_ context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, q, args...)
		if err != nil {
			return fmt.Errorf("list notifications: %w", err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			n, serr := scanNotification(rows.Scan)
			if serr != nil {
				return fmt.Errorf("list notifications: %w", serr)
			}
			out = append(out, n)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GetNotification answers `pulseq notifications show <id>`.
func (s *Store) GetNotification(ctx context.Context, id int64) (NotificationSummary, error) {
	var got NotificationSummary
	err := s.withRead(ctx, func(_ context.Context, r reader) error {
		row := r.QueryRowContext(ctx,
			`SELECT `+notificationColumns+` FROM outbox WHERE id = ?`, id) // constant column list #nosec G202
		n, serr := scanNotification(row.Scan)
		if errors.Is(serr, sql.ErrNoRows) {
			return fmt.Errorf("show notification %d: %w", id, ErrNotificationNotFound)
		}
		if serr != nil {
			return fmt.Errorf("show notification %d: %w", id, serr)
		}
		got = n
		return nil
	})
	if err != nil {
		return NotificationSummary{}, err
	}
	return got, nil
}

// RetryOutbox gives a failed row another chance: available_at moves to the
// store's own now, attempts keep their count - the operator asked for a
// retry, not a rewrite of history - and failed_at clears. A delivered row
// refuses: it did go out, and history does not resend itself.
func (s *Store) RetryOutbox(ctx context.Context, id int64) (string, error) {
	now := s.clk.Now()
	next := ""
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var state string
		qerr := tx.QueryRowContext(ctx,
			`SELECT CASE
			   WHEN delivered_at IS NOT NULL THEN 'delivered'
			   WHEN failed_at IS NOT NULL THEN 'failed'
			   ELSE 'pending' END FROM outbox WHERE id = ?`, id).Scan(&state)
		if errors.Is(qerr, sql.ErrNoRows) {
			return fmt.Errorf("retry notification %d: %w", id, ErrNotificationNotFound)
		}
		if qerr != nil {
			return fmt.Errorf("retry notification %d: %w", id, qerr)
		}
		if state == "delivered" {
			return fmt.Errorf("retry notification %d: row already %w, nothing to retry", id, ErrNotificationDelivered)
		}
		if state == "failed" {
			if _, uerr := tx.ExecContext(ctx,
				`UPDATE outbox SET failed_at = NULL, available_at = ?
				  WHERE id = ?`, now.UTC().UnixMilli(), id); uerr != nil {
				return fmt.Errorf("retry notification %d: %w", id, uerr)
			}
			next = "failed"
			return nil
		}
		if _, uerr := tx.ExecContext(ctx,
			`UPDATE outbox SET available_at = ? WHERE id = ?`,
			now.UTC().UnixMilli(), id); uerr != nil {
			return fmt.Errorf("retry notification %d: %w", id, uerr)
		}
		next = "pending"
		return nil
	})
	if err != nil {
		return "", err
	}
	return next, nil
}

// SLAEpisodeChange is one job's freshness verdict, applied under the episode
// guard. Breaching with no episode row opens one and emits Notes once;
// recovery deletes the row silently; steady-state breach changes nothing -
// one notification per breach episode, not per check.
type SLAEpisodeChange struct {
	Job       string
	Breaching bool
	Notes     []model.Notification // targets expanded by the caller at transition time
}

// ApplySLAEpisodes applies a checker tick's verdicts. One transaction for the
// batch keeps flip-flop races between ticks impossible and writes nothing for
// jobs whose verdict matches their current episode.
func (s *Store) ApplySLAEpisodes(ctx context.Context, changes []SLAEpisodeChange, now time.Time) error {
	if len(changes) == 0 {
		return nil
	}
	nowMs := now.UTC().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		for _, ch := range changes {
			if ch.Job == "" {
				return fmt.Errorf("sla episode change without a job name")
			}
			var breachedAny bool
			perr := tx.QueryRow(`SELECT 1 FROM sla_episodes WHERE job = ?`, ch.Job).
				Scan(&breachedAny)
			exists := perr == nil
			if perr != nil && !errors.Is(perr, sql.ErrNoRows) {
				return fmt.Errorf("read sla episode for %s: %w", ch.Job, perr)
			}

			switch {
			case ch.Breaching && !exists:
				if _, ierr := tx.Exec(
					`INSERT INTO sla_episodes(job, breached_at) VALUES (?, ?)`,
					ch.Job, nowMs); ierr != nil {
					return fmt.Errorf("open sla episode for %s: %w", ch.Job, ierr)
				}
				if err := insertNotificationsTx(tx, ch.Notes); err != nil {
					return err
				}
			case !ch.Breaching && exists:
				if _, derr := tx.Exec(`DELETE FROM sla_episodes WHERE job = ?`, ch.Job); derr != nil {
					return fmt.Errorf("close sla episode for %s: %w", ch.Job, derr)
				}
			}
		}
		return nil
	})
}

// MetricsNotificationsPending counts undelivered, ungiven-up rows for
// /metrics (pulseq_notifications_pending). The partial index answers in
// O(pending), so a quiet system's scrape reads nothing at all.
func (s *Store) MetricsNotificationsPending(ctx context.Context) (int64, error) {
	var n int64
	err := s.withRead(ctx, func(_ context.Context, r reader) error {
		return r.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM outbox
			  WHERE delivered_at IS NULL AND failed_at IS NULL`).Scan(&n)
	})
	return n, err
}

// MetricsNotificationsFailedTotal counts rows we gave up on
// (pulseq_notifications_failed_total). Failed rows are kept forever, so the
// number rises monotonically over any horizon retention cannot touch.
func (s *Store) MetricsNotificationsFailedTotal(ctx context.Context) (int64, error) {
	var n int64
	err := s.withRead(ctx, func(_ context.Context, r reader) error {
		return r.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM outbox WHERE failed_at IS NOT NULL`).Scan(&n)
	})
	return n, err
}

// delivery buckets for pulseq_notification_delivery_seconds: cumulative
// upper bounds plus +Inf, mirroring Prometheus textformat le semantics.
var deliveryBuckets = [...]float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30}

// DeliveryBuckets exposes the bucket edges for the scrape side. Never mutate.
func DeliveryBuckets() []float64 { return deliveryBuckets[:] }

type deliveryHist struct {
	// counts[i] is the cumulative number of observations at or below
	// deliveryBuckets[i]; counts[len-1] equals total by construction.
	counts   [len(deliveryBuckets)]atomic.Uint64
	sumNanos atomic.Int64
	total    atomic.Uint64
}

// ObserveDelivery records one notifier send attempt wall time (success or
// failure alike): what an operator watches is how long alerting blocks the
// loop, whatever came of it.
func (s *Store) ObserveDelivery(d time.Duration) {
	if d < 0 {
		d = 0
	}
	v := d.Seconds()
	for i := range deliveryBuckets {
		if v <= deliveryBuckets[i] {
			s.delivery.counts[i].Add(1)
		}
	}
	s.delivery.sumNanos.Add(int64(d))
	s.delivery.total.Add(1)
}

// DeliverySnapshot is one scrape-side reading of the histogram: cumulative
// bucket counts ready to print, plus sum and total for the complementary series.
type DeliverySnapshot struct {
	BucketCounts []uint64 // aligned with DeliveryBuckets(); +Inf computed as TotalCount
	SumSeconds   float64
	TotalCount   uint64
}

// TakeDelivery returns the histogram state. Cumulative counters are read
// directly rather than reset: Prometheus owns rate arithmetic. The separate
// total counter is the +Inf bucket and stays exact even if two bucket reads
// straddle a concurrent add.
func (s *Store) TakeDelivery() DeliverySnapshot {
	out := DeliverySnapshot{BucketCounts: make([]uint64, len(deliveryBuckets))}
	for i := range deliveryBuckets {
		out.BucketCounts[i] = s.delivery.counts[i].Load()
	}
	out.TotalCount = s.delivery.total.Load()
	out.SumSeconds = float64(s.delivery.sumNanos.Load()) / float64(time.Second)
	return out
}

// PruneDeliveredNotificationsBatch deletes delivered rows past the retention
// horizon, batched like every other deletion here. Failed rows never age out.
func (s *Store) PruneDeliveredNotificationsBatch(ctx context.Context, cutoff time.Time) (int64, error) {
	const q = `
DELETE FROM outbox
 WHERE id IN (
   SELECT id FROM outbox
    WHERE delivered_at IS NOT NULL
      AND delivered_at < ?
    ORDER BY id
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, cutoff.UnixMilli(), PruneBatchLimit)
}

// PruneOrphanedWindowsBatch drops throttle bookkeeping whose opener row
// retention has taken away. Cheap: bounded by the windows table itself.
func (s *Store) PruneOrphanedWindowsBatch(ctx context.Context) (int64, error) {
	const q = `
DELETE FROM outbox_windows
 WHERE opener_id IN (
   SELECT w.rowid FROM outbox_windows w
     LEFT JOIN outbox o ON o.id = w.opener_id
    WHERE o.id IS NULL
    LIMIT ?
 )`
	return s.runPruneBatch(ctx, q, PruneBatchLimit)
}
