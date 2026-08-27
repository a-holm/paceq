package store

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// aNote is a valid notification the cases tweak. Deterministic timestamps,
// because every throttle decision here compares against them.
func aNote(topic, subject, target, dedup string) model.Notification {
	at := time.UnixMilli(1_000_000).UTC()
	return model.Notification{
		Topic:       topic,
		Subject:     subject,
		Target:      target,
		Payload:     `{"event":"` + topic + `"}`,
		DedupKey:    dedup,
		CreatedAt:   at,
		AvailableAt: at,
	}
}

func countOutbox(t *testing.T, s *Store) int64 {
	t.Helper()
	var n int64
	if err := s.withRead(context.Background(), func(_ context.Context, r reader) error {
		return r.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM outbox`).Scan(&n)
	}); err != nil {
		t.Fatalf("count outbox: %v", err)
	}
	return n
}

func rawWindow(t *testing.T, s *Store, topic, target, group string) (openerID, openedAt, suppressed int64, ok bool) {
	t.Helper()
	err := s.withRead(context.Background(), func(ctx context.Context, r reader) error {
		row := r.QueryRowContext(ctx, `SELECT opener_id, opened_at, suppressed FROM outbox_windows
			WHERE topic = ? AND target = ? AND group_key = ?`, topic, target, group)
		switch err := row.Scan(&openerID, &openedAt, &suppressed); {
		case errors.Is(err, sql.ErrNoRows):
			return nil
		case err != nil:
			return err
		}
		ok = true
		return nil
	})
	if err != nil {
		t.Fatalf("read window: %v", err)
	}
	return openerID, openedAt, suppressed, ok
}

// TestOutboxSchemaShape pins AC facts that golden text alone could drift
// past: STRICT typing and exactly the three documented indexes.
func TestOutboxSchemaShape(t *testing.T) {
	s := migratedStore(t)

	var ddl string
	if err := s.r.QueryRow(`SELECT sql FROM sqlite_master WHERE type='table' AND name='outbox'`).Scan(&ddl); err != nil {
		t.Fatalf("read outbox ddl: %v", err)
	}
	if !strings.Contains(ddl, "STRICT") {
		t.Errorf("outbox is not STRICT:\n%s", ddl)
	}
	for _, idx := range []string{"ux_outbox_dedup", "idx_outbox_pending", "idx_outbox_subject"} {
		var name string
		err := s.r.QueryRow(`SELECT name FROM sqlite_master WHERE type='index' AND name=?`, idx).Scan(&name)
		if err != nil {
			t.Errorf("index %s missing from the migrated schema: %v", idx, err)
		}
	}
}

// TestFinishRunWritesNotificationsAtomically is test plan 1: the alert rides
// the state change transaction. A rolled back finish leaves neither a verdict
// nor an alert row; a committed one leaves both.
func TestFinishRunWritesNotificationsAtomically(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	run := seedRunForNotify(t, s, false)

	note := aNote(model.TopicRunSucceeded, run.JobName, "vakt",
		model.TopicRunSucceeded+"|"+run.JobName+"|vakt|"+run.ID+":1")

	got, err := s.FinishRun(ctx, run.ID, leaseTestRef(), FinishReason{Code: reason.RUNSucceeded}, note)
	if err != nil {
		t.Fatalf("finish with notification: %v", err)
	}
	if got != "succeeded" {
		t.Fatalf("state = %s, want succeeded", got)
	}
	if n := countOutbox(t, s); n != 1 {
		t.Fatalf("outbox rows = %d, want 1", n)
	}

	// The refusal path: a lost lease must leave the notification unwritten.
	rerun := seedRunForNotify(t, s, true)
	staleNote := aNote(model.TopicRunFailed, rerun.JobName, "vakt",
		model.TopicRunFailed+"|"+rerun.JobName+"|vakt|"+rerun.ID+":1")
	stale := LeaseRef{Owner: "ghost", Epoch: 99}
	if _, ferr := s.FinishRun(ctx, rerun.ID, stale, FinishReason{Code: reason.RUNFailedStep}, staleNote); !errors.Is(ferr, ErrLeaseLost) {
		t.Fatalf("finish under a ghost lease = %v, want ErrLeaseLost", ferr)
	}
	if n := countOutbox(t, s); n != 1 {
		t.Errorf("a refused finish leaked %d rows into the outbox, want still 1", n)
	}
}

// TestDedupYieldsExactlyOneRow is test plan 3: calling the notification path
// a hundred times for one logical event still yields one row.
func TestDedupYieldsExactlyOneRow(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()

	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	note := aNote(model.TopicRunFailed, "backup-db", "vakt", "run.failed|backup-db|vakt|01JQ:1")
	for i := 0; i < 100; i++ {
		if ierr := insertNotificationsTx(tx, []model.Notification{note}); ierr != nil {
			t.Fatalf("insert %d: %v", i, ierr)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	if n := countOutbox(t, s); n != 1 {
		t.Fatalf("100 identical events produced %d rows, want 1", n)
	}
}

// TestThrottleCollapsesRepeatedGroupsIntoOne is test plan 4's store half:
// fifty failures inside one window keep one deliverable row and count 49 in
// the bookkeeping; the first event after the window delivers fresh.
func TestThrottleCollapsesRepeatedGroupsIntoOne(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	win := 15 * time.Minute
	base := time.UnixMilli(10_000_000).UTC()

	flush := func(notes []model.Notification) {
		tx, terr := s.w.BeginTx(ctx, nil)
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		if ierr := insertNotificationsTx(tx, notes); ierr != nil {
			t.Fatalf("insert batch: %v", ierr)
		}
		if cerr := tx.Commit(); cerr != nil {
			t.Fatalf("commit: %v", cerr)
		}
	}

	grouped := func(at time.Time, seq int) model.Notification {
		n := aNote(model.TopicRunFailed, "backup-db", "vakt",
			"run.failed|backup-db|vakt|01JQ:"+itoa(seq))
		n.Throttle = win
		n.GroupKey = "reason_code=STEP_FAILED_NONZERO_EXIT"
		n.CreatedAt = at
		n.AvailableAt = at
		return n
	}

	// Fifty failures five seconds apart: all of them land INSIDE one fifteen
	// minute window, which is the situation throttle exists for.
	const step = 5 * time.Second
	for i := 0; i < 50; i++ {
		flush([]model.Notification{grouped(base.Add(step*time.Duration(i)), i)})
	}
	if n := countOutbox(t, s); n != 1 {
		t.Fatalf("fifty grouped failures produced %d rows, want 1", n)
	}
	_, _, suppressed, ok := rawWindow(t, s, model.TopicRunFailed, "vakt", "reason_code=STEP_FAILED_NONZERO_EXIT")
	if !ok {
		t.Fatal("the window vanished")
	}
	if suppressed != 49 {
		t.Fatalf("suppressed = %d, want 49 collapsed events", suppressed)
	}

	flush([]model.Notification{grouped(base.Add(20*time.Minute), 50)}) // first failure after the window
	if n := countOutbox(t, s); n != 2 {
		t.Fatalf("first event after the window did not insert (rows=%d), want 2", n)
	}
	_, newOpened, newSuppressed, _ := rawWindow(t, s, model.TopicRunFailed, "vakt", "reason_code=STEP_FAILED_NONZERO_EXIT")
	if newOpened != base.Add(20*time.Minute).UnixMilli() || newSuppressed != 0 {
		t.Errorf("window did not reopen: opened=%d suppressed=%d", newOpened, newSuppressed)
	}

	claim := time.UnixMilli(newOpened + 1_000)
	msgs, cerr := s.ClaimOutbox(ctx, 10, claim, 30*time.Second)
	if cerr != nil {
		t.Fatalf("claim: %v", cerr)
	}
	if len(msgs) != 2 {
		t.Fatalf("claimed %d rows, want both openers", len(msgs))
	}
	for _, m := range msgs {
		if m.Suppressed != 0 {
			t.Errorf("opener row reported %d suppressed of its own, want 0", m.Suppressed)
		}
	}
}

// TestClaimMarksAttemptsAndVisibilityCoversCrashRecovery drives the delivery
// lifecycle: claims raise attempts, reschedules come back due, unknown
// targets fail permanently, and delivered rows never re-claim.
func TestClaimMarksAttemptsAndVisibilityCoversCrashRecovery(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	// aNote's stamps live at 1_000_000 ms, so every "now" here sits after them.
	now := time.UnixMilli(1_100_000).UTC()

	seed := func(key string) int64 {
		tx, terr := s.w.BeginTx(ctx, nil)
		if terr != nil {
			t.Fatalf("begin: %v", terr)
		}
		n := aNote(model.TopicRunFailed, "backup-db", "vakt", key)
		if ierr := insertNotificationsTx(tx, []model.Notification{n}); ierr != nil {
			t.Fatalf("insert: %v", ierr)
		}
		if cerr := tx.Commit(); cerr != nil {
			t.Fatalf("commit: %v", cerr)
		}
		rows, lerr := s.ListNotifications(context.Background(), NotificationFilter{})
		if lerr != nil || len(rows) == 0 {
			t.Fatalf("list after seed: %d rows (%v)", len(rows), lerr)
		}
		return rows[0].ID
	}
	id := seed("one")

	msgs, err := s.ClaimOutbox(ctx, 5, now, 30*time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("first claim = %d msgs (%v), want 1", len(msgs), err)
	}
	if msgs[0].ID != id {
		t.Fatalf("claimed id %d, want %d", msgs[0].ID, id)
	}

	inside, err := s.ClaimOutbox(ctx, 5, now.Add(29*time.Second), 30*time.Second)
	if err != nil || len(inside) != 0 {
		t.Fatalf("row visible before its visibility elapsed: %d msgs (%v)", len(inside), err)
	}

	back, err := s.ClaimOutbox(ctx, 5, now.Add(31*time.Second), 30*time.Second)
	if err != nil || len(back) != 1 {
		t.Fatalf("crashed row was not handed back: %d msgs (%v)", len(back), err)
	}
	if back[0].ID != id || back[0].Attempts != 2 {
		t.Fatalf("re-claim = id %d attempts %d, want same row with attempts 2", back[0].ID, back[0].Attempts)
	}

	if derr := s.RescheduleOutbox(ctx, id, now.Add(32*time.Second), "notifier exited 1"); derr != nil {
		t.Fatalf("reschedule: %v", derr)
	}
	if ferr := s.MarkOutboxDelivered(ctx, id, now.Add(33*time.Second)); ferr != nil {
		t.Fatalf("deliver: %v", ferr)
	}
	last, err := s.ClaimOutbox(ctx, 5, now.Add(10*time.Minute), 30*time.Second)
	if err != nil || len(last) != 0 {
		t.Fatalf("a delivered row came back for delivery: %d msgs (%v)", len(last), err)
	}
	rows, lerr := s.ListNotifications(ctx, NotificationFilter{State: "delivered"})
	if lerr != nil || len(rows) != 1 || rows[0].ID != id {
		t.Fatalf("delivered history wrong: %+v (%v)", rows, lerr)
	}
}

// TestMaxAttemptsGivesUpForeverAndRetryUnlocks pins AC five's tail: failed_at
// seals the row into retention (never deleted silently) and an operator's
// retry unlocks it without resetting attempts.
func TestMaxAttemptsGivesUpForeverAndRetryUnlocks(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	now := time.UnixMilli(1_050_000).UTC()
	tx, _ := s.w.BeginTx(ctx, nil)
	n := aNote(model.TopicRunFailed, "backup-db", "vakt", "give-up")
	if err := insertNotificationsTx(tx, []model.Notification{n}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	msgs, err := s.ClaimOutbox(ctx, 5, now, time.Second)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("claim: %d msgs (%v)", len(msgs), err)
	}
	id := msgs[0].ID
	if ferr := s.MarkOutboxFailed(ctx, id, now.Add(30*time.Second), "gave up after max_attempts"); ferr != nil {
		t.Fatalf("mark failed: %v", ferr)
	}

	dueAgain, err := s.ClaimOutbox(ctx, 5, now.Add(24*time.Hour), time.Second)
	if err != nil || len(dueAgain) != 0 {
		t.Fatalf("a given-up row re-entered rotation: %d msgs (%v)", len(dueAgain), err)
	}
	kept, kerr := s.ListNotifications(ctx, NotificationFilter{State: "failed"})
	if kerr != nil || len(kept) != 1 {
		t.Fatalf("failed rows must survive forever: %d rows (%v)", len(kept), kerr)
	}

	next, rerr := s.RetryOutbox(ctx, id, now.Add(25*time.Hour))
	if rerr != nil || next != "failed" {
		t.Fatalf("retry = %q (%v), want unlocking the failed row", next, rerr)
	}
	retried, qerr := s.ClaimOutbox(ctx, 5, now.Add(25*time.Hour+time.Second), time.Second)
	if qerr != nil || len(retried) != 1 || retried[0].Attempts != msgs[0].Attempts+1 {
		t.Fatalf("retried row claims as %+v (%v): attempts must rise, not reset", retried, qerr)
	}

	if _, uerr := s.RetryOutbox(ctx, 999999, now); !errors.Is(uerr, ErrNotificationNotFound) {
		t.Errorf("retrying an unknown id = %v, want NotFound", uerr)
	}
	if derr := s.MarkOutboxDelivered(ctx, id, now.Add(26*time.Hour)); derr != nil {
		t.Fatalf("deliver after retry: %v", derr)
	}
	if _, xerr := s.RetryOutbox(ctx, id, now.Add(27*time.Hour)); xerr == nil {
		t.Errorf("retrying a delivered row succeeded, want refusal")
	}
}

// TestApplySLAEpisodesEmitsOncePerEpisode is the guarded transition behind AC
// eight: breach opens once, further checks stay silent, recovery resets, and
// the next breach emits again.
func TestApplySLAEpisodesEmitsOncePerEpisode(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	now := time.UnixMilli(200_000).UTC()
	note := func(tag string) model.Notification {
		n := aNote(model.TopicSLABreached, "backup-db", "vakt", "")
		n.DedupKey = "job.sla_breached|backup-db|vakt|" + tag
		return n
	}

	open := SLAEpisodeChange{Job: "backup-db", Breaching: true, Notes: []model.Notification{note("ep1")}}
	if err := s.ApplySLAEpisodes(ctx, []SLAEpisodeChange{open}, now); err != nil {
		t.Fatalf("open episode: %v", err)
	}
	if n := countOutbox(t, s); n != 1 {
		t.Fatalf("first breach emitted %d rows, want 1", n)
	}

	steady := SLAEpisodeChange{Job: "backup-db", Breaching: true}
	for i := 0; i < 9; i++ { // nine more checks inside the same breach
		if err := s.ApplySLAEpisodes(ctx, []SLAEpisodeChange{steady}, now.Add(time.Duration(i+1)*time.Minute)); err != nil {
			t.Fatalf("steady check %d: %v", i, err)
		}
	}
	if n := countOutbox(t, s); n != 1 {
		t.Fatalf("steady-state breach kept emitting: %d rows, want 1", n)
	}

	recovered := SLAEpisodeChange{Job: "backup-db", Breaching: false}
	if err := s.ApplySLAEpisodes(ctx, []SLAEpisodeChange{recovered}, now.Add(2*time.Hour)); err != nil {
		t.Fatalf("recover: %v", err)
	}
	var episodes int64
	if err := s.withRead(ctx, func(c context.Context, r reader) error {
		return r.QueryRowContext(c, `SELECT COUNT(*) FROM sla_episodes`).Scan(&episodes)
	}); err != nil {
		t.Fatalf("count episodes: %v", err)
	}
	if episodes != 0 {
		t.Fatalf("episode survived recovery: %d rows", episodes)
	}
	if n := countOutbox(t, s); n != 1 {
		t.Errorf("recovery wrote rows: %d", n)
	}

	second := SLAEpisodeChange{Job: "backup-db", Breaching: true, Notes: []model.Notification{note("ep2")}}
	if serr := s.ApplySLAEpisodes(ctx, []SLAEpisodeChange{second}, now.Add(3*time.Hour)); serr != nil {
		t.Fatalf("second episode: %v", serr)
	}
	if n := countOutbox(t, s); n != 2 {
		t.Fatalf("next breach episode left %d rows total, want 2", n)
	}
}

// TestPruneOnlyTouchesDeliveredRowsUnderHorizon covers the janitor half of
// AC twelve at the store seam.
func TestPruneOnlyTouchesDeliveredRowsUnderHorizon(t *testing.T) {
	s := migratedStore(t)
	ctx := context.Background()
	base := time.UnixMilli(300_000).UTC()

	add := func(key string) int64 {
		tx, _ := s.w.BeginTx(ctx, nil)
		n := aNote(model.TopicRunFailed, "j", "t", key)
		n.CreatedAt = base
		n.AvailableAt = base
		if err := insertNotificationsTx(tx, []model.Notification{n}); err != nil {
			t.Fatalf("insert %s: %v", key, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatalf("commit: %v", err)
		}
		row, gerr := s.ListNotifications(ctx, NotificationFilter{})
		if gerr != nil {
			t.Fatalf("list: %v", gerr)
		}
		return row[0].ID
	}
	oldDelivered := add("old-delivered")
	oldFailed := add("old-failed")
	stillPending := add("old-pending")

	cut := base.Add(120 * 24 * time.Hour)
	if err := s.MarkOutboxDelivered(ctx, oldDelivered, cut.Add(-31*24*time.Hour)); err != nil {
		t.Fatalf("deliver old: %v", err)
	}
	if ferr := s.MarkOutboxFailed(ctx, oldFailed, cut.Add(-48*time.Hour), "broken shell"); ferr != nil {
		t.Fatalf("fail: %v", ferr)
	}
	// The pending row keeps its original available_at (base), so it looks due;
	// retention must leave it alone all the same.

	deleted, err := s.PruneDeliveredNotificationsBatch(ctx, cut)
	if err != nil {
		t.Fatalf("prune: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("pruned %d rows, want only the delivered one", deleted)
	}
	left, lerr := s.ListNotifications(ctx, NotificationFilter{Limit: 100})
	if lerr != nil {
		t.Fatalf("list after prune: %v", lerr)
	}
	found := map[int64]string{}
	for _, n := range left {
		found[n.ID] = n.State
	}
	if found[oldDelivered] != "" {
		t.Errorf("the ancient delivered row survived pruning")
	}
	if found[oldFailed] != "failed" || found[stillPending] != "pending" {
		t.Errorf("retention touched non-delivered rows: %+v", found)
	}
}

// --- fixtures ---------------------------------------------------------------

func leaseTestRef() LeaseRef { return LeaseRef{Owner: "test", Epoch: 1} }

func seedRunForNotify(t *testing.T, s *Store, fail bool) Run {
	t.Helper()
	ctx := context.Background()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, serr := s.w.ExecContext(ctx,
		`INSERT OR IGNORE INTO jobs (name, created_at, updated_at) VALUES ('notify-job', 1, 1)`,
	); serr != nil {
		t.Fatalf("seed job row: %v", serr)
	}
	const versionID = "01JVERSION00000000000000000X"
	if _, verr := s.w.ExecContext(ctx,
		`INSERT OR IGNORE INTO job_versions (id, job_name, version, spec_hash, spec_json, created_at)
		 VALUES (?, 'notify-job', 1, 'sha256:x', '{}', 2)`, versionID); verr != nil {
		t.Fatalf("seed job version: %v", verr)
	}
	run, err := s.CreateRunWithSteps(ctx, NewRun{
		JobName:      "notify-job",
		JobVersionID: versionID,
		Origin:       "manual",
		Steps:        []NewStep{{Name: "dump"}},
	})
	if err != nil {
		t.Fatalf("create run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, run.ID, LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	out := StepOutcome{Event: "step_succeeded", ReasonCode: reason.STEPSucceeded}
	if fail {
		out = StepOutcome{Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit}
	}
	if serr := s.StartStep(ctx, run.ID, "dump", leaseTestRef()); serr != nil {
		t.Fatalf("start step: %v", serr)
	}
	if oerr := s.RecordStepOutcome(ctx, run.ID, "dump", out, leaseTestRef()); oerr != nil {
		t.Fatalf("record outcome: %v", oerr)
	}
	return run
}
