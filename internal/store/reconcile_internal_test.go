package store

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
)

// The store half of startup reconciliation (#62): the outage row that explains
// a downtime gap, the synthetic missed ticks that carry the explanation into
// history, and the hanging-tick failure for evaluations the dying daemon left
// unfinished. SQL lives here and nowhere else (TestSQLStaysInStore).

var gapOrigin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

func gapStore(t *testing.T, clk clock.Clock) *Store {
	t.Helper()
	return sessionStore(t, clk, constantBoot("boot-one"))
}

func seedTick(t *testing.T, s *Store, name, outcome string, startedAt int64, code string) {
	t.Helper()
	const q = `INSERT INTO ticks (id, source_kind, source_name, scheduled_for, started_at,
last_started_at, finished_at, outcome, reason_code)
VALUES (?, 'schedule', ?, ?, ?, ?, NULL, ?, ?)`
	if _, err := s.w.Exec(q, "tick-"+name+"-"+outcome, name, startedAt, startedAt, startedAt, outcome, nullString(code)); err != nil {
		t.Fatalf("seed a %s tick for %s: %v", outcome, name, err)
	}
}

func nullString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

// TestRecordOutageWritesTheGapRow covers the row that answers "was the system
// up at all": the window, why it ended, whose death it follows on from.
func TestRecordOutageWritesTheGapRow(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	// The outages row names the session that died through a real foreign
	// key, so the test opens one the same way a crash left it: open.
	prev, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("open the doomed session: %v", err)
	}

	id, err := s.RecordOutage(ctx, OutageInput{
		From:        gapOrigin.Add(-30 * time.Minute),
		To:          gapOrigin,
		Kind:        "boot",
		PrevSession: prev.ID,
	})
	if err != nil {
		t.Fatalf("record an outage: %v", err)
	}
	if id <= 0 {
		t.Fatalf("outage id is %d, want a rowid", id)
	}

	rows, err := s.Outages(ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("%d outages stored, want 1", len(rows))
	}
	o := rows[0]
	if !o.From.Equal(gapOrigin.Add(-30*time.Minute)) || !o.To.Equal(gapOrigin) {
		t.Errorf("outage window is [%s, %s], want [-30m, %s]", o.From, o.To, gapOrigin)
	}
	if o.Kind != "boot" {
		t.Errorf("kind is %q, want %q (the schema vocabulary, not the sketch's)", o.Kind, "boot")
	}
	if o.PrevSession != prev.ID {
		t.Errorf("prev_session is %q, want the stale session's id %q", o.PrevSession, prev.ID)
	}
	if !o.DetectedAt.Equal(gapOrigin) {
		t.Errorf("detected_at is %s, want the clock's now %s", o.DetectedAt, gapOrigin)
	}
}

// TestRecordOutageRefusesAnUnknownKind keeps the CHECK constraint's vocabulary
// the only one reachable: callers spell kinds from the schema, not ad hoc.
func TestRecordOutageRefusesAnUnknownKind(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	if _, err := s.RecordOutage(ctx, OutageInput{From: gapOrigin, To: gapOrigin, Kind: "reboot"}); err == nil {
		t.Error("the sketch's kind 'reboot' was accepted; the schema says 'boot'")
	}
}

// TestRecordMissedTicksPointAtTheOutage covers AC2's storage side: every
// synthetic tick carries TICK_MISSED_DAEMON_DOWN and names its outages row in
// reason_data, so explain can walk from the hole to the cause.
func TestRecordMissedTicksPointAtTheOutage(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	outageID, err := s.RecordOutage(ctx, OutageInput{
		From: gapOrigin.Add(-30 * time.Minute),
		To:   gapOrigin,
		Kind: "crash",
	})
	if err != nil {
		t.Fatalf("record an outage: %v", err)
	}

	batch := []MissedTick{
		{SourceName: "nightly", ScheduledFor: gapOrigin.Add(-20 * time.Minute)},
		{SourceName: "nightly", ScheduledFor: gapOrigin.Add(-10 * time.Minute)},
	}
	n, err := s.RecordMissedTicks(ctx, "01J0THISSESSION", outageID, batch)
	if err != nil {
		t.Fatalf("record missed ticks: %v", err)
	}
	if n != 2 {
		t.Fatalf("first pass inserted %d rows, want 2", n)
	}

	// The second pass over the same slots must write nothing: reconciliation
	// repeats, and a repeated explanation must not double count.
	n, err = s.RecordMissedTicks(ctx, "01J0THISSESSION", outageID, batch)
	if err != nil {
		t.Fatalf("record missed ticks again: %v", err)
	}
	if n != 0 {
		t.Fatalf("second pass inserted %d rows, want 0: the sweep is not idempotent", n)
	}

	rows, err := s.Outages(ctx)
	if err != nil {
		t.Fatalf("list outages: %v", err)
	}
	if rows[0].MissedTicks != 2 {
		t.Errorf("outage counts %d missed ticks, want 2", rows[0].MissedTicks)
	}

	var code, data, session string
	if err := s.r.QueryRow(`SELECT reason_code, reason_data, daemon_session_id FROM ticks
WHERE source_kind = 'schedule' AND source_name = 'nightly'
ORDER BY scheduled_for LIMIT 1`).
		Scan(&code, &data, &session); err != nil {
		t.Fatalf("read the synthetic tick back: %v", err)
	}
	if code != string(reason.TICKMissedDaemonDown) {
		t.Errorf("synthetic tick carries %q, want %q", code, reason.TICKMissedDaemonDown)
	}
	if !strings.Contains(data, `"outage_id":`) {
		t.Errorf("reason_data %q does not name its outages row", data)
	}
	if session != "01J0THISSESSION" {
		t.Errorf("daemon_session_id is %q, want the reconciling session", session)
	}
}

// TestFailHangingTicksOnlyClosesInheritedWork pins R4's safety edge: a tick
// that was running when the previous daemon died closes as
// TICK_ERROR_DAEMON_CRASHED, while a tick this very session opened mid flight
// is nobody's business to close. The cutoff is the session start, not age.
func TestFailHangingTicksOnlyClosesInheritedWork(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	before := gapOrigin.UnixMilli()
	after := gapOrigin.Add(5 * time.Second).UnixMilli()
	seedTick(t, s, "inherited", "running", before, "")
	seedTick(t, s, "freshwork", "running", after, "")

	closed, err := s.FailHangingTicks(ctx, gapOrigin.Add(1*time.Second))
	if err != nil {
		t.Fatalf("fail hanging ticks: %v", err)
	}
	if len(closed) != 1 {
		t.Fatalf("closed %d ticks, want exactly the inherited one", len(closed))
	}

	var inherited, fresh string
	if err := s.r.QueryRow(`SELECT outcome FROM ticks WHERE source_name = 'inherited'`).Scan(&inherited); err != nil {
		t.Fatal(err)
	}
	if err := s.r.QueryRow(`SELECT outcome FROM ticks WHERE source_name = 'freshwork'`).Scan(&fresh); err != nil {
		t.Fatal(err)
	}
	if inherited != "error" {
		t.Errorf("inherited tick outcome is %q, want error", inherited)
	}
	if fresh != "running" {
		t.Errorf("this session's tick was closed too: outcome %q, want running", fresh)
	}

	var code string
	if err := s.r.QueryRow(`SELECT reason_code FROM ticks WHERE source_name = 'inherited'`).Scan(&code); err != nil {
		t.Fatal(err)
	}
	if code != string(reason.TICKErrorDaemonCrashed) {
		t.Errorf("hanging tick carries %q, want %q", code, reason.TICKErrorDaemonCrashed)
	}
}

// TestFailHangingTicksIsQuietWhenClean covers the periodic variant's promise:
// nothing to close means no ids and no error, so no transaction had a reason
// to open in the first place.
func TestFailHangingTicksIsQuietWhenClean(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	closed, err := s.FailHangingTicks(ctx, gapOrigin)
	if err != nil {
		t.Fatalf("fail hanging ticks on a clean store: %v", err)
	}
	if len(closed) != 0 {
		t.Errorf("closed %d ticks on a clean store, want none", len(closed))
	}
}

// TestAttemptBaselinesTrackTheirStep pins the persistence behind the orphan
// sweep (#62): the pid and its /proc start ticks are written when an attempt
// starts, they stay visible as long as the step and its run are still running,
// and they survive the attempt's death as history the sweep verifies against.
func TestAttemptBaselinesTrackTheirStep(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	const runID = "01J0RUN1"
	if _, _, err := s.ClaimRun(ctx, runID, LeaseInput{Owner: "exec-1", TTL: time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", LeaseRef{Owner: "exec-1", Epoch: 1}); err != nil {
		t.Fatalf("start step: %v", err)
	}
	if err := s.RecordAttemptProcess(ctx, runID, "build", LeaseRef{Owner: "exec-1", Epoch: 1}, 4242, 1234567); err != nil {
		t.Fatalf("record the attempt's process: %v", err)
	}

	live, err := s.ActiveAttempts(ctx)
	if err != nil {
		t.Fatalf("list active attempts: %v", err)
	}
	if len(live) != 1 || live[0].PID != 4242 || live[0].StartTicks != 1234567 ||
		live[0].RunID != runID || live[0].Step != "build" {
		t.Fatalf("active attempts %+v, want the recorded baseline of run %s", live, runID)
	}

	// The executor's verdict path closes the step under its own lease. The
	// baseline stays readable as history, but nothing about it counts as
	// active any more.
	outcome := StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedExecutorLost,
		FinishedAt: time.Now().UTC(),
	}
	if err := s.RecordStepOutcome(ctx, runID, "build", outcome, LeaseRef{Owner: "exec-1", Epoch: 1}); err != nil {
		t.Fatalf("close the lost step: %v", err)
	}

	live, err = s.ActiveAttempts(ctx)
	if err != nil {
		t.Fatalf("list active attempts after the loss: %v", err)
	}
	if len(live) != 0 {
		t.Errorf("active attempts after the step closed: %+v, want none", live)
	}

	known, err := s.KnownAttempts(ctx)
	if err != nil {
		t.Fatalf("list known attempts: %v", err)
	}
	if len(known) != 1 || known[0].PID != 4242 || known[0].StartTicks != 1234567 {
		t.Errorf("known attempts %+v, want the dead attempt's baseline kept as history", known)
	}
}

// TestGapSchedulesSkipPausedOnes pins the gap walk's input rule: a paused
// schedule was not owed anything during the outage, so it must not collect
// synthetic missed ticks that would read as downtime.
func TestGapSchedulesSkipPausedOnes(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	if _, err := s.w.Exec(`INSERT INTO schedules (id, job_name, name, kind, expr, timezone,
next_tick_at, created_at, updated_at)
VALUES ('01J0SCH2', 'nightly', 'paused-one', 'cron', '*/5 * * * *', 'UTC', 2000, 1000, 1000)`); err != nil {
		t.Fatalf("seed a second schedule: %v", err)
	}
	if _, err := s.w.Exec(`UPDATE schedules SET paused = 1 WHERE name = 'paused-one'`); err != nil {
		t.Fatalf("pause it: %v", err)
	}

	got, err := s.GapSchedules(ctx)
	if err != nil {
		t.Fatalf("list gap schedules: %v", err)
	}
	for _, g := range got {
		if g.Name == "paused-one" {
			t.Errorf("the paused schedule joined the gap walk: %+v", got)
		}
	}
	if len(got) == 0 {
		t.Fatal("no schedules came back, want the enabled one")
	}
	if got[0].Expr == "" || got[0].Timezone == "" {
		t.Errorf("gap schedule %+v carries no expression or zone to walk", got[0])
	}
}

// TestOutagesWithoutTicksNamesTheUnfinishedOnes covers the marker query the
// crash recovery leans on: a row with zero ticks is either genuinely
// slotless or half written, and both come back for a completing pass.
func TestOutagesWithoutTicksNamesTheUnfinishedOnes(t *testing.T) {
	clk := clock.NewFake(gapOrigin)
	s := gapStore(t, clk)
	ctx := context.Background()

	first, err := s.RecordOutage(ctx, OutageInput{From: gapOrigin.Add(-time.Hour), To: gapOrigin, Kind: "crash"})
	if err != nil {
		t.Fatalf("record the first outage: %v", err)
	}
	if _, err := s.RecordOutage(ctx, OutageInput{From: gapOrigin.Add(-2 * time.Hour), To: gapOrigin, Kind: "boot"}); err != nil {
		t.Fatalf("record the second outage: %v", err)
	}
	// Give the first one its evidence; it must drop off the list.
	if _, err := s.RecordMissedTicks(ctx, "01J0S", first, []MissedTick{
		{SourceName: "nightly", ScheduledFor: gapOrigin.Add(-30 * time.Minute)},
	}); err != nil {
		t.Fatalf("write the first outage's ticks: %v", err)
	}

	pending, err := s.OutagesWithoutTicks(ctx)
	if err != nil {
		t.Fatalf("list unfinished outages: %v", err)
	}
	if len(pending) != 1 || pending[0].Kind != "boot" {
		t.Fatalf("unfinished outages %+v, want only the boot row", pending)
	}
}

// TestRunExistsAndOrphanKillAudit covers the sweep's two lookups: a run this
// database owns is found, and every kill lands in the event chain with the
// run's live state on both ends, so fsck's chain check stays clean.
func TestRunExistsAndOrphanKillAudit(t *testing.T) {
	s := seededStore(t)
	ctx := context.Background()

	ok, err := s.RunExists(ctx, "01J0RUN1")
	if err != nil {
		t.Fatalf("look up a seeded run: %v", err)
	}
	if !ok {
		t.Fatal("RunExists denied the seeded run")
	}
	ok, err = s.RunExists(ctx, "01J0NOTARUN0000000000")
	if err != nil {
		t.Fatalf("look up a foreign run: %v", err)
	}
	if ok {
		t.Fatal("RunExists accepted a run this database has never heard of")
	}

	if err := s.RecordOrphanKill(ctx, "01J0RUN1", 4242); err != nil {
		t.Fatalf("record an orphan kill: %v", err)
	}
	var kind, actor, fromState, toState, detail string
	if err := s.r.QueryRow(`SELECT kind, actor, from_state, to_state, detail_json
FROM run_events WHERE run_id = '01J0RUN1' ORDER BY id DESC LIMIT 1`).
		Scan(&kind, &actor, &fromState, &toState, &detail); err != nil {
		t.Fatalf("read the kill event back: %v", err)
	}
	if kind != "run.orphan_killed" || actor != "reconcile" {
		t.Fatalf("the kill event is %s by %s, want run.orphan_killed by reconcile", kind, actor)
	}
	if !strings.Contains(detail, `"pid":4242`) {
		t.Errorf("the kill event does not name the pid: %s", detail)
	}
	if fromState != "queued" || toState != "queued" {
		t.Errorf("the kill event moves %s -> %s, want the run's live state on both ends", fromState, toState)
	}

	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck after the kill event: %+v, want a clean chain", violations)
	}
}
