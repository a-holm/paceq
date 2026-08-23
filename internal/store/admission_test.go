package store

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// The admission decision (#68): one fire-time, one read of the occupancy, one
// branch, all inside the transaction that owns the tick. These tests walk the
// policy matrix; the 50 way property test in race_test.go walks it under load.

// admitJob applies a job whose spec carries max_concurrent and nothing else
// surprising, so every test here reads its limit from one place.
func admitJob(t *testing.T, s *Store, name string, limit int) {
	t.Helper()

	h := spec.Compile(&spec.Job{
		Name:          name,
		MaxConcurrent: limit,
		Steps:         []spec.Step{{Name: "build", Run: []string{"/bin/true"}}},
	})
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:       name,
		SpecHash:      h.Hash,
		SpecJSON:      string(h.Canonical),
		SourcePath:    "jobs/" + name + ".yaml",
		MaxConcurrent: limit,
	}); err != nil {
		t.Fatalf("apply job %s: %v", name, err)
	}
}

// admitSchedule gives the job a schedule with an overlap policy.
func admitSchedule(t *testing.T, s *Store, job string, overlap string) ScheduleRow {
	t.Helper()

	row, err := s.UpsertSchedule(context.Background(), ScheduleInput{
		JobName:    job,
		Name:       "nightly",
		Kind:       "cron",
		Expr:       "* * * * *",
		Overlap:    overlap,
		NextTickAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("upsert schedule for %s: %v", job, err)
	}
	return row
}

// admitTick materialises one fire-time through the same door the scheduler
// loop uses. Every call gets its own fire-time, because the UNIQUE gate on
// (source_kind, source_name, scheduled_for) is doing its job.
func admitTick(t *testing.T, s *Store, sched ScheduleRow, n int) TickResult {
	t.Helper()

	fire := time.Now().Add(-time.Hour + time.Duration(n)*time.Minute).Truncate(time.Second)
	res, err := s.MaterializeTick(context.Background(), TickInput{
		Schedule:       sched,
		ScheduledFor:   fire,
		Outcome:        OutcomeTriggered,
		RunKey:         fmt.Sprintf("%s/nightly:%s", sched.JobName, fire.Format(time.RFC3339)),
		NextTickAt:     time.Now().Add(2 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
	})
	if err != nil {
		t.Fatalf("materialise tick %d for %s: %v", n, sched.JobName, err)
	}
	return res
}

// seedActiveRun plants a running run row the way a claim would have left it.
func seedActiveRun(t *testing.T, s *Store, job, id string) {
	t.Helper()

	ctx := context.Background()
	var versionID string
	if err := s.r.QueryRowContext(ctx,
		`SELECT current_version_id FROM jobs WHERE name = ?`, job).Scan(&versionID); err != nil {
		t.Fatalf("read the current version of %s: %v", job, err)
	}
	now := time.Now().UnixMilli()
	stmt := `INSERT INTO runs (id, job_name, job_version_id, origin, state, available_at,
		 lease_owner, lease_epoch, lease_expires_at, created_at, updated_at)
	VALUES (?, ?, ?, 'schedule', 'running', ?, 'test-owner', 1, ?, ?, ?)`
	expires := time.Now().Add(DefaultRunLeaseTTL).UnixMilli()
	if _, err := s.w.ExecContext(ctx, stmt, id, job, versionID, now, expires, now, now); err != nil {
		t.Fatalf("seed a running run on %s: %v", job, err)
	}
}

// tickRow reads one tick row back with its reason columns.
type tickRow struct {
	outcome    string
	reasonCode string
	reasonText string
	reasonData map[string]any
}

func readTickRow(t *testing.T, s *Store, source string, n int) tickRow {
	t.Helper()

	fire := time.Now().Add(-time.Hour + time.Duration(n)*time.Minute).Truncate(time.Second).UnixMilli()
	ctx := context.Background()
	var row tickRow
	var code, text, data sql.NullString
	err := s.r.QueryRowContext(ctx, `SELECT outcome, COALESCE(reason_code,''),
COALESCE(reason_text,''), COALESCE(reason_data,'{}') FROM ticks
WHERE source_kind = 'schedule' AND source_name = ? AND scheduled_for = ?`,
		source, fire).Scan(&row.outcome, &code, &text, &data)
	if err != nil {
		t.Fatalf("read the tick row of %s at %d: %v", source, fire, err)
	}
	row.reasonCode = code.String
	row.reasonText = text.String
	if err := json.Unmarshal([]byte(data.String), &row.reasonData); err != nil {
		t.Fatalf("the tick's reason_data is not JSON (%s): %v", data.String, err)
	}
	return row
}

func countWhere(t *testing.T, s *Store, query string, args ...any) int {
	t.Helper()
	var n int
	if err := s.r.QueryRowContext(context.Background(), query, args...).Scan(&n); err != nil {
		t.Fatalf("count rows: %v", err)
	}
	return n
}

func TestAFreeSlotAdmitsTheRunQueuedNow(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "")

	res := admitTick(t, s, sched, 0)
	if !res.Claimed || res.Deferred || res.Run.ID == "" {
		t.Fatalf("a free slot did not admit a run: %+v", res)
	}
	run, err := s.GetRun(context.Background(), res.Run.ID)
	if err != nil {
		t.Fatalf("read the admitted run back: %v", err)
	}
	if run.State != "queued" || run.DeferReason != "" {
		t.Fatalf("an admitted run is %q with defer reason %q, want queued with none",
			run.State, run.DeferReason)
	}
	if delta := time.Until(run.AvailableAt); delta > 2*time.Second || delta < -2*time.Second {
		t.Errorf("an admitted run is available in %v, want about now", delta)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM triggers`); got != 1 {
		t.Errorf("the admitted tick wrote %d trigger rows, want 1", got)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM run_events WHERE kind = 'run.queued'`); got != 1 {
		t.Errorf("the admitted run carries %d queued events, want 1", got)
	}
}

func TestAHeldSlotUnderSkipStandsTheTickDownNamingTheBlocker(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "")
	seedActiveRun(t, s, "build", "01J0ADMBLK")

	res := admitTick(t, s, sched, 0)
	if res.Run.ID != "" {
		t.Fatalf("a held slot materialised run %s under skip", res.Run.ID)
	}
	row := readTickRow(t, s, "build/nightly", 0)
	if row.outcome != OutcomeSkipped {
		t.Fatalf("the stand-down recorded outcome %q, want skipped", row.outcome)
	}
	if row.reasonCode != "TICK_SKIPPED_OVERLAP" {
		t.Fatalf("the stand-down names %q, want TICK_SKIPPED_OVERLAP", row.reasonCode)
	}
	blocking, _ := row.reasonData["blocking_run_id"].(string)
	if blocking != "01J0ADMBLK" {
		t.Errorf("reason_data.blocking_run_id is %#v, want the seeded running run", row.reasonData["blocking_run_id"])
	}
	if active, _ := row.reasonData["active"].(float64); int(active) != 1 {
		t.Errorf("reason_data.active is %v, want 1", row.reasonData["active"])
	}
	if limit, _ := row.reasonData["limit"].(float64); int(limit) != 1 {
		t.Errorf("reason_data.limit is %v, want 1", row.reasonData["limit"])
	}
	if !strings.Contains(row.reasonText, "01J0ADMBLK") {
		t.Errorf("the human line does not name the blocker: %q", row.reasonText)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM runs`); got != 1 {
		t.Errorf("the stand-down left %d run rows, want only the seeded one", got)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM triggers`); got != 0 {
		t.Errorf("the stand-down wrote %d trigger rows, want none", got)
	}
	// The cursor still moved: a stand-down was decided, not deferred to be
	// re-decided forever.
	last, _, err := s.ScheduleCursor(context.Background(), "build", "nightly")
	if err != nil || last == nil {
		t.Fatalf("the cursor did not move past the stood-down fire-time: %v", err)
	}
}

func TestAMultiRunCeilingNamesConcurrencyNotOverlap(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 3)
	sched := admitSchedule(t, s, "build", "")
	seedActiveRun(t, s, "build", "01J0ADMCA")
	seedActiveRun(t, s, "build", "01J0ADMCB")
	seedActiveRun(t, s, "build", "01J0ADMCC")

	admitTick(t, s, sched, 0)
	row := readTickRow(t, s, "build/nightly", 0)
	if row.reasonCode != "TICK_SKIPPED_CONCURRENCY" {
		t.Fatalf("three held slots named %q, want TICK_SKIPPED_CONCURRENCY", row.reasonCode)
	}
	if active, _ := row.reasonData["active"].(float64); int(active) != 3 {
		t.Errorf("reason_data.active is %v, want 3", row.reasonData["active"])
	}
}

func TestQueuePolicyDefersTheRunWithAReasonAndABlockingPointer(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "queue")
	seedActiveRun(t, s, "build", "01J0ADMQBL")

	before := time.Now()
	res := admitTick(t, s, sched, 0)
	if res.Run.ID == "" || !res.Deferred {
		t.Fatalf("queue policy neither materialised nor deferred: %+v", res)
	}
	run, err := s.GetRun(context.Background(), res.Run.ID)
	if err != nil {
		t.Fatalf("read the deferred run back: %v", err)
	}
	if run.State != "queued" {
		t.Fatalf("a deferred run is %q, want queued: deferred is not a state", run.State)
	}
	if !run.AvailableAt.After(before.Add(100 * time.Millisecond)) {
		t.Fatalf("the deferred run is available at %v, want strictly after now (available_at > now)", run.AvailableAt)
	}
	if run.DeferReason == "" {
		t.Fatal("the deferred run carries no defer_reason; the CHECK would have refused NULL, empty passes fsck I14 by luck alone")
	}
	if run.ReasonCode != "RUN_QUEUED_CONCURRENCY" {
		t.Errorf("the deferred run names %q, want RUN_QUEUED_CONCURRENCY", run.ReasonCode)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(run.ReasonData), &data); err != nil {
		t.Fatalf("the deferred run's reason_data is not JSON: %v", err)
	}
	if data["blocking_run_id"] != "01J0ADMQBL" {
		t.Errorf("reason_data.blocking_run_id is %#v, want the seeded run", data["blocking_run_id"])
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM run_events WHERE kind = 'run.deferred'`); got != 1 {
		t.Errorf("the deferral wrote %d run.deferred events, want 1", got)
	}
	if got := countWhere(t, s, `SELECT COUNT(*) FROM triggers WHERE outcome = 'accepted'`); got != 1 {
		t.Errorf("the deferral wrote %d accepted triggers, want 1", got)
	}
}

// TestADeferredRunIsNotActive is the self deadlock guard, admission side: the
// queue must never count a run that waits FOR a slot against the slots. With
// one deferred run and nothing running, the next fire-time admits.
func TestADeferredRunIsNotActive(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "queue")

	// Produce a deferred run honestly: hold the slot, defer, then free it.
	seedActiveRun(t, s, "build", "01J0ADMDH")
	first := admitTick(t, s, sched, 0)
	if first.Run.ID == "" || !first.Deferred {
		t.Fatalf("the fixture deferred nothing: %+v", first)
	}
	if _, err := s.w.ExecContext(context.Background(),
		`DELETE FROM runs WHERE id = '01J0ADMDH'`); err != nil {
		t.Fatalf("free the slot: %v", err)
	}

	n, blocking, err := s.ActiveRunsForJob(context.Background(), "build")
	if err != nil {
		t.Fatalf("read the occupancy: %v", err)
	}
	if n != 0 {
		t.Fatalf("the deferred run counts as active (%d active via %q); the queue would deadlock against itself", n, blocking)
	}

	second := admitTick(t, s, sched, 1)
	if second.Run.ID == "" || second.Deferred {
		t.Fatalf("with a free slot and one waiting run, the next fire-time neither admitted nor ran now: %+v", second)
	}
}

// poisonedConnector fails every connection, so any read that reaches the read
// pool during MaterializeTick turns into an error instead of a silent answer.
type poisonedConnector struct{}

func (poisonedConnector) Connect(context.Context) (driver.Conn, error) {
	return nil, errors.New("poisoned read pool was used")
}
func (poisonedConnector) Driver() driver.Driver { return poisonedDriver{} }

type poisonedDriver struct{}

func (poisonedDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("poisoned read pool was used")
}

// TestTheAdmissionDecisionReadsNothingOutsideItsTransaction pins the shape
// the reviewer looks for first: no TOCTOU. The store's read pool is swapped
// for one that cannot answer, then a full admission runs. It can only pass if
// every read the decision needs went through the transaction handle.
func TestTheAdmissionDecisionReadsNothingOutsideItsTransaction(t *testing.T) {
	s := migratedStore(t)
	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "")

	poisoned := sql.OpenDB(poisonedConnector{})
	defer func() { _ = poisoned.Close() }()
	realPool := s.r
	s.r = poisoned
	defer func() { s.r = realPool }()

	fire := time.Now().Add(-time.Hour).Truncate(time.Second)
	if _, err := s.MaterializeTick(context.Background(), TickInput{
		Schedule:       sched,
		ScheduledFor:   fire,
		Outcome:        OutcomeTriggered,
		RunKey:         "build/nightly:intx",
		NextTickAt:     time.Now().Add(2 * time.Minute),
		UpdateProgress: true,
	}); err != nil {
		t.Fatalf("admission touched something outside its transaction: %v", err)
	}
}
