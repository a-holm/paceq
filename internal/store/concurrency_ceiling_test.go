package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// The job ceiling on the keyed start (#199). A keyless deferral leaves the
// queue through the keyed start and nowhere else, so max_concurrent has to
// hold there as firmly as it holds on the ordinary queue path. These tests
// walk the cases where the two paths meet inside one claim transaction.

// ceilingJob applies a job whose runs each carry their own key, so a fire
// only ever defers against a blocker planted for exactly that key.
func ceilingJob(t *testing.T, s *Store, name string, limit int) ScheduleRow {
	t.Helper()

	concApplyLimit(t, s, name, &spec.ConcurrencyKey{FromRunKey: true}, "", limit)
	return admitSchedule(t, s, name, "queue")
}

// blockKey plants a queued run holding the key the n-th fire will want. A
// queued blocker holds the key without holding a slot, which is what lets a
// job below its ceiling still collect key deferrals.
func blockKey(t *testing.T, s *Store, sched ScheduleRow, n int) {
	t.Helper()

	keyedQueuedRun(t, s, sched.JobName, fmt.Sprintf("01J0BLOCKER%015d", n),
		canonicalConcurrencyKey(sched.JobName, admitRunKey(sched, n)))
}

// deferFire materialises the n-th fire against its planted blocker and
// returns the keyless deferral it becomes.
func deferFire(t *testing.T, s *Store, sched ScheduleRow, n int) string {
	t.Helper()

	res := admitTick(t, s, sched, n)
	if res.Run.DeferReason != model.DeferReasonConcurrencyKey || res.Run.ConcurrencyKey != "" {
		t.Fatalf("setup: fire %d is not a keyless key deferral: %+v", n, res.Run)
	}
	return res.Run.ID
}

// freeTheKeys takes every planted blocker terminal and brings every deferral
// due, which is the state one claim pass then has to rule on: free keys, no
// running rows, and more due deferrals than the ceiling allows.
func freeTheKeys(t *testing.T, s *Store, job string) {
	t.Helper()

	ctx := context.Background()
	now := time.Now().UnixMilli()
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET state = 'succeeded', finished_at = ?, reason_code = ?, updated_at = ?
		 WHERE job_name = ? AND state = 'queued' AND concurrency_key IS NOT NULL`,
		now, string(reason.RUNSucceeded), now, job); err != nil {
		t.Fatalf("finish the blockers of %s: %v", job, err)
	}
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET available_at = ? WHERE job_name = ? AND defer_reason = 'concurrency_key'`,
		time.Now().Add(-time.Minute).UnixMilli(), job); err != nil {
		t.Fatalf("make the deferrals of %s due: %v", job, err)
	}
}

// runningCount is what the ceiling is a ceiling on.
func runningCount(t *testing.T, s *Store, job string) int {
	t.Helper()

	var n int
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE job_name = ? AND state = 'running'`, job).Scan(&n); err != nil {
		t.Fatalf("count the running runs of %s: %v", job, err)
	}
	return n
}

// ceilingViolations is the production I12 sweep filtered to one job, so an
// unrelated finding from a planted fixture row cannot mask or fake this one.
func ceilingViolations(t *testing.T, s *Store, job string) []Violation {
	t.Helper()

	all, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	var out []Violation
	for _, v := range all {
		if v.Check == "I12" && v.Subject == "job "+job {
			out = append(out, v)
		}
	}
	return out
}

// Two deferrals, two free keys, one pass. The keyed start walks its
// candidates in a loop, so without a ceiling test between iterations both
// start and the job runs twice against a ceiling of one.
func TestTwoKeyDeferralsCannotStartPastTheJobCeiling(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sched := ceilingJob(t, s, "ceiling", 1)

	for n := range 2 {
		blockKey(t, s, sched, n)
	}
	first := deferFire(t, s, sched, 0)
	second := deferFire(t, s, sched, 1)
	freeTheKeys(t, s, "ceiling")

	if n := runningCount(t, s, "ceiling"); n != 0 {
		t.Fatalf("setup: %d runs are already running", n)
	}
	if v := ceilingViolations(t, s, "ceiling"); len(v) != 0 {
		t.Fatalf("setup: the fixture is already over the ceiling: %+v", v)
	}
	before := deferralEvents(t, s, first) + deferralEvents(t, s, second)

	claims, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "w1", TTL: time.Minute, Limit: 2})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if n := runningCount(t, s, "ceiling"); n != 1 {
		t.Errorf("%d runs of ceiling are running against max_concurrent 1 (%d claimed)", n, len(claims))
	}
	if v := ceilingViolations(t, s, "ceiling"); len(v) != 0 {
		t.Errorf("fsck reports the claim's own work as corruption: %+v", v)
	}
	waiting := wholeDeferrals(t, s, "ceiling")
	if waiting != 1 {
		t.Errorf("%d deferrals are still waiting whole, want 1", waiting)
	}
	if after := deferralEvents(t, s, first) + deferralEvents(t, s, second); after != before+1 {
		t.Errorf("%d run events after the pass, want %d: a refusal writes nothing",
			after, before+1)
	}
}

// The ordinary path runs first and spends the slot. The keyed path has to see
// that inside the same transaction, or the run it starts is the second one.
func TestAnOrdinaryStartTakesTheSlotAKeyDeferralWanted(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sched := ceilingJob(t, s, "mixed", 1)

	blockKey(t, s, sched, 0)
	deferred := deferFire(t, s, sched, 0)
	freeTheKeys(t, s, "mixed")
	plainQueuedRun(t, s, "mixed", "01J0PLAIN00000000000000000")

	claims, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "w1", TTL: time.Minute, Limit: 2})
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	if len(claims) != 1 || claims[0].ID != "01J0PLAIN00000000000000000" {
		t.Errorf("the pass claimed %+v, want the ordinary queued run alone", claims)
	}
	if n := runningCount(t, s, "mixed"); n != 1 {
		t.Errorf("%d runs of mixed are running against max_concurrent 1", n)
	}
	if v := ceilingViolations(t, s, "mixed"); len(v) != 0 {
		t.Errorf("fsck reports the claim's own work as corruption: %+v", v)
	}
	var state, key, deferReason string
	if err := s.r.QueryRowContext(ctx,
		`SELECT state, COALESCE(concurrency_key, ''), COALESCE(defer_reason, '')
		 FROM runs WHERE id = ?`, deferred).Scan(&state, &key, &deferReason); err != nil {
		t.Fatalf("read the deferral: %v", err)
	}
	if state != "queued" || key != "" || deferReason != model.DeferReasonConcurrencyKey {
		t.Errorf("the refused deferral changed: state=%q key=%q defer_reason=%q",
			state, key, deferReason)
	}
}

// A ceiling of two takes exactly two of three free keys, which is the case
// the accounting between iterations exists for.
func TestThreeKeyDeferralsFillACeilingOfTwoExactly(t *testing.T) {
	ctx := context.Background()
	s := migratedStore(t)
	sched := ceilingJob(t, s, "pairwise", 2)

	for n := range 3 {
		blockKey(t, s, sched, n)
		deferFire(t, s, sched, n)
	}
	freeTheKeys(t, s, "pairwise")

	if _, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "w1", TTL: time.Minute, Limit: 3}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	if n := runningCount(t, s, "pairwise"); n != 2 {
		t.Errorf("%d runs of pairwise are running against max_concurrent 2", n)
	}
	if waiting := wholeDeferrals(t, s, "pairwise"); waiting != 1 {
		t.Errorf("%d deferrals are still waiting whole, want 1", waiting)
	}
	if v := ceilingViolations(t, s, "pairwise"); len(v) != 0 {
		t.Errorf("fsck reports the claim's own work as corruption: %+v", v)
	}
}

// The keyed start pays for its two guards with index searches only. It runs
// once per due deferral per claim pass, so a table scan here would be a scan
// per waiting run per tick.
func TestTheKeyedStartPlansThroughItsIndexes(t *testing.T) {
	s := migratedStore(t)

	plan := queryPlan(t, s, keyedStartSQL, "w1", int64(1000), int64(61_000), "01J0RUN1", "job:k")
	if strings.Contains(plan, "SCAN runs") || strings.Contains(plan, "SCAN busy") {
		t.Fatalf("the keyed start scans the runs table:\n%s", plan)
	}
	for _, want := range []string{"sqlite_autoindex_runs_1", "ux_runs_conc_key", "SEARCH busy", "SEARCH jobs"} {
		if !strings.Contains(plan, want) {
			t.Errorf("the keyed start plans without %s:\n%s", want, plan)
		}
	}
}

// plainQueuedRun plants a due queued run with no key at all: the ordinary
// claim candidate the keyed path has to account for.
func plainQueuedRun(t *testing.T, s *Store, job, id string) {
	t.Helper()

	now := time.Now().UnixMilli()
	if _, err := s.w.ExecContext(context.Background(),
		`INSERT INTO runs (id, job_name, job_version_id, origin, state,
		 available_at, created_at, updated_at)
		 VALUES (?, ?, ?, 'schedule', 'queued', ?, ?, ?)`,
		id, job, readVersion(t, s, job), now, now, now); err != nil {
		t.Fatalf("seed a queued run on %s: %v", job, err)
	}
}

// wholeDeferrals counts the rows that are still exactly what a deferral is:
// queued, keyless, and saying why.
func wholeDeferrals(t *testing.T, s *Store, job string) int {
	t.Helper()

	var n int
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM runs WHERE job_name = ? AND state = 'queued'
		 AND concurrency_key IS NULL AND defer_reason = 'concurrency_key'`, job).Scan(&n); err != nil {
		t.Fatalf("count the waiting deferrals of %s: %v", job, err)
	}
	return n
}

// deferralEvents counts one run's events, so a pass that refuses a row can be
// held to writing nothing about it.
func deferralEvents(t *testing.T, s *Store, runID string) int {
	t.Helper()

	events, err := s.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the events of %s: %v", runID, err)
	}
	return len(events)
}
