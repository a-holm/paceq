package store

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The property behind the whole feature (#17): whatever order fires,
// finishes and claim passes arrive in, no concurrency key ever has two
// active holders, and every deferred row is a whole deferral (keyless,
// backed off, naming its wanted key and its blocker). The house pattern
// here is a seeded generator instead of a property library: the seeds are
// fixed, so a failure reproduces exactly, and no new dependency rides in.
//
// Each seed drives one scenario over three jobs and their three keys. An
// action materialises a fire, finishes an active run, or moves the clock
// and runs one claim pass. After every action the invariants read straight
// from the tables. A scenario ends with a bounded drain: clock forward,
// claim, finish, again and again until nothing is active. A drain that
// cannot finish would mean two deferred runs blocking each other, which is
// the deadlock this property exists to rule out.

var propKeys = []string{"a", "b", "c"}

// propJob is one scenario job: its name, the canonical key its runs carry,
// its schedule row, and how many fires it has had.
type propJob struct {
	job   string
	key   string
	sched ScheduleRow
	fire  int
}

func TestRandomSequencesKeepOneHolderPerKeyAndWholeDeferrals(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			runKeyScenario(t, seed)
		})
	}
}

func runKeyScenario(t *testing.T, seed int64) {
	t.Helper()

	base := time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC)
	clk := clock.NewFake(base)
	s := propStore(t, clk)
	ctx := context.Background()

	type keyed = propJob
	jobs := make([]keyed, 0, len(propKeys))
	for i, k := range propKeys {
		name := fmt.Sprintf("prop%d", i)
		concApply(t, s, name, constKey(k), "")
		jobs = append(jobs, keyed{
			job:   name,
			key:   name + ":" + k,
			sched: concSchedule(t, s, name),
		})
	}

	rng := rand.New(rand.NewSource(seed))
	const actions = 40
	for step := 0; step < actions; step++ {
		switch roll := rng.Intn(100); {
		case roll < 55:
			// One fire on one job. Every fire names its own slot, so the
			// dedup gate never folds two of them together.
			j := &jobs[rng.Intn(len(jobs))]
			res, err := s.MaterializeTick(ctx, TickInput{
				Schedule:       j.sched,
				ScheduledFor:   base.Add(time.Duration(j.fire) * time.Minute),
				Outcome:        OutcomeTriggered,
				RunKey:         fmt.Sprintf("%s/nightly:%d", j.job, j.fire),
				NextTickAt:     clk.Now().Add(time.Hour),
				UpdateProgress: true,
				Actor:          "property",
			})
			j.fire++
			if err != nil {
				t.Fatalf("step %d materialise: %v", step, err)
			}
			if res.Run.ID != "" && res.Run.ConcurrencyKey == "" &&
				res.Run.DeferReason != model.DeferReasonConcurrencyKey {
				t.Fatalf("step %d: a keyless run arrived without its deferral reason: %+v",
					step, res.Run)
			}

		case roll < 80:
			// One active run goes terminal, whichever it is.
			j := jobs[rng.Intn(len(jobs))]
			propFinishAnyActive(t, s, ctx, j.job, clk)

		default:
			// The ticker alone: clock moves past the backoff and one claim
			// pass runs. No bus anywhere in this file.
			clk.Advance(2 * DefaultDeferBackoff)
			if _, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "prop", TTL: time.Minute, Limit: 8}); err != nil {
				t.Fatalf("step %d claim: %v", step, err)
			}
		}
		propCheckInvariants(t, s, ctx, jobs, step)
	}

	// The drain: nothing may be stuck. Every pass either starts waiting
	// work or ends running work, so the count of active rows must fall to
	// zero inside the bound.
	for i := 0; i < 500; i++ {
		var left int
		if err := s.r.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM runs WHERE state IN ('queued', 'running')`).Scan(&left); err != nil {
			t.Fatalf("drain count: %v", err)
		}
		if left == 0 {
			propCheckInvariants(t, s, ctx, jobs, actions+i)
			return
		}
		clk.Advance(2 * DefaultDeferBackoff)
		if _, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "prop", TTL: time.Minute, Limit: 64}); err != nil {
			t.Fatalf("drain claim %d: %v", i, err)
		}
		for _, j := range jobs {
			propFinishEveryRunning(t, s, ctx, j.job, clk)
		}
	}
	t.Fatal("the scenario never drained: something is stuck holding or wanting a key")
}

// propStore opens a store on the given clock, migrated.
func propStore(t *testing.T, clk *clock.Fake) *Store {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(context.Background(), path, Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// propFinishAnyActive sends one active run of the job to succeeded, through
// the same door every other store test uses for a terminal write outside a
// claim.
func propFinishAnyActive(t *testing.T, s *Store, ctx context.Context, job string, clk *clock.Fake) {
	t.Helper()
	var id string
	err := s.r.QueryRowContext(ctx,
		`SELECT id FROM runs WHERE job_name = ? AND state IN ('queued', 'running')
		 ORDER BY RANDOM() LIMIT 1`, job).Scan(&id)
	if err != nil {
		return // nothing active for this job right now
	}
	now := clk.Now().UnixMilli()
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET state = 'succeeded', finished_at = ?, reason_code = ?, updated_at = ?
		 WHERE id = ?`,
		now, string(reason.RUNSucceeded), now, id); err != nil {
		t.Fatalf("finish %s: %v", id, err)
	}
}

func propFinishEveryRunning(t *testing.T, s *Store, ctx context.Context, job string, clk *clock.Fake) {
	t.Helper()
	now := clk.Now().UnixMilli()
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET state = 'succeeded', finished_at = ?, reason_code = ?, updated_at = ?
		 WHERE job_name = ? AND state = 'running'`,
		now, string(reason.RUNSucceeded), now, job); err != nil {
		t.Fatalf("end the running runs of %s: %v", job, err)
	}
}

// propCheckInvariants reads the tables and stops at the first broken rule.
func propCheckInvariants(t *testing.T, s *Store, ctx context.Context, jobs []propJob, step int) {
	t.Helper()

	// Rule 1: at most one active holder per key.
	for _, j := range jobs {
		var n int
		if err := s.r.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM runs
			 WHERE concurrency_key = ? AND state IN ('queued', 'running')`,
			j.key).Scan(&n); err != nil {
			t.Fatalf("step %d: count holders of %s: %v", step, j.key, err)
		}
		if n > 1 {
			t.Fatalf("step %d: %d active runs hold %s", step, n, j.key)
		}
	}

	// Rule 2: every keyless active row of these keyed jobs is a whole
	// deferral, and nothing else sits keyless.
	rows, err := s.r.QueryContext(ctx,
		`SELECT id, job_name, state, COALESCE(concurrency_key, ''),
		        defer_reason, available_at, reason_data FROM runs
		 WHERE concurrency_key IS NULL AND state IN ('queued', 'running')`)
	if err != nil {
		t.Fatalf("step %d: read the keyless actives: %v", step, err)
	}
	defer rows.Close()
	for rows.Next() {
		var id, job, state, key, deferReason, data string
		var available any
		if err := rows.Scan(&id, &job, &state, &key, &deferReason, &available, &data); err != nil {
			t.Fatalf("step %d: scan a keyless active: %v", step, err)
		}
		if deferReason != model.DeferReasonConcurrencyKey {
			t.Fatalf("step %d: run %s (%s) is keyless and not a key deferral (%q)",
				step, id, state, deferReason)
		}
		if state != "queued" {
			t.Fatalf("step %d: the deferral %s is %s, want queued", step, id, state)
		}
		if available == nil {
			t.Fatalf("step %d: the deferral %s has no backoff", step, id)
		}
		var parsed struct {
			Key      string `json:"concurrency_key"`
			Blocking string `json:"blocking_run_id"`
		}
		if err := json.Unmarshal([]byte(data), &parsed); err != nil ||
			parsed.Key == "" || parsed.Blocking == "" {
			t.Fatalf("step %d: the deferral %s does not name its key and blocker: %q",
				step, id, data)
		}
		want := ""
		for _, j := range jobs {
			if j.job == job {
				want = j.key
			}
		}
		if parsed.Key != want {
			t.Fatalf("step %d: the deferral %s wants %q, want %q", step, id, parsed.Key, want)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("step %d: walk the keyless actives: %v", step, err)
	}

	// Rule 3: every other active row carries exactly its job's key, and no
	// terminal write ever left a reason code behind unread. Only the first
	// half belongs to #17, so the second stays out of here.
	for _, j := range jobs {
		rows, err := s.r.QueryContext(ctx,
			`SELECT id, COALESCE(concurrency_key, ''), COALESCE(defer_reason, ''),
			        COALESCE(reason_code, '')
			 FROM runs WHERE job_name = ? AND state IN ('queued', 'running')
			              AND defer_reason IS NULL`, j.job)
		if err != nil {
			t.Fatalf("step %d: read the keyed actives of %s: %v", step, j.job, err)
		}
		for rows.Next() {
			var id, key, deferReason, code string
			if err := rows.Scan(&id, &key, &deferReason, &code); err != nil {
				t.Fatalf("step %d: scan a keyed active: %v", step, err)
			}
			if key != j.key {
				t.Fatalf("step %d: run %s carries %q, want %q", step, id, key, j.key)
			}
			if deferReason != "" || strings.HasPrefix(code, "RUN_DEFERRED") {
				t.Fatalf("step %d: run %s holds a key and still reads as deferred", step, id)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("step %d: walk the keyed actives: %v", step, err)
		}
		rows.Close()
	}
}
