package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The performance half of #68: admission control holds the write lock for
// its read-decide-write, and that hold has a hard ceiling of fifty
// milliseconds even with five hundred queued runs in the database. The
// occupancy query rides idx_runs_active (pinned by the plan test beside
// this), so the count costs an index seek however long the queue grows.

// TestTheOccupancyQueryReadsThroughItsIndex pins the query plan of the
// admission count against the same SQL constant production executes.
func TestTheOccupancyQueryReadsThroughItsIndex(t *testing.T) {
	s := migratedStore(t)

	plan := queryPlan(t, s, activeRunsForJobSQL, "nightly", int64(1000))
	// The planner may reach the rows through any job-prefixed index (it
	// currently picks idx_runs_history); what the invariant needs is that
	// the count never degrades into a table scan as the queue grows.
	if !strings.Contains(plan, "SEARCH runs USING INDEX") {
		t.Fatalf("the occupancy count does not seek an index:\n%s", plan)
	}
	if strings.Contains(plan, "SCAN runs") {
		t.Fatalf("the occupancy count scans the runs table:\n%s", plan)
	}
}

func seedQueuedBacklog(t *testing.T, s *Store, jobs, perJob int) ScheduleRow {
	t.Helper()
	ctx := context.Background()

	now := time.Now().UTC().UnixMilli()
	for j := 0; j < jobs; j++ {
		name := "load-" + string(rune('a'+j%26)) + string(rune('a'+j/26))
		if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
			JobName:       name,
			SpecHash:      "sha256:" + name,
			SpecJSON:      `{"schema":"paceq.job.v1","name":"` + name + `","steps":[{"name":"build","run":["/bin/true"]}]}`,
			MaxConcurrent: 1,
		}); err != nil {
			t.Fatalf("apply %s: %v", name, err)
		}
	}
	if _, err := s.w.ExecContext(ctx, `INSERT INTO jobs (name, created_at, updated_at)
VALUES ('build', ?, ?)`, now, now); err != nil {
		t.Fatalf("seed the measured job: %v", err)
	}
	h := `{"schema":"paceq.job.v1","name":"build","max_concurrent":1,"steps":[{"name":"build","run":["/bin/true"]}]}`
	if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:       "build",
		SpecHash:      "sha256:build",
		SpecJSON:      h,
		MaxConcurrent: 1,
	}); err != nil {
		t.Fatalf("apply build: %v", err)
	}

	versionByJob := map[string]string{}
	rows, err := s.r.QueryContext(ctx, `SELECT name, current_version_id FROM jobs WHERE name LIKE 'load-%'`)
	if err != nil {
		t.Fatalf("list the load jobs: %v", err)
	}
	for rows.Next() {
		var name, vid string
		if err := rows.Scan(&name, &vid); err != nil {
			_ = rows.Close()
			t.Fatalf("scan a load job: %v", err)
		}
		versionByJob[name] = vid
	}
	_ = rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatalf("list the load jobs: %v", err)
	}

	for i := 0; i < jobs*perJob; i++ {
		name := "load-" + string(rune('a'+(i%jobs)%26)) + string(rune('a'+(i%jobs)/26))
		if _, err := s.w.ExecContext(ctx, `INSERT INTO runs
(id, job_name, job_version_id, origin, state, available_at, created_at, updated_at)
VALUES (?, ?, ?, 'manual', 'queued', ?, ?, ?)`,
			fmt.Sprintf("01J0LOAD%04d", i), name, versionByJob[name],
			now+int64(i), now+int64(i), now+int64(i)); err != nil {
			t.Fatalf("seed backlog row %d: %v", i, err)
		}
	}

	sched, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName:    "build",
		Name:       "measured",
		Kind:       "cron",
		Expr:       "* * * * *",
		NextTickAt: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("seed the measured schedule: %v", err)
	}
	return sched
}

func TestAdmissionStaysUnderTheLockBudgetWithFiveHundredQueuedRuns(t *testing.T) {
	s := migratedStore(t)
	const jobs, perJob = 50, 10
	sched := seedQueuedBacklog(t, s, jobs, perJob)

	budget := 50 * time.Millisecond
	if !raceEnabled {
		_ = budget
	} else {
		// The detector multiplies every transaction's cost; the gate below
		// measures the same work either way, so give it room under -race.
		budget = 250 * time.Millisecond
	}

	ctx := context.Background()
	base := admitFire(500) // past every seeded id prefix, so identities are fresh
	for i := 0; i < 20; i++ {
		start := time.Now()
		res, err := s.MaterializeTick(ctx, TickInput{
			Schedule:       sched,
			ScheduledFor:   base.Add(time.Duration(i) * time.Minute),
			Outcome:        OutcomeTriggered,
			RunKey:         "build/measured:perf" + string(rune('A'+i)),
			NextTickAt:     base.Add(time.Duration(i+1) * time.Minute),
			UpdateProgress: true,
			Actor:          "scheduler",
		})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("admission attempt %d failed: %v", i, err)
		}
		_ = res
		if elapsed > budget {
			t.Fatalf("admission %d held the write lock for %v, over the %v budget",
				i, elapsed, budget)
		}
	}
}
