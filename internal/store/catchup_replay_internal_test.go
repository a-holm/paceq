package store

import (
	"context"
	"testing"
	"time"
)

// The predicate that decides whether a catch-up attempt may take a fire-time
// back from the gap walk (#211). Three row shapes can hold the slot, and only
// one of them is an explanation with nothing behind it.

var replayFire = time.Date(2026, 5, 1, 10, 30, 0, 0, time.UTC)

// replayFixture is one schedule whose job admits runs, ready to have a
// fire-time contested.
func replayFixture(t *testing.T) (*Store, ScheduleRow) {
	t.Helper()
	ctx := context.Background()
	s := migratedStore(t)
	if _, _, err := s.UpsertJobVersion(ctx, JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:replay",
		SpecJSON: `{"schema":"paceq.job.v1","name":"nightly","max_concurrent":10,"steps":[{"name":"build","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	sched, err := s.UpsertSchedule(ctx, ScheduleInput{
		JobName: "nightly", Name: "default", Kind: "cron",
		Expr: "*/5 * * * *", Timezone: "UTC", Catchup: "last",
		NextTickAt: replayFire,
	})
	if err != nil {
		t.Fatalf("seed the schedule: %v", err)
	}
	return s, sched
}

// plantTick writes one row straight into the slot, in whatever shape the case
// under test needs the incumbent to have.
func plantTick(t *testing.T, s *Store, outcome, code string, triggers int) {
	t.Helper()
	if _, err := s.w.ExecContext(context.Background(), `INSERT INTO ticks
(id, source_kind, source_name, scheduled_for, started_at, last_started_at,
 outcome, reason_code, trigger_count)
VALUES ('01J0INCUMBENT0000000000', 'schedule', 'nightly/default', ?, ?, ?, ?, ?, ?)`,
		replayFire.UnixMilli(), replayFire.UnixMilli(), replayFire.UnixMilli(),
		outcome, nullIfEmpty(code), triggers); err != nil {
		t.Fatalf("plant the incumbent %s row: %v", outcome, err)
	}
}

// attemptReplay runs the catch-up attempt that contests the planted slot.
func attemptReplay(t *testing.T, s *Store, sched ScheduleRow) TickResult {
	t.Helper()
	res, err := s.MaterializeTick(context.Background(), TickInput{
		Schedule:       sched,
		ScheduledFor:   replayFire,
		Outcome:        OutcomeTriggered,
		RunKey:         "nightly/default:2026-05-01T10:30:00Z",
		NextTickAt:     replayFire.Add(5 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
	})
	if err != nil {
		t.Fatalf("the catch-up attempt: %v", err)
	}
	return res
}

// TestAChildlessMissedRowYieldsToACatchupAttempt is the one shape the
// predicate lets through: the gap walk said nobody was there, and nothing ran.
func TestAChildlessMissedRowYieldsToACatchupAttempt(t *testing.T) {
	s, sched := replayFixture(t)
	plantTick(t, s, OutcomeMissed, "TICK_MISSED_DAEMON_DOWN", 0)

	res := attemptReplay(t, s, sched)
	if !res.Claimed || !res.Replayed {
		t.Fatalf("the attempt came back Claimed=%v Replayed=%v LostTo=%q, want a replay",
			res.Claimed, res.Replayed, res.LostTo)
	}
	if res.LostTo != LossNone {
		t.Fatalf("a claimed evaluation reported LostTo=%q, want none", res.LostTo)
	}
	if res.Run.ID == "" {
		t.Fatal("the replay claimed the fire-time but materialised no run")
	}
	if n := countTicks(t, s); n != 1 {
		t.Fatalf("the replay left %d rows on one fire-time, want one: the slot uniqueness is the point", n)
	}
}

// TestAMissedRowThatOwnsATriggerKeepsItsSlot guards the predicate's second
// half. trigger_count is what separates an explanation from an evaluation
// that really started something.
func TestAMissedRowThatOwnsATriggerKeepsItsSlot(t *testing.T) {
	s, sched := replayFixture(t)
	plantTick(t, s, OutcomeMissed, "TICK_MISSED_DAEMON_DOWN", 1)

	res := attemptReplay(t, s, sched)
	if res.Claimed || res.Replayed {
		t.Fatalf("a missed row owning a trigger was taken over: Claimed=%v Replayed=%v",
			res.Claimed, res.Replayed)
	}
	if res.LostTo != LossMissed {
		t.Fatalf("the loss was reported as %q, want %q", res.LostTo, LossMissed)
	}
}

// TestARealEvaluationIsStillRefused is AC7: a rival holder's row, or this
// loop's own earlier pass, keeps the fire-time, and the caller can tell that
// loss from a loss to the gap walk.
func TestARealEvaluationIsStillRefused(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome string
		code    string
		trigger int
	}{
		{"a rival holder already triggered it", OutcomeTriggered, "", 1},
		{"an earlier pass explained it as paused", OutcomeSkipped, "TICK_SKIPPED_PAUSED", 0},
		{"an earlier pass recorded a config error", OutcomeError, "TICK_ERROR_CONFIG", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, sched := replayFixture(t)
			plantTick(t, s, tc.outcome, tc.code, tc.trigger)

			res := attemptReplay(t, s, sched)
			if res.Claimed || res.Replayed {
				t.Fatalf("a real %s evaluation was overwritten: Claimed=%v Replayed=%v",
					tc.outcome, res.Claimed, res.Replayed)
			}
			if res.LostTo != LossEvaluation {
				t.Fatalf("the loss was reported as %q, want %q", res.LostTo, LossEvaluation)
			}
			if n := countTicks(t, s); n != 1 {
				t.Fatalf("the refused attempt left %d rows on one fire-time, want one", n)
			}
		})
	}
}

// TestASkipDecisionNeverOverwritesTheGapWalk pins the asymmetry the fix rests
// on: a skip explains that nothing ran, which "missed while the daemon was
// down" already says, and says better, because it names the outage.
func TestASkipDecisionNeverOverwritesTheGapWalk(t *testing.T) {
	s, sched := replayFixture(t)
	plantTick(t, s, OutcomeMissed, "TICK_MISSED_DAEMON_DOWN", 0)

	res, err := s.MaterializeTick(context.Background(), TickInput{
		Schedule:       sched,
		ScheduledFor:   replayFire,
		Outcome:        OutcomeSkipped,
		ReasonCode:     "TICK_SKIPPED_CATCHUP_LAST_ONLY",
		NextTickAt:     replayFire.Add(5 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
	})
	if err != nil {
		t.Fatalf("the skip decision: %v", err)
	}
	if res.Claimed || res.Replayed {
		t.Fatalf("a skip decision took over the gap row: Claimed=%v Replayed=%v",
			res.Claimed, res.Replayed)
	}
	if res.LostTo != LossMissed {
		t.Fatalf("the skip's loss was reported as %q, want %q", res.LostTo, LossMissed)
	}
	var outcome, code string
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT outcome, reason_code FROM ticks WHERE scheduled_for = ?`,
		replayFire.UnixMilli()).Scan(&outcome, &code); err != nil {
		t.Fatalf("read the slot back: %v", err)
	}
	if outcome != OutcomeMissed || code != "TICK_MISSED_DAEMON_DOWN" {
		t.Fatalf("the gap row became %s/%s, want it untouched", outcome, code)
	}
}

func countTicks(t *testing.T, s *Store) int {
	t.Helper()
	var n int
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT count(*) FROM ticks WHERE scheduled_for = ?`, replayFire.UnixMilli()).Scan(&n); err != nil {
		t.Fatalf("count the rows on the fire-time: %v", err)
	}
	return n
}

// TestShadowModeReplaysTheSameSlotTheRealPathWould keeps the recorder honest
// (#32): a shadow instance must reach the same verdict about a fire-time as a
// real one, or its report understates what catch-up would have run.
func TestShadowModeReplaysTheSameSlotTheRealPathWould(t *testing.T) {
	s, sched := replayFixture(t)
	plantTick(t, s, OutcomeMissed, "TICK_MISSED_DAEMON_DOWN", 0)

	res, err := s.MaterializeTick(context.Background(), TickInput{
		Schedule:       sched,
		ScheduledFor:   replayFire,
		Outcome:        OutcomeTriggered,
		RunKey:         "nightly/default:2026-05-01T10:30:00Z",
		NextTickAt:     replayFire.Add(5 * time.Minute),
		UpdateProgress: true,
		Actor:          "scheduler",
		Shadow:         true,
	})
	if err != nil {
		t.Fatalf("the shadow attempt: %v", err)
	}
	if !res.Claimed || !res.Replayed {
		t.Fatalf("the shadow attempt came back Claimed=%v Replayed=%v, want a replay",
			res.Claimed, res.Replayed)
	}
	if res.Run.ID != "" {
		t.Fatalf("shadow mode materialised run %s; nothing may execute", res.Run.ID)
	}
	var outcome string
	if err := s.r.QueryRowContext(context.Background(),
		`SELECT outcome FROM ticks WHERE scheduled_for = ?`, replayFire.UnixMilli()).Scan(&outcome); err != nil {
		t.Fatalf("read the slot back: %v", err)
	}
	if outcome != OutcomeShadowTriggered {
		t.Fatalf("the replayed slot is %s, want %s", outcome, OutcomeShadowTriggered)
	}
}
