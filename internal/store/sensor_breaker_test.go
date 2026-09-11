package store

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The breaker's count is a fact of the sensors row, because that row is what
// every health surface reads: `paceq status`, `paceq status sensor/<name>`,
// `paceq sensors show` and the Prometheus gauge all end at consecutive_failures
// and breaker_opened_at. An evaluation that stops a sensor without writing them
// is a sensor nobody can see is stopped (#220).

// commitEvaluation commits one finished evaluation of the fixture sensor and
// returns the cursor_version to fence the next one against.
func commitEvaluation(t *testing.T, s *Store, outcome string, exempt bool, version int64) int64 {
	t.Helper()
	ctx := context.Background()
	begin, err := s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName: sensorName, CursorBefore: "a",
	})
	if err != nil {
		t.Fatalf("begin a sensor tick: %v", err)
	}
	code := reason.TICKErrorSensorFailed
	if outcome == OutcomeSkipped {
		code = reason.TICKSkippedSensor
	}
	out, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    sensorName,
		JobName:       sensorJob,
		CursorVersion: version,
		Outcome:       outcome,
		ReasonCode:    code,
		BreakerExempt: exempt,
		NextEvalAt:    60000,
		DurationMs:    3,
	})
	if err != nil {
		t.Fatalf("commit a sensor tick: %v", err)
	}
	if out.Fenced {
		t.Fatalf("the commit at version %d was fenced", version)
	}
	return version + 1
}

// readBreaker reads the two columns every health surface reports.
func readBreaker(t *testing.T, s *Store, name string) (failures int, openedAt sql.NullInt64) {
	t.Helper()
	if err := s.r.QueryRowContext(context.Background(),
		"SELECT consecutive_failures, breaker_opened_at FROM sensors WHERE name = ?",
		name).Scan(&failures, &openedAt); err != nil {
		t.Fatalf("read the breaker state of %s: %v", name, err)
	}
	return failures, openedAt
}

// TestSensorHealthSurfacesSeeAHardDownSensor is the defect itself: a sensor
// whose every evaluation errors until the breaker opens must be reported as
// failing by the columns and by the store read `paceq status` counts its
// deviations from. Before #220 the breaker counted in daemon memory and this
// row stayed at zero, so all four surfaces called a stopped sensor healthy.
func TestSensorHealthSurfacesSeeAHardDownSensor(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	version := int64(0)
	for range model.SensorBreakerThreshold {
		version = commitEvaluation(t, s, OutcomeError, false, version)
	}

	failures, openedAt := readBreaker(t, s, sensorName)
	if failures != model.SensorBreakerThreshold {
		t.Errorf("consecutive_failures = %d after %d errored evaluations, want %d",
			failures, model.SensorBreakerThreshold, model.SensorBreakerThreshold)
	}
	if !model.SensorBreakerOpen(failures) {
		t.Errorf("the breaker reads closed at %d failures", failures)
	}
	if !openedAt.Valid {
		t.Error("breaker_opened_at is NULL on a sensor whose breaker is open")
	}

	row, err := s.GetSensor(context.Background(), sensorName)
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.ConsecutiveFailures != model.SensorBreakerThreshold {
		t.Errorf("`sensors show` reads %d consecutive failures, want %d",
			row.ConsecutiveFailures, model.SensorBreakerThreshold)
	}

	errs, err := s.StatusSensorErrorCounts(context.Background())
	if err != nil {
		t.Fatalf("StatusSensorErrorCounts: %v", err)
	}
	if errs[sensorJob] != 1 {
		t.Errorf("`paceq status` counts %v sensor deviations, want one for %s", errs, sensorJob)
	}
}

// TestSensorBreakerCountSurvivesARestart is the rearm loop. The count is
// durable, so a daemon that restarts into a hard-down sensor reads the same
// number the daemon before it wrote and does not hand the sensor a fresh
// budget of ten attempts.
func TestSensorBreakerCountSurvivesARestart(t *testing.T) {
	ctx := context.Background()
	path := tempPath(t)

	first, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	if err := first.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedSensorJob(t, first)
	seedSensor(t, first, sensorName, "a", 0)

	version := int64(0)
	for range model.SensorBreakerThreshold {
		version = commitEvaluation(t, first, OutcomeError, false, version)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close the store: %v", err)
	}

	// The restart: a new process, a new store handle, the same file.
	second, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("reopen the store: %v", err)
	}
	t.Cleanup(func() {
		if err := second.Close(); err != nil {
			t.Errorf("close the reopened store: %v", err)
		}
	})

	failures, openedAt := readBreaker(t, second, sensorName)
	if failures != model.SensorBreakerThreshold {
		t.Errorf("consecutive_failures = %d after a restart, want the %d written before it",
			failures, model.SensorBreakerThreshold)
	}
	if !model.SensorBreakerOpen(failures) {
		t.Error("the restart rearmed the breaker: it reads closed on a hard-down sensor")
	}
	if !openedAt.Valid {
		t.Error("breaker_opened_at did not survive the restart")
	}
}

// TestSensorBreakerCountFollowsTheOutcome walks the three ways an evaluation
// moves the count, against the same fold the runtime admits work by.
func TestSensorBreakerCountFollowsTheOutcome(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	version := int64(0)
	version = commitEvaluation(t, s, OutcomeError, false, version)
	version = commitEvaluation(t, s, OutcomeError, false, version)
	if failures, _ := readBreaker(t, s, sensorName); failures != 2 {
		t.Fatalf("two permanent failures left consecutive_failures = %d, want 2", failures)
	}

	// Exit 75 and exit 64 never burn budget.
	version = commitEvaluation(t, s, OutcomeError, true, version)
	if failures, _ := readBreaker(t, s, sensorName); failures != 2 {
		t.Errorf("an exempt failure moved consecutive_failures to %d, want 2", failures)
	}

	// A skip is an answer: the sensor ran and reported, so the count clears.
	version = commitEvaluation(t, s, OutcomeSkipped, false, version)
	failures, openedAt := readBreaker(t, s, sensorName)
	if failures != 0 {
		t.Errorf("a skipped evaluation left consecutive_failures = %d, want 0", failures)
	}
	if openedAt.Valid {
		t.Errorf("a skipped evaluation left breaker_opened_at = %d, want NULL", openedAt.Int64)
	}

	// And the budget is whole again: the count climbs from zero.
	commitEvaluation(t, s, OutcomeError, false, version)
	if failures, _ := readBreaker(t, s, sensorName); failures != 1 {
		t.Errorf("after a success the next failure counts %d, want 1", failures)
	}
}

// TestResumeClearsTheWholeBreakerState pins the operator's recovery: resume
// clears both columns, so the sensor is admitted on its next due tick instead
// of sitting out a cooldown while every surface calls it healthy.
func TestResumeClearsTheWholeBreakerState(t *testing.T) {
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)
	ctx := context.Background()

	version := int64(0)
	for range model.SensorBreakerThreshold {
		version = commitEvaluation(t, s, OutcomeError, false, version)
	}
	if err := s.PauseSensor(ctx, sensorName, "backoff"); err != nil {
		t.Fatalf("PauseSensor: %v", err)
	}
	if err := s.ResumeSensor(ctx, sensorName); err != nil {
		t.Fatalf("ResumeSensor: %v", err)
	}

	failures, openedAt := readBreaker(t, s, sensorName)
	if failures != 0 {
		t.Errorf("resume left consecutive_failures = %d, want 0", failures)
	}
	if openedAt.Valid {
		t.Errorf("resume left breaker_opened_at = %d, want NULL", openedAt.Int64)
	}
	if !model.SensorBreakerAdmits(failures, nullableMillis(openedAt), time.Now().UTC(), time.Hour) {
		t.Error("a resumed sensor is still refused evaluations")
	}
}
