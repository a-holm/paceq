package daemon

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// A daemon restart is the case the in-memory breaker could not answer: the map
// it counted in died with the process, so the next daemon handed a hard-down
// sensor a fresh budget of attempts, then another hour of silence, forever.
// The breaker state is the sensors row now, so this drives the real wiring over
// a real database and counts the ticks the restart produced (#220).

// seedBreakerSensor records a job and one sensor on it.
func seedBreakerSensor(t *testing.T, ctx context.Context, s *store.Store, name string, argv string) {
	t.Helper()
	const job = "polling-job"
	const spec = `{"schema":"paceq.job.v1","name":"polling-job","max_concurrent":10,` +
		`"timeout_ms":3600000,"steps":[{"name":"c","run":["/bin/true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: job, SpecHash: "sha256:polling", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: name, JobName: job, ExecJSON: `["` + argv + `"]`,
	}); err != nil {
		t.Fatalf("seed sensor %s: %v", name, err)
	}
}

// failUntilTripped records consecutive errored evaluations of one sensor until
// its breaker is open, stamped an hour ago so a later step is inside a cooldown
// longer than that. It writes through the same commit the daemon uses.
func failUntilTripped(t *testing.T, ctx context.Context, s *store.Store, name string) {
	t.Helper()
	then := time.Now().UTC().Add(-time.Hour)
	version := int64(0)
	for i := range model.SensorBreakerThreshold {
		begin, err := s.BeginSensorTick(ctx, store.BeginSensorTickInput{
			SensorName: name, Now: then,
		})
		if err != nil {
			t.Fatalf("begin evaluation %d of %s: %v", i, name, err)
		}
		out, err := s.CommitSensorTick(ctx, store.SensorTickCommitInput{
			TickID:        begin.TickID,
			SensorName:    name,
			JobName:       "polling-job",
			CursorVersion: version,
			Outcome:       store.OutcomeError,
			ReasonCode:    reason.TICKErrorSensorFailed,
			NextEvalAt:    then.UnixMilli(),
			Now:           then,
		})
		if err != nil {
			t.Fatalf("commit evaluation %d of %s: %v", i, name, err)
		}
		if out.Fenced {
			t.Fatalf("evaluation %d of %s was fenced", i, name)
		}
		version++
	}
}

func countTicks(t *testing.T, ctx context.Context, s *store.Store, name string) int {
	t.Helper()
	ticks, err := s.SensorTicks(ctx, name, 1000)
	if err != nil {
		t.Fatalf("read the ticks of %s: %v", name, err)
	}
	return len(ticks)
}

// TestRestartDoesNotEvaluateATrippedSensor is acceptance item five: a daemon
// that comes up with consecutive_failures at the threshold writes no new tick
// for that sensor before its cooldown elapses. The healthy sensor beside it is
// evaluated in the same wake, so the silence is the breaker's and not the
// wake's.
func TestRestartDoesNotEvaluateATrippedSensor(t *testing.T) {
	ctx := context.Background()
	s := testutil.TempStore(t)
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	seedBreakerSensor(t, ctx, s, "down", "/bin/false")
	seedBreakerSensor(t, ctx, s, "healthy", "/bin/true")
	failUntilTripped(t, ctx, s, "down")

	row, err := s.GetSensor(ctx, "down")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if !model.SensorBreakerOpen(row.ConsecutiveFailures) || row.BreakerOpenedAt.IsZero() {
		t.Fatalf("the sensor is not tripped: %d failures, opened at %v",
			row.ConsecutiveFailures, row.BreakerOpenedAt)
	}
	if err := s.SetSensorDue(ctx, "down"); err != nil {
		t.Fatalf("SetSensorDue: %v", err)
	}
	downBefore := countTicks(t, ctx, s, "down")

	// The restart: a runtime with an empty head, over the state the last
	// daemon left. Its cooldown outlasts the stamp, so nothing but the row can
	// let the sensor through.
	clk := clock.System()
	rt := sensor.NewRuntime(sensor.NewEvaluator(sensor.Config{}, clk), sensor.RuntimeConfig{
		Source:          sensorSource{st: s, clk: clk, log: slog.Default()},
		Sink:            sensorSink{st: s, clk: clk},
		MaxParallel:     4,
		DrainTimeout:    2 * time.Second,
		BreakerCooldown: 24 * time.Hour,
		Clock:           clk,
		Log:             slog.Default(),
	})
	for range 3 {
		if err := rt.Step(ctx); err != nil {
			t.Fatalf("Step: %v", err)
		}
	}
	// Wait for the healthy sensor's evaluation to land, which is also the
	// window in which a wrongly-admitted one would declare itself.
	deadline := time.Now().Add(5 * time.Second)
	for countTicks(t, ctx, s, "healthy") == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the healthy sensor was never evaluated; the wake did nothing at all")
		}
		time.Sleep(20 * time.Millisecond)
	}
	time.Sleep(300 * time.Millisecond)

	if got := countTicks(t, ctx, s, "down"); got != downBefore {
		t.Fatalf("the restart wrote %d new ticks for a tripped sensor, want none", got-downBefore)
	}
}
