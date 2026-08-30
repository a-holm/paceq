package status

import (
	"context"
	"testing"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
	"github.com/a-holm/paceq/internal/testutil"
)

// The status report borrows the breaker's answer; it does not hold a second
// opinion about it. A report that called the breaker open at a count the
// runtime still evaluates on would send an operator looking for a sensor that
// is running fine, and one that called it closed at a count the runtime refuses
// would hide a sensor that has stopped (#220).

// sensorAtFailures plants one sensor and drives its row to that many
// consecutive failures through the writer production uses, so what the report
// reads is what an evaluation would have left there.
func sensorAtFailures(t *testing.T, s *store.Store, name string, failures int) {
	t.Helper()
	ctx := context.Background()
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "watched",
		SpecHash: "sha256:watched",
		SpecJSON: `{"schema":"paceq.job.v1","name":"watched","steps":[{"name":"collect","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: name, JobName: "watched", ExecJSON: `["/bin/echo","{}"]`,
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}
	for i := range failures {
		begin, err := s.BeginSensorTick(ctx, store.BeginSensorTickInput{SensorName: name})
		if err != nil {
			t.Fatalf("begin evaluation %d: %v", i, err)
		}
		out, err := s.CommitSensorTick(ctx, store.SensorTickCommitInput{
			TickID:        begin.TickID,
			SensorName:    name,
			JobName:       "watched",
			CursorVersion: begin.CursorVersion,
			Outcome:       store.OutcomeError,
			ReasonCode:    reason.TICKErrorSensorFailed,
			NextEvalAt:    60000,
		})
		if err != nil {
			t.Fatalf("commit evaluation %d: %v", i, err)
		}
		if out.Fenced {
			t.Fatalf("evaluation %d was fenced", i)
		}
	}
}

// TestSensorBreakerOpenReadsTheRuntimeThreshold walks the count across the
// threshold and holds the report's answer to the runtime's. Before #220 the
// report used a threshold of its own, half the runtime's, so between the two
// numbers it reported a breaker that was not open.
func TestSensorBreakerOpenReadsTheRuntimeThreshold(t *testing.T) {
	ctx := context.Background()
	for _, failures := range []int{
		0, 1,
		model.SensorBreakerThreshold / 2,
		model.SensorBreakerThreshold - 1,
		model.SensorBreakerThreshold,
		model.SensorBreakerThreshold + 3,
	} {
		s := testutil.TempStore(t)
		if err := s.Migrate(ctx); err != nil {
			t.Fatalf("migrate: %v", err)
		}
		sensorAtFailures(t, s, "dropzone", failures)

		rep, err := BuildSubject(ctx, s, SubjectRef{Kind: "sensor", Sensor: "dropzone"}, Options{})
		if err != nil {
			t.Fatalf("BuildSubject at %d failures: %v", failures, err)
		}
		want := model.SensorBreakerOpen(failures)
		if rep.Sensor.BreakerOpen != want {
			t.Errorf("at %d consecutive failures the report says breaker_open=%t, the runtime says %t",
				failures, rep.Sensor.BreakerOpen, want)
		}
		if rep.Sensor.ConsecutiveFailures != failures {
			t.Errorf("the report reads %d consecutive failures, want %d",
				rep.Sensor.ConsecutiveFailures, failures)
		}
		wantState := StateOK
		if failures > 0 {
			wantState = StateFailed
		}
		if rep.State != wantState {
			t.Errorf("at %d consecutive failures the sensor reads %q, want %q",
				failures, rep.State, wantState)
		}
	}
}
