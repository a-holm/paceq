package daemon

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
)

// fakeSensorStore is the database under the sensor wiring. It records what the
// sink asked for so a test can assert on the call, and it lets a test move the
// row between Begin and Commit, which is the race the CAS exists to lose.
type fakeSensorStore struct {
	row store.SensorSummary

	beginIn  store.BeginSensorTickInput
	beginOut store.BeginSensorTickResult
	beginErr error

	// onBegin runs after Begin has answered, so a test can change the row the
	// way a reset or a cursor set landing mid-evaluation would.
	onBegin func(*fakeSensorStore)

	commitIn  store.SensorTickCommitInput
	commitErr error
	commits   int
}

func (f *fakeSensorStore) DueSensors(context.Context, int64, int) ([]store.SensorSummary, error) {
	return []store.SensorSummary{f.row}, nil
}

func (f *fakeSensorStore) GetSensor(context.Context, string) (store.SensorSummary, error) {
	return f.row, nil
}

func (f *fakeSensorStore) BeginSensorTick(_ context.Context, in store.BeginSensorTickInput) (store.BeginSensorTickResult, error) {
	f.beginIn = in
	if f.beginErr != nil {
		return store.BeginSensorTickResult{}, f.beginErr
	}
	if f.onBegin != nil {
		f.onBegin(f)
	}
	return f.beginOut, nil
}

func (f *fakeSensorStore) CommitSensorTick(_ context.Context, in store.SensorTickCommitInput) (store.SensorTickCommitResult, error) {
	f.commitIn = in
	f.commits++
	return store.SensorTickCommitResult{}, f.commitErr
}

func sinkFixture(f *fakeSensorStore) sensorSink {
	return sensorSink{st: f, clk: clock.System(), session: "serve:1"}
}

func specFixture() sensor.Spec {
	cur := "event-7"
	return sensor.Spec{Name: "dropzone", Job: "drop", Cursor: &cur}
}

// TestSensorSinkBeginCarriesTheCursorItStartedFrom: the intention row records
// the cursor the evaluation is about to read, not whatever the row holds when
// the sensor finishes.
func TestSensorSinkBeginCarriesTheCursorItStartedFrom(t *testing.T) {
	f := &fakeSensorStore{beginOut: store.BeginSensorTickResult{TickID: "t1", CursorVersion: 4}}

	tk, err := sinkFixture(f).Begin(t.Context(), specFixture())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if f.beginIn.CursorBefore != "event-7" {
		t.Errorf("cursor_before = %q, want the spec's cursor", f.beginIn.CursorBefore)
	}
	if f.beginIn.DaemonSessionID != "serve:1" {
		t.Errorf("session = %q, want the daemon's", f.beginIn.DaemonSessionID)
	}
	if tk.ID != "t1" || tk.Version != 4 {
		t.Errorf("ticket = %+v, want the store's tick id and version", tk)
	}
}

// TestSensorSinkFencesOnTheVersionBeginRead is the regression guard for the
// ordering bug: the commit must CAS on the version read BEFORE the sensor ran.
// Reading it after lets a reset that landed mid-evaluation be overwritten by a
// result computed from the cursor it replaced.
func TestSensorSinkFencesOnTheVersionBeginRead(t *testing.T) {
	f := &fakeSensorStore{
		row:      store.SensorSummary{Name: "dropzone", JobName: "drop", IntervalMS: 1000, CursorVersion: 4},
		beginOut: store.BeginSensorTickResult{TickID: "t1", CursorVersion: 4},
	}
	// A cursor reset lands while the sensor runs: the row's version moves on.
	f.onBegin = func(s *fakeSensorStore) { s.row.CursorVersion = 5 }

	sink := sinkFixture(f)
	tk, err := sink.Begin(t.Context(), specFixture())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	after := "event-8"
	res := sensor.Result{Outcome: sensor.Triggered, CursorAfter: &after}
	if err := sink.Commit(t.Context(), specFixture(), tk, res); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if f.commitIn.CursorVersion != 4 {
		t.Errorf("commit fenced at version %d, want 4, the version read before the sensor ran",
			f.commitIn.CursorVersion)
	}
}

// TestSensorSinkTranslatesEveryOutcome: the three outcomes reach the store as
// the store's own words, with the reason travelling verbatim for a skip.
func TestSensorSinkTranslatesEveryOutcome(t *testing.T) {
	after := "event-8"
	cases := []struct {
		name    string
		res     sensor.Result
		want    string
		wantRun int
	}{
		{
			name: "triggered",
			res: sensor.Result{
				Outcome: sensor.Triggered, CursorAfter: &after,
				Triggers: []sensor.Trigger{{RunKey: "k1"}, {RunKey: "k2"}},
			},
			want: store.OutcomeTriggered, wantRun: 2,
		},
		{
			name: "skipped",
			res:  sensor.Result{Outcome: sensor.Skipped, ReasonCode: reason.TICKSkippedSensor},
			want: store.OutcomeSkipped,
		},
		{
			name: "errored",
			res:  sensor.Result{Outcome: sensor.Errored, ReasonCode: reason.TICKErrorSensorFailed, ExitCode: 3},
			want: store.OutcomeError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeSensorStore{
				row:      store.SensorSummary{Name: "dropzone", JobName: "drop", IntervalMS: 1000},
				beginOut: store.BeginSensorTickResult{TickID: "t1", CursorVersion: 1},
			}
			sink := sinkFixture(f)
			tk, err := sink.Begin(t.Context(), specFixture())
			if err != nil {
				t.Fatalf("Begin: %v", err)
			}
			if err := sink.Commit(t.Context(), specFixture(), tk, tc.res); err != nil {
				t.Fatalf("Commit: %v", err)
			}
			if f.commitIn.Outcome != tc.want {
				t.Errorf("outcome = %q, want %q", f.commitIn.Outcome, tc.want)
			}
			if got := len(f.commitIn.Triggers); got != tc.wantRun {
				t.Errorf("triggers = %d, want %d", got, tc.wantRun)
			}
			if f.commitIn.TickID != "t1" {
				t.Errorf("tick id = %q, want the one Begin opened", f.commitIn.TickID)
			}
		})
	}
}

// TestSensorSinkPacesTheNextEvaluationByTheInterval: next_eval_at is what makes
// the sensor due again, so a commit that forgets it stops the sensor forever.
func TestSensorSinkPacesTheNextEvaluationByTheInterval(t *testing.T) {
	f := &fakeSensorStore{
		row:      store.SensorSummary{Name: "dropzone", JobName: "drop", IntervalMS: 30000},
		beginOut: store.BeginSensorTickResult{TickID: "t1", CursorVersion: 1},
	}
	sink := sinkFixture(f)
	before := time.Now().UTC()
	tk, err := sink.Begin(t.Context(), specFixture())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := sink.Commit(t.Context(), specFixture(), tk, sensor.Result{Outcome: sensor.Skipped}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	gap := f.commitIn.NextEvalAt - before.UnixMilli()
	if gap < 29000 || gap > 40000 {
		t.Errorf("next_eval_at is %d ms out, want about the 30000 ms interval", gap)
	}
}

// TestSensorSinkSurfacesAFailedCommit: a commit the store refused is not
// swallowed, or a fenced result would look like a recorded one.
func TestSensorSinkSurfacesAFailedCommit(t *testing.T) {
	boom := errors.New("cursor moved")
	f := &fakeSensorStore{
		row:       store.SensorSummary{Name: "dropzone", JobName: "drop", IntervalMS: 1000},
		beginOut:  store.BeginSensorTickResult{TickID: "t1", CursorVersion: 1},
		commitErr: boom,
	}
	sink := sinkFixture(f)
	tk, _ := sink.Begin(t.Context(), specFixture())
	if err := sink.Commit(t.Context(), specFixture(), tk, sensor.Result{Outcome: sensor.Skipped}); !errors.Is(err, boom) {
		t.Fatalf("Commit error = %v, want the store's", err)
	}
}

// TestSensorSinkBeginFailureStopsTheEvaluation: the runtime must not evaluate
// a sensor whose tick could not be opened, so the error has to come back.
func TestSensorSinkBeginFailureStopsTheEvaluation(t *testing.T) {
	boom := errors.New("no tick")
	f := &fakeSensorStore{beginErr: boom}

	if _, err := sinkFixture(f).Begin(t.Context(), specFixture()); !errors.Is(err, boom) {
		t.Fatalf("Begin error = %v, want the store's", err)
	}
	if f.commits != 0 {
		t.Errorf("committed %d times after a failed Begin, want 0", f.commits)
	}
}
