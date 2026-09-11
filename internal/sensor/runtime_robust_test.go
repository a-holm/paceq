//go:build unix

package sensor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
)

// These tests prove the runtime actually uses the breaker and truncation: a
// sensor that keeps failing is not hammered past its trip threshold, and a
// recovering sensor comes back on a half-open probe. They drive the fake clock
// and poll the recording sink, so nothing sleeps against a wall clock.
//
// The breaker lives in the sensors row, so a test double that only handed out
// a fixed spec would prove nothing about the loop. rowSource is that row: it
// answers Due from the current breaker state and folds every committed
// evaluation back into it with the same functions the store's commit
// transaction uses, so what these tests exercise is the whole circuit.

// rowSource is one sensors row: the spec, its breaker columns, and the fold
// that the commit transaction applies to them.
type rowSource struct {
	mu       sync.Mutex
	spec     Spec
	failures int
	openedAt time.Time
	clk      clock.Clock
}

func (r *rowSource) Due(_ context.Context, _ int) ([]Spec, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	spec := r.spec
	spec.ConsecutiveFailures = r.failures
	spec.BreakerOpenedAt = r.openedAt
	return []Spec{spec}, nil
}

// commit folds one finished evaluation into the row, exactly as
// store.CommitSensorTick does.
func (r *rowSource) commit(res Result) {
	r.mu.Lock()
	defer r.mu.Unlock()
	before := r.failures
	after := model.SensorFailuresAfter(before, res.Outcome == Errored,
		BreakerExempt(ClassifyFailure(res.ExitCode)))
	r.openedAt = model.SensorBreakerOpenedAt(before, after, r.openedAt, r.clk.Now())
	r.failures = after
}

func (r *rowSource) state() (int, time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failures, r.openedAt
}

// rowSink records commits like recSink and folds each one into the row.
type rowSink struct {
	recSink
	row *rowSource
}

func (s *rowSink) Commit(ctx context.Context, spec Spec, tk Ticket, res Result) error {
	if err := s.recSink.Commit(ctx, spec, tk, res); err != nil {
		return err
	}
	s.row.commit(res)
	return nil
}

// newRowRuntime wires a runtime over one row. Each call is a fresh runtime over
// the same row, which is what a daemon restart is.
func newRowRuntime(row *rowSource, sink Sink, clk clock.Clock, cooldown time.Duration) *Runtime {
	return NewRuntime(newTestEvaluator(), RuntimeConfig{
		Source:          row,
		Sink:            sink,
		MaxParallel:     4,
		DrainTimeout:    2 * time.Second,
		BreakerCooldown: cooldown,
		Clock:           clk,
	})
}

// TestRuntimeBreakerStopsHammeringAFailingSensor is the fail-safe proof: a
// sensor that fails every time is evaluated at most SensorBreakerThreshold
// times, then the runtime leaves it alone instead of pressing the down service
// forever (plan 02 section 5.5).
func TestRuntimeBreakerStopsHammeringAFailingSensor(t *testing.T) {
	fc := fakecmd(t)
	fake := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	row := &rowSource{clk: fake, spec: Spec{
		Name: "failing", Job: "job",
		Argv: []string{fc, "exit", "1"}, Timeout: time.Second, MaxTriggers: 100,
	}}
	sink := &rowSink{row: row}
	rt := newRowRuntime(row, sink, fake, time.Hour)

	// The breaker only moves when an evaluation commits, so each wake must let
	// the one the previous wake dispatched land before the next wake reads the
	// row. Once tripped, a wake dispatches nothing and the count must not
	// climb.
	for want := 1; want <= model.SensorBreakerThreshold; want++ {
		if err := rt.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitCommits(t, &sink.recSink, want, 3*time.Second)
	}
	// The next wake sees a tripped breaker: it must not dispatch.
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Give any wrongly-admitted evaluation a moment to declare itself.
	time.Sleep(400 * time.Millisecond)
	if got, want := sink.total(), model.SensorBreakerThreshold; got != want {
		t.Fatalf("a failing sensor was evaluated %d times, want exactly %d (then tripped)", got, want)
	}
	failures, openedAt := row.state()
	if failures != model.SensorBreakerThreshold || openedAt.IsZero() {
		t.Fatalf("the row reads %d failures opened at %v, want %d and a stamp",
			failures, openedAt, model.SensorBreakerThreshold)
	}
}

// TestRuntimeRestartDoesNotRearmATrippedSensor is the rearm loop #220 names: a
// daemon that restarts into a hard-down sensor must not hand it a fresh budget
// of attempts. The breaker state is the row, so the second runtime, which
// remembers nothing, refuses the sensor exactly as the first one did.
func TestRuntimeRestartDoesNotRearmATrippedSensor(t *testing.T) {
	fc := fakecmd(t)
	fake := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	row := &rowSource{clk: fake, spec: Spec{
		Name: "failing", Job: "job",
		Argv: []string{fc, "exit", "1"}, Timeout: time.Second, MaxTriggers: 100,
	}}
	sink := &rowSink{row: row}

	first := newRowRuntime(row, sink, fake, time.Hour)
	for want := 1; want <= model.SensorBreakerThreshold; want++ {
		if err := first.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitCommits(t, &sink.recSink, want, 3*time.Second)
	}
	tripped := sink.total()

	// The restart: a new runtime with an empty head, over the same row.
	second := newRowRuntime(row, sink, fake, time.Hour)
	for range 3 {
		if err := second.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(400 * time.Millisecond)
	if got := sink.total(); got != tripped {
		t.Fatalf("a restart evaluated a tripped sensor %d more times; the breaker was rearmed",
			got-tripped)
	}
}

// TestRuntimeBreakerRecoversOnAProbe proves the half-open window at the runtime
// level: after the cooldown the runtime gives the sensor one probe; a
// successful probe re-closes the breaker and the sensor runs normally again.
func TestRuntimeBreakerRecoversOnAProbe(t *testing.T) {
	fc := fakecmd(t)
	fake := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	// A sensor that succeeds from the start (exit 0, no stdout => skip).
	row := &rowSource{clk: fake, spec: Spec{
		Name: "recover", Job: "job",
		Argv: []string{fc, "sensor-empty"}, Timeout: time.Second, MaxTriggers: 100,
	}}
	sink := &rowSink{row: row}
	const cooldown = 5 * time.Minute
	rt := newRowRuntime(row, sink, fake, cooldown)

	// The row arrives already tripped, the way it does after a restart.
	row.failures = model.SensorBreakerThreshold
	row.openedAt = fake.Now()

	// Inside the cooldown the runtime refuses to start it.
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if sink.total() != 0 {
		t.Fatalf("a tripped sensor inside its cooldown was evaluated %d times", sink.total())
	}

	// Past the cooldown, one probe is admitted.
	fake.Advance(cooldown + time.Second)
	if err := rt.Step(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitCommits(t, &sink.recSink, 1, 3*time.Second)
	// The probe succeeded (sensor-empty is a skip, a success), so the row is
	// closed again and the next wake runs the sensor normally.
	failures, openedAt := row.state()
	if model.SensorBreakerOpen(failures) || !openedAt.IsZero() {
		t.Fatalf("after a successful probe the row reads %d failures opened at %v, want a closed breaker",
			failures, openedAt)
	}
}

// TestRuntimeTransientExit75NeverTrips pins the exit-75 class at the runtime
// level: a sensor that fails with EX_TEMPFAIL (a flaky endpoint, rate limit) is
// retried without ever burning trip budget, so a healthy sensor is not paused
// over a temporary glitch (plan 05 section 6.2).
func TestRuntimeTransientExit75NeverTrips(t *testing.T) {
	fc := fakecmd(t)
	fake := clock.NewFake(time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC))
	row := &rowSource{clk: fake, spec: Spec{
		Name: "flaky", Job: "job",
		Argv: []string{fc, "exit", "75"}, Timeout: time.Second, MaxTriggers: 100,
	}}
	sink := &rowSink{row: row}
	rt := newRowRuntime(row, sink, fake, time.Hour)

	// Ten transient failures, all sequenced: the row must stay closed and every
	// failure must be evaluated (no trip).
	for want := 1; want <= model.SensorBreakerThreshold; want++ {
		if err := rt.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitCommits(t, &sink.recSink, want, 3*time.Second)
	}
	if failures, _ := row.state(); model.SensorBreakerOpen(failures) {
		t.Fatalf("after %d transient failures the row reads %d failures, a tripped breaker",
			model.SensorBreakerThreshold, failures)
	}
}
