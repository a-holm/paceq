package model_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
)

// The breaker fold is the whole of the breaker's logic, so these tests are the
// whole of its proof: one table of outcomes drives the count, the open state
// and the admission together, which is what stops the number the runtime
// refuses work on drifting from the number the health surfaces print.

func TestSensorFailuresAfterFoldsEveryKindOfEvaluation(t *testing.T) {
	cases := []struct {
		name    string
		before  int
		errored bool
		exempt  bool
		want    int
	}{
		{"a triggered evaluation clears the count", 7, false, false, 0},
		{"a skipped evaluation clears it too", 7, false, false, 0},
		{"a permanent failure counts one", 7, true, false, 8},
		{"a permanent failure counts from zero", 0, true, false, 1},
		{"an exempt failure holds the count", 7, true, true, 7},
		{"an exempt failure on a healthy sensor stays at zero", 0, true, true, 0},
		{"a success after a trip clears the whole count", model.SensorBreakerThreshold + 3, false, false, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := model.SensorFailuresAfter(c.before, c.errored, c.exempt); got != c.want {
				t.Errorf("SensorFailuresAfter(%d, errored=%t, exempt=%t) = %d, want %d",
					c.before, c.errored, c.exempt, got, c.want)
			}
		})
	}
}

// TestSensorBreakerOpensExactlyAtTheThreshold pins the one number: one failure
// short of it the sensor still evaluates, at it the breaker is open.
func TestSensorBreakerOpensExactlyAtTheThreshold(t *testing.T) {
	if model.SensorBreakerOpen(model.SensorBreakerThreshold - 1) {
		t.Errorf("the breaker is open at %d failures, one short of the threshold",
			model.SensorBreakerThreshold-1)
	}
	if !model.SensorBreakerOpen(model.SensorBreakerThreshold) {
		t.Errorf("the breaker is closed at %d failures, the threshold itself",
			model.SensorBreakerThreshold)
	}
	if !model.SensorBreakerOpen(model.SensorBreakerThreshold + 5) {
		t.Error("the breaker is closed past its threshold")
	}
}

// TestSensorFailuresReachTheThresholdByCounting walks the fold from a healthy
// sensor to a tripped one, so the count and the open state are proven on the
// same sequence rather than each on its own.
func TestSensorFailuresReachTheThresholdByCounting(t *testing.T) {
	n := 0
	for i := 1; i < model.SensorBreakerThreshold; i++ {
		n = model.SensorFailuresAfter(n, true, false)
		if model.SensorBreakerOpen(n) {
			t.Fatalf("failure %d (count %d) already opened the breaker", i, n)
		}
	}
	n = model.SensorFailuresAfter(n, true, false)
	if n != model.SensorBreakerThreshold || !model.SensorBreakerOpen(n) {
		t.Fatalf("failure %d left count %d open=%t, want %d and open",
			model.SensorBreakerThreshold, n, model.SensorBreakerOpen(n), model.SensorBreakerThreshold)
	}
}

func TestSensorBreakerOpenedAtFollowsTheCount(t *testing.T) {
	tripped := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	now := tripped.Add(time.Hour)
	max := model.SensorBreakerThreshold

	cases := []struct {
		name           string
		before, after  int
		openedAt, want time.Time
	}{
		{"a closed breaker carries no stamp", 3, 4, time.Time{}, time.Time{}},
		{"the trip stamps now", max - 1, max, time.Time{}, now},
		{"a failed probe restarts the cooldown", max, max + 1, tripped, now},
		{"an exempt failure leaves the cooldown running", max, max, tripped, tripped},
		{"a success clears the stamp", max, 0, tripped, time.Time{}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := model.SensorBreakerOpenedAt(c.before, c.after, c.openedAt, now)
			if !got.Equal(c.want) {
				t.Errorf("SensorBreakerOpenedAt(%d, %d, %v) = %v, want %v",
					c.before, c.after, c.openedAt, got, c.want)
			}
		})
	}
}

func TestSensorBreakerAdmitsOneProbePerCooldown(t *testing.T) {
	const cooldown = time.Hour
	tripped := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	max := model.SensorBreakerThreshold

	if !model.SensorBreakerAdmits(max-1, time.Time{}, tripped, cooldown) {
		t.Error("a sensor one failure short of the threshold was refused an evaluation")
	}
	if model.SensorBreakerAdmits(max, tripped, tripped.Add(cooldown-time.Second), cooldown) {
		t.Error("a tripped sensor was evaluated inside its cooldown")
	}
	if !model.SensorBreakerAdmits(max, tripped, tripped.Add(cooldown), cooldown) {
		t.Error("a tripped sensor was refused its probe after the cooldown elapsed")
	}
	// A failed probe restamps, and the new cooldown refuses again.
	restamped := model.SensorBreakerOpenedAt(max, max+1, tripped, tripped.Add(cooldown))
	if model.SensorBreakerAdmits(max+1, restamped, tripped.Add(cooldown+time.Second), cooldown) {
		t.Error("a failed probe did not restart the cooldown")
	}
	// A row from before the stamp existed must be able to probe its way out.
	if !model.SensorBreakerAdmits(max, time.Time{}, tripped, cooldown) {
		t.Error("a tripped sensor with no stamp is refused forever")
	}
}
