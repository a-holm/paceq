package obs_test

import (
	"os"
	"regexp"
	"strconv"
	"testing"

	"github.com/a-holm/paceq/internal/model"
)

// The shipped alert rule is a reader of the breaker like any other surface, so
// it compares against the same threshold the runtime refuses work at. A rule
// with a number of its own either pages before a sensor has stopped or stays
// quiet after it has (#220).

const alertRulesPath = "../../deploy/pulseq-alerts.yml"

var sensorErrorRateExpr = regexp.MustCompile(
	`pulseq_sensor_consecutive_failures\s*(>=|>|==)\s*(\d+)`)

func TestSensorAlertRuleFiresAtTheBreakerThreshold(t *testing.T) {
	raw, err := os.ReadFile(alertRulesPath)
	if err != nil {
		t.Fatalf("read %s: %v", alertRulesPath, err)
	}
	m := sensorErrorRateExpr.FindStringSubmatch(string(raw))
	if m == nil {
		t.Fatalf("%s has no comparison on pulseq_sensor_consecutive_failures", alertRulesPath)
	}
	op, want := m[1], m[2]
	n, err := strconv.Atoi(want)
	if err != nil {
		t.Fatalf("the rule compares against %q, which is not a number", want)
	}
	if op != ">=" || n != model.SensorBreakerThreshold {
		t.Errorf("the rule fires at %s %d, the breaker opens at >= %d",
			op, n, model.SensorBreakerThreshold)
	}
	// The comparison and the threshold agreeing is only half of it: the count
	// the rule reads must actually reach that value, which is the fold the
	// commit transaction applies.
	count := 0
	for range model.SensorBreakerThreshold {
		count = model.SensorFailuresAfter(count, true, false)
	}
	if !model.SensorBreakerOpen(count) || count < n {
		t.Errorf("a sensor failing every evaluation reaches %d, and the rule needs %s %d",
			count, op, n)
	}
}
