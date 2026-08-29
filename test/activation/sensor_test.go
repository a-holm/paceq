package activation

import (
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The names the sensor half of the proof uses.
const (
	sensorJob  = "activation-sensor"
	sensorName = "activation-probe"
)

// sensorJobYAML is a job whose sensor is the fixture binary. The interval and
// the floor are both one second, which is the lowest the format allows, so the
// sensor is due the moment apply writes it and due again a second later.
func sensorJobYAML(fixture string) string {
	return fmt.Sprintf(`name: %s
description: The sensor half of the activation proof.

steps:
  - name: say-hello
    run: ["/bin/echo", "a sensor fired this"]

sensors:
  - name: %s
    kind: exec
    run: [%q]
    interval: 1s
    min_interval: 1s
    timeout: 30s
`, sensorJob, sensorName, fixture)
}

// TestAppliedSensorIsEvaluatedByTheDaemon is the sensor half of #182. A job
// file declaring an exec sensor is applied with the real binary, a real daemon
// is started on the same state directory, and the row waits for the two pieces
// of state only an evaluation can write: a tick against source_kind 'sensor',
// and a cursor that is no longer NULL.
//
// Nothing here writes through the store seam. The sensor row, the tick and the
// cursor all have to arrive through apply and the daemon, which is exactly the
// path that was never joined: the runtime was built with a nil Source and a
// nil Sink, so the loop woke on every tick interval, found nothing, and slept.
func TestAppliedSensorIsEvaluatedByTheDaemon(t *testing.T) {
	ws := newWorkspace(t)
	path := writeJob(t, ws, "activation-sensor.yaml", sensorJobYAML(sensorFixture(t)))
	applyJob(t, ws, path)

	// Apply materialises the declaration and no drift state. Reading it here
	// separates the two failures: a sensor row that never appeared is a
	// broken apply, and everything after this point is the daemon.
	before := readSensor(t, ws, sensorName)
	if before.JobName != sensorJob {
		t.Fatalf("the applied sensor belongs to job %q, want %q", before.JobName, sensorJob)
	}
	if before.Cursor != nil {
		t.Fatalf("apply wrote a cursor of %s; the evaluator owns that column", quote(before.Cursor))
	}
	if ticks := readSensorTicks(t, ws, sensorName); len(ticks) != 0 {
		t.Fatalf("%d ticks exist before any daemon ran: %+v", len(ticks), ticks)
	}

	p := startServe(t, ws)
	p.waitReady(t)

	var ticks []store.SensorTickView
	var row store.SensorSummary
	waitFor(t, p, "the daemon to evaluate the applied sensor", func() bool {
		row = readSensor(t, ws, sensorName)
		ticks = readSensorTicks(t, ws, sensorName)
		return len(ticks) > 0 && row.Cursor != nil
	})

	newest := ticks[0]
	if newest.Outcome != "triggered" {
		t.Errorf("the newest sensor tick is %q with reason %q, want triggered\ndaemon stderr:\n%s",
			newest.Outcome, newest.ReasonCode, p.stderr.snapshot())
	}
	if newest.TriggerCount != 1 {
		t.Errorf("the newest sensor tick carries %d triggers, want the one the fixture answers with",
			newest.TriggerCount)
	}
	// The cursor is the fixture's own sequence, so a committed cursor can
	// only have come from the fixture's answer. Which number it stands on
	// depends on how many wakes fit in the wait, so the prefix is what is
	// asserted and the value is logged.
	if !strings.HasPrefix(*row.Cursor, "event-") {
		t.Errorf("the committed cursor is %s, want the fixture's event- sequence", quote(row.Cursor))
	}
	t.Logf("the sensor committed cursor %s after %d ticks", quote(row.Cursor), len(ticks))

	if row.LastTickAt == nil {
		t.Error("the sensor row names no last tick, so nothing joined the tick to the sensor")
	}
}
