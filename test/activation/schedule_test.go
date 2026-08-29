package activation

import (
	"fmt"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The names the schedule half of the proof uses.
const (
	scheduleJob  = "activation-schedule"
	scheduleName = "every-second"
	// scheduleExpr fires on every second of the epoch grid. It is the fastest
	// expression the parser accepts, and the daemon's own one second tick
	// means the newest occurrence is always inside the catchup policy's
	// "happening right now" window, so every wake attempts rather than
	// records a backlog skip.
	scheduleExpr = "@every 1s"
)

// scheduleJobYAML is a job with one schedule and one short step.
func scheduleJobYAML() string {
	return fmt.Sprintf(`name: %s
description: The schedule half of the activation proof.

steps:
  - name: say-hello
    run: ["/bin/echo", "a schedule fired this"]

schedules:
  - name: %s
    cron: %q
`, scheduleJob, scheduleName, scheduleExpr)
}

// TestAppliedScheduleFiresUnderTheDaemon is the schedule half of #182. A job
// file declaring a cron schedule is applied with the real binary, a real
// daemon is started, and the row waits for a tick against source_kind
// 'schedule' and the run that tick fired.
//
// UpsertSchedule is never called. That is the whole point: the scheduler loop
// was wired to a real store all along, but apply wrote job versions and
// sensors and no schedule row at all, so DueSchedules answered with nothing
// however long the daemon ran.
func TestAppliedScheduleFiresUnderTheDaemon(t *testing.T) {
	ws := newWorkspace(t)
	path := writeJob(t, ws, "activation-schedule.yaml", scheduleJobYAML())
	applyJob(t, ws, path)

	// Apply owns the row, the scheduler owns its calendar. Reading it before
	// any daemon exists separates the two failures.
	rows := readSchedules(t, ws)
	if len(rows) != 1 {
		t.Fatalf("apply left %d schedule rows, want the one the file declares: %+v", len(rows), rows)
	}
	row := rows[0]
	if row.JobName != scheduleJob || row.Name != scheduleName {
		t.Fatalf("the applied schedule is %s/%s, want %s/%s",
			row.JobName, row.Name, scheduleJob, scheduleName)
	}
	if row.Expr != scheduleExpr {
		t.Fatalf("the applied schedule reads %q, want %q", row.Expr, scheduleExpr)
	}
	if row.Paused {
		t.Fatal("apply wrote the schedule paused, so no daemon could ever pick it up")
	}
	if row.LastTickAt != nil {
		t.Fatalf("apply wrote a last tick of %v; the scheduler owns that column", row.LastTickAt)
	}

	p := startServe(t, ws)
	p.waitReady(t)

	var ticks []store.TickView
	var runs []store.JobRunSummary
	waitFor(t, p, "the daemon to fire the applied schedule", func() bool {
		ticks = readScheduleTicks(t, ws, scheduleJob, scheduleName)
		runs = readRuns(t, ws, scheduleJob)
		return len(ticks) > 0 && len(runs) > 0
	})

	var triggered *store.TickView
	for i := range ticks {
		if ticks[i].Outcome == "triggered" {
			triggered = &ticks[i]
			break
		}
	}
	if triggered == nil {
		t.Fatalf("none of the %d schedule ticks triggered: %+v\ndaemon stderr:\n%s",
			len(ticks), ticks, p.stderr.snapshot())
	}
	if triggered.ScheduledFor.IsZero() {
		t.Error("the triggered tick carries no fire-time, so nothing claimed a slot on the calendar")
	}
	if triggered.TriggerCount != 1 {
		t.Errorf("the triggered tick carries %d triggers, want 1", triggered.TriggerCount)
	}
	t.Logf("the schedule fired for %s and left %d runs", triggered.ScheduledFor.Format("15:04:05"), len(runs))

	// The run is the other half of the claim: a tick that decides to fire and
	// materialises nothing would satisfy the tick assertion alone.
	if runs[0].JobName != scheduleJob {
		t.Errorf("the newest run belongs to job %q, want %q", runs[0].JobName, scheduleJob)
	}
	if runs[0].StepsTotal != 1 {
		t.Errorf("the run carries %d steps, want the one the file declares", runs[0].StepsTotal)
	}
}
