package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// Hooks ride the frozen canonical bytes exactly like every other job field,
// so this fixture mirrors aQueuedRun and adds the notify block. The tag
// keeps every variant's SpecHash distinct even when steps repeat.
func (f *engineFixture) aQueuedRunWithHooks(t *testing.T, tag string,
	steps, args []string, needs map[string]string, timeoutMS int64, notifyJSON string,
) string {
	t.Helper()

	var encoded []string
	for i, name := range steps {
		parts := strings.Fields(args[i])
		quoted := make([]string, len(parts)+1)
		quoted[0] = strconv.Quote(f.fakeCmd(t))
		for j, p := range parts {
			quoted[j+1] = strconv.Quote(p)
		}
		run := strings.Join(quoted, ",")
		step := fmt.Sprintf(`{"name":%q,"run":[%s],"shell":false}`, name, run)
		if needs[name] != "" {
			step = fmt.Sprintf(`{"name":%q,"needs":[%q],"run":[%s],"shell":false}`,
				name, needs[name], run)
		}
		encoded = append(encoded, step)
	}
	members := []string{
		`"max_concurrent":1`,
		`"name":"e2e"`,
		`"schema":"paceq.job.v1"`,
		fmt.Sprintf(`"steps":[%s]`, strings.Join(encoded, ",")),
		fmt.Sprintf(`"timeout_ms":%d`, timeoutMS),
	}
	if notifyJSON != "" {
		members = append(members, notifyJSON)
	}
	spec := "{" + strings.Join(members, ",") + "}"

	if _, _, err := f.Store.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "e2e",
		SpecHash: "sha256:e2e-" + strings.Join(steps, "-") + "-" + tag,
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	out, err := f.Store.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "e2e",
		Actor:   "cli:1000",
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return out.Run.ID
}

// wireNotify turns the planner on, with defaults that name one failure
// target, group the way the issue's sketch does, and throttle repeats.
func (f *engineFixture) wireNotify(now time.Time) {
	f.Engine.Notify = notify.NewPlanner(model.NotifyDefaults{
		OnFailure: []string{"vakt"},
		Throttle:  15 * time.Minute,
		GroupBy:   []string{"job", "reason_code"},
	}, func() time.Time { return now })
}

// TestFailedRunLeavesExactlyOneNotification is M6's exit criterion end to
// end through the engine: an induced failure yields ONE outbox row whose
// payload carries the full contract (run id, step, reason code, error tail,
// an executable retry command).
func TestFailedRunLeavesExactlyOneNotification(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 17, 4, 0, 0, 0, time.UTC)
	f := newFixtureWithClock(t, clock.NewFake(now))
	f.wireNotify(now)

	runID := f.aQueuedRunWithHooks(t, "fail-one",
		[]string{"dump"}, []string{"bogus"}, nil, 60000,
		`"notify":{"on_failure":["vakt"]}`)

	if state := f.mustFinish(t, runID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}

	rows, err := f.Store.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("an induced failure left %d outbox rows, want exactly 1:\n%+v",
			len(rows), rows)
	}
	row := rows[0]
	if row.State != "pending" || row.Topic != model.TopicRunFailed ||
		row.Subject != "e2e" || row.Target != "vakt" {
		t.Fatalf("row identity wrong: %+v", row)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(row.Payload), &payload); err != nil {
		t.Fatalf("payload is not JSON: %v\n%s", err, row.Payload)
	}
	required := map[string]string{
		"event":       model.TopicRunFailed,
		"job":         "e2e",
		"run_id":      runID,
		"state":       "failed",
		"reason_code": "RUN_FAILED_STEP",
		"step":        "dump",
		"explain_cmd": "paceq explain run " + runID,
		"retry_cmd":   "paceq runs retry " + runID,
	}
	for key, want := range required {
		got := fmt.Sprint(payload[key])
		if got != want {
			t.Errorf("payload[%q] = %q, want %q", key, got, want)
		}
	}
	if v, ok := payload["error_tail"]; !ok || !strings.Contains(fmt.Sprint(v), "unknown mode") {
		t.Errorf("payload error_tail missing the failing step's output: %v", payload["error_tail"])
	}
	exitCode, ok := payload["exit_code"].(float64)
	if !ok || int(exitCode) != 3 {
		t.Errorf("payload exit_code = %v (%T), want 3", payload["exit_code"], payload["exit_code"])
	}
	for _, key := range []string{"started_at", "finished_at", "duration_ms", "attempt"} {
		if _, ok := payload[key]; !ok {
			t.Errorf("payload lacks %s; recipe authors were promised it", key)
		}
	}
}

// TestJobHooksOverrideDefaultsPerSide pins hook resolution: explicit empty
// lists silence that side even when daemon defaults name a target, while a
// success under empty defaults stays silent too.
func TestJobHooksOverrideDefaultsPerSide(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 17, 5, 0, 0, 0, time.UTC)
	f := newFixtureWithClock(t, clock.NewFake(now))
	f.wireNotify(now)

	// Deliberately silent: empty on_failure beats the default target.
	runID := f.aQueuedRunWithHooks(t, "silent-hook",
		[]string{"dump"}, []string{"bogus"}, nil, 60000,
		`"notify":{"on_failure":[]}`)
	if state := f.mustFinish(t, runID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}
	rows, err := f.Store.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("a deliberately silent job still notified: %+v", rows)
	}

	// No notify block at all and no default on_success: success notifies
	// nobody, while the failure hooks stay ready for real failures.
	runID = f.aQueuedRunWithHooks(t, "plain-success",
		[]string{"dump"}, []string{"exit 0"}, nil, 60000, "")
	state := f.mustFinish(t, runID)
	if state != "succeeded" {
		t.Fatalf("the happy run ended %s", state)
	}
	rows, err = f.Store.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil {
		t.Fatalf("list after success: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("success notified despite empty on_success defaults: %+v", rows)
	}
}
