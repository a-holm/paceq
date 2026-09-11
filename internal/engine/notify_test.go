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
	"github.com/a-holm/paceq/internal/reason"
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

// aCancelledStepSurvivingARetry builds the shape #194 is about: an attempt
// leaves one step cancelled and one failed, the run closes, and an operator
// reopen puts only the failed step back. The cancelled step survives, because
// a reopen reopens failed and skipped steps alone.
func (f *engineFixture) aCancelledStepSurvivingARetry(t *testing.T, runID string, cancelled, failed string) {
	t.Helper()

	ctx := context.Background()
	_, epoch, err := f.Store.ClaimRun(ctx, runID, store.LeaseInput{Owner: "exec-0", TTL: time.Minute})
	if err != nil {
		t.Fatalf("claim the run for the seeding attempt: %v", err)
	}
	ref := store.LeaseRef{Owner: "exec-0", Epoch: epoch}

	if err := f.Store.StartStep(ctx, runID, cancelled, ref); err != nil {
		t.Fatalf("start step %s: %v", cancelled, err)
	}
	if err := f.Store.RecordStepOutcome(ctx, runID, cancelled, store.StepOutcome{
		Event:      string(model.EvCancelObserved),
		ReasonCode: reason.STEPCancelled,
	}, ref); err != nil {
		t.Fatalf("cancel step %s: %v", cancelled, err)
	}

	if err := f.Store.StartStep(ctx, runID, failed, ref); err != nil {
		t.Fatalf("start step %s: %v", failed, err)
	}
	exit := 3
	if err := f.Store.RecordStepOutcome(ctx, runID, failed, store.StepOutcome{
		Event:      string(model.EvStepFailed),
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   &exit,
	}, ref); err != nil {
		t.Fatalf("fail step %s: %v", failed, err)
	}

	if _, err := f.Store.FinishRun(ctx, runID, ref,
		store.FinishReason{Code: reason.RUNFailedStep}); err != nil {
		t.Fatalf("close the seeding attempt: %v", err)
	}
	out, err := f.Store.ReopenTerminalRunByOperator(ctx, runID, "cli:1000", store.ReopenOpts{})
	if err != nil {
		t.Fatalf("reopen the run: %v", err)
	}
	if len(out.Reopened) != 1 || out.Reopened[0] != failed {
		t.Fatalf("the reopen took %v, want only the failed step %s", out.Reopened, failed)
	}
}

// TestACancelledStepFinishesUnderASuccessDefault walks engine, planner,
// outbox and finish transaction together, which is the only level #194 is
// visible from. The run ends RUN_CANCELLED_MANUAL, a verdict no notification
// topic covers, while a daemon-wide on_success default names a target. The
// run has to finish anyway and leave nothing in the outbox: a notification
// planned out of that verdict carries an empty topic and subject,
// insertNotificationsTx refuses such a row inside FinishRun's transaction,
// and the refusal takes the verdict and the event with it.
func TestACancelledStepFinishesUnderASuccessDefault(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 17, 6, 0, 0, 0, time.UTC)
	f := newFixtureWithClock(t, clock.NewFake(now))
	// A daemon-wide success default over a job with no notify: block. This
	// is the shape reachable without configuring anything per job.
	f.Engine.Notify = notify.NewPlanner(model.NotifyDefaults{
		OnSuccess: []string{"exec:notify-me"},
		OnFailure: []string{"exec:notify-me"},
	}, func() time.Time { return now })

	runID := f.aQueuedRunWithHooks(t, "cancelled-step",
		[]string{"cancelled", "reopened"}, []string{"exit 0", "exit 0"}, nil, 60000, "")
	f.aCancelledStepSurvivingARetry(t, runID, "cancelled", "reopened")

	state, err := f.Engine.ExecuteRun(ctx, runID)
	if err != nil {
		t.Fatalf("the run could not finish: %v", err)
	}
	if state != string(model.RunCancelled) {
		t.Fatalf("run ended %s, want cancelled", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if detail.Run.ReasonCode != string(reason.RUNCancelledManual) {
		t.Errorf("reason_code is %q, want %s", detail.Run.ReasonCode, reason.RUNCancelledManual)
	}
	if detail.Run.CrashCount != 0 {
		t.Errorf("crash_count is %d: the run finished on its own attempt, "+
			"not after the reaper burned the budget", detail.Run.CrashCount)
	}

	rows, err := f.Store.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(rows) != 0 {
		t.Fatalf("a cancelled run planned %d notifications, want none:\n%+v", len(rows), rows)
	}

	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %d violations over the finished run:\n%+v",
			len(violations), violations)
	}

	events, err := f.Store.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read the run events: %v", err)
	}
	cancelled := 0
	for _, e := range events {
		if e.Kind == "run.cancelled" {
			cancelled++
		}
	}
	if cancelled != 1 {
		t.Errorf("the run left %d run.cancelled events, want exactly 1", cancelled)
	}
}

// TestEveryRunEndingNotifiesWhatItsVerdictSays walks the endings finishReason
// can reach and reads back what each one left in the outbox. It is the proof
// that the reason code and the notification topic are decided together and
// stay in step: a success carries run.succeeded, a failed step and a spent run
// budget carry run.failed, and a cancellation carries nothing at all.
func TestEveryRunEndingNotifiesWhatItsVerdictSays(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 17, 7, 0, 0, 0, time.UTC)
	target := []string{"exec:notify-me"}

	cases := []struct {
		name       string
		step       string
		arg        string
		timeoutMS  int64
		wantState  string
		wantReason string
		wantTopic  string
	}{
		{
			"succeeded", "work", "exit 0", 60000, "succeeded",
			string(reason.RUNSucceeded), model.TopicRunSucceeded,
		},
		{
			"failed step", "work", "exit 3", 60000, "failed",
			string(reason.RUNFailedStep), model.TopicRunFailed,
		},
		{
			"timed out", "hangs", "sleep 5s", 250, "failed",
			string(reason.RUNTimedOut), model.TopicRunFailed,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixtureWithClock(t, clock.NewFake(now))
			// Both sides named, so the row that appears proves which side
			// the verdict routed to rather than which side had a target.
			f.Engine.Notify = notify.NewPlanner(model.NotifyDefaults{
				OnSuccess: target,
				OnFailure: target,
			}, func() time.Time { return now })

			runID := f.aQueuedRunWithHooks(t, tc.name,
				[]string{tc.step}, []string{tc.arg}, nil, tc.timeoutMS, "")
			if state := f.mustFinish(t, runID); state != tc.wantState {
				t.Fatalf("run ended %s, want %s", state, tc.wantState)
			}

			detail, err := f.Store.GetRun(ctx, runID)
			if err != nil {
				t.Fatalf("read the run back: %v", err)
			}
			if detail.Run.ReasonCode != tc.wantReason {
				t.Errorf("reason_code = %q, want %q", detail.Run.ReasonCode, tc.wantReason)
			}
			rows, err := f.Store.ListNotifications(ctx, store.NotificationFilter{})
			if err != nil {
				t.Fatalf("list notifications: %v", err)
			}
			if len(rows) != 1 {
				t.Fatalf("%s left %d outbox rows, want 1:\n%+v", tc.wantReason, len(rows), rows)
			}
			if rows[0].Topic != tc.wantTopic || rows[0].Subject != "e2e" {
				t.Errorf("row is %s/%s, want %s/e2e", rows[0].Topic, rows[0].Subject, tc.wantTopic)
			}
		})
	}
}

// TestAFailedStepOutranksACancelledOneAtTheFinish holds the finish verdict to
// the order the machine ranks by. model.TerminalVerdict puts a failure above a
// cancellation, and FinishRun writes the run's state from it, so a finish that
// read its own reason off the first terminal step in index order would stamp
// RUN_CANCELLED_MANUAL on a row whose state is failed. The run then notifies
// nobody, because a cancellation carries no topic, and fsck sees nothing wrong:
// it compares the state to the step aggregate, never to the reason.
func TestAFailedStepOutranksACancelledOneAtTheFinish(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 9, 17, 8, 0, 0, 0, time.UTC)
	f := newFixtureWithClock(t, clock.NewFake(now))
	f.Engine.Notify = notify.NewPlanner(model.NotifyDefaults{
		OnSuccess: []string{"exec:notify-me"},
		OnFailure: []string{"exec:notify-me"},
	}, func() time.Time { return now })

	// The cancelled step sits at index 0, ahead of the step that fails.
	runID := f.aQueuedRunWithHooks(t, "cancel-then-fail",
		[]string{"cancelled", "reopened"}, []string{"exit 0", "exit 3"}, nil, 60000, "")
	f.aCancelledStepSurvivingARetry(t, runID, "cancelled", "reopened")

	state, err := f.Engine.ExecuteRun(ctx, runID)
	if err != nil {
		t.Fatalf("the run could not finish: %v", err)
	}
	if state != string(model.RunFailed) {
		t.Fatalf("run ended %s, want failed", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	if detail.Run.ReasonCode != string(reason.RUNFailedStep) {
		t.Errorf("a failed run reads reason_code %q, want %s: the reason and the"+
			" state come from the same steps and cannot disagree",
			detail.Run.ReasonCode, reason.RUNFailedStep)
	}

	rows, err := f.Store.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("a failed run left %d outbox rows, want exactly 1:\n%+v", len(rows), rows)
	}
	if rows[0].Topic != model.TopicRunFailed || rows[0].Subject != "e2e" {
		t.Errorf("row is %s/%s, want %s/e2e", rows[0].Topic, rows[0].Subject, model.TopicRunFailed)
	}
}
