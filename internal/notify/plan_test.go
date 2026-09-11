package notify_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
)

var planClock = func() time.Time { return time.Date(2026, 9, 17, 6, 0, 0, 0, time.UTC) }

// TestPlanRefusesFactsTheStoreWouldReject holds the planner's half of #194.
// A RunFacts without a topic routes to the success side, because an empty
// topic is not TopicRunFailed, and the row it used to build carried an empty
// topic and subject. insertNotificationsTx rejects that row inside FinishRun's
// transaction, so the refusal takes the run's verdict with it and the run
// cannot finish. The planner never builds a row the store will not take,
// whatever it is handed.
func TestPlanRefusesFactsTheStoreWouldReject(t *testing.T) {
	target := []string{"exec:notify-me"}

	defaults := []struct {
		name string
		def  model.NotifyDefaults
	}{
		{"no defaults", model.NotifyDefaults{}},
		{"success default", model.NotifyDefaults{OnSuccess: target}},
		{"failure default", model.NotifyDefaults{OnFailure: target}},
		{"both defaults", model.NotifyDefaults{OnSuccess: target, OnFailure: target}},
	}
	hooks := []struct {
		name  string
		hooks *notify.JobHooks
	}{
		{"nil hooks", nil},
		{"empty hooks", &notify.JobHooks{}},
		{"on_success hook", &notify.JobHooks{OnSuccess: target}},
		{"on_failure hook", &notify.JobHooks{OnFailure: target}},
		{"both hooks", &notify.JobHooks{OnSuccess: target, OnFailure: target}},
	}
	facts := []struct {
		name string
		f    notify.RunFacts
	}{
		{"zero facts", notify.RunFacts{}},
		{"no topic", notify.RunFacts{JobName: "nightly", RunID: "01ABC"}},
		{"no job name", notify.RunFacts{Topic: model.TopicRunSucceeded, RunID: "01ABC"}},
	}

	for _, d := range defaults {
		for _, h := range hooks {
			for _, f := range facts {
				p := notify.NewPlanner(d.def, planClock)
				got := p.Plan(f.f, h.hooks)
				if got != nil {
					t.Errorf("%s / %s / %s: planned %d notifications, want none:\n%+v",
						d.name, h.name, f.name, len(got), got)
				}
			}
		}
	}
}

// TestPlanStillBuildsForCompleteFacts is the other side of the guard above:
// refusing incomplete facts must not silence a real verdict.
func TestPlanStillBuildsForCompleteFacts(t *testing.T) {
	p := notify.NewPlanner(model.NotifyDefaults{OnSuccess: []string{"exec:ok"}}, planClock)
	got := p.Plan(notify.RunFacts{
		Topic:   model.TopicRunSucceeded,
		JobName: "nightly",
		RunID:   "01ABC",
		State:   "succeeded",
	}, nil)
	if len(got) != 1 {
		t.Fatalf("a complete success planned %d notifications, want 1", len(got))
	}
	if got[0].Topic != model.TopicRunSucceeded || got[0].Subject != "nightly" ||
		got[0].Target != "exec:ok" {
		t.Errorf("row identity wrong: %+v", got[0])
	}
}

// TestPlanKeepsDeliberateSilence pins the behaviour the guard must not
// disturb: a job whose hooks name an empty list for a side silences that side
// even when the daemon defaults name a target.
func TestPlanKeepsDeliberateSilence(t *testing.T) {
	p := notify.NewPlanner(model.NotifyDefaults{OnSuccess: []string{"exec:ok"}}, planClock)
	got := p.Plan(notify.RunFacts{
		Topic:   model.TopicRunSucceeded,
		JobName: "nightly",
		RunID:   "01ABC",
	}, &notify.JobHooks{OnSuccess: []string{}})
	if got != nil {
		t.Errorf("a deliberately silent job planned %+v", got)
	}
}

// TestSLAPlanRefusesAnUnnamedJob is the same refusal on the breach path: the
// job name is the row's subject, and the store will not take a row without
// one.
func TestSLAPlanRefusesAnUnnamedJob(t *testing.T) {
	p := notify.NewPlanner(model.NotifyDefaults{OnFailure: []string{"exec:vakt"}}, planClock)
	got := p.SLAPlan("", planClock(), time.Time{}, time.Hour, "host-a")
	if got != nil {
		t.Errorf("an unnamed job planned %d breach notifications, want none:\n%+v", len(got), got)
	}
	if named := p.SLAPlan("nightly", planClock(), time.Time{}, time.Hour, "host-a"); len(named) != 1 {
		t.Errorf("a named job planned %d breach notifications, want 1", len(named))
	}
}
