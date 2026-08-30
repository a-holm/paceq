package engine

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// TestNotifyRunFactsAnswersWhetherAVerdictNotifies holds #194's contract in
// one place: the second result, not the returned struct, says whether there is
// anything to send. The three verdicts that carry a topic build complete facts;
// a cancellation, and any ending whose topic this builder cannot render, plan
// nothing. A zero RunFacts is never a usable value, so it never travels with
// ok true.
func TestNotifyRunFactsAnswersWhetherAVerdictNotifies(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 17, 6, 0, 0, 0, time.UTC))
	detail := store.RunDetail{
		Run: store.Run{ID: "01ABC", JobName: "nightly", Attempt: 1},
		Steps: []store.Step{
			{
				Name: "work", State: string(model.StepFailed), ErrorTail: "boom",
				ExitCode: 3, HasExitCode: true,
			},
		},
	}

	cases := []struct {
		name      string
		verdict   runVerdict
		wantOK    bool
		wantTopic string
		wantState string
	}{
		{
			name: "succeeded",
			verdict: runVerdict{
				reason: store.FinishReason{Code: reason.RUNSucceeded},
				topic:  model.TopicRunSucceeded,
			},
			wantOK: true, wantTopic: model.TopicRunSucceeded, wantState: "succeeded",
		},
		{
			name: "failed step",
			verdict: runVerdict{
				reason: store.FinishReason{Code: reason.RUNFailedStep},
				topic:  model.TopicRunFailed,
			},
			wantOK: true, wantTopic: model.TopicRunFailed, wantState: "failed",
		},
		{
			name: "timed out",
			verdict: runVerdict{
				reason: store.FinishReason{Code: reason.RUNTimedOut},
				topic:  model.TopicRunFailed,
			},
			wantOK: true, wantTopic: model.TopicRunFailed, wantState: "failed",
		},
		{
			name: "cancelled",
			verdict: runVerdict{
				reason: store.FinishReason{Code: reason.RUNCancelledManual},
				topic:  noNotification,
			},
			wantOK: false,
		},
		{
			name: "an ending nobody gave a renderable topic",
			verdict: runVerdict{
				reason: store.FinishReason{Code: reason.RUNPoisoned},
				topic:  "run.invented",
			},
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			facts, ok := notifyRunFacts(detail, tc.verdict, clk, "host-a")
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v (facts %+v)", ok, tc.wantOK, facts)
			}
			if !ok {
				if facts != (notify.RunFacts{}) {
					t.Errorf("a verdict with nothing to send returned %+v, want the zero value", facts)
				}
				return
			}
			if facts.Topic != tc.wantTopic || facts.State != tc.wantState {
				t.Errorf("topic/state = %q/%q, want %q/%q",
					facts.Topic, facts.State, tc.wantTopic, tc.wantState)
			}
			if facts.JobName == "" || facts.RunID == "" ||
				facts.ReasonCode != string(tc.verdict.reason.Code) {
				t.Errorf("facts the store needs are missing: %+v", facts)
			}
		})
	}
}

// TestEveryEndingNamesItsNotificationTopic pins the pairing finishReason
// writes down. It is the whole mapping: there is no second switch that can
// answer a code it does not know, because there is no second switch.
func TestEveryEndingNamesItsNotificationTopic(t *testing.T) {
	want := map[reason.Code]string{
		reason.RUNSucceeded:       model.TopicRunSucceeded,
		reason.RUNFailedStep:      model.TopicRunFailed,
		reason.RUNTimedOut:        model.TopicRunFailed,
		reason.RUNCancelledManual: noNotification,
	}
	for code, topic := range want {
		e, ok := reason.Lookup(code)
		if !ok {
			t.Errorf("%s ends a run and the catalogue does not know it", code)
			continue
		}
		if !e.Terminal {
			t.Errorf("%s ends a run and the catalogue does not call it terminal", code)
		}
		if topic != noNotification && topic != model.TopicRunSucceeded &&
			topic != model.TopicRunFailed {
			t.Errorf("%s names topic %q, which no notification carries", code, topic)
		}
	}
}
