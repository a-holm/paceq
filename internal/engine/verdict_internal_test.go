package engine

import (
	"testing"

	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/spool"
	"github.com/a-holm/paceq/internal/store"
)

// The two verdict surfaces over one attempt: verdictFor, which the live
// executor calls with what it watched, and store.SpoolVerdict, which
// recovery calls with what the shim left on disk. The same attempt must
// leave both of them saying the same thing, or a crash rewrites history
// (#204).
func TestTheLiveVerdictAndTheSpooledVerdictAgree(t *testing.T) {
	for _, tc := range []struct {
		name string
		live runner.Result
		file spool.Result
	}{
		{
			name: "succeeded",
			live: runner.Result{Outcome: runner.Succeeded},
			file: spool.Result{Outcome: spool.OutcomeSucceeded},
		},
		{
			name: "nonzero exit",
			live: runner.Result{Outcome: runner.Failed, ExitCode: 3},
			file: spool.Result{Outcome: spool.OutcomeFailed, ExitCode: 3},
		},
		{
			name: "timed out",
			live: runner.Result{Outcome: runner.TimedOut, Signal: "SIGKILL"},
			file: spool.Result{Outcome: spool.OutcomeTimedOut, Signal: "SIGKILL"},
		},
		{
			name: "signalled from outside",
			live: runner.Result{Outcome: runner.Signalled, Signal: "SIGTERM"},
			file: spool.Result{Outcome: spool.OutcomeSignalled, Signal: "SIGTERM", KilledBy: "signal:SIGTERM"},
		},
		{
			name: "signalled answering a cancellation",
			live: runner.Result{
				Outcome:    runner.Signalled,
				Signal:     "SIGTERM",
				ReasonData: map[string]any{"cancelled": true},
			},
			file: spool.Result{Outcome: spool.OutcomeSignalled, Signal: "SIGTERM", KilledBy: spool.KilledByCancel},
		},
		{
			name: "spawn failed",
			live: runner.Result{Outcome: runner.SpawnFailed},
			file: spool.Result{Outcome: spool.OutcomeSpawnFailed},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			live, err := verdictFor(tc.live, "", 0, false)
			if err != nil {
				t.Fatalf("the live verdict refused the result: %v", err)
			}
			spooled, err := store.SpoolVerdict(tc.file)
			if err != nil {
				t.Fatalf("the spool verdict refused the result: %v", err)
			}
			if live.Event != spooled.Event || live.ReasonCode != spooled.ReasonCode {
				t.Fatalf("live said %s/%s, the spool said %s/%s",
					live.Event, live.ReasonCode, spooled.Event, spooled.ReasonCode)
			}
			if live.Signal != spooled.Signal {
				t.Fatalf("live signal %q, spool signal %q", live.Signal, spooled.Signal)
			}
			if !sameExitCode(live.ExitCode, spooled.ExitCode) {
				t.Fatalf("live exit code %v, spool exit code %v", live.ExitCode, spooled.ExitCode)
			}
		})
	}
}

func sameExitCode(a, b *int) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
