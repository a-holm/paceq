package engine_test

// Which attempt index does the engine feed retry.Delay? The other retry
// proofs cannot say: their policies are fixed, and a fixed policy pays the
// same delay for every index, so a stale or off-by-one index produces a
// schedule indistinguishable from the right one. This proof grows the delay
// exponentially instead, 30 minutes then 60 minutes, so each scheduled gap
// names exactly the attempt that failed. Any other index lands beside these
// two values.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/store"
)

func TestARetryScheduleNamesTheAttemptIndexFedToDelay(t *testing.T) {
	fakecmdPath := fakecmd(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}

	synctest.Test(t, func(t *testing.T) {
		s := openBubbleStore(t, stateDir)
		eng := &engine.Engine{
			Store:        s,
			LogRoot:      logsink.NewRoot(stateDir),
			Clock:        clock.System(),
			Owner:        "exec-1",
			PollInterval: 10 * time.Millisecond,
			// The claim must span the simulated hours of backoff:
			// finishing the run requires holding the lease.
			LeaseTTL: 48 * time.Hour,
		}
		ctx := context.Background()
		// Exponential 30 minutes doubling under a 2 hour cap:
		// Delay(policy, 1) = 1800000 ms and Delay(policy, 2) = 3600000 ms,
		// two values no other index can produce. Attempts 1 and 2 fail
		// with exit 75, attempt 3 succeeds, so the run carries exactly two
		// scheduled retries.
		expGaps := `{"max":3,"backoff":"exponential","initial_ms":1800000,"max_delay_ms":7200000,"jitter":"none"}`
		runID := runRetrySpec(t, s, fakecmdPath, 3, expGaps)

		done := make(chan error, 1)
		go func() {
			_, err := eng.ExecuteRun(context.Background(), runID)
			done <- err
		}()
		if err := <-done; err != nil {
			t.Fatalf("execute run: %v", err)
		}

		events, err := s.RunEvents(ctx, runID)
		if err != nil {
			t.Fatalf("read the final events: %v", err)
		}
		violations, err := s.Fsck(ctx)
		if err != nil {
			t.Fatalf("fsck: %v", err)
		}
		if len(violations) != 0 {
			t.Errorf("fsck found %d violations after a retried run: %+v", len(violations), violations)
		}

		// want[i] is Delay(policy, i+1), what the failure of attempt i+1
		// must pay. Both stamps of every gap come from one virtual clock.
		want := []int64{1_800_000, 3_600_000}
		var scheduled []store.RunEvent
		for _, e := range events {
			if e.Kind == "step.retry_scheduled" {
				scheduled = append(scheduled, e)
			}
		}
		if len(scheduled) != len(want) {
			t.Fatalf("%d step.retry_scheduled events, want %d", len(scheduled), len(want))
		}
		for i, e := range scheduled {
			var f map[string]any
			if err := json.Unmarshal([]byte(e.DetailJSON), &f); err != nil {
				t.Fatalf("retry detail is not an object: %q", e.DetailJSON)
			}
			if f["attempt"] != float64(i+1) {
				t.Errorf("retry %d names attempt %v, want %d (the one that just failed)", i, f["attempt"], i+1)
			}
			backoffMS, _ := f["backoff_ms"].(float64)
			if int64(backoffMS) != want[i] {
				t.Errorf("the retry after attempt %d paid %d ms, want %d ms: the delay must grow with the ATTEMPT THAT FAILED",
					i+1, int64(backoffMS), want[i])
			}
			nextMS, ok := f["next_attempt_at"].(float64)
			if !ok {
				t.Fatal("next_attempt_at missing from the retry detail")
			}
			if gap := int64(nextMS) - msOf(e.At); gap != want[i] {
				t.Errorf("the retry after attempt %d sits %d ms away, want exactly %d ms", i+1, gap, want[i])
			}
		}

		detail, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read the finished run: %v", err)
		}
		if flaky := detail.Steps[0]; flaky.State != "succeeded" || flaky.Attempt != 3 {
			t.Errorf("flaky = %s attempt %d, want succeeded attempt 3", flaky.State, flaky.Attempt)
		}
	})
}
