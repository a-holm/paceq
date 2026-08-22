package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The retry proofs. The wall clock inside a testing/synctest bubble is
// virtual, and so are its timers: an hour of backoff passes in an instant
// once every goroutine in the bubble is parked waiting for it, while the
// real process work between the waits costs only its own milliseconds.
// That is what makes these end to end tests deterministic instead of merely
// patient: the parked step can be inspected mid-wait, and the gate's exact
// millisecond arithmetic is asserted on real rows.

const (
	// One hour of backoff with no jitter. A wait this long could never
	// pass by accident or by patience; only a virtual clock crosses it.
	hourBackoff     = `{"max":3,"backoff":"fixed","initial_ms":3600000,"max_delay_ms":3600000,"jitter":"none"}`
	hourBackoffMax2 = `{"max":2,"backoff":"fixed","initial_ms":3600000,"max_delay_ms":3600000,"jitter":"none"}`
)

// runRetrySpec seeds a job whose flaky step runs fakecmd exit-unless-attempt
// N under the given retry block, followed by a dependent step, then
// materialises one manual run of it. It needs an open store.
func runRetrySpec(t *testing.T, s *store.Store, fakecmdPath string, giveUpAt int, retryBlock string) string {
	t.Helper()

	spec := fmt.Sprintf(`{"max_concurrent":1,"name":"flakyjob","schema":"paceq.job.v1",`+
		`"steps":[`+
		`{"name":"flaky","run":[%q,"exit-unless-attempt","%d"],"shell":false,"retry":%s},`+
		`{"name":"after","needs":["flaky"],"run":[%q,"exit","0"],"shell":false}],`+
		// The job's own budget must comfortably cover the simulated
		// hours of backoff: waiting is real elapsed time.
		`"timeout_ms":86400000}`,
		fakecmdPath, giveUpAt, retryBlock, fakecmdPath)
	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "flakyjob",
		SpecHash: "sha256:flakyjob-" + fmt.Sprint(giveUpAt),
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "flakyjob",
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return out.Run.ID
}

// openBubbleStore opens the store the bubble tests run through. The clock is
// the system one, which inside the bubble reads virtual time and grows
// virtual timers, so the store's gate and the engine's waits share one
// timeline.
func openBubbleStore(t *testing.T, stateDir string) *store.Store {
	t.Helper()

	s, err := store.Open(context.Background(), filepath.Join(stateDir, "state.db"),
		store.Options{Clock: clock.System()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func TestARetryParksUntilTheGateThenTheNextAttemptRuns(t *testing.T) {
	// The fixture is built outside the bubble: a real subprocess build
	// has no business inside one.
	fakecmdPath := fakecmd(t)
	stateDir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}

	var (
		runID          string
		parked         store.Step
		eventsAtPark   []store.RunEvent
		attemptsAtPark []logsink.AttemptFile
	)

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
		runID = runRetrySpec(t, s, fakecmdPath, 2, hourBackoff)

		done := make(chan error, 1)
		go func() {
			_, err := eng.ExecuteRun(context.Background(), runID)
			done <- err
		}()

		// Wait until attempt one has failed and been booked as a
		// scheduled retry, then let every goroutine park. What the
		// engine must be doing at that point is sitting in its timer,
		// one full hour away from the next attempt.
		for {
			events, err := s.RunEvents(ctx, runID)
			if err != nil {
				t.Fatalf("read events while parking: %v", err)
			}
			if hasEvent(events, "step.retry_scheduled") {
				eventsAtPark = events
				break
			}
			time.Sleep(time.Millisecond)
		}
		synctest.Wait()

		got, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read the parked run: %v", err)
		}
		parked = got.Steps[0]
		attemptsAtPark, err = logsink.NewRoot(stateDir).AttemptFiles(runID, "flaky")
		if err != nil {
			t.Fatalf("list attempts at the park: %v", err)
		}

		if err := <-done; err != nil {
			t.Fatalf("execute run: %v", err)
		}
		assertParkedAndFinished(t, s, stateDir, runID, parked, eventsAtPark, attemptsAtPark)
	})
}

// assertParkedAndFinished holds every assertion of the parked-retry proof,
// so the bubble body stays a straight line. It runs after the bubble has
// closed, on the outer test goroutine.
func assertParkedAndFinished(t *testing.T, s *store.Store, stateDir, runID string,
	parked store.Step, eventsAtPark []store.RunEvent, attemptsAtPark []logsink.AttemptFile,
) {
	t.Helper()
	ctx := context.Background()

	// At the park: attempt one done exactly once, the step pending and
	// parked past a gate one hour away, nothing else started.
	if parked.State != "pending" || parked.Attempt != 1 {
		t.Errorf("parked step = %s attempt %d, want pending attempt 1", parked.State, parked.Attempt)
	}
	if parked.MaxAttempts != 4 {
		t.Errorf("max_attempts = %d, want 4 (retry.max 3 plus the first run)", parked.MaxAttempts)
	}
	if got := countKind(eventsAtPark, "step.started"); got != 1 {
		t.Errorf("%d step.started events at the park, want exactly 1", got)
	}
	if len(attemptsAtPark) != 1 {
		t.Errorf("%d log files at the park, want only attempt 1's", len(attemptsAtPark))
	}
	retryEvent := lastEvent(eventsAtPark, "step.retry_scheduled")
	if retryEvent == nil {
		t.Fatal("no step.retry_scheduled event found")
	}
	var facts map[string]any
	if err := json.Unmarshal([]byte(retryEvent.DetailJSON), &facts); err != nil {
		t.Fatalf("retry detail is not an object: %q", retryEvent.DetailJSON)
	}
	if facts["attempt"] != float64(1) {
		t.Errorf("retry detail attempt = %v, want 1 (the attempt that just failed)", facts["attempt"])
	}
	if facts["transient"] != true {
		t.Errorf("retry detail transient = %v, want true for exit 75", facts["transient"])
	}
	if backoffMS, _ := facts["backoff_ms"].(float64); backoffMS != 3600000 {
		t.Errorf("backoff_ms = %v, want 3600000", facts["backoff_ms"])
	}
	nextMS, ok := facts["next_attempt_at"].(float64)
	if !ok {
		t.Fatal("next_attempt_at missing from the retry detail")
	}
	if gap := int64(nextMS) - msOf(retryEvent.At); gap != 3600000 {
		t.Errorf("next_attempt_at sits %d ms after the failure, want exactly the 3600000 ms backoff", gap)
	}
	if !parked.NextAttemptAt.Equal(msToTime(int64(nextMS))) {
		t.Errorf("row next_attempt_at = %s, want the detail's %s",
			parked.NextAttemptAt, msToTime(int64(nextMS)))
	}

	detail, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read the finished run: %v", err)
	}
	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read the final events: %v", err)
	}
	attempts, err := logsink.NewRoot(stateDir).AttemptFiles(runID, "flaky")
	if err != nil {
		t.Fatalf("list the final attempts: %v", err)
	}
	violations, err := s.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %d violations after a retried run: %+v", len(violations), violations)
	}

	// The finish: attempt two succeeded, the dependent step ran, and
	// both attempts kept their own log streams.
	flaky := detail.Steps[0]
	if flaky.State != "succeeded" || flaky.Attempt != 2 {
		t.Errorf("flaky = %s attempt %d, want succeeded attempt 2", flaky.State, flaky.Attempt)
	}
	if detail.Steps[1].State != "succeeded" {
		t.Errorf("after = %s, want succeeded behind the recovered step", detail.Steps[1].State)
	}
	wantChain := []struct{ step, kind, from, to string }{
		{"", "run.queued", "", "queued"},
		{"", "run.started", "queued", "running"},
		{"flaky", "step.started", "pending", "running"},
		{"flaky", "step.retry_scheduled", "running", "pending"},
		{"flaky", "step.started", "pending", "running"},
		{"flaky", "step.succeeded", "running", "succeeded"},
		{"after", "step.started", "pending", "running"},
		{"after", "step.succeeded", "running", "succeeded"},
		{"", "run.succeeded", "running", "succeeded"},
	}
	if len(events) != len(wantChain) {
		t.Fatalf("event rows = %d, want exactly %d", len(events), len(wantChain))
	}
	for i, w := range wantChain {
		e := events[i]
		if e.StepName != w.step || e.Kind != w.kind || e.FromState != w.from || e.ToState != w.to {
			t.Errorf("event[%d] = (%q %s %s->%s), want (%q %s %s->%s)",
				i, e.StepName, e.Kind, e.FromState, e.ToState, w.step, w.kind, w.from, w.to)
		}
	}
	if events[3].ReasonCode != string(reason.STEPRetryScheduled) {
		t.Errorf("retry reason_code = %q, want %q", events[3].ReasonCode, reason.STEPRetryScheduled)
	}
	if len(attempts) != 2 {
		t.Fatalf("%d log files after the run, want one per attempt", len(attempts))
	}
	logOne, err := readAttemptLog(stateDir, attempts, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logOne, "attempt 1") {
		t.Errorf("attempt 1's log lost its marker line: %q", logOne)
	}
	logTwo, err := readAttemptLog(stateDir, attempts, 2)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(logTwo, "attempt 2") {
		t.Errorf("attempt 2's log lost its marker line: %q", logTwo)
	}
}

func TestARetryBudgetThatRunsOutFailsPermanentlyAndSkipsWhatNeededIt(t *testing.T) {
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
		// give up at 99: no attempt number ever reaches it, so the
		// budget has to run out on its own.
		runID := runRetrySpec(t, s, fakecmdPath, 99, hourBackoffMax2)

		done := make(chan error, 1)
		go func() {
			_, err := eng.ExecuteRun(context.Background(), runID)
			done <- err
		}()
		if err := <-done; err != nil {
			t.Fatalf("execute run: %v", err)
		}

		detail, err := s.GetRun(ctx, runID)
		if err != nil {
			t.Fatalf("read the finished run: %v", err)
		}
		events, err := s.RunEvents(ctx, runID)
		if err != nil {
			t.Fatalf("read the final events: %v", err)
		}
		attempts, err := logsink.NewRoot(stateDir).AttemptFiles(runID, "flaky")
		if err != nil {
			t.Fatalf("list the attempts: %v", err)
		}
		violations, err := s.Fsck(ctx)
		if err != nil {
			t.Fatalf("fsck: %v", err)
		}
		if len(violations) != 0 {
			t.Errorf("fsck found %d violations after exhaustion: %+v", len(violations), violations)
		}

		if detail.State != "failed" && detail.ReasonCode != string(reason.RUNFailedStep) {
			t.Errorf("run = %s/%s, want failed/%q", detail.State, detail.ReasonCode, reason.RUNFailedStep)
		}

		flaky := detail.Steps[0]
		if flaky.State != "failed" || flaky.Attempt != 3 || flaky.MaxAttempts != 3 {
			t.Errorf("flaky = %s attempt %d/%d, want failed at 3 of 3 (max 2 retries)",
				flaky.State, flaky.Attempt, flaky.MaxAttempts)
		}
		if flaky.ReasonCode != string(reason.STEPRetriesExhausted) {
			t.Errorf("flaky reason = %q, want %q", flaky.ReasonCode, reason.STEPRetriesExhausted)
		}
		var facts map[string]any
		if err := json.Unmarshal([]byte(flaky.ReasonData), &facts); err != nil {
			t.Fatalf("exhaustion reason_data is not an object: %q", flaky.ReasonData)
		}
		if facts["attempt"] != float64(3) || facts["max_attempts"] != float64(3) {
			t.Errorf("exhaustion detail = %v, want attempt 3 of max_attempts 3", facts)
		}
		if after := detail.Steps[1]; after.State != "skipped" ||
			after.ReasonCode != string(reason.STEPSkippedUpstreamFailed) {
			t.Errorf("after = %s/%s, want skipped/%s",
				after.State, after.ReasonCode, reason.STEPSkippedUpstreamFailed)
		}

		if got := countKind(events, "step.retry_scheduled"); got != 2 {
			t.Errorf("%d step.retry_scheduled events, want 2 (one per bought attempt)", got)
		}
		if got := countKind(events, "step.started"); got != 3 {
			t.Errorf("%d step.started events, want 3 runs total", got)
		}
		// Each scheduled retry sat exactly one hour away from the
		// failure it answered: the policy said fixed 1h, so both gaps
		// are exact.
		for _, e := range events {
			if e.Kind != "step.retry_scheduled" {
				continue
			}
			var f map[string]any
			if err := json.Unmarshal([]byte(e.DetailJSON), &f); err != nil {
				t.Fatalf("retry detail is not an object: %q", e.DetailJSON)
			}
			nextMS, _ := f["next_attempt_at"].(float64)
			if gap := int64(nextMS) - msOf(e.At); gap != 3600000 {
				t.Errorf("a retry sat %d ms away, want exactly 3600000", gap)
			}
		}
		if len(attempts) != 3 {
			t.Fatalf("%d log files, want one per attempt: evidence survives", len(attempts))
		}
		for _, a := range attempts {
			body, err := readAttemptLog(stateDir, attempts, a.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(body, fmt.Sprintf("attempt %d", a.Attempt)) {
				t.Errorf("attempt %d's log does not name itself: %q", a.Attempt, body)
			}
		}
	})
}

// The helpers below keep the bubble bodies short.

func hasEvent(events []store.RunEvent, kind string) bool {
	for _, e := range events {
		if e.Kind == kind {
			return true
		}
	}
	return false
}

func countKind(events []store.RunEvent, kind string) int {
	n := 0
	for _, e := range events {
		if e.Kind == kind {
			n++
		}
	}
	return n
}

func lastEvent(events []store.RunEvent, kind string) *store.RunEvent {
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Kind == kind {
			return &events[i]
		}
	}
	return nil
}

func msOf(t time.Time) int64 {
	return t.UnixMilli()
}

func msToTime(ms int64) time.Time {
	return time.UnixMilli(ms).UTC()
}

func readAttemptLog(root string, attempts []logsink.AttemptFile, want int) (string, error) {
	for _, a := range attempts {
		if a.Attempt != want {
			continue
		}
		// The database's log paths are relative to the log root, which
		// lives at <state>/logs.
		body, err := os.ReadFile(filepath.Join(root, "logs", a.RelPath))
		if err != nil {
			return "", fmt.Errorf("read attempt %d's log: %w", want, err)
		}
		return string(body), nil
	}
	return "", fmt.Errorf("no log file for attempt %d among %+v", want, attempts)
}
