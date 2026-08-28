package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spool"
)

// The spool committer's admission tests (issue #39). A result file may only
// settle the attempt it names, under the lease it was written under, while
// that lease is provably dead — and all of it in the same transaction as the
// commit.

func seedSpoolableStep(t *testing.T, ttl time.Duration) (*Store, string) {
	t.Helper()

	s, err := Open(context.Background(), filepath.Join(t.TempDir(), "state.db"),
		Options{Clock: clock.NewFake(time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC))})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	spec := `{"schema":"paceq.job.v1","name":"spooled","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  "spooled",
		SpecHash: "sha256:spooled",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{
		JobName: "spooled", Actor: "test",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, _, err := s.ClaimRun(context.Background(), res.Run.ID,
		LeaseInput{Owner: "exec-doomed", TTL: ttl}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(context.Background(), res.Run.ID, "only",
		LeaseRef{Owner: "exec-doomed", Epoch: 1}); err != nil {
		t.Fatalf("start the step: %v", err)
	}
	return s, res.Run.ID
}

func aSpoolOutcome(exit int, code reason.Code) StepOutcome {
	event := "step_failed"
	if code == reason.STEPSucceeded {
		event = "step_succeeded"
	}
	exitCopy := exit
	return StepOutcome{
		Event:         event,
		ReasonCode:    code,
		ExitCode:      &exitCopy,
		FinishedAt:    time.Date(2026, 9, 17, 3, 0, 1, 0, time.UTC),
		OutcomeSource: "spool",
	}
}

// expireLease runs the fake clock past any TTL the seed claimed with. The
// spool committer only accepts a result once the lease is provably dead, so
// every happy-path test has to bury its executor first.
func expireLease(t *testing.T, s *Store) {
	t.Helper()
	s.clk.(*clock.Fake).Advance(6 * time.Minute)
}

func TestCommitSpooledOutcomeSettlesTheAttempt(t *testing.T) {
	s, runID := seedSpoolableStep(t, time.Minute)
	expireLease(t, s)

	err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome:    aSpoolOutcome(0, reason.STEPSucceeded),
	})
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "succeeded" {
		t.Fatalf("state = %s, want succeeded", step.State)
	}
	if !step.HasExitCode || step.ExitCode != 0 {
		t.Fatalf("exit code = %d/%v, want 0", step.ExitCode, step.HasExitCode)
	}
	if step.OutcomeSource != "spool" {
		t.Fatalf("outcome_source = %q, want spool", step.OutcomeSource)
	}
}

func TestCommitSpooledOutcomeRefusesAStaleEpoch(t *testing.T) {
	s, runID := seedSpoolableStep(t, time.Minute)
	expireLease(t, s)

	err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 999,
		Outcome:    aSpoolOutcome(1, reason.STEPFailedNonzeroExit),
	})
	if !errors.Is(err, ErrEpochMismatch) {
		t.Fatalf("err = %v, want ErrEpochMismatch: a stale result must never overwrite the row", err)
	}
}

func TestCommitSpooledOutcomeRefusesAnUnknownAttempt(t *testing.T) {
	s, runID := seedSpoolableStep(t, time.Minute)
	expireLease(t, s)

	if err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      "01J0NO SUCH RUN",
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome:    aSpoolOutcome(1, reason.STEPFailedNonzeroExit),
	}); !errors.Is(err, ErrUnknownAttempt) {
		t.Fatalf("unknown run: err = %v, want ErrUnknownAttempt", err)
	}
	if err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    7,
		ClaimEpoch: 1,
		Outcome:    aSpoolOutcome(1, reason.STEPFailedNonzeroExit),
	}); !errors.Is(err, ErrUnknownAttempt) {
		t.Fatalf("wrong attempt: err = %v, want ErrUnknownAttempt", err)
	}
}

func TestCommitSpooledOutcomeRefusesALiveLease(t *testing.T) {
	s, runID := seedSpoolableStep(t, 5*time.Minute)

	err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome:    aSpoolOutcome(1, reason.STEPFailedNonzeroExit),
	})
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("err = %v, want ErrLeaseLost: a live executor owns this attempt's story", err)
	}
}

// A result written on another boot voids the lease: the machine restarted,
// so the holder is provably dead no matter what the expiry says.
func TestCommitSpooledOutcomeHonoursTheBootChange(t *testing.T) {
	s, runID := seedSpoolableStep(t, 5*time.Minute)

	if _, err := s.StartSession(context.Background(), "test"); err != nil {
		t.Fatalf("start the session that records this boot: %v", err)
	}

	err := s.CommitSpooledOutcome(context.Background(), SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		BootID:     "the-boot-the-attempt-was-written-on",
		Outcome:    aSpoolOutcome(0, reason.STEPSucceeded),
	})
	if err != nil {
		t.Fatalf("commit after a boot change: %v", err)
	}
}

func TestCommitSpooledOutcomeIsIdempotentThroughRefusal(t *testing.T) {
	s, runID := seedSpoolableStep(t, time.Minute)

	in := SpoolOutcome{
		RunID:      runID,
		Step:       "only",
		Attempt:    1,
		ClaimEpoch: 1,
		Outcome:    aSpoolOutcome(0, reason.STEPSucceeded),
	}
	expireLease(t, s)
	if err := s.CommitSpooledOutcome(context.Background(), in); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// The second consumer finds no running attempt: nothing on the row
	// moves a second time, which is what makes two consumers safe.
	if err := s.CommitSpooledOutcome(context.Background(), in); !errors.Is(err, ErrUnknownAttempt) {
		t.Fatalf("second commit: err = %v, want ErrUnknownAttempt", err)
	}
}

func TestSpoolVerdictMirrorsTheClassifier(t *testing.T) {
	for _, tc := range []struct {
		outcome  string
		event    string
		code     reason.Code
		exitCode int
		wantExit bool
	}{
		{spool.OutcomeSucceeded, "step_succeeded", reason.STEPSucceeded, 0, true},
		{spool.OutcomeFailed, "step_failed", reason.STEPFailedNonzeroExit, 3, true},
		{spool.OutcomeTimedOut, "step_failed", reason.STEPFailedTimeout, 0, false},
		{spool.OutcomeSignalled, "step_failed", reason.STEPFailedSignal, 0, false},
		{spool.OutcomeSpawnFailed, "step_failed", reason.STEPFailedSpawn, 127, false},
	} {
		res := spool.Result{Outcome: tc.outcome, ExitCode: tc.exitCode, EndedAt: 1767512400000}
		out, err := SpoolVerdict(res)
		if err != nil {
			t.Fatalf("%s: %v", tc.outcome, err)
		}
		if out.Event != tc.event || out.ReasonCode != tc.code {
			t.Fatalf("%s: event/reason = %s/%s, want %s/%s", tc.outcome, out.Event, out.ReasonCode, tc.event, tc.code)
		}
		if tc.wantExit && (out.ExitCode == nil || *out.ExitCode != tc.exitCode) {
			t.Fatalf("%s: exit code %v, want %d", tc.outcome, out.ExitCode, tc.exitCode)
		}
		if !tc.wantExit && out.ExitCode != nil {
			t.Fatalf("%s: exit code %v, want none", tc.outcome, out.ExitCode)
		}
		if out.OutcomeSource != "spool" {
			t.Fatalf("%s: source %q, want spool", tc.outcome, out.OutcomeSource)
		}
	}
	if _, err := SpoolVerdict(spool.Result{Outcome: "mystery"}); !errors.Is(err, spool.ErrFormat) {
		t.Fatalf("an unknown outcome was accepted: %v", err)
	}
}

func TestRecordSpoolArchiveWritesTheWarnEvent(t *testing.T) {
	s, runID := seedSpoolableStep(t, time.Minute)

	if err := s.RecordSpoolArchive(context.Background(), runID, "only",
		"stale-result.json", "the attempt was reclaimed by a newer lease"); err != nil {
		t.Fatalf("record: %v", err)
	}
	events, err := s.ExplainRunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the events: %v", err)
	}
	found := false
	for _, e := range events {
		if e.Kind == "run.spool_archived" {
			found = true
			if e.Actor != "reconcile" {
				t.Fatalf("actor = %q, want reconcile", e.Actor)
			}
		}
	}
	if !found {
		t.Fatal("no run.spool_archived event; a rejected result nobody can read about is a lost one")
	}
}
