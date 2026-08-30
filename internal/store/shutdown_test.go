package store

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// The drain half of the transition layer. A daemon that stops cleanly hands
// its in flight work back instead of inventing verdicts for it, and these
// tests hold the two writes that do it to the same bargain as every other
// transition: the machine decides, the row and one event row tell the story.

// seedDrainRun materialises one single step run, claims it with a live lease
// and starts its only step, so the step sits running with one attempt spent.
func seedDrainRun(t *testing.T, s *Store) string {
	t.Helper()

	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	spec := `{"schema":"paceq.job.v1","name":"drainjob","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["sleep","60"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  "drainjob",
		SpecHash: "sha256:drainjob",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{
		JobName: "drainjob", Actor: "test",
	})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, _, err := s.ClaimRun(context.Background(), res.Run.ID,
		LeaseInput{Owner: "exec-drained", TTL: 5 * time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(context.Background(), res.Run.ID, "only", LeaseRef{Owner: "exec-drained", Epoch: 1}); err != nil {
		t.Fatalf("start step: %v", err)
	}
	return res.Run.ID
}

// stepRow reads one step back whole.
func stepRow(t *testing.T, s *Store, runID, name string) Step {
	t.Helper()

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	for _, st := range detail.Steps {
		if st.Name == name {
			return st
		}
	}
	t.Fatalf("no step named %s", name)
	return Step{}
}

func TestDrainRunRestoresTheInterruptedAttempt(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	handed, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !handed {
		t.Fatal("DrainRun reported nothing handed back for its own live lease")
	}

	st := stepRow(t, s, runID, "only")
	if st.State != "pending" {
		t.Errorf("step state is %s, want pending", st.State)
	}
	if st.Attempt != 0 {
		t.Errorf("attempt is %d, want 0: the interrupted start must not spend a retry", st.Attempt)
	}
	if st.ReasonCode != string(reason.RUNInterruptedShutdown) {
		t.Errorf("row reason_code is %q, want %q", st.ReasonCode, reason.RUNInterruptedShutdown)
	}

	events, err := s.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	var interrupted *RunEvent
	for i := range events {
		if events[i].Kind == "step.interrupted" {
			interrupted = &events[i]
		}
	}
	if interrupted == nil {
		t.Fatalf("no step.interrupted event in %+v", events)
	}
	last := interrupted
	if last.StepName != "only" {
		t.Errorf("the event names step %q, want only", last.StepName)
	}
	if last.ReasonCode != string(reason.RUNInterruptedShutdown) {
		t.Errorf("event reason_code is %q, want %q", last.ReasonCode, reason.RUNInterruptedShutdown)
	}
	if last.FromState != "running" || last.ToState != "pending" {
		t.Errorf("the event moved %s->%s, want running->pending", last.FromState, last.ToState)
	}
}

func TestDrainRunWritesNothingOnceTheLeaseHasMovedOn(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	// A holder that is not the row's owner gets a quiet no, not an error:
	// nothing of theirs is out there to hand back.
	handed, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "someone-else", Epoch: 9}, reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("drain under a foreign ref: %v", err)
	}
	if handed {
		t.Fatal("a foreign drain claimed it handed the run back")
	}

	// The real drain lands; a second one has nothing left to give back.
	if _, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("the first drain: %v", err)
	}
	handed, err = s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("the second drain: %v", err)
	}
	if handed {
		t.Fatal("a second drain claimed it handed something back")
	}
	st := stepRow(t, s, runID, "only")
	if st.Attempt != 0 || st.State != "pending" {
		t.Errorf("the extra drains moved the row: state %s attempt %d", st.State, st.Attempt)
	}
}

func TestDrainRunHandsTheRunBackWithoutACrash(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if _, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("requeue after drain: %v", err)
	}

	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if detail.Run.State != "queued" {
		t.Errorf("state is %s, want queued", detail.Run.State)
	}
	if detail.Run.DeferReason != model.DeferReasonAfterShutdown {
		t.Errorf("defer_reason is %q, want %q", detail.Run.DeferReason, model.DeferReasonAfterShutdown)
	}
	if detail.Run.LeaseOwner != "" || !detail.Run.LeaseExpiresAt.IsZero() {
		t.Errorf("the lease survived the drain: owner %q expires %v",
			detail.Run.LeaseOwner, detail.Run.LeaseExpiresAt)
	}
	if detail.Run.LeaseEpoch != 2 {
		t.Errorf("lease_epoch is %d after the drain, want 2: the drained attempt stays fenced out",
			detail.Run.LeaseEpoch)
	}
	if crashCount(t, s, runID) != 0 {
		t.Errorf("crash_count is %d, want 0: a clean stop is not a crash", crashCount(t, s, runID))
	}

	events, err := s.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "run.drained" {
		t.Errorf("the last event is %s, want run.drained", last.Kind)
	}
	if last.FromState != "running" || last.ToState != "queued" {
		t.Errorf("the event moved %s->%s, want running->queued", last.FromState, last.ToState)
	}

	violations, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %v after a legal drain", violations)
	}
}

// TestDrainRunHandsBackALapsedDeadlineItStillOwns: the deadline is when the
// reaper may take the run, not who owns it (#202). Until the reaper acts the
// row still names this daemon at this token, so a clean stop hands the work
// back instead of leaving it running with nobody driving it.
func TestDrainRunHandsBackALapsedDeadlineItStillOwns(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	s.clk.(*clock.Fake).Advance(6 * time.Minute)

	handed, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	if !handed {
		t.Fatal("DrainRun reported nothing handed back for a run its own token still holds")
	}
	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if detail.Run.State != "queued" {
		t.Errorf("the drain left the run %s, want queued", detail.Run.State)
	}
	if detail.Run.LeaseEpoch != 2 {
		t.Errorf("lease_epoch is %d, want 2: the handback still fences the drained attempt",
			detail.Run.LeaseEpoch)
	}
	if detail.Run.CrashCount != 0 {
		t.Errorf("crash_count is %d, want 0: the executor left on purpose", detail.Run.CrashCount)
	}
}

func TestDrainSequenceLeavesTheRunClaimable(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if _, err := s.DrainRun(context.Background(), runID,
		LeaseRef{Owner: "exec-drained", Epoch: 1}, reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("drain: %v", err)
	}

	ids, err := s.ClaimableRunIDs(context.Background())
	if err != nil {
		t.Fatalf("claimable runs: %v", err)
	}
	if len(ids) != 1 || ids[0] != runID {
		t.Errorf("claimable runs are %v, want [%s]", ids, runID)
	}

	violations, err := s.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %v after the full drain sequence", violations)
	}
}

func TestClaimableRunIDsSkipsRunsThatAreNotDue(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	spec := `{"schema":"paceq.job.v1","name":"queuejob","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(context.Background(), JobVersionInput{
		JobName:  "queuejob",
		SpecHash: "sha256:queuejob",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	version, err := s.CurrentJobVersion(context.Background(), "queuejob")
	if err != nil {
		t.Fatalf("read the version: %v", err)
	}
	newRun := func(available time.Time, why string) string {
		t.Helper()
		run, err := s.CreateRunWithSteps(context.Background(), NewRun{
			JobName:      "queuejob",
			JobVersionID: version.ID,
			Origin:       "manual",
			AvailableAt:  available,
			DeferReason:  why,
			Steps:        []NewStep{{Name: "only"}},
		})
		if err != nil {
			t.Fatalf("create a run: %v", err)
		}
		return run.ID
	}

	due := newRun(time.Time{}, "")                             // zero means now
	later := newRun(time.Now().Add(time.Hour), "parked_later") // parked in the future

	ids, err := s.ClaimableRunIDs(context.Background())
	if err != nil {
		t.Fatalf("claimable runs: %v", err)
	}
	if len(ids) != 1 || ids[0] != due {
		t.Fatalf("claimable runs are %v, want [%s] only; %s is not due yet", ids, due, later)
	}

	// A claimed run stops being claimable the way any queue read must see it.
	if _, _, err := s.ClaimRun(context.Background(), due, LeaseInput{Owner: "exec-1"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ids, err = s.ClaimableRunIDs(context.Background())
	if err != nil {
		t.Fatalf("claimable runs after the claim: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("claimable runs are %v after claiming the only due run, want none", ids)
	}
}

func TestCheckpointTruncateEmptiesTheWal(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if err := s.CheckpointTruncate(context.Background()); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}

	wal := s.Path() + "-wal"
	info, err := os.Stat(wal)
	if err != nil {
		t.Fatalf("stat %s: %v", wal, err)
	}
	const onePage = 8192
	if info.Size() > onePage {
		t.Errorf("the wal file is %d bytes after the checkpoint, want at most one page (%d)",
			info.Size(), onePage)
	}
	if runID == "" {
		t.Fatal("the seed vanished")
	}
}
