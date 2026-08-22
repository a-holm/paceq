package store

import (
	"context"
	"errors"
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
	if _, err := s.ClaimRun(context.Background(), res.Run.ID,
		LeaseInput{Owner: "exec-drained", TTL: 5 * time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := s.StartStep(context.Background(), res.Run.ID, "only"); err != nil {
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

func TestInterruptStepForShutdownRestoresTheAttempt(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if err := s.InterruptStepForShutdown(context.Background(), runID, "only",
		reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("interrupt: %v", err)
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
	last := events[len(events)-1]
	if last.Kind != "step.interrupted" {
		t.Errorf("the last event is %s, want step.interrupted", last.Kind)
	}
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

func TestInterruptStepForShutdownRefusesAStepThatIsNotRunning(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	err := s.InterruptStepForShutdown(context.Background(), runID, "nope",
		reason.RUNInterruptedShutdown)
	if !errors.Is(err, ErrRunNotFound) {
		t.Errorf("an unknown step gave %v, want ErrRunNotFound", err)
	}

	// The first interrupt is the legal one; the step is pending from then
	// on, and a second interrupt has no attempt left to restore.
	if err := s.InterruptStepForShutdown(context.Background(), runID, "only",
		reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("the first interrupt: %v", err)
	}
	err = s.InterruptStepForShutdown(context.Background(), runID, "only",
		reason.RUNInterruptedShutdown)
	if err == nil {
		t.Fatal("interrupting a pending step succeeded")
	}
	st := stepRow(t, s, runID, "only")
	if st.Attempt != 0 || st.State != "pending" {
		t.Errorf("the refusal moved the row: state %s attempt %d", st.State, st.Attempt)
	}
}

func TestRequeueRunAfterDrainHandsTheRunBackWithoutACrash(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if err := s.RequeueRunAfterDrain(context.Background(), runID); err != nil {
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

func TestRequeueRunAfterDrainRefusesWithoutTheLease(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	// The lease lapses: from here the run belongs to the reaper's
	// lease_expired path, not to a clean drain.
	s.clk.(*clock.Fake).Advance(6 * time.Minute)

	if err := s.RequeueRunAfterDrain(context.Background(), runID); err == nil {
		t.Fatal("a drain requeue succeeded on an expired lease")
	}
	detail, err := s.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read the run: %v", err)
	}
	if detail.Run.State != "running" {
		t.Errorf("the refusal moved the run to %s", detail.Run.State)
	}
}

func TestDrainSequenceLeavesTheRunClaimable(t *testing.T) {
	s := openTestStore(t, Options{Clock: clock.NewFake(time.Now())})
	runID := seedDrainRun(t, s)

	if err := s.InterruptStepForShutdown(context.Background(), runID, "only",
		reason.RUNInterruptedShutdown); err != nil {
		t.Fatalf("interrupt: %v", err)
	}
	if err := s.RequeueRunAfterDrain(context.Background(), runID); err != nil {
		t.Fatalf("requeue: %v", err)
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
	if _, err := s.ClaimRun(context.Background(), due, LeaseInput{Owner: "exec-1"}); err != nil {
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
