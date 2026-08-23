package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The run lease is the whole of issue #60: one statement claims, one
// transaction renews every held run at once, the reaper takes what expired,
// and every write from a holder carries the fencing token so a frozen worker
// can never overwrite its successor's verdict.

// aMultiRun seeds n manual runs of the single step job.
func aMultiRun(t *testing.T, s *store.Store, n int) []string {
	t.Helper()

	aCanonicalJob(t, s, "nightly", singleStepSpec)
	var ids []string
	for i := 0; i < n; i++ {
		out, err := s.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
			JobName: "nightly",
		})
		if err != nil {
			t.Fatalf("materialise run %d: %v", i, err)
		}
		ids = append(ids, out.Run.ID)
	}
	return ids
}

func ref(owner string, epoch int64) store.LeaseRef {
	return store.LeaseRef{Owner: owner, Epoch: epoch}
}

// aRetryableQueuedRun seeds one manual run whose single step carries a retry
// budget, so a lost attempt comes back pending rather than failing the run.
func aRetryableQueuedRun(t *testing.T, s *store.Store) string {
	t.Helper()

	ctx := context.Background()
	aCanonicalJob(t, s, "nightly", singleStepSpec)
	version, err := s.CurrentJobVersion(ctx, "nightly")
	if err != nil {
		t.Fatalf("read the version: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		MaxAttempts:  2,
		Steps:        []store.NewStep{{Name: "build", MaxAttempts: 2}},
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return run.ID
}

func TestClaimRunsTakesEveryLeaseInOneStatement(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	aMultiRun(t, s, 3)

	got, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "exec-1", TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("claimed %d runs, want 3", len(got))
	}

	for _, cl := range got {
		if cl.LeaseEpoch != 1 {
			t.Errorf("run %s claimed at epoch %d, want 1", cl.ID, cl.LeaseEpoch)
		}
		if cl.JobName != "nightly" || cl.JobVersionID == "" || cl.ParamsJSON == "" {
			t.Errorf("run %s came back incomplete: %+v", cl.ID, cl)
		}
		run := mustGetRun(t, ctx, s, cl.ID)
		if run.State != string(model.RunRunning) {
			t.Errorf("run %s is %s, want running", cl.ID, run.State)
		}
		if run.LeaseOwner != "exec-1" {
			t.Errorf("run %s lease_owner = %q", cl.ID, run.LeaseOwner)
		}
		if run.HeartbeatAt.IsZero() {
			t.Errorf("run %s has no heartbeat_at stamp from the claim", cl.ID)
		}
		if !run.StartedAt.Equal(clk.Now()) {
			t.Errorf("run %s started_at = %s, want the claim instant", cl.ID, run.StartedAt)
		}
	}

	events, err := s.RunEvents(ctx, got[0].ID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	if len(events) != 2 || events[1].Kind != "run.started" || events[1].Actor != "exec-1" {
		t.Fatalf("claim did not write exactly one run.started event by the owner: %+v", events)
	}
}

func TestClaimRunsSkipsWhatIsNotDueAndClosesCancelledQueuedRuns(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	aCanonicalJob(t, s, "nightly", singleStepSpec)

	// One run held for the future, one with a cancel request waiting, one due.
	version := aCanonicalJob(t, s, "nightly", singleStepSpec)
	future, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName:      "nightly",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		AvailableAt:  clk.Now().Add(time.Hour),
		DeferReason:  "held for the test",
		MaxAttempts:  1,
		Steps:        []store.NewStep{{Name: "build"}},
	})
	if err != nil {
		t.Fatalf("materialise the future run: %v", err)
	}
	cancelled, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "nightly"})
	if err != nil {
		t.Fatalf("materialise cancelled: %v", err)
	}
	if _, err := s.RequestCancel(ctx, cancelled.Run.ID, "cli:1000", "changed my mind"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	due := aMultiRun(t, s, 1)

	got, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "exec-1", TTL: time.Minute, Limit: 10})
	if err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if len(got) != 1 || got[0].ID != due[0] {
		t.Fatalf("claimed %+v, want only the due run %s", got, due[0])
	}

	detail := mustGetRun(t, ctx, s, cancelled.Run.ID)
	if detail.Run.State != string(model.RunCancelled) {
		t.Errorf("the cancel-requested run is %s, want cancelled without ever being claimed",
			detail.Run.State)
	}
	if detail.Run.ReasonCode != string(reason.RUNCancelledManual) {
		t.Errorf("reason_code = %q, want %q", detail.Run.ReasonCode, reason.RUNCancelledManual)
	}
	if detail.Run.CancelRequestedBy != "cli:1000" {
		t.Errorf("the cancellation event should name who asked, run says %q", detail.Run.CancelRequestedBy)
	}

	futureRun := mustGetRun(t, ctx, s, future.ID)
	if futureRun.State != string(model.RunQueued) {
		t.Errorf("the future run is %s, want still queued", futureRun.State)
	}
}

func TestClaimRunReturnsTheFencingToken(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	state, epoch, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "exec-1", TTL: time.Minute})
	if err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if state != "running" || epoch != 1 {
		t.Fatalf("claim returned (%q, %d), want (running, 1)", state, epoch)
	}

	// A second claim of the same run is refused: the row left queued.
	_, _, err = s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "exec-2"})
	if !errors.Is(err, store.ErrNotClaimable) {
		t.Fatalf("second claim error = %v, want ErrNotClaimable", err)
	}
}

func TestRenewRunLeasesRenewsEverythingTheOwnerHolds(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	ids := aMultiRun(t, s, 2)

	if _, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "exec-1", TTL: 30 * time.Second, Limit: 10}); err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	other := aMultiRun(t, s, 1)
	if _, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "exec-2", TTL: 30 * time.Second, Limit: 10}); err != nil {
		t.Fatalf("ClaimRuns for the other owner: %v", err)
	}

	clk.Advance(5 * time.Second)
	before := mustGetRun(t, ctx, s, ids[0]).Run

	renewed, err := s.RenewRunLeases(ctx, "exec-1", 30*time.Second)
	if err != nil {
		t.Fatalf("RenewRunLeases: %v", err)
	}
	if len(renewed) != 2 {
		t.Fatalf("renewal answered for %d runs, want the 2 exec-1 holds", len(renewed))
	}
	seen := map[string]int64{}
	for _, r := range renewed {
		seen[r.ID] = r.LeaseEpoch
		if !r.CancelRequestedAt.IsZero() {
			t.Errorf("run %s answered a cancellation nobody asked for", r.ID)
		}
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("run %s renewed at epoch %d, want 1: a renewal never moves the token", id, seen[id])
		}
	}
	if _, ok := seen[other[0]]; ok {
		t.Errorf("the renewal touched run %s, which another owner holds", other[0])
	}

	after := mustGetRun(t, ctx, s, ids[0]).Run
	if !after.LeaseExpiresAt.After(before.LeaseExpiresAt.Add(4 * time.Second)) {
		t.Errorf("lease_expires_at moved from %s to %s, want a full ttl further",
			before.LeaseExpiresAt, after.LeaseExpiresAt)
	}
	if after.HeartbeatAt.Before(before.HeartbeatAt.Add(4 * time.Second)) {
		t.Errorf("heartbeat_at did not move forward: %s -> %s", before.HeartbeatAt, after.HeartbeatAt)
	}
	if after.LeaseEpoch != before.LeaseEpoch {
		t.Errorf("lease_epoch moved on a renewal: %d -> %d", before.LeaseEpoch, after.LeaseEpoch)
	}
}

func TestRenewRunLeasesCarriesTheCancelRequestBack(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "exec-1", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}

	renewed, err := s.RenewRunLeases(ctx, "exec-1", time.Minute)
	if err != nil {
		t.Fatalf("RenewRunLeases: %v", err)
	}
	if len(renewed) != 1 {
		t.Fatalf("renewal answered for %d runs, want 1", len(renewed))
	}
	r := renewed[0]
	if r.ID != runID || r.CancelRequestedAt.IsZero() || r.CancelRequestedBy != "cli:1000" {
		t.Fatalf("renewal answer %+v, want the cancel request and who asked", r)
	}
}

func TestReaperRequeuesAnExpiredRunForANewHolder(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryableQueuedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", ref("doomed", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}
	// Out-wait the ttl plus the skew allowance: the reaper is deliberately
	// late, that is the whole asymmetry with the owner's early give-up.
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)

	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 1 || reaped[0].ID != runID {
		t.Fatalf("reaped %+v, want exactly run %s", reaped, runID)
	}
	r := reaped[0]
	if r.State != string(model.RunQueued) {
		t.Errorf("reap sent the run to %s, want queued", r.State)
	}
	if r.LeaseEpoch != 2 {
		t.Errorf("reap left lease_epoch = %d, want 2", r.LeaseEpoch)
	}
	if r.CrashCount != 1 {
		t.Errorf("crash_count = %d, want 1", r.CrashCount)
	}

	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.LeaseOwner != "" || !detail.Run.LeaseExpiresAt.IsZero() {
		t.Errorf("the reaped run still shows a lease: owner %q expires %v",
			detail.Run.LeaseOwner, detail.Run.LeaseExpiresAt)
	}
	if detail.Run.DeferReason != model.DeferReasonAfterCrash {
		t.Errorf("defer_reason = %q, want %q (I14)", detail.Run.DeferReason, model.DeferReasonAfterCrash)
	}
	if !detail.Run.AvailableAt.After(clk.Now()) {
		t.Errorf("available_at = %s, want a backoff into the future", detail.Run.AvailableAt)
	}
	step := detail.Steps[0]
	if step.State != string(model.StepPending) || step.ReasonCode != string(reason.STEPFailedExecutorLost) {
		t.Errorf("the running step came back %s/%q, want pending under %q",
			step.State, step.ReasonCode, reason.STEPFailedExecutorLost)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "run.requeued" || last.Actor != "reaper" || last.ToState != string(model.RunQueued) {
		t.Fatalf("the reap event is %+v, want run.requeued to queued by the reaper", last)
	}
	if !strings.Contains(last.DetailJSON, `"lease_epoch":2`) {
		t.Errorf("the reap event does not name the new fencing token: %s", last.DetailJSON)
	}
}

func TestReaperQuarantinesAPoisonedRun(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	// Five crashes are absorbed; the sixth poisons the run.
	for i := 0; i < 6; i++ {
		if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
			t.Fatalf("claim %d: %v", i, err)
		}
		clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
		reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
		if err != nil {
			t.Fatalf("reap %d: %v", i, err)
		}
		if len(reaped) != 1 {
			t.Fatalf("reap %d took %+v, want the run", i, reaped)
		}
		if i < 5 && reaped[0].State != string(model.RunQueued) {
			t.Fatalf("crash %d sent the run to %s, want queued while the budget holds", i+1, reaped[0].State)
		}
		if i == 5 {
			if reaped[0].State != string(model.RunFailed) {
				t.Fatalf("crash 6 sent the run to %s, want failed quarantine", reaped[0].State)
			}
			if reaped[0].ReasonCode != string(reason.RUNPoisoned) {
				t.Errorf("quarantine reason = %q, want %q", reaped[0].ReasonCode, reason.RUNPoisoned)
			}
			if reaped[0].LeaseEpoch != 12 {
				t.Errorf("quarantine epoch = %d, want 12 (six claims plus six reaps)", reaped[0].LeaseEpoch)
			}
		}
		// Let the requeue backoff pass, so the next claim finds the run due.
		clk.Advance(store.DefaultRequeueBackoff)
	}

	// Quarantine is final: nothing is running any more, so further sweeps
	// leave the row alone. The system never loops on a job that kills it.
	clk.Advance(time.Hour)
	again, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("reap after quarantine: %v", err)
	}
	if len(again) != 0 {
		t.Fatalf("the sweep touched %+v after quarantine, want nothing running", again)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var poisoned bool
	for _, e := range events {
		if e.Kind == "run.poisoned" && e.Actor == "reaper" {
			poisoned = true
		}
	}
	if !poisoned {
		t.Error("no run.poisoned event by the reaper in the history")
	}
}

func TestReaperCompletesACancelRequestWhenTheOwnerIsDead(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed"}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop it"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)

	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 1 || reaped[0].State != string(model.RunCancelled) {
		t.Fatalf("reaped %+v, want the run cancelled: a dead owner's cancel request is completed, not requeued", reaped)
	}
	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.ReasonCode != string(reason.RUNCancelledManual) {
		t.Errorf("reason_code = %q, want %q", detail.Run.ReasonCode, reason.RUNCancelledManual)
	}
}

func TestReaperLeavesALiveLeaseAloneInsideTheSkew(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "alive", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	// Past the expiry but inside the skew allowance: the owner may merely be
	// slow, and the wall clock allowance is what keeps the takeover late.
	clk.Advance(time.Minute + 5*time.Second)
	reaped, err := s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	if len(reaped) != 0 {
		t.Fatalf("the reaper took %+v inside the skew window, want nothing", reaped)
	}
	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.LeaseEpoch != 1 || detail.Run.LeaseOwner != "alive" {
		t.Errorf("the live lease moved: epoch %d owner %q", detail.Run.LeaseEpoch, detail.Run.LeaseOwner)
	}

	// And past expiry plus skew, it goes.
	clk.Advance(10 * time.Second)
	reaped, err = s.ReapExpiredRuns(ctx, store.ReapOptions{})
	if err != nil {
		t.Fatalf("ReapExpiredRuns past the skew: %v", err)
	}
	if len(reaped) != 1 {
		t.Fatalf("the reaper took %+v once the skew passed, want the run", reaped)
	}
}

// TestAFrozenWorkerCannotOverwriteItsSuccessor is the named scenario from the
// issue: a worker freezes for 61 seconds, the reaper hands the run to a new
// holder, the frozen worker wakes and tries to write succeeded over the
// successor's world. Every result write must answer RowsAffected() == 0 and
// the verdict must be discarded, not applied.
func TestAFrozenWorkerCannotOverwriteItsSuccessor(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aRetryableQueuedRun(t, s)

	// The worker claims and starts work. The ttl is thirty seconds so the
	// sixty one second freeze below is unambiguously dead.
	frozen, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: "frozen-worker", TTL: 30 * time.Second})
	if err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if len(frozen) != 1 || frozen[0].ID != runID {
		t.Fatalf("claimed %+v, want %s", frozen, runID)
	}
	stale := ref("frozen-worker", frozen[0].LeaseEpoch)
	if err := s.StartStep(ctx, runID, "build", stale); err != nil {
		t.Fatalf("StartStep: %v", err)
	}

	// Sixty one seconds pass. The reaper waits out the expiry plus the skew
	// allowance and then gives the run to a successor; the requeue backoff
	// passes too, so the run is due again when the successor shows up.
	clk.Advance(61 * time.Second)
	if _, err := s.ReapExpiredRuns(ctx, store.ReapOptions{}); err != nil {
		t.Fatalf("ReapExpiredRuns: %v", err)
	}
	clk.Advance(store.DefaultRequeueBackoff)
	state, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "successor"})
	if err != nil {
		t.Fatalf("the successor could not claim: %v", err)
	}
	if state != "running" {
		t.Fatalf("successor claim state = %q", state)
	}

	// The frozen worker wakes up holding only its stale token.
	outcome := store.StepOutcome{
		Event:      string(model.EvStepSucceeded),
		ReasonCode: reason.STEPSucceeded,
		FinishedAt: clk.Now(),
	}
	if err := s.RecordStepOutcome(ctx, runID, "build", outcome, stale); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("stale RecordStepOutcome error = %v, want ErrLeaseLost", err)
	}
	var staleState string
	staleState, err = s.FinishRun(ctx, runID, stale, store.FinishReason{Code: reason.RUNSucceeded})
	if !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("stale FinishRun error = %v, want ErrLeaseLost", err)
	}
	if staleState != "" {
		t.Errorf("FinishRun reported a next state %q off a stale lease, want none", staleState)
	}
	// An operator asked for cancellation during the freeze; the stale holder
	// must not be the one to complete it.
	if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop"); err != nil {
		t.Fatalf("RequestCancel: %v", err)
	}
	if err := s.ObserveRunCancel(ctx, runID, stale, "cli:1000", reason.RUNCancelledManual); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("stale ObserveRunCancel error = %v, want ErrLeaseLost", err)
	}

	// Nothing the frozen worker tried landed: the successor owns a clean,
	// untouched running run.
	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.State != string(model.RunRunning) || detail.Run.LeaseOwner != "successor" {
		t.Fatalf("after the stale writes the run is %s owned by %q, want running owned by successor",
			detail.Run.State, detail.Run.LeaseOwner)
	}
	if detail.Steps[0].State != string(model.StepPending) {
		t.Errorf("the step is %s, want pending: the stale verdict never landed", detail.Steps[0].State)
	}
	if detail.Run.ReasonCode != "" {
		t.Errorf("the stale succeeded verdict leaked onto the row as %q", detail.Run.ReasonCode)
	}

	// The successor does the work honestly and finishes, and the history
	// shows exactly one end.
	live := ref("successor", mustGetRun(t, ctx, s, runID).Run.LeaseEpoch)
	if err := s.StartStep(ctx, runID, "build", live); err != nil {
		t.Fatalf("the successor could not start the step: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, runID, "build", store.StepOutcome{
		Event:      string(model.EvStepSucceeded),
		ReasonCode: reason.STEPSucceeded,
	}, live); err != nil {
		t.Fatalf("the successor could not record its verdict: %v", err)
	}
	final, err := s.FinishRun(ctx, runID, live, store.FinishReason{Code: reason.RUNSucceeded})
	if err != nil || final != "succeeded" {
		t.Fatalf("successor finish = (%q, %v), want succeeded", final, err)
	}
	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	ends := 0
	for _, e := range events {
		if strings.HasPrefix(e.Kind, "run.") && (e.ToState == "succeeded" || e.ToState == "failed") {
			ends++
		}
	}
	if ends != 1 {
		t.Errorf("%d run-level ends in the history, want exactly the successor's one: %+v", ends, events)
	}
}

func TestAWriteFromTheSystemNeedsADeadLease(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "alive", TTL: time.Minute}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", ref("alive", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}

	// Recovery writes without a holder, but only against a dead lease. On a
	// live lease there is someone else driving the run, and the write refuses.
	outcome := store.StepOutcome{
		Event:      string(model.EvStepFailed),
		ReasonCode: reason.STEPFailedExecutorLost,
	}
	if err := s.RecordStepOutcome(ctx, runID, "build", outcome, store.LeaseRef{}); !errors.Is(err, store.ErrLeaseLost) {
		t.Errorf("system write on a live lease error = %v, want ErrLeaseLost", err)
	}

	clk.Advance(store.DefaultRunLeaseTTL + 2*store.DefaultClockSkewAllowance)
	if err := s.RecordStepOutcome(ctx, runID, "build", outcome, store.LeaseRef{}); err != nil {
		t.Errorf("system write on a dead lease error = %v, want none", err)
	}
}

func TestDrainRunHandsOneRunBackUnderItsOwnToken(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	runID := aMaterializedRun(t, s)

	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "draining"}); err != nil {
		t.Fatalf("ClaimRun: %v", err)
	}
	if err := s.StartStep(ctx, runID, "build", ref("draining", 1)); err != nil {
		t.Fatalf("StartStep: %v", err)
	}

	handed, err := s.DrainRun(ctx, runID, ref("draining", 1), reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("DrainRun: %v", err)
	}
	if !handed {
		t.Fatal("DrainRun reported nothing handed back")
	}
	detail := mustGetRun(t, ctx, s, runID)
	if detail.Run.State != string(model.RunQueued) {
		t.Fatalf("the drained run is %s, want queued", detail.Run.State)
	}
	if detail.Run.LeaseEpoch != 2 {
		t.Errorf("drain left epoch %d, want 2: the drained attempt stays fenced out", detail.Run.LeaseEpoch)
	}
	step := detail.Steps[0]
	if step.State != string(model.StepPending) || step.Attempt != 0 {
		t.Errorf("the interrupted step is %s at attempt %d, want pending with the attempt restored",
			step.State, step.Attempt)
	}

	// A stale drain attempt answers quietly: someone else owns the run now.
	handed, err = s.DrainRun(ctx, runID, ref("draining", 1), reason.RUNInterruptedShutdown)
	if err != nil {
		t.Fatalf("second DrainRun: %v", err)
	}
	if handed {
		t.Error("a stale drain claimed it handed something back")
	}
}

func TestClaimConcurrencyClaimsEachRunExactlyOnce(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	const total, racers = 200, 32
	aMultiRun(t, s, total)

	type result struct {
		id  string
		err error
	}
	claims := make(chan result, racers*total/racers+racers)
	start := make(chan struct{})
	for i := 0; i < racers; i++ {
		go func(n int) {
			<-start
			owner := "racer-" + string(rune('a'+n%26)) + string(rune('0'+n/26))
			for {
				got, err := s.ClaimRuns(ctx, store.ClaimSpec{Owner: owner, TTL: time.Minute, Limit: 8})
				for _, cl := range got {
					claims <- result{id: cl.ID}
				}
				if err != nil {
					claims <- result{err: err}
					return
				}
				if len(got) == 0 {
					return
				}
			}
		}(i)
	}
	close(start)

	seen := map[string]int{}
	for i := 0; i < total; i++ {
		r := <-claims
		if r.err != nil {
			t.Fatalf("a racer failed: %v", r.err)
		}
		seen[r.id]++
		if seen[r.id] > 1 {
			t.Fatalf("run %s was claimed more than once", r.id)
		}
	}
	if len(seen) != total {
		t.Fatalf("%d distinct runs claimed, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("run %s was claimed %d times", id, n)
		}
	}
}
