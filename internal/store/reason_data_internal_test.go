package store

import (
	"context"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
)

// #191: DataKeys is a promise. The catalogue publishes it into
// docs/reference/reason-codes.md and into `paceq error`, and until this file
// nothing held a written row against it.
//
// The guard is row shaped rather than writer shaped: it drives the store's own
// doors, reads the run row back and asks the catalogue what the payload is
// short of. Nothing here maps a level to a table, because a level is the
// object a code explains and not the row it lands on (#193): the one run level
// code that never reaches a run row is named below with the test that proves
// where it does land.

// reasonDataStore is a migrated store on a frozen clock, so a reap, a backoff
// and a deferral can all be walked without waiting for wall time.
func reasonDataStore(t *testing.T) (*Store, *clock.Fake) {
	t.Helper()

	clk := clock.NewFake(time.Date(2026, 6, 1, 9, 0, 0, 0, time.UTC))
	s := openTestStore(t, Options{Clock: clk})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, clk
}

// aManualRunOf materialises one manual run of an already applied job.
func aManualRunOf(t *testing.T, s *Store, job string) string {
	t.Helper()

	out, err := s.MaterializeManualTrigger(context.Background(), ManualTriggerInput{
		JobName: job,
		Actor:   "cli:1000",
	})
	if err != nil {
		t.Fatalf("materialise a manual run of %s: %v", job, err)
	}
	return out.Run.ID
}

// claimIt takes the run for owner and answers with the lease the caller has
// to prove on every later write.
func claimIt(t *testing.T, s *Store, runID, owner string) LeaseRef {
	t.Helper()

	state, epoch, err := s.ClaimRun(context.Background(), runID, LeaseInput{Owner: owner})
	if err != nil {
		t.Fatalf("claim run %s: %v", runID, err)
	}
	if state != string(model.RunRunning) {
		t.Fatalf("claim run %s left it %s, want running", runID, state)
	}
	return LeaseRef{Owner: owner, Epoch: epoch}
}

// failTheStep drives one step of a claimed run to failed through the machine.
func failTheStep(t *testing.T, s *Store, runID, step string, ref LeaseRef, clk *clock.Fake) {
	t.Helper()

	ctx := context.Background()
	if err := s.StartStep(ctx, runID, step, ref); err != nil {
		t.Fatalf("start step %s of run %s: %v", step, runID, err)
	}
	one := 1
	if err := s.RecordStepOutcome(ctx, runID, step, StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   &one,
		FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("fail step %s of run %s: %v", step, runID, err)
	}
}

// succeedTheStep drives one step of a claimed run to succeeded through the
// machine.
func succeedTheStep(t *testing.T, s *Store, runID, step string, ref LeaseRef, clk *clock.Fake) {
	t.Helper()

	ctx := context.Background()
	if err := s.StartStep(ctx, runID, step, ref); err != nil {
		t.Fatalf("start step %s of run %s: %v", step, runID, err)
	}
	zero := 0
	if err := s.RecordStepOutcome(ctx, runID, step, StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   &zero,
		FinishedAt: clk.Now(),
	}, ref); err != nil {
		t.Fatalf("succeed step %s of run %s: %v", step, runID, err)
	}
}

// crashTheRun loses n executors under the run, leaving it queued and due.
func crashTheRun(t *testing.T, s *Store, clk *clock.Fake, runID string, n int) {
	t.Helper()

	ctx := context.Background()
	for i := 0; i < n; i++ {
		claimIt(t, s, runID, "doomed")
		clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)
		reaped, err := s.ReapExpiredRuns(ctx, ReapOptions{})
		if err != nil {
			t.Fatalf("reap %d of run %s: %v", i, runID, err)
		}
		if len(reaped) != 1 || reaped[0].State != string(model.RunQueued) {
			t.Fatalf("crash %d of run %s took %+v, want it requeued", i+1, runID, reaped)
		}
		clk.Advance(DefaultRequeueBackoff)
	}
}

// runSeam is one production writer that can leave a code on a run row, driven
// end to end through the store's own doors.
type runSeam struct {
	name  string
	drive func(t *testing.T, s *Store, clk *clock.Fake) string
}

// runPromise places one run level code that promises reason_data keys. Either
// this package has writers for it, or it says where the code lands instead and
// which test proves that.
type runPromise struct {
	seams []runSeam

	// notWrittenHere is set exactly when seams is empty: no writer in this
	// package leaves the code on a run row, and the text names the test that
	// covers the code where it really lands.
	notWrittenHere string
}

func runLevelPromises() map[reason.Code]runPromise {
	return map[reason.Code]runPromise{
		reason.RUNQueuedConcurrency: {seams: []runSeam{{
			name: "overlap queue defers under the job ceiling",
			drive: func(t *testing.T, s *Store, _ *clock.Fake) string {
				admitJob(t, s, "build", 1)
				sched := admitSchedule(t, s, "build", "queue")
				claimIt(t, s, aManualRunOf(t, s, "build"), "holder")

				res := admitTick(t, s, sched, 0)
				if res.Run.ID == "" || !res.Deferred {
					t.Fatalf("the fixture deferred nothing: %+v", res)
				}
				return res.Run.ID
			},
		}}},

		reason.RUNDeferredConcurrencyKey: {seams: []runSeam{{
			name: "a held concurrency key defers the loser",
			drive: func(t *testing.T, s *Store, _ *clock.Fake) string {
				concApply(t, s, "waiter", constKey("k"), "")
				sched := concSchedule(t, s, "waiter")
				admitTick(t, s, sched, 0)

				loser := admitTick(t, s, sched, 1)
				if loser.Run.ID == "" || loser.Run.DeferReason != model.DeferReasonConcurrencyKey {
					t.Fatalf("the fixture produced no key deferral: %+v", loser.Run)
				}
				return loser.Run.ID
			},
		}}},

		reason.RUNReopenedOperator: {seams: []runSeam{{
			name: "an operator reopens a terminal run",
			drive: func(t *testing.T, s *Store, clk *clock.Fake) string {
				ctx := context.Background()
				admitJob(t, s, "build", 1)
				runID := aManualRunOf(t, s, "build")
				ref := claimIt(t, s, runID, "exec-1")
				failTheStep(t, s, runID, "build", ref, clk)
				if _, err := s.FinishRun(ctx, runID, ref, FinishReason{
					Code: reason.RUNFailedStep,
					Data: `{"attempt":1,"step":"build"}`,
				}); err != nil {
					t.Fatalf("finish run %s: %v", runID, err)
				}
				if _, err := s.ReopenTerminalRunByOperator(ctx, runID, "ops", ReopenOpts{}); err != nil {
					t.Fatalf("reopen run %s: %v", runID, err)
				}
				return runID
			},
		}}},

		reason.RUNPoisoned: {seams: []runSeam{{
			name: "the reaper quarantines a run past its crash budget",
			drive: func(t *testing.T, s *Store, clk *clock.Fake) string {
				admitJob(t, s, "build", 1)
				runID := aManualRunOf(t, s, "build")
				crashTheRun(t, s, clk, runID, DefaultMaxCrashCount)

				claimIt(t, s, runID, "doomed")
				clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)
				reaped, err := s.ReapExpiredRuns(context.Background(), ReapOptions{})
				if err != nil {
					t.Fatalf("the quarantine reap: %v", err)
				}
				if len(reaped) != 1 || reaped[0].ReasonCode != string(reason.RUNPoisoned) {
					t.Fatalf("the sweep took %+v, want the run quarantined", reaped)
				}
				return runID
			},
		}}},

		reason.RUNFailedStep: {seams: []runSeam{
			{
				name: "the reaper closes a run whose steps had already finished",
				drive: func(t *testing.T, s *Store, clk *clock.Fake) string {
					admitJob(t, s, "build", 1)
					runID := aManualRunOf(t, s, "build")
					crashTheRun(t, s, clk, runID, DefaultMaxCrashCount)

					// The last executor does the work and then dies before it
					// can record the run's own verdict.
					ref := claimIt(t, s, runID, "doomed")
					failTheStep(t, s, runID, "build", ref, clk)
					clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)
					reaped, err := s.ReapExpiredRuns(context.Background(), ReapOptions{})
					if err != nil {
						t.Fatalf("the aggregate reap: %v", err)
					}
					if len(reaped) != 1 || reaped[0].ReasonCode != string(reason.RUNFailedStep) {
						t.Fatalf("the sweep took %+v, want the failure its steps show", reaped)
					}
					return runID
				},
			},
			{
				name: "the holder observes a cancellation that arrived after the failure",
				drive: func(t *testing.T, s *Store, clk *clock.Fake) string {
					ctx := context.Background()
					admitJob(t, s, "build", 1)
					runID := aManualRunOf(t, s, "build")
					ref := claimIt(t, s, runID, "exec-1")
					failTheStep(t, s, runID, "build", ref, clk)
					if _, err := s.RequestCancel(ctx, runID, "cli:1000", "stop it"); err != nil {
						t.Fatalf("request the cancel of run %s: %v", runID, err)
					}
					if err := s.ObserveRunCancel(ctx, runID, ref, "cli:1000",
						reason.RUNCancelledManual); err != nil {
						t.Fatalf("observe the cancel of run %s: %v", runID, err)
					}
					return runID
				},
			},
		}},

		reason.RUNTimedOut: {notWrittenHere: "no store door produces it: the run deadline is the " +
			"engine's, and internal/engine's TestFinishReasonCarriesThePromisedKeys covers the writer"},

		reason.RUNRejectedDiskLow: {notWrittenHere: "the hold creates no run row at all: the code " +
			"lands on the tick and the trigger that refused the run, which " +
			"TestTheDiskHoldRecordsItsCodeOnTheTickAndTheTriggerAndNoRun proves"},
	}
}

// TestEveryRunLevelPromiseIsKeptOnItsRow is the guard: every run level code
// that declares DataKeys is either driven here through a real writer and held
// against its promise, or says in one line why no writer here can reach it and
// which test does. A new code with DataKeys fails until it is placed.
func TestEveryRunLevelPromiseIsKeptOnItsRow(t *testing.T) {
	promises := runLevelPromises()

	placed := 0
	for _, entry := range reason.All() {
		if entry.Level != reason.LevelRun || len(entry.DataKeys) == 0 {
			continue
		}
		placed++
		promise, ok := promises[entry.Code]
		if !ok {
			t.Errorf("%s promises reason_data %v and this guard neither drives a writer for it "+
				"nor says where it lands; place it in runLevelPromises", entry.Code, entry.DataKeys)
			continue
		}
		if len(promise.seams) == 0 {
			if promise.notWrittenHere == "" {
				t.Errorf("%s is placed with no writer and no reason; an unexplained gap is not a decision",
					entry.Code)
			}
			continue
		}
		for _, seam := range promise.seams {
			t.Run(string(entry.Code)+"/"+seam.name, func(t *testing.T) {
				s, clk := reasonDataStore(t)
				runID := seam.drive(t, s, clk)

				run, err := s.GetRun(context.Background(), runID)
				if err != nil {
					t.Fatalf("read run %s back: %v", runID, err)
				}
				if run.Run.ReasonCode != string(entry.Code) {
					t.Fatalf("the seam left reason_code %q on run %s, want %s",
						run.Run.ReasonCode, runID, entry.Code)
				}
				if missing := reason.MissingDataKeys(entry.Code, run.Run.ReasonData); len(missing) > 0 {
					t.Errorf("run %s carries %s with reason_data %q, which is missing the promised %v",
						runID, entry.Code, run.Run.ReasonData, missing)
				}
			})
		}
	}
	if placed < 7 {
		t.Fatalf("only %d run level codes promise reason_data; the catalogue has changed shape "+
			"and this guard has stopped covering it", placed)
	}
}

// TestTheOrdinaryClaimClearsTheDeferralResidue is the other half of #191: a run
// born deferred carries the deferral's reason columns, and starting it makes
// all three false at once. The keyed start has always cleared them; the
// ordinary claim is the path a job overlap deferral has to take, and it left
// them standing, so `paceq runs list` reads the finished run's state off a
// defer_reason from hours earlier and prints deferred for ever.
func TestTheOrdinaryClaimClearsTheDeferralResidue(t *testing.T) {
	ctx := context.Background()
	s, clk := reasonDataStore(t)

	admitJob(t, s, "build", 1)
	sched := admitSchedule(t, s, "build", "queue")
	holder := aManualRunOf(t, s, "build")
	holderRef := claimIt(t, s, holder, "holder")

	res := admitTick(t, s, sched, 0)
	if res.Run.ID == "" || !res.Deferred {
		t.Fatalf("the fixture deferred nothing: %+v", res)
	}
	deferred := res.Run.ID

	before, err := s.GetRun(ctx, deferred)
	if err != nil {
		t.Fatalf("read the deferred run: %v", err)
	}
	if before.Run.DeferReason == "" || before.Run.ReasonCode == "" || before.Run.ReasonData == "" {
		t.Fatalf("the fixture wrote no deferral residue to clear: %+v", before.Run)
	}

	// Free the slot and let the backoff pass, then claim through the ordinary
	// queue, which is the only exit a job overlap deferral has.
	succeedTheStep(t, s, holder, "build", holderRef, clk)
	if _, err := s.FinishRun(ctx, holder, holderRef, FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish the holding run: %v", err)
	}
	clk.Advance(DefaultDeferBackoff)
	claimed, err := s.ClaimRuns(ctx, ClaimSpec{Owner: "exec-1", Limit: 4})
	if err != nil {
		t.Fatalf("ClaimRuns: %v", err)
	}
	if len(claimed) != 1 || claimed[0].ID != deferred {
		t.Fatalf("the claim took %+v, want the deferred run %s", claimed, deferred)
	}

	after, err := s.GetRun(ctx, deferred)
	if err != nil {
		t.Fatalf("read the claimed run: %v", err)
	}
	if after.Run.DeferReason != "" {
		t.Errorf("the started run still says defer_reason %q; a run that is running is not deferred, "+
			"and paceq runs list reads its state column off this field",
			after.Run.DeferReason)
	}
	if after.Run.ReasonCode != "" {
		t.Errorf("the started run still carries reason_code %q from its deferral", after.Run.ReasonCode)
	}
	if after.Run.ReasonData != "" && after.Run.ReasonData != "{}" {
		t.Errorf("the started run still carries reason_data %q from its deferral", after.Run.ReasonData)
	}

	// The symptom the operator sees: the listing must call a finished run by
	// its state, not by a decision that ended before it started.
	ref := LeaseRef{Owner: "exec-1", Epoch: claimed[0].LeaseEpoch}
	succeedTheStep(t, s, deferred, "build", ref, clk)
	if _, err := s.FinishRun(ctx, deferred, ref, FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish the run: %v", err)
	}

	rows, err := s.ListRuns(ctx, RunFilter{Limit: 10})
	if err != nil {
		t.Fatalf("ListRuns: %v", err)
	}
	var listed *RunSummary
	for i := range rows {
		if rows[i].ID == deferred {
			listed = &rows[i]
		}
	}
	if listed == nil {
		t.Fatalf("the succeeded run is not in the listing: %+v", rows)
	}
	if listed.State != string(model.RunSucceeded) {
		t.Fatalf("the listing says %s, want succeeded", listed.State)
	}
	if listed.DeferReason != "" {
		t.Errorf("the succeeded run lists with defer_reason %q, so paceq runs list prints it as "+
			"deferred beside the outcome every step succeeded", listed.DeferReason)
	}
}

// TestAReapedRunWritesItsOwnReasonData pins the reaper's terminal writers
// against an older payload standing under a newer verdict. The residue is
// planted on the running row rather than inherited from a claim, so the
// assertion holds even when the claim path is the one keeping the column
// clean: a terminal writer owes its own explanation either way.
func TestAReapedRunWritesItsOwnReasonData(t *testing.T) {
	ctx := context.Background()
	s, clk := reasonDataStore(t)

	admitJob(t, s, "build", 1)
	runID := aManualRunOf(t, s, "build")
	crashTheRun(t, s, clk, runID, DefaultMaxCrashCount)
	claimIt(t, s, runID, "doomed")

	const stale = `{"active":1,"blocking_run_id":"01J0OLDDEFERRAL","limit":1,"scope":"job"}`
	if _, err := s.w.ExecContext(ctx,
		`UPDATE runs SET reason_code = ?, reason_data = ? WHERE id = ?`,
		string(reason.RUNQueuedConcurrency), stale, runID); err != nil {
		t.Fatalf("plant the older deferral's payload: %v", err)
	}

	clk.Advance(DefaultRunLeaseTTL + 2*DefaultClockSkewAllowance)
	reaped, err := s.ReapExpiredRuns(ctx, ReapOptions{})
	if err != nil {
		t.Fatalf("the quarantine reap: %v", err)
	}
	if len(reaped) != 1 || reaped[0].ReasonCode != string(reason.RUNPoisoned) {
		t.Fatalf("the sweep took %+v, want the run quarantined", reaped)
	}

	run, err := s.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("read the quarantined run: %v", err)
	}
	if run.Run.ReasonData == stale {
		t.Fatalf("the quarantined run still carries the older deferral's payload %s", run.Run.ReasonData)
	}
	if missing := reason.MissingDataKeys(reason.RUNPoisoned, run.Run.ReasonData); len(missing) > 0 {
		t.Errorf("the quarantined run carries reason_data %q, missing the promised %v",
			run.Run.ReasonData, missing)
	}
}
