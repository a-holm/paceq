package explain

// The M5-02 why-didnt-run checklist: one named, executable scenario per way
// work can silently not happen, each asserting all three layers the plans
// demand (06 §2.1, 09 §10.2):
//
//	(a) storage   - the decision is a real row in ticks/triggers/runs/steps,
//	                read back through the store's own explain reads;
//	(b) attribution - its reason_code is a catalogue code, terminal where the
//	                  scenario says so, carrying at least one remedy;
//	(c) presentation - `explain` shows the code with remediation in BOTH the
//	                   JSON contract and the text render.
//
// The suite is a checklist, not a coverage claim: TestEveryTerminalReasonHas-
// Scenario crosses this table against internal/reason so a future code path
// that declines work without a stored, explainable reason fails CI instead of
// rotting into UNKNOWN.
//
// Every setup drives the SAME store seams production drives - MaterializeTick's
// admission gate, CommitSensorTick's dedup and fence, the engine's
// claim/start/verdict/finish ladder, the reaper, gap detection - against a
// real SQLite file under a frozen clock. No :memory:, no time.Sleep, no mocks
// (00-SYNTESE §4.10 pkt 7). Where the deciding logic lives above the store
// (scheduler DST/catchup policy, the sensor breaker) the setup writes exactly
// what that layer writes once it has decided, and the row documents the
// producer; those layers pin their own decisions in their own packages.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// scenario is one row of the checklist. Name is the stable id the debugging
// page (M5-08) anchors link against; renaming one is a docs break.
type scenario struct {
	name string

	// code is the reason_code the scenario must produce.
	code reason.Code

	level reason.Level

	// outcome narrows the stored verdict beside the code: skipped, error,
	// missed for ticks; accepted/deduped/rejected for triggers; the run or
	// step state otherwise. Empty means any.
	outcome string

	// subjectJob is the job whose explain output must show the decision.
	// Run-level codes render on the job timeline (where runs carry their code
	// beside remedies); step codes render on the run report's step ladder.
	subjectJob string

	// setup provokes the state through the store seams.
	setup func(w *world)

	// wantIn are substrings that MUST appear in the text render: the code and
	// its remediation are asserted for every row, these pin the distinctive
	// advice.
	wantIn []string

	// extra carries side assertions beyond the three layers, such as the
	// breaker's paused_reason.
	extra func(t *testing.T, w *world)
}

// world is one scenario's private database, clock and fixtures.
type world struct {
	t   *testing.T
	ctx context.Context
	st  *store.Store
	clk *clock.Fake

	// focusRun is the run the row's layer-(a) read targets; setups set it.
	focusRun string
}

const scenarioWindow = 72 * time.Hour

func newWorld(t *testing.T) *world {
	t.Helper()
	w := &world{t: t, ctx: context.Background(), clk: clock.NewFake(frozenNow)}
	st, err := store.Open(context.Background(), t.TempDir()+"/state.db", store.Options{Clock: w.clk})
	if err != nil {
		t.Fatalf("open the scenario store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate the scenario store: %v", err)
	}
	w.st = st
	return w
}

// --- fixture builders -------------------------------------------------------

// seedJob applies a job whose frozen spec carries the given steps. extra
// splices raw top-level members (concurrency_key, on_conflict) into the JSON.
func (w *world) seedJob(name string, maxConcurrent int, extra string, steps ...string) string {
	w.t.Helper()
	if extra != "" {
		extra = "," + extra
	}
	spec := fmt.Sprintf(`{"schema":"paceq.job.v1","name":%q,"max_concurrent":%d,"steps":[%s]%s}`,
		name, maxConcurrent, strings.Join(steps, ","), extra)
	version, _, err := w.st.UpsertJobVersion(w.ctx, store.JobVersionInput{
		JobName:       name,
		SpecHash:      "sha256:scenario-" + name,
		SpecJSON:      spec,
		MaxConcurrent: maxConcurrent,
	})
	if err != nil {
		w.t.Fatalf("seed job %s: %v", name, err)
	}
	return version.ID
}

// seedSchedule creates one cron schedule due again eight hours out.
func (w *world) seedSchedule(job, name string, mut func(*store.ScheduleInput)) store.ScheduleRow {
	w.t.Helper()
	in := store.ScheduleInput{
		JobName:    job,
		Name:       name,
		Kind:       "cron",
		Expr:       "0 2 * * *",
		Timezone:   "UTC",
		NextTickAt: frozenNow.Add(8 * time.Hour),
	}
	if mut != nil {
		mut(&in)
	}
	if _, err := w.st.UpsertSchedule(w.ctx, in); err != nil {
		w.t.Fatalf("seed schedule %s.%s: %v", job, name, err)
	}
	sch, err := w.st.GetSchedule(w.ctx, job, name)
	if err != nil {
		w.t.Fatalf("read schedule %s.%s: %v", job, name, err)
	}
	return sch
}

func (w *world) seedSensor(name, job string) {
	w.t.Helper()
	if err := w.st.UpsertSensor(w.ctx, store.SensorSeedInput{
		Name: name, JobName: job, ExecJSON: `["/bin/echo","{}"]`,
	}); err != nil {
		w.t.Fatalf("seed sensor %s: %v", name, err)
	}
}

// fireTick materialises one schedule evaluation the way the loop does, pause
// and admission gates included.
func (w *world) fireTick(sch store.ScheduleRow, scheduledFor time.Time, outcome string, code reason.Code, text, runKey string, progress bool) store.TickResult {
	w.t.Helper()
	res, err := w.st.MaterializeTick(w.ctx, store.TickInput{
		Schedule:       sch,
		ScheduledFor:   scheduledFor,
		Outcome:        outcome,
		ReasonCode:     code,
		ReasonText:     text,
		RunKey:         runKey,
		NextTickAt:     frozenNow.Add(8 * time.Hour),
		UpdateProgress: progress,
	})
	if err != nil {
		w.t.Fatalf("materialise tick for %s/%s: %v", sch.JobName, sch.Name, err)
	}
	return res
}

// sensorBegin opens one evaluation's intention row.
func (w *world) sensorBegin(sensor string) store.BeginSensorTickResult {
	w.t.Helper()
	begun, err := w.st.BeginSensorTick(w.ctx, store.BeginSensorTickInput{
		SensorName:      sensor,
		DaemonSessionID: "session-scenario",
	})
	if err != nil {
		w.t.Fatalf("begin sensor tick for %s: %v", sensor, err)
	}
	return begun
}

// sensorCommit closes an evaluation the runtime would have decided.
func (w *world) sensorCommit(in store.SensorTickCommitInput) store.SensorTickCommitResult {
	w.t.Helper()
	res, err := w.st.CommitSensorTick(w.ctx, in)
	if err != nil {
		w.t.Fatalf("commit sensor tick for %s: %v", in.SensorName, err)
	}
	return res
}

// sensorEvaluation pairs a begin with a prefilled commit input.
func (w *world) sensorEvaluation(sensor, job string) store.SensorTickCommitInput {
	w.t.Helper()
	begun := w.sensorBegin(sensor)
	return store.SensorTickCommitInput{
		TickID:        begun.TickID,
		SensorName:    sensor,
		JobName:       job,
		CursorVersion: begun.CursorVersion,
		DedupEpoch:    0,
		NextEvalAt:    frozenNow.Add(time.Hour).UnixMilli(),
		DurationMs:    12,
	}
}

// manualRun queues a run the way `paceq run` does: tick, accepted trigger,
// queued run with the spec's steps and edges.
func (w *world) manualRun(job string) store.Run {
	w.t.Helper()
	res, err := w.st.MaterializeManualTrigger(w.ctx, store.ManualTriggerInput{JobName: job, Actor: "ops"})
	if err != nil {
		w.t.Fatalf("materialise manual run of %s: %v", job, err)
	}
	w.focusRun = res.Run.ID
	return res.Run
}

// scheduledRun queues a run through the schedule's real chain - fired tick,
// accepted trigger, run with the spec's steps - so the whole producer lineage
// renders on the job timeline the way an operator will read it.
func (w *world) scheduledRun(job, key string) store.Run {
	w.t.Helper()
	sch := w.seedSchedule(job, "nightly", nil)
	res := w.fireTick(sch, frozenNow.Add(-2*time.Hour), store.OutcomeTriggered, "", "", key, true)
	if res.Run.ID == "" {
		w.t.Fatalf("the scheduled run of %s did not materialise", job)
	}
	w.focusRun = res.Run.ID
	return res.Run
}

// claim takes the run's lease for an owner with a short TTL the clock can
// outlive deterministically.
func (w *world) claim(runID, owner string, ttl time.Duration) store.LeaseRef {
	w.t.Helper()
	state, epoch, err := w.st.ClaimRun(w.ctx, runID, store.LeaseInput{Owner: owner, TTL: ttl})
	if err != nil {
		w.t.Fatalf("claim run %s: %v", runID, err)
	}
	if state != "running" {
		w.t.Fatalf("claim run %s: got state %q, want running", runID, state)
	}
	return store.LeaseRef{Owner: owner, Epoch: epoch}
}

// beginStep takes the next runnable step through the executor's real path -
// internal/worker's ClaimNextStep, the whole decision AND reservation in one
// statement, which is also what bumps the run's attempt counter - and returns
// the step name already running under the lease.
func (w *world) beginStep(runID string, ref store.LeaseRef) string {
	w.t.Helper()
	cs, err := w.st.ClaimNextStep(w.ctx, runID, ref)
	if err != nil {
		w.t.Fatalf("claim the next step of %s: %v", runID, err)
	}
	if cs == nil {
		w.t.Fatalf("claim the next step of %s: nothing was runnable", runID)
	}
	return cs.Name
}

// verdict records one step outcome the engine's verdictFor shape carries.
func (w *world) verdict(runID, name string, ref store.LeaseRef, out store.StepOutcome) {
	w.t.Helper()
	if out.FinishedAt.IsZero() {
		out.FinishedAt = w.clk.Now()
	}
	if err := w.st.RecordStepOutcome(w.ctx, runID, name, out, ref); err != nil {
		w.t.Fatalf("record %s on step %s of %s: %v", out.Event, name, runID, err)
	}
}

// driveStep runs one step from claim to verdict.
func (w *world) driveStep(runID string, ref store.LeaseRef, ev string, code reason.Code, exit *int, signal string) string {
	w.t.Helper()
	name := w.beginStep(runID, ref)
	w.verdict(runID, name, ref, store.StepOutcome{
		Event:      ev,
		ReasonCode: code,
		ExitCode:   exit,
		Signal:     signal,
	})
	return name
}

// finishRun closes a claimed run with the engine's verdict vocabulary.
func (w *world) finishRun(runID string, ref store.LeaseRef, code reason.Code, data string) string {
	w.t.Helper()
	state, err := w.st.FinishRun(w.ctx, runID, ref, store.FinishReason{Code: code, Data: data})
	if err != nil {
		w.t.Fatalf("finish run %s as %s: %v", runID, code, err)
	}
	return state
}

// expireLease advances the frozen clock past a claim's TTL plus the reaper's
// skew allowance.
func (w *world) expireLease() {
	w.t.Helper()
	w.clk.Advance(store.DefaultRunLeaseTTL + store.DefaultClockSkewAllowance + time.Minute)
}

// --- the checklist ----------------------------------------------------------

var scenarios = []scenario{
	// -- ticks: the schedule stood down -------------------------------------

	{
		name:       "paused_schedule",
		subjectJob: "pauser",
		code:       reason.TICKSkippedPaused,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("pauser", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("pauser", "nightly", nil)
			if _, err := w.st.PauseSchedule(w.ctx, "pauser", "nightly"); err != nil {
				w.t.Fatalf("pause: %v", err)
			}
			// A fired evaluation lands AFTER the pause; the gate inside
			// MaterializeTick rewrites it into the recorded stand-down.
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "p-1", true)
		},
		wantIn: []string{"resume"},
	},

	{
		name:       "paused_sensor",
		subjectJob: "senpauser",
		code:       reason.TICKSkippedPaused,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("senpauser", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("hold", "senpauser")
			in := w.sensorEvaluation("hold", "senpauser")
			in.Outcome = store.OutcomeSkipped
			in.ReasonCode = reason.TICKSkippedPaused
			in.ReasonText = "the source is paused"
			w.sensorCommit(in)
		},
		wantIn: []string{"resume"},
	},

	{
		name:       "previous_run_overlap",
		subjectJob: "ovr",
		code:       reason.TICKSkippedOverlap,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("ovr", 1, "", stepSpec("go", "/bin/true", nil, -1))
			run := w.manualRun("ovr")
			w.claim(run.ID, "exec-ovr", store.DefaultRunLeaseTTL)
			sch := w.seedSchedule("ovr", "nightly", nil)
			// One held slot against a limit of one: the classic stand-down,
			// decided by the admission gate inside the tick transaction.
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "ovr-1", false)
		},
		wantIn: []string{"max_concurrent"},
	},

	{
		name:       "concurrency_cap_reached",
		subjectJob: "capped",
		code:       reason.TICKSkippedConcurrency,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("capped", 2, "", stepSpec("go", "/bin/true", nil, -1))
			for i := range 2 {
				run := w.manualRun("capped")
				w.claim(run.ID, fmt.Sprintf("exec-cap-%d", i), store.DefaultRunLeaseTTL)
			}
			sch := w.seedSchedule("capped", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "cap-1", false)
		},
		wantIn: []string{"raise the limit"},
	},

	{
		name:       "disk_low_holds_new_runs",
		subjectJob: "diskheld",
		code:       reason.RUNRejectedDiskLow,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("diskheld", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("diskheld", "nightly", nil)
			// The disk-guard's hold, installed exactly as the daemon installs
			// it: the admission gate reads it inside the tick transaction and
			// converts the fire into a stand-down. The code is a run-level
			// label because it names the refused run; the row it lives on is
			// the evaluation that refused it.
			w.st.SetRunHold(func() *store.RunHold {
				return &store.RunHold{
					Code: reason.RUNRejectedDiskLow,
					Text: "the filesystem holding the state is under its free-space floor",
				}
			})
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "disk-1", false)
		},
		wantIn: []string{"paceq prune"},
	},

	{
		name:       "tick_config_error",
		subjectJob: "misconfigured",
		code:       reason.TICKErrorConfig,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("misconfigured", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("misconfigured", "nightly", nil)
			// A config failure must stay due: progress does not move.
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeError, reason.TICKErrorConfig,
				"the schedule's job could not be resolved", "", false)
		},
		wantIn: []string{"paceq validate"},
	},

	{
		name:       "catchup_disabled",
		subjectJob: "catchless",
		code:       reason.TICKSkippedCatchupDisabled,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("catchless", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("catchless", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-6*time.Hour), store.OutcomeSkipped, reason.TICKSkippedCatchupDisabled,
				"missed while nothing ran; catchup is skip", "", true)
		},
		wantIn: []string{"catchup"},
	},

	{
		name:       "catchup_last_only",
		subjectJob: "catchlast",
		code:       reason.TICKSkippedCatchupLastOnly,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("catchlast", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("catchlast", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-7*time.Hour), store.OutcomeSkipped, reason.TICKSkippedCatchupLastOnly,
				"older than the kept moment; catchup is last", "", true)
		},
		wantIn: []string{"set catchup to all"},
	},

	{
		name:       "catchup_window",
		subjectJob: "catchwindow",
		code:       reason.TICKSkippedCatchupWindow,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("catchwindow", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("catchwindow", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-8*time.Hour), store.OutcomeSkipped, reason.TICKSkippedCatchupWindow,
				"owed but older than catchup_window", "", true)
		},
		wantIn: []string{"catchup_window"},
	},

	{
		name:       "dst_spring_forward_nonexistent",
		subjectJob: "springer",
		code:       reason.TICKSkippedDSTNonexistent,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("springer", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("springer", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-9*time.Hour), store.OutcomeSkipped, reason.TICKSkippedDSTNonexistent,
				"02:30 does not exist on this date in Europe/Oslo", "", true)
		},
		wantIn: []string{"spring_forward"},
	},

	{
		name:       "dst_fall_back_duplicate",
		subjectJob: "faller",
		code:       reason.TICKSkippedDSTDuplicate,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("faller", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("faller", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-10*time.Hour), store.OutcomeSkipped, reason.TICKSkippedDSTDuplicate,
				"02:30 happened twice; fall_back is first", "", true)
		},
		wantIn: []string{"fall_back"},
	},

	{
		name:       "daemon_down_gap",
		subjectJob: "gapjob",
		code:       reason.TICKMissedDaemonDown,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("gapjob", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSchedule("gapjob", "nightly", nil)
			outageID, err := w.st.RecordOutage(w.ctx, store.OutageInput{
				From: frozenNow.Add(-30 * time.Minute),
				To:   frozenNow.Add(-20 * time.Minute),
				Kind: "crash",
			})
			if err != nil {
				w.t.Fatalf("record the outage: %v", err)
			}
			inserted, err := w.st.RecordMissedTicks(w.ctx, "session-gap", outageID, []store.MissedTick{{
				SourceName:   "gapjob/nightly",
				ScheduledFor: frozenNow.Add(-25 * time.Minute),
			}})
			if err != nil || inserted != 1 {
				w.t.Fatalf("record the missed tick: %d inserted (%v)", inserted, err)
			}
		},
		wantIn: []string{"outage evidence"},
	},

	{
		name:       "daemon_crashed_mid_tick",
		subjectJob: "midcrash",
		code:       reason.TICKErrorDaemonCrashed,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("midcrash", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("died", "midcrash")
			w.sensorBegin("died") // the intention row nobody will close...
			// ...until startup reconciliation closes it as the daemon's death.
			closed, err := w.st.FailHangingTicks(w.ctx, frozenNow.Add(time.Minute))
			if err != nil || len(closed) != 1 {
				w.t.Fatalf("close the hanging tick: %v closed=%v", err, closed)
			}
		},
		wantIn: []string{"outages row"},
	},

	{
		name:       "lease_lost_fence",
		subjectJob: "raced",
		code:       reason.TICKMissedLeaseLost,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("raced", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("overtaken", "raced")
			first := w.sensorEvaluation("overtaken", "raced")
			first.Outcome = store.OutcomeTriggered
			first.CursorAfter = "cursor-1"
			first.Triggers = []store.SensorTrigger{{RunKey: "race-1"}}
			w.sensorCommit(first)

			stale := w.sensorEvaluation("overtaken", "raced")
			stale.CursorVersion = first.CursorVersion - 1 // an evaluation from before the takeover
			stale.Outcome = store.OutcomeTriggered
			stale.CursorAfter = "cursor-old"
			stale.Triggers = []store.SensorTrigger{{RunKey: "race-stale"}}
			res := w.sensorCommit(stale)
			if !res.Fenced {
				w.t.Fatalf("the stale commit was not fenced: %+v", res)
			}
		},
		wantIn: []string{"multi instance"},
	},

	// -- ticks: the sensor answered -----------------------------------------

	{
		name:       "sensor_says_skip",
		subjectJob: "watched",
		code:       reason.TICKSkippedSensor,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("watched", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("calm", "watched")
			in := w.sensorEvaluation("calm", "watched")
			in.Outcome = store.OutcomeSkipped
			in.ReasonCode = reason.TICKSkippedSensor
			in.ReasonText = "no new files since cursor"
			w.sensorCommit(in)
		},
		wantIn: []string{"reason_text"},
	},

	{
		name:       "sensor_paused_by_breaker",
		subjectJob: "brittle",
		code:       reason.TICKErrorSensorFailed,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("brittle", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("flaky", "brittle")
			in := w.sensorEvaluation("flaky", "brittle")
			in.Outcome = store.OutcomeError
			in.ReasonCode = reason.TICKErrorSensorFailed
			in.ReasonText = `exit 1`
			w.sensorCommit(in)
			// What the runtime's breaker does once consecutive failures pass
			// the policy (internal/sensor pins that decision).
			if err := w.st.PauseSensor(w.ctx, "flaky", "breaker: 3 consecutive failures"); err != nil {
				w.t.Fatalf("pause the sensor: %v", err)
			}
		},
		wantIn: []string{"by hand"},
		extra: func(t *testing.T, w *world) {
			sum, err := w.st.GetSensor(w.ctx, "flaky")
			if err != nil {
				t.Fatalf("read the sensor: %v", err)
			}
			if !sum.Paused || sum.PausedReason == "" {
				t.Fatalf("the breaker's pause is not on record: paused=%t reason=%q", sum.Paused, sum.PausedReason)
			}
		},
	},

	{
		name:       "sensor_timeout",
		subjectJob: "slowsource",
		code:       reason.TICKErrorSensorTimeout,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("slowsource", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("dawdler", "slowsource")
			in := w.sensorEvaluation("dawdler", "slowsource")
			in.Outcome = store.OutcomeError
			in.ReasonCode = reason.TICKErrorSensorTimeout
			in.ReasonText = "killed at the evaluation deadline"
			w.sensorCommit(in)
		},
		wantIn: []string{"timeout_ms"},
	},

	{
		name:       "sensor_output_invalid",
		subjectJob: "garbled",
		code:       reason.TICKErrorSensorOutput,
		level:      reason.LevelTick,
		setup: func(w *world) {
			w.seedJob("garbled", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("rambler", "garbled")
			in := w.sensorEvaluation("rambler", "garbled")
			in.Outcome = store.OutcomeError
			in.ReasonCode = reason.TICKErrorSensorOutput
			in.ReasonText = "stdout was not the contract's JSON"
			w.sensorCommit(in)
		},
		wantIn: []string{"print one JSON object on stdout"},
	},

	// -- triggers -------------------------------------------------------------

	{
		name:       "trigger_accepted",
		subjectJob: "fires",
		code:       reason.TRIGGERAccepted,
		level:      reason.LevelTrigger,
		setup: func(w *world) {
			w.seedJob("fires", 1, "", stepSpec("go", "/bin/true", nil, -1))
			sch := w.seedSchedule("fires", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "acc-1", true)
		},
		wantIn: []string{"follow the run id"},
	},

	{
		name:       "trigger_deduped_run_key",
		subjectJob: "twice",
		code:       reason.TRIGGERDedupedRunKey,
		level:      reason.LevelTrigger,
		setup: func(w *world) {
			w.seedJob("twice", 1, "", stepSpec("go", "/bin/true", nil, -1))
			w.seedSensor("replayy", "twice")
			first := w.sensorEvaluation("replayy", "twice")
			first.Outcome = store.OutcomeTriggered
			first.CursorAfter = "cursor-a"
			first.Triggers = []store.SensorTrigger{{RunKey: "dup-42"}}
			w.sensorCommit(first)

			again := w.sensorEvaluation("replayy", "twice")
			again.Outcome = store.OutcomeTriggered
			again.CursorAfter = "cursor-b"
			again.Triggers = []store.SensorTrigger{{RunKey: "dup-42"}} // the same key twice
			res := w.sensorCommit(again)
			if res.Deduped != 1 {
				w.t.Fatalf("the repeated key did not fold: %+v", res)
			}
		},
		wantIn: []string{"original_run_id"},
	},

	{
		name:       "trigger_rejected_concurrency_key",
		subjectJob: "laned",
		code:       reason.TRIGGERRejectedConcurrencyKey,
		level:      reason.LevelTrigger,
		setup: func(w *world) {
			w.seedJob("laned", 2,
				`"concurrency_key":{"constant":"lane"},"on_conflict":"skip"`,
				stepSpec("go", "/bin/true", nil, -1))
			w.manualRun("laned") // holds the key while queued
			sch := w.seedSchedule("laned", "nightly", nil)
			w.fireTick(sch, frozenNow.Add(-time.Hour), store.OutcomeTriggered, "", "", "lane-2", false)
		},
		wantIn: []string{"on_conflict"},
	},

	// -- runs -----------------------------------------------------------------

	{
		name:       "run_succeeded",
		subjectJob: "smooth",
		code:       reason.RUNSucceeded,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("smooth", 1, "", stepSpec("go", "/bin/true", nil, -1))
			run := w.scheduledRun("smooth", "rs")
			ref := w.claim(run.ID, "exec-smooth", store.DefaultRunLeaseTTL)
			zero := 0
			w.driveStep(run.ID, ref, "step_succeeded", reason.STEPSucceeded, &zero, "")
			w.finishRun(run.ID, ref, reason.RUNSucceeded, "")
		},
		wantIn: []string{"nothing to fix"},
	},

	{
		name:       "run_failed_step",
		subjectJob: "unlucky",
		code:       reason.RUNFailedStep,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("unlucky", 1, "", stepSpec("go", "/bin/false", nil, -1))
			run := w.scheduledRun("unlucky", "rf")
			ref := w.claim(run.ID, "exec-unlucky", store.DefaultRunLeaseTTL)
			one := 1
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedNonzeroExit, &one, "")
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"go"}`)
		},
		wantIn: []string{"open the step"},
	},

	{
		name:       "run_timed_out",
		subjectJob: "overlong",
		code:       reason.RUNTimedOut,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("overlong", 1, "", stepSpec("go", "/bin/sleep", nil, -1))
			run := w.scheduledRun("overlong", "rt")
			ref := w.claim(run.ID, "exec-overlong", store.DefaultRunLeaseTTL)
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedTimeout, nil, "KILL")
			w.finishRun(run.ID, ref, reason.RUNTimedOut, "")
		},
		wantIn: []string{"timeout"},
	},

	{
		name:       "run_cancelled_manual",
		subjectJob: "haltable",
		code:       reason.RUNCancelledManual,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("haltable", 1, "",
				stepSpec("first", "/bin/true", nil, -1),
				stepSpec("second", "/bin/true", []string{"first"}, -1))
			run := w.scheduledRun("haltable", "rc")
			ref := w.claim(run.ID, "exec-halt", store.DefaultRunLeaseTTL)
			name := w.beginStep(run.ID, ref)
			if _, err := w.st.RequestCancel(w.ctx, run.ID, "ops", "stop before deploy"); err != nil {
				w.t.Fatalf("request the cancel: %v", err)
			}
			w.verdict(run.ID, name, ref, store.StepOutcome{
				Event:      "cancel_observed",
				ReasonCode: reason.STEPCancelled,
			})
			if err := w.st.ObserveRunCancel(w.ctx, run.ID, ref, "ops", reason.RUNCancelledManual); err != nil {
				w.t.Fatalf("observe the cancel: %v", err)
			}
		},
		wantIn: []string{"cancel_reason"},
	},

	{
		name:       "run_poisoned",
		subjectJob: "crashloop",
		code:       reason.RUNPoisoned,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("crashloop", 1, "", stepSpec("go", "/bin/true", nil, -1))
			run := w.scheduledRun("crashloop", "rp")
			// First life: the lease dies, the reaper counts one crash and
			// requeues behind the backoff.
			w.claim(run.ID, "doomed-1", time.Second)
			w.expireLease()
			if _, err := w.st.ReapExpiredRuns(w.ctx, store.ReapOptions{MaxCrashCount: 1}); err != nil {
				w.t.Fatalf("first reap: %v", err)
			}
			// Second life ends the same way; the quarantine closes in.
			w.clk.Advance(store.DefaultRequeueBackoff + time.Minute)
			w.claim(run.ID, "doomed-2", time.Second)
			w.expireLease()
			reaped, err := w.st.ReapExpiredRuns(w.ctx, store.ReapOptions{MaxCrashCount: 1})
			if err != nil || len(reaped) != 1 || reaped[0].ReasonCode != string(reason.RUNPoisoned) {
				w.t.Fatalf("second reap: %v %+v", err, reaped)
			}
		},
		wantIn: []string{"memory pressure"},
	},

	{
		name:       "run_reopened_operator",
		subjectJob: "redone",
		code:       reason.RUNReopenedOperator,
		level:      reason.LevelRun,
		setup: func(w *world) {
			w.seedJob("redone", 1, "", stepSpec("go", "/bin/false", nil, -1))
			run := w.scheduledRun("redone", "rr")
			ref := w.claim(run.ID, "exec-redo", store.DefaultRunLeaseTTL)
			one := 1
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedNonzeroExit, &one, "")
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"go"}`)
			if _, err := w.st.ReopenTerminalRunByOperator(w.ctx, run.ID, "ops", store.ReopenOpts{}); err != nil {
				w.t.Fatalf("reopen: %v", err)
			}
		},
		wantIn: []string{"crash_count"},
	},

	// -- steps ----------------------------------------------------------------

	{
		name:       "step_succeeded",
		subjectJob: "fine",
		code:       reason.STEPSucceeded,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("fine", 1, "", stepSpec("build", "/bin/true", nil, -1))
			run := w.scheduledRun("fine", "ss")
			ref := w.claim(run.ID, "exec-fine", store.DefaultRunLeaseTTL)
			zero := 0
			w.driveStep(run.ID, ref, "step_succeeded", reason.STEPSucceeded, &zero, "")
			w.finishRun(run.ID, ref, reason.RUNSucceeded, "")
		},
		wantIn: []string{"nothing to fix"},
	},

	{
		name:       "step_failed_nonzero_exit",
		subjectJob: "exitcode",
		code:       reason.STEPFailedNonzeroExit,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("exitcode", 1, "", stepSpec("build", "/bin/false", nil, -1))
			run := w.scheduledRun("exitcode", "sx")
			ref := w.claim(run.ID, "exec-exit", store.DefaultRunLeaseTTL)
			three := 3
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedNonzeroExit, &three, "")
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"build"}`)
		},
		wantIn: []string{"tail of the log"},
	},

	{
		name:       "step_failed_timeout",
		subjectJob: "pastdue",
		code:       reason.STEPFailedTimeout,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("pastdue", 1, "", stepSpec("build", "/bin/sleep", nil, -1))
			run := w.scheduledRun("pastdue", "st")
			ref := w.claim(run.ID, "exec-due", store.DefaultRunLeaseTTL)
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedTimeout, nil, "KILL")
			w.finishRun(run.ID, ref, reason.RUNTimedOut, "")
		},
		wantIn: []string{"timeout"},
	},

	{
		name:       "step_failed_spawn",
		subjectJob: "missing",
		code:       reason.STEPFailedSpawn,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("missing", 1, "", stepSpec("build", "/finnes/ikke", nil, -1))
			run := w.scheduledRun("missing", "sp")
			ref := w.claim(run.ID, "exec-spawn", store.DefaultRunLeaseTTL)
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedSpawn, nil, "")
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"build"}`)
		},
		wantIn: []string{"executable"},
	},

	{
		name:       "step_failed_signal",
		subjectJob: "oomfood",
		code:       reason.STEPFailedSignal,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("oomfood", 1, "", stepSpec("build", "/bin/true", nil, -1))
			run := w.scheduledRun("oomfood", "sg")
			ref := w.claim(run.ID, "exec-sig", store.DefaultRunLeaseTTL)
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedSignal, nil, "KILL")
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"build"}`)
		},
		wantIn: []string{"OOM killer"},
	},

	{
		name:       "step_retries_exhausted",
		subjectJob: "stubborn",
		code:       reason.STEPRetriesExhausted,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("stubborn", 1, "", stepSpec("build", "/bin/false", nil, 1)) // retry.max 1 => two attempts
			run := w.scheduledRun("stubborn", "sr")
			ref := w.claim(run.ID, "exec-retry", store.DefaultRunLeaseTTL)
			one := 1
			w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedNonzeroExit, &one, "") // attempt 1 -> pending
			w.driveStep(run.ID, ref, "step_failed", reason.STEPRetriesExhausted, &one, "")  // attempt 2: budget spent
			w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"build"}`)
		},
		wantIn: []string{"compare the attempts"},
	},

	{
		name:       "step_skipped_run_timed_out",
		subjectJob: "overtime",
		code:       reason.STEPSkippedRunTimedOut,
		level:      reason.LevelStep,
		setup: func(w *world) {
			// Two steps with no edge between them: when the run's own
			// budget stops the loop, the one that never started is not
			// waiting on anything that failed.
			w.seedJob("overtime", 1, `"timeout_ms":250`,
				stepSpec("slow", "/bin/sleep", nil, -1),
				stepSpec("unrelated", "/bin/true", nil, -1))
			run := w.scheduledRun("overtime", "ot")
			ref := w.claim(run.ID, "exec-overtime", store.DefaultRunLeaseTTL)
			w.beginStep(run.ID, ref)
			w.verdict(run.ID, "slow", ref, store.StepOutcome{
				Event:            "step_failed",
				ReasonCode:       reason.STEPFailedTimeout,
				DetailJSON:       `{"scope":"run","timeout_ms":250}`,
				NoFurtherAttempt: true,
			})
			w.verdict(run.ID, "unrelated", ref, store.StepOutcome{
				Event:      "upstream_failed",
				ReasonCode: reason.STEPSkippedRunTimedOut,
			})
			w.finishRun(run.ID, ref, reason.RUNTimedOut, `{"timeout_ms":250}`)
		},
		wantIn: []string{"where the time went"},
	},

	{
		name:       "step_skipped_upstream_failed",
		subjectJob: "diamond",
		code:       reason.STEPSkippedUpstreamFailed,
		level:      reason.LevelStep,
		setup:      diamondWorld,
		wantIn:     []string{"fix or retry"},
	},

	{
		name:       "step_skipped_upstream_skipped",
		subjectJob: "diamond",
		code:       reason.STEPSkippedUpstreamSkipped,
		level:      reason.LevelStep,
		setup:      diamondWorld,
		wantIn:     []string{"upstream chain"},
	},

	{
		name:       "step_skipped_replay_reused",
		subjectJob: "again",
		code:       reason.STEPSkippedReplayReused,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("again", 1, "",
				stepSpec("build", "/bin/true", nil, -1),
				stepSpec("test", "/bin/true", []string{"build"}, -1))
			srcRun := w.scheduledRun("again", "sl")
			ref := w.claim(srcRun.ID, "exec-again", store.DefaultRunLeaseTTL)
			zero := 0
			w.driveStep(srcRun.ID, ref, "step_succeeded", reason.STEPSucceeded, &zero, "")
			w.driveStep(srcRun.ID, ref, "step_succeeded", reason.STEPSucceeded, &zero, "")
			w.finishRun(srcRun.ID, ref, reason.RUNSucceeded, "")
			res, err := w.st.MaterializeReplay(w.ctx, srcRun.ID, store.ReplayOpts{Actor: "ops", FailedOnly: true})
			if err != nil {
				w.t.Fatalf("replay: %v", err)
			}
			w.focusRun = res.NewRunID
		},
		wantIn: []string{"replayed run"},
	},

	{
		name:       "step_failed_executor_lost",
		subjectJob: "beheaded",
		code:       reason.STEPFailedExecutorLost,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("beheaded", 1, "", stepSpec("go", "/bin/true", nil, -1))
			run := w.scheduledRun("beheaded", "se")
			ref := w.claim(run.ID, "dead-behead", time.Second)
			w.beginStep(run.ID, ref) // running when its holder dies
			w.expireLease()
			if _, err := w.st.ReapExpiredRuns(w.ctx, store.ReapOptions{}); err != nil {
				w.t.Fatalf("reap: %v", err)
			}
		},
		wantIn: []string{"PACEQ_IDEMPOTENCY_KEY"},
	},

	{
		name:       "step_cancelled",
		subjectJob: "stopped",
		code:       reason.STEPCancelled,
		level:      reason.LevelStep,
		setup: func(w *world) {
			w.seedJob("stopped", 1, "",
				stepSpec("first", "/bin/true", nil, -1),
				stepSpec("second", "/bin/true", []string{"first"}, -1))
			run := w.scheduledRun("stopped", "sc")
			ref := w.claim(run.ID, "exec-stop", store.DefaultRunLeaseTTL)
			name := w.beginStep(run.ID, ref)
			if _, err := w.st.RequestCancel(w.ctx, run.ID, "ops", "stop"); err != nil {
				w.t.Fatalf("request the cancel: %v", err)
			}
			w.verdict(run.ID, name, ref, store.StepOutcome{
				Event:      "cancel_observed",
				ReasonCode: reason.STEPCancelled,
			})
			if err := w.st.ObserveRunCancel(w.ctx, run.ID, ref, "ops", reason.RUNCancelledManual); err != nil {
				w.t.Fatalf("observe the cancel: %v", err)
			}
		},
		wantIn: []string{"who cancelled it"},
	},
}

// diamondWorld builds a->b->c where a fails: the store's skip propagation
// closes b as directly-downstream-failed and c as transitively-skipped.
func diamondWorld(w *world) {
	w.seedJob("diamond", 1, "",
		stepSpec("a", "/bin/false", nil, -1),
		stepSpec("b", "/bin/true", []string{"a"}, -1),
		stepSpec("c", "/bin/true", []string{"b"}, -1))
	run := w.scheduledRun("diamond", "dia")
	ref := w.claim(run.ID, "exec-diamond", store.DefaultRunLeaseTTL)
	one := 1
	w.driveStep(run.ID, ref, "step_failed", reason.STEPFailedNonzeroExit, &one, "")
	w.finishRun(run.ID, ref, reason.RUNFailedStep, `{"step":"a"}`)
}

// stepSpec renders one step object for a job spec. retryMax below zero means
// no retry block.
func stepSpec(name, run string, needs []string, retryMax int) string {
	parts := []string{fmt.Sprintf(`"name":%q`, name), fmt.Sprintf(`"run":[%q]`, run)}
	if len(needs) > 0 {
		quoted := make([]string, len(needs))
		for i, n := range needs {
			quoted[i] = fmt.Sprintf("%q", n)
		}
		parts = append(parts, fmt.Sprintf(`"needs":[%s]`, strings.Join(quoted, ",")))
	}
	if retryMax >= 0 {
		parts = append(parts, fmt.Sprintf(`"retry":{"max":%d}`, retryMax))
	}
	return "{" + strings.Join(parts, ",") + "}"
}

// --- the three-layer assertion ----------------------------------------------

// TestScenarioChecklist walks every row: provoke, then hold all three layers.
func TestScenarioChecklist(t *testing.T) {
	for i := range scenarios {
		s := &scenarios[i]
		t.Run(s.name, func(t *testing.T) {
			w := newWorld(t)
			s.setup(w)

			// (a) storage: the decision is a real row with the right code.
			findStoredRow(t, w, s)

			// (b) attribution: the code is a catalogue member with remedies.
			entry, ok := reason.Lookup(s.code)
			if !ok {
				t.Fatalf("%s: reason_code %s is not in the catalogue", s.name, s.code)
			}
			if s.code == "" || s.code == "UNKNOWN" {
				t.Fatalf("%s: stored a placeholder code", s.name)
			}
			if len(entry.Remedy) == 0 {
				t.Fatalf("%s: %s carries no tiltaksforslag in the catalogue", s.name, s.code)
			}

			// (c) presentation: explain shows code + remedy in both modes.
			assertExplained(t, w, s)

			if s.extra != nil {
				s.extra(t, w)
			}
		})
	}
}

// findStoredRow holds layer (a): the row exists with the expected code (and
// outcome, when pinned), read back through the store's explain reads.
func findStoredRow(t *testing.T, w *world, s *scenario) {
	t.Helper()
	switch s.level {
	case reason.LevelTick:
		res, err := Resolve(w.ctx, w.st, s.subject(w))
		if err != nil {
			t.Fatalf("resolve %s: %v", s.name, err)
		}
		ticks, err := w.st.ExplainTicks(w.ctx, res.Sources, frozenNow.Add(-scenarioWindow), "", 200)
		if err != nil {
			t.Fatalf("read ticks for %s: %v", s.name, err)
		}
		for _, tk := range ticks {
			if tk.ReasonCode == string(s.code) && (s.outcome == "" || tk.Outcome == s.outcome) {
				return
			}
		}
		t.Fatalf("%s: no stored tick row carries %s (saw %d ticks)", s.name, s.code, len(ticks))

	case reason.LevelTrigger:
		res, err := Resolve(w.ctx, w.st, s.subject(w))
		if err != nil {
			t.Fatalf("resolve %s: %v", s.name, err)
		}
		ticks, err := w.st.ExplainTicks(w.ctx, res.Sources, frozenNow.Add(-scenarioWindow), "", 200)
		if err != nil {
			t.Fatalf("read ticks for %s: %v", s.name, err)
		}
		ids := make([]string, 0, len(ticks))
		for _, tk := range ticks {
			ids = append(ids, tk.ID)
		}
		byTick, err := w.st.ExplainTriggersByTicks(w.ctx, ids)
		if err != nil {
			t.Fatalf("read triggers for %s: %v", s.name, err)
		}
		for _, list := range byTick {
			for _, tg := range list {
				if tg.ReasonCode == string(s.code) && (s.outcome == "" || tg.Outcome == s.outcome) {
					return
				}
			}
		}
		t.Fatalf("%s: no stored trigger row carries %s", s.name, s.code)

	case reason.LevelRun:
		detail, err := w.st.GetRun(w.ctx, w.focusRun)
		if err != nil {
			t.Fatalf("read run %s for %s: %v", w.focusRun, s.name, err)
		}
		if detail.Run.ReasonCode != string(s.code) {
			t.Fatalf("%s: run row says reason_code %q, want %s (state %s)",
				s.name, detail.Run.ReasonCode, s.code, detail.Run.State)
		}

	case reason.LevelStep:
		detail, err := w.st.GetRun(w.ctx, w.focusRun)
		if err != nil {
			t.Fatalf("read run %s for %s: %v", w.focusRun, s.name, err)
		}
		for _, st := range detail.Steps {
			if st.ReasonCode == string(s.code) && (s.outcome == "" || st.State == s.outcome) {
				return
			}
		}
		t.Fatalf("%s: no step row of %s carries %s", s.name, w.focusRun, s.code)

	default:
		t.Fatalf("%s: unhandled level %s", s.name, s.level)
	}
}

// assertExplained holds layer (c): the built report surfaces the code with
// hints in the JSON contract and in the text render.
func assertExplained(t *testing.T, w *world, s *scenario) {
	t.Helper()

	res, err := Resolve(w.ctx, w.st, s.subject(w))
	if err != nil {
		t.Fatalf("resolve %s for explain: %v", s.name, err)
	}
	report, err := Build(w.ctx, w.st, res, Options{
		Since: frozenNow.Add(-scenarioWindow),
		Clock: w.clk,
	})
	if err != nil {
		t.Fatalf("build the report for %s: %v", s.name, err)
	}

	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal the report for %s: %v", s.name, err)
	}
	jsonOut := string(raw)
	if !strings.Contains(jsonOut, fmt.Sprintf(`"reason_code":%q`, string(s.code))) {
		t.Fatalf("%s: the JSON form never names %s:\n%s", s.name, s.code, jsonOut)
	}

	var shown bool
	var walk func(entries []Entry)
	walk = func(entries []Entry) {
		for i := range entries {
			if entries[i].ReasonCode == string(s.code) && len(entries[i].Hints) > 0 {
				shown = true
			}
			walk(entries[i].Children)
		}
	}
	walk(report.Entries)
	if !shown {
		t.Fatalf("%s: no rendered entry shows %s with remediation hints", s.name, s.code)
	}

	var out bytes.Buffer
	RenderText(&out, report, StyleASCII())
	text := out.String()
	if !strings.Contains(text, string(s.code)) {
		t.Fatalf("%s: the text form never names %s:\n%s", s.name, s.code, text)
	}
	if !strings.Contains(text, "-> ") {
		t.Fatalf("%s: the text form shows no remediation arrow:\n%s", s.name, text)
	}
	for _, want := range s.wantIn {
		if !strings.Contains(text, want) {
			t.Fatalf("%s: the text form misses %q:\n%s", s.name, want, text)
		}
	}
}

// subject resolves what explain is asked about. Step codes live on the run
// report's step ladder; everything else renders on the owning job's timeline,
// where ticks, triggers and runs carry their codes beside their remedies.
func (s *scenario) subject(w *world) string {
	if s.level == reason.LevelStep {
		return "run/" + w.focusRun
	}
	return "job/" + s.subjectJob
}

// --- the special row and the gate -------------------------------------------

// TestNoTickDueWritesNothing pins 06 §1.2: a moment where nothing is due
// writes NO tick row - there is deliberately no TICK_SKIPPED_NOT_DUE code -
// while explain still answers with the future instead of an empty table.
func TestNoTickDueWritesNothing(t *testing.T) {
	ctx := context.Background()
	w := newWorld(t)
	w.seedJob("quiet", 1, "", stepSpec("go", "/bin/true", nil, -1))
	w.seedSchedule("quiet", "nightly", nil)

	res, err := Resolve(ctx, w.st, "job/quiet")
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ticks, err := w.st.ExplainTicks(ctx, res.Sources, frozenNow.Add(-scenarioWindow), "", 200)
	if err != nil {
		t.Fatalf("read ticks: %v", err)
	}
	if len(ticks) != 0 {
		t.Fatalf("an idle window wrote %d tick rows; silence must stay silent", len(ticks))
	}
	if reason.IsKnown("TICK_SKIPPED_NOT_DUE") {
		t.Fatalf("TICK_SKIPPED_NOT_DUE exists in the catalogue; 06 §1.2 forbids it")
	}

	report, err := Build(ctx, w.st, res, Options{Since: frozenNow.Add(-scenarioWindow), Clock: w.clk})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if len(report.Entries) != 0 {
		t.Fatalf("an idle window produced %d entries, want none", len(report.Entries))
	}
	if report.Summary.NextTickAt == nil {
		t.Fatalf("the summary owes the next due tick; an empty answer without a future is a shrug")
	}

	var out bytes.Buffer
	RenderText(&out, report, StyleASCII())
	if !strings.Contains(out.String(), "no decisions recorded") {
		t.Fatalf("the idle answer lost its explanation:\n%s", out.String())
	}
	raw, err := json.Marshal(report)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !strings.Contains(string(raw), `"next_tick_at_ms"`) {
		t.Fatalf("the JSON form lost the next due tick:\n%s", raw)
	}
}

// minimumScenarioNames is the issue's M5 minimum list. The stable ids are part
// of the contract: the debugging page (M5-08) links them.
var minimumScenarioNames = []string{
	"paused_schedule", "paused_sensor", "no_tick_due", "previous_run_overlap",
	"concurrency_cap_reached", "sensor_says_skip", "trigger_deduped_run_key",
	"step_skipped_upstream_failed", "daemon_down_gap", "catchup_disabled",
	"dst_spring_forward_nonexistent", "sensor_paused_by_breaker", "run_poisoned",
	"sensor_timeout", "step_failed_spawn", "step_failed_signal",
	"step_retries_exhausted", "run_cancelled_manual",
}

// TestMinimumScenarioListIsPresent keeps the issue's named minimum from being
// quietly thinned even while the gate would still pass on remaining rows.
func TestMinimumScenarioListIsPresent(t *testing.T) {
	names := map[string]bool{}
	for _, s := range scenarios {
		names[s.name] = true
	}
	for _, want := range minimumScenarioNames {
		if want == "no_tick_due" {
			continue // covered by its dedicated absence test, not a table row
		}
		if !names[want] {
			t.Fatalf("scenario %q is gone from the checklist; it is part of the M5 minimum list", want)
		}
	}
}

// TestEveryTerminalReasonHasScenario is the gate that makes the checklist grow
// with the code: every terminal code in internal/reason needs either a row in
// the table above or an explicit, reasoned exemption in the catalogue. A new
// code path that declines work without storing an explainable reason now
// breaks CI here, exactly where the plans demand it (06 §15 risiko 8).
func TestEveryTerminalReasonHasScenario(t *testing.T) {
	covered := map[reason.Code]bool{}
	for _, s := range scenarios {
		if s.code != "" {
			covered[s.code] = true
		}
	}

	var missing, exempt []string
	for _, e := range reason.All() {
		if !e.Terminal {
			continue // only terminal outcomes owe a why-didnt-run story
		}
		if e.ScenarioExempt {
			if strings.TrimSpace(e.ExemptReason) == "" {
				t.Errorf("%s is ScenarioExempt with an empty ExemptReason: exemptions must argue themselves", e.Code)
			}
			exempt = append(exempt, string(e.Code))
			continue
		}
		if !covered[e.Code] {
			missing = append(missing, string(e.Code))
		}
	}

	if len(missing) > 0 {
		t.Fatalf("terminal reason codes without an explain scenario: %v\n"+
			"Add a named row to the scenarios table in internal/explain/scenarios_test.go "+
			"(setup through the store seams, then the three layers assert themselves), "+
			"or mark the code ScenarioExempt in internal/reason/codes.go WITH a non-empty ExemptReason.",
			missing)
	}
	if len(exempt) == 0 && len(missing) == 0 && len(covered) < 30 {
		t.Fatalf("only %d distinct codes covered; the checklist or the catalogue shrank unexpectedly", len(covered))
	}
}
