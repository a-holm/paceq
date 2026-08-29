package reconcile

import (
	"context"
	"os"
	"os/exec"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The orphan sweep's proofs. The decision core runs against an injected scan,
// so every refusal rule is provable without touching a real process; one
// integration test then proves the whole chain against a real child spawned
// with Setpgid, exactly as the runner spawns jobs.

// recSignals records everything the sweep sent.
type recSignals struct {
	mu     sync.Mutex
	terms  []int
	kills  []int
	killed chan struct{}
}

func newRecSignals() *recSignals {
	return &recSignals{killed: make(chan struct{}, 8)}
}

func (r *recSignals) Term(pgid int) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.terms = append(r.terms, pgid)
	return nil
}

func (r *recSignals) Kill(pgid int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.kills = append(r.kills, pgid)
	select {
	case r.killed <- struct{}{}:
	default:
	}
}

func (r *recSignals) sent() (terms, kills []int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]int(nil), r.terms...), append([]int(nil), r.kills...)
}

// sweepWorld is a store plus the seams the decision core needs. Its one run
// carries a persisted baseline whose numbers the test chooses, which is how
// a tampered or stale baseline is simulated without touching /proc.
type sweepWorld struct {
	w      *testing.T
	store  *store.Store
	clk    *clock.Fake
	ctx    context.Context
	signs  *recSignals
	reread func(pid int) (int64, bool)
	procs  []Process
	runID  string
}

func newSweepWorld(t *testing.T) *sweepWorld {
	t.Helper()
	clk := clock.NewFake(origin)
	s, err := store.Open(context.Background(), t.TempDir()+"/state.db", store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close the store: %v", err)
		}
	})
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if _, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:sweep",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":1,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}

	sw := &sweepWorld{w: t, store: s, clk: clk, ctx: context.Background(), signs: newRecSignals()}
	sw.reread = func(int) (int64, bool) { return 0, false }
	return sw
}

// seedBaseline records that attempt pid of a fresh run started at startTicks.
// The step stays pending, so nothing counts the attempt as active: the exact
// shape of an executor that died between spawning its job and writing any
// verdict.
func (sw *sweepWorld) seedBaseline(pid int, startTicks int64) string {
	sw.w.Helper()
	out, err := sw.store.MaterializeManualTrigger(sw.ctx, store.ManualTriggerInput{
		JobName: "nightly", Actor: "test",
	})
	if err != nil {
		sw.w.Fatalf("materialise the run: %v", err)
	}
	if _, _, err := sw.store.ClaimRun(sw.ctx, out.Run.ID, store.LeaseInput{
		Owner: "exec-1", TTL: time.Hour,
	}); err != nil {
		sw.w.Fatalf("claim the run: %v", err)
	}
	if err := sw.store.RecordAttemptProcess(sw.ctx, out.Run.ID, "work",
		store.LeaseRef{Owner: "exec-1", Epoch: 1}, pid, startTicks); err != nil {
		sw.w.Fatalf("record the baseline: %v", err)
	}
	return out.Run.ID
}

func (sw *sweepWorld) run() error {
	return sw.sweep(sw.ctx, sw.clk)
}

// sweep runs the decision core with the given clock, so timing proofs can
// swap the fake for a synctest bubble's virtualised system clock.
func (sw *sweepWorld) sweep(ctx context.Context, clk clock.Clock) error {
	opts := Options{
		Clock:       clk,
		ScanProcs:   func() ([]Process, error) { return sw.procs, nil },
		Signals:     sw.signs,
		SelfPID:     99999,
		SelfPGID:    99998,
		rereadTicks: sw.reread,
	}
	return sweepProcesses(ctx, sw.store, &opts)
}

// TestTheSweepKillsOnlyWhatItCanProve walks the refusal ladder: a proven
// orphan dies and every flavour of doubt walks away untouched.
func TestTheSweepKillsOnlyWhatItCanProve(t *testing.T) {
	const (
		orphanPID  = 101
		orphanPGID = 111
		actualTick = 700
	)
	newProc := func(seen int64) Process {
		return Process{
			PID: orphanPID, PGID: orphanPGID, RunID: "",
			StartTicks: seen, TicksOK: true,
		}
	}

	t.Run("a proven orphan is signalled, audited and escalated", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			sw := newSweepWorld(t)
			sw.runID = sw.seedBaseline(orphanPID, actualTick)
			p := newProc(actualTick)
			p.RunID = sw.runID
			sw.procs = []Process{p}
			stable := func(int) (int64, bool) { return actualTick, true }
			sw.reread = stable

			// Inside the bubble real timers are virtual, so the grace
			// elapses only when this test sleeps. That is what makes the
			// ordering TERM ... then KILL provable without wall-clock
			// margins.
			if err := sw.sweep(context.Background(), clock.System()); err != nil {
				t.Fatalf("sweep: %v", err)
			}

			terms, kills := sw.signs.sent()
			if len(terms) != 1 || terms[0] != orphanPGID {
				t.Errorf("TERM went to %v, want the orphan's group %d", terms, orphanPGID)
			}
			if len(kills) != 0 {
				t.Errorf("SIGKILL fired before its grace: %v", kills)
			}

			events, err := sw.store.RunEvents(sw.ctx, sw.runID)
			if err != nil {
				t.Fatalf("read events: %v", err)
			}
			if len(events) == 0 || events[len(events)-1].Kind != "run.orphan_killed" {
				t.Errorf("the kill left no audit event: %+v", events)
			}

			time.Sleep(DefaultOrphanGrace)
			synctest.Wait()
			if _, kills := sw.signs.sent(); len(kills) != 1 || kills[0] != orphanPGID {
				t.Errorf("escalation hit %v, want the group exactly once after the grace", kills)
			}
		})
	})

	t.Run("a manipulated baseline refuses the kill (AC6)", func(t *testing.T) {
		sw := newSweepWorld(t)
		// The machine says the process started at actualTick; the baseline on
		// file says something else. That disagreement is exactly what a
		// tampered or recycled identity looks like, and the answer is no.
		sw.runID = sw.seedBaseline(orphanPID, actualTick+1)
		p := newProc(actualTick)
		p.RunID = sw.runID
		sw.procs = []Process{p}
		sw.reread = func(int) (int64, bool) { return actualTick, true }

		if err := sw.run(); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if terms, kills := sw.signs.sent(); len(terms)+len(kills) != 0 {
			t.Errorf("signals flew on a mismatched identity: %v %v", terms, kills)
		}
	})

	t.Run("an identity that changes between reads refuses the kill", func(t *testing.T) {
		sw := newSweepWorld(t)
		sw.runID = sw.seedBaseline(orphanPID, actualTick)
		p := newProc(actualTick)
		p.RunID = sw.runID
		sw.procs = []Process{p}
		// The re-read disagrees: this pid was recycled mid sweep.
		recycled := func(int) (int64, bool) { return actualTick + 99, true }
		sw.reread = recycled

		if err := sw.run(); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if terms, kills := sw.signs.sent(); len(terms)+len(kills) != 0 {
			t.Errorf("signals flew through a pid reuse: %v %v", terms, kills)
		}
	})

	t.Run("a live worker of an active attempt is spared", func(t *testing.T) {
		sw := newSweepWorld(t)
		sw.runID = sw.seedBaseline(orphanPID, actualTick)
		detail, err := sw.store.GetRun(sw.ctx, sw.runID)
		if err != nil {
			t.Fatal(err)
		}
		_ = detail
		// Make the attempt active: the step starts running under the lease.
		if err := sw.store.StartStep(sw.ctx, sw.runID, "work",
			store.LeaseRef{Owner: "exec-1", Epoch: 1}); err != nil {
			t.Fatalf("start the step: %v", err)
		}
		p := newProc(actualTick)
		p.RunID = sw.runID
		sw.procs = []Process{p}
		sw.reread = func(int) (int64, bool) { return actualTick, true }

		if err := sw.run(); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if terms, kills := sw.signs.sent(); len(terms)+len(kills) != 0 {
			t.Errorf("the sweep signalled a legitimate worker: %v %v", terms, kills)
		}
	})

	t.Run("a process nobody can vouch for is left alone", func(t *testing.T) {
		sw := newSweepWorld(t)
		stranger := newProc(actualTick)
		stranger.RunID = "01J0NOTARUN00000000000000" // env marker without a run row
		sw.procs = []Process{stranger}

		if err := sw.run(); err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if terms, kills := sw.signs.sent(); len(terms)+len(kills) != 0 {
			t.Errorf("signals flew at a stranger: %v %v", terms, kills)
		}
	})
}

// TestOwnershipGradesEveryProcessTheSweepCanMeet pins the predicate itself
// (#189). The sweep and doctor's orphan check both classify through it, so
// each case here is one branch of both, and neither consumer can drift into
// its own idea of what "ours" means without changing this table.
func TestOwnershipGradesEveryProcessTheSweepCanMeet(t *testing.T) {
	worker := store.AttemptProcess{RunID: "01J0OURRUN0000000000000AA", Step: "work", PID: 101, StartTicks: 700}
	leftover := store.AttemptProcess{RunID: "01J0OURRUN0000000000000BB", Step: "work", PID: 102, StartTicks: 800}
	own := NewOwnership([]store.AttemptProcess{worker, leftover}, []store.AttemptProcess{worker})

	proc := func(a store.AttemptProcess) Process {
		return Process{PID: a.PID, PGID: a.PID, RunID: a.RunID, StartTicks: a.StartTicks, TicksOK: true}
	}
	recycled := proc(leftover)
	recycled.StartTicks++
	unreadable := proc(leftover)
	unreadable.TicksOK = false
	stranger := proc(worker)
	stranger.RunID = "01J0ANOTHERINSTALLATION00"
	unheldPID := proc(worker)
	unheldPID.PID = 999

	cases := []struct {
		name     string
		proc     Process
		claim    Claim
		baseline int64
	}{
		{"a worker an active attempt names", proc(worker), ClaimRunning, worker.StartTicks},
		{"a pid of ours nothing active names", proc(leftover), ClaimOrphan, leftover.StartTicks},
		{"another installation's run", stranger, ClaimForeign, 0},
		{"our run id on a pid we never held", unheldPID, ClaimForeign, 0},
		{"a pid recycled since the baseline", recycled, ClaimMismatch, leftover.StartTicks},
		{"start ticks that could not be read", unreadable, ClaimMismatch, leftover.StartTicks},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			claim, baseline := own.Classify(c.proc)
			if claim != c.claim {
				t.Errorf("claim is %d, want %d", claim, c.claim)
			}
			if baseline != c.baseline {
				t.Errorf("baseline ticks are %d, want %d", baseline, c.baseline)
			}
		})
	}
}

// TestEscalationFiresOnTheCallerClock keeps the timing promise visible: the
// timer belongs to the clock, and inside a synctest bubble the grace passes
// only when this test says so.
func TestEscalationFiresOnTheCallerClock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		signs := newRecSignals()
		escalate(context.Background(), clock.System(), signs, 4242, time.Minute)

		time.Sleep(59 * time.Second)
		synctest.Wait()
		if _, kills := signs.sent(); len(kills) != 0 {
			t.Fatalf("escalation fired %v before the grace passed", kills)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if _, kills := signs.sent(); len(kills) != 1 || kills[0] != 4242 {
			t.Fatalf("escalation hit %v, want the orphan group once after the grace", kills)
		}
	})
}

// TestSweepKillsARealOrphanGroup is AC5 against reality: a child spawned the
// way the runner spawns jobs survives its parent, carries PACEQ_RUN_ID in its
// environment, matches the persisted baseline, and is taken down TERM then
// KILL inside the grace, leaving an audit event behind.
func TestSweepKillsARealOrphanGroup(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process groups and /proc are linux features")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:sweep",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":1,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "nightly"})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	runID := out.Run.ID
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "doomed", TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "PACEQ_RUN_ID="+runID)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn the orphan: %v", err)
	}
	pid := cmd.Process.Pid
	// Reap as soon as it dies: an unreaped zombie would keep the group
	// lookup answering "alive" forever and the proof would lie.
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})

	ticks, ok := store.ReadProcessStartTicks(pid)
	if !ok {
		t.Fatalf("could not read the start ticks of %d", pid)
	}
	if err := s.RecordAttemptProcess(ctx, runID, "work",
		store.LeaseRef{Owner: "doomed", Epoch: 1}, pid, ticks); err != nil {
		t.Fatalf("record the baseline: %v", err)
	}

	opts := Options{Clock: clock.System(), OrphanGrace: 200 * time.Millisecond}
	if err := sweepProcesses(ctx, s, &opts); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	groupAlive := func() bool {
		return syscall.Kill(-pid, 0) == nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for groupAlive() {
		if time.Now().After(deadline) {
			t.Fatalf("orphan group %d survived TERM and KILL", pid)
		}
		time.Sleep(5 * time.Millisecond)
	}

	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	killed := false
	for _, e := range events {
		if e.Kind == "run.orphan_killed" && e.Actor == "reconcile" {
			killed = true
		}
	}
	if !killed {
		t.Errorf("no run.orphan_killed event among %+v", events)
	}
}

// TestSweepSparesARealLiveWorker is AC6's other half against reality: when
// the baseline on file does not match the running child, the sweep refuses to
// signal anything, and the child keeps living.
func TestSweepSparesARealLiveWorker(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("process groups and /proc are linux features")
	}
	ctx := context.Background()
	s, err := store.Open(ctx, t.TempDir()+"/state.db", store.Options{})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer s.Close()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:sweep",
		SpecJSON: `{"name":"nightly","schema":"paceq.job.v1","max_concurrent":1,"steps":[{"name":"work","run":["/bin/true"],"shell":false}]}`,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}
	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "nightly"})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	runID := out.Run.ID
	if _, _, err := s.ClaimRun(ctx, runID, store.LeaseInput{Owner: "holder", TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}

	cmd := exec.Command("sleep", "60")
	cmd.Env = append(os.Environ(), "PACEQ_RUN_ID="+runID)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("spawn the worker: %v", err)
	}
	pid := cmd.Process.Pid
	go func() { _ = cmd.Wait() }()
	t.Cleanup(func() {
		_ = syscall.Kill(-pid, syscall.SIGKILL)
	})

	ticks, ok := store.ReadProcessStartTicks(pid)
	if !ok {
		t.Fatalf("could not read the start ticks of %d", pid)
	}
	// The tampered baseline: off by one from what /proc really says.
	if err := s.RecordAttemptProcess(ctx, runID, "work",
		store.LeaseRef{Owner: "holder", Epoch: 1}, pid, ticks+1); err != nil {
		t.Fatalf("record the tampered baseline: %v", err)
	}

	opts := Options{Clock: clock.System(), OrphanGrace: 50 * time.Millisecond}
	if err := sweepProcesses(ctx, s, &opts); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	// Long enough that any TERM/KILL pair would have landed; short enough
	// that the sleep cannot have exited on its own.
	time.Sleep(2 * time.Second)
	if syscall.Kill(pid, 0) != nil {
		t.Fatal("the sweep killed a process whose baseline did not verify")
	}
	events, err := s.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	for _, e := range events {
		if e.Kind == "run.orphan_killed" {
			t.Errorf("a refused kill still wrote its audit row: %+v", e)
		}
	}
}
