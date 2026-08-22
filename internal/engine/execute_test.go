package engine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/logsink"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The end to end proofs. Every command a job runs here is the fakecmd
// fixture, the same one the runner tests use, so what executes is a real
// process group on this kernel and not a stub.

var (
	fakeCmdOnce sync.Once
	fakeCmdPath string
)

// fakecmd builds the runner package's fixture once per test binary.
func fakecmd(t *testing.T) string {
	t.Helper()

	fakeCmdOnce.Do(func() {
		root := moduleRoot(t)
		dir, err := os.MkdirTemp("", "paceq-engine-fakecmd-")
		if err != nil {
			t.Fatalf("tempdir for fakecmd: %v", err)
		}
		path := filepath.Join(dir, "fakecmd")
		build := exec.Command("go", "build", "-o", path, "./testdata/fakecmd")
		build.Dir = filepath.Join(root, "internal", "runner")
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build fakecmd: %v\n%s", err, out)
		}
		fakeCmdPath = path
	})
	if fakeCmdPath == "" {
		t.Fatal("fakecmd was not built")
	}
	return fakeCmdPath
}

func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the module root: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the engine package")
		}
		dir = parent
	}
}

// engineFixture is one migrated state directory with its log root and an
// engine wired to both. The clock is fake, which makes ids share a prefix and
// every stamp a stated time; the poll interval is short, because a real
// process has to be watching while the test pulls the trigger.
type engineFixture struct {
	Dir      string
	Store    *store.Store
	Clock    clock.Clock
	LogRoot  logsink.Root
	Engine   *engine.Engine
	fakeCmdA string
}

func newFixture(t *testing.T) *engineFixture {
	return newFixtureWithClock(t, clock.NewFake(time.Date(2026, 9, 17, 3, 0, 0, 0, time.UTC)))
}

// newFixtureWithClock wires one state directory, log root and engine to the
// given clock. The fake serves tests that steer time by hand; the system
// clock serves the retry tests, which run inside a testing/synctest bubble
// where timers and Now are virtual together.
func newFixtureWithClock(t *testing.T, clk clock.Clock) *engineFixture {
	t.Helper()

	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	s, err := store.Open(context.Background(), filepath.Join(stateDir, "state.db"),
		store.Options{Clock: clk})
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
	return &engineFixture{
		Dir:     dir,
		Store:   s,
		Clock:   clk,
		LogRoot: logsink.NewRoot(stateDir),
		Engine: &engine.Engine{
			Store:        s,
			LogRoot:      logsink.NewRoot(stateDir),
			Clock:        clk,
			Owner:        "exec-1",
			PollInterval: 10 * time.Millisecond,
		},
	}
}

// aQueuedRun seeds a job whose steps run fakecmd with the given arguments,
// one per name, plus any needs edges, then materialises a manual run of it.
// Every spec here is a real canonical document, because the engine decodes
// the frozen bytes through spec.FromIR.
func (f *engineFixture) aQueuedRun(t *testing.T, steps []string, args []string, needs map[string]string,
	timeoutMS int64,
) string {
	t.Helper()

	var encoded []string
	for i, name := range steps {
		// Fields splits the shorthand ("exit 7") into argv.
		parts := strings.Fields(args[i])
		quoted := make([]string, len(parts)+1)
		quoted[0] = strconv.Quote(f.fakeCmd(t))
		for j, p := range parts {
			quoted[j+1] = strconv.Quote(p)
		}
		run := strings.Join(quoted, ",")
		step := fmt.Sprintf(`{"name":%q,"run":[%s],"shell":false}`, name, run)
		if needs[name] != "" {
			step = fmt.Sprintf(`{"name":%q,"needs":[%q],"run":[%s],"shell":false}`,
				name, needs[name], run)
		}
		encoded = append(encoded, step)
	}
	spec := fmt.Sprintf(`{"max_concurrent":1,"name":"e2e","schema":"paceq.job.v1",
"steps":[%s],"timeout_ms":%d}`, strings.Join(encoded, ","), timeoutMS)

	if _, _, err := f.Store.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "e2e",
		SpecHash: "sha256:e2e-" + strings.Join(steps, "-"),
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	out, err := f.Store.MaterializeManualTrigger(context.Background(), store.ManualTriggerInput{
		JobName: "e2e",
		Actor:   "cli:1000",
	})
	if err != nil {
		t.Fatalf("materialise the run: %v", err)
	}
	return out.Run.ID
}

func (f *engineFixture) fakeCmd(t *testing.T) string {
	t.Helper()

	if f.fakeCmdA == "" {
		f.fakeCmdA = fakecmd(t)
	}
	return f.fakeCmdA
}

// mustFinish drives ExecuteRun to completion or fails the test.
func (f *engineFixture) mustFinish(t *testing.T, runID string) string {
	t.Helper()

	state, err := f.Engine.ExecuteRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("execute run %s: %v", runID, err)
	}
	return state
}

func TestExecuteRunCarriesThreeStepsToEndToEnd(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	// Step two speaks, so its log carries bytes; the other two are silent,
	// which is itself worth asserting.
	runID := f.aQueuedRun(t, []string{"one", "two", "three"},
		[]string{"exit 0", "env-dump", "exit 0"}, nil, 60000)

	if state := f.mustFinish(t, runID); state != "succeeded" {
		t.Fatalf("run ended %s, want succeeded", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if len(detail.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(detail.Steps))
	}
	for _, step := range detail.Steps {
		if step.State != "succeeded" {
			t.Errorf("step %s = %s, want succeeded", step.Name, step.State)
		}
		if !step.HasExitCode || step.ExitCode != 0 {
			t.Errorf("step %s exit code = %d/%v, want a stored zero",
				step.Name, step.ExitCode, step.HasExitCode)
		}
		if step.LogPath == "" {
			t.Errorf("step %s has no log path recorded", step.Name)
		}
	}
	if detail.Steps[1].LogBytes == 0 {
		t.Error("the speaking step's log recorded no bytes")
	}
	for _, i := range []int{0, 2} {
		if detail.Steps[i].LogBytes != 0 {
			t.Errorf("the silent step %s recorded %d log bytes",
				detail.Steps[i].Name, detail.Steps[i].LogBytes)
		}
	}

	events, err := f.Store.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	want := []struct{ step, kind, from, to string }{
		{"", "run.queued", "", "queued"},
		{"", "run.started", "queued", "running"},
		{"one", "step.started", "pending", "running"},
		{"one", "step.succeeded", "running", "succeeded"},
		{"two", "step.started", "pending", "running"},
		{"two", "step.succeeded", "running", "succeeded"},
		{"three", "step.started", "pending", "running"},
		{"three", "step.succeeded", "running", "succeeded"},
		{"", "run.succeeded", "running", "succeeded"},
	}
	if len(events) != len(want) {
		t.Fatalf("event rows = %d, want exactly the %d transitions:",
			len(events), len(want))
		for i, e := range events {
			t.Logf("  [%d] %s %s", i, e.Kind, e.ToState)
		}
		return
	}
	for i, w := range want {
		got := events[i]
		if got.StepName != w.step || got.Kind != w.kind ||
			got.FromState != w.from || got.ToState != w.to {
			t.Errorf("event[%d] = (%q %s %s->%s), want (%q %s %s->%s)",
				i, got.StepName, got.Kind, got.FromState, got.ToState,
				w.step, w.kind, w.from, w.to)
		}
	}

	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations after a clean run: %+v", violations)
	}
}

func TestExecuteRunAFailedStepSkipsWhatNeededIt(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"first", "second", "third"},
		[]string{"exit 0", "exit 7", "exit 0"},
		map[string]string{"third": "second"}, 60000)

	if state := f.mustFinish(t, runID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.ReasonCode != string(reason.RUNFailedStep) {
		t.Errorf("reason_code = %q, want %q", detail.ReasonCode, reason.RUNFailedStep)
	}
	var data map[string]any
	if err := json.Unmarshal([]byte(detail.ReasonData), &data); err != nil {
		t.Fatalf("reason_data is not an object: %q", detail.ReasonData)
	}
	if data["step"] != "second" {
		t.Errorf("reason_data.step = %v, want second", data["step"])
	}

	states := map[string]string{}
	for _, step := range detail.Steps {
		states[step.Name] = step.State
	}
	if states["second"] != "failed" || states["third"] != "skipped" {
		t.Errorf("second = %s, third = %s, want failed and skipped",
			states["second"], states["third"])
	}
	third := detail.Steps[2]
	if third.ReasonCode != string(reason.STEPSkippedUpstreamFailed) {
		t.Errorf("third reason = %q, want %q", third.ReasonCode, reason.STEPSkippedUpstreamFailed)
	}

	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations after a failure run: %+v", violations)
	}
}

// runArgv renders a run array for a fixture spec: the fakecmd path plus the
// words of the shorthand argument, each quoted.
func runArgv(fakeCmdPath, shorthand string) string {
	parts := strings.Fields(shorthand)
	quoted := make([]string, len(parts)+1)
	quoted[0] = strconv.Quote(fakeCmdPath)
	for i, p := range parts {
		quoted[i+1] = strconv.Quote(p)
	}
	return strings.Join(quoted, ",")
}

func TestExecuteRunAStepTimeoutKillsAndRecordsItself(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	if _, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "e2e-short",
		SpecHash: "sha256:e2e-short",
		SpecJSON: fmt.Sprintf(`{"name":"e2e-short","schema":"paceq.job.v1",
"steps":[{"name":"hangs","run":[%s],"shell":false,"timeout_ms":150}],
"timeout_ms":60000}`, runArgv(f.fakeCmd(t), "sleep 30s")),
	}); err != nil {
		t.Fatalf("record the short job: %v", err)
	}
	out, err := f.Store.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "e2e-short"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	runID := out.Run.ID

	started := time.Now()
	if state := f.mustFinish(t, runID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Errorf("the timeout took %s to bite", elapsed)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	step := detail.Steps[0]
	if step.State != "failed" || step.ReasonCode != string(reason.STEPFailedTimeout) {
		t.Errorf("step = %s/%s, want failed/%s",
			step.State, step.ReasonCode, reason.STEPFailedTimeout)
	}
	if detail.ReasonCode != string(reason.RUNFailedStep) {
		t.Errorf("a step timeout fails the run as %q, want %q",
			detail.ReasonCode, reason.RUNFailedStep)
	}
	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations after a timeout: %+v", violations)
	}
}

func TestExecuteRunARunTimeoutStopsSchedulingWork(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	if _, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "slowjob",
		SpecHash: "sha256:slowjob",
		SpecJSON: fmt.Sprintf(`{"name":"slowjob","schema":"paceq.job.v1",
"steps":[{"name":"hangs","run":[%s],"shell":false}],
"timeout_ms":250}`, runArgv(f.fakeCmd(t), "sleep 30s")),
	}); err != nil {
		t.Fatalf("record the slow job: %v", err)
	}
	out, err := f.Store.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "slowjob"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}

	if state := f.mustFinish(t, out.Run.ID); state != "failed" {
		t.Fatalf("run ended %s, want failed", state)
	}

	detail, err := f.Store.GetRun(ctx, out.Run.ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.ReasonCode != string(reason.RUNTimedOut) {
		t.Errorf("reason_code = %q, want %q", detail.ReasonCode, reason.RUNTimedOut)
	}
	step := detail.Steps[0]
	if step.State != "failed" || step.ReasonCode != string(reason.STEPFailedTimeout) {
		t.Errorf("the running step = %s/%s, want failed/%s: the run timeout"+
			" must terminate the step that was in flight",
			step.State, step.ReasonCode, reason.STEPFailedTimeout)
	}
	var scope map[string]any
	if err := json.Unmarshal([]byte(step.ReasonData), &scope); err != nil {
		t.Fatalf("step reason_data is not an object: %q", step.ReasonData)
	}
	if scope["scope"] != "run" {
		t.Errorf("the kill is attributed to %v, want the run budget", scope["scope"])
	}
	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations after a run timeout: %+v", violations)
	}
}

func TestExecuteRunACancelRequestedBeforeTheClaimCancelsCleanly(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"never"}, []string{"exit 0"}, nil, 60000)

	if _, err := f.Store.RequestCancel(ctx, runID, "cli:1000", "changed my mind"); err != nil {
		t.Fatalf("request cancel: %v", err)
	}
	if state := f.mustFinish(t, runID); state != "cancelled" {
		t.Fatalf("run ended %s, want cancelled", state)
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.StartedAt.Equal(detail.CreatedAt) && detail.State != "cancelled" {
		t.Errorf("unexpected run state %+v", detail.Run)
	}
	step := detail.Steps[0]
	if step.State != "skipped" {
		t.Errorf("the never started step = %s, want skipped", step.State)
	}
	events, err := f.Store.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "run.cancelled" || last.Actor != "cli:1000" {
		t.Errorf("last event = %s by %q, want run.cancelled by cli:1000",
			last.Kind, last.Actor)
	}
}

func TestExecuteRunACancelMidRunKillsTheGroupWithinThePollWindow(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	runID := f.aQueuedRun(t, []string{"hangs"}, []string{"sleep 60s"}, nil, 120000)

	done := make(chan error, 1)
	go func() {
		_, err := f.Engine.ExecuteRun(context.Background(), runID)
		done <- err
	}()

	// Wait until the step is genuinely running before pulling the trigger,
	// so the proof covers the mid-run window and not the between-steps one.
	waitForState(t, ctx, f.Store, runID, func(d store.RunDetail) bool {
		return d.Steps[0].State == "running"
	}, 5*time.Second)

	if _, err := f.Store.RequestCancel(ctx, runID, "cli:4242", "too slow"); err != nil {
		t.Fatalf("request cancel: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the run did not observe the cancellation within the poll window")
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.State != "cancelled" {
		t.Errorf("run = %s, want cancelled, not %s", detail.State, detail.ReasonCode)
	}
	step := detail.Steps[0]
	if step.State != "cancelled" || step.ReasonCode != string(reason.STEPCancelled) {
		t.Errorf("step = %s/%s, want cancelled/%s",
			step.State, step.ReasonCode, reason.STEPCancelled)
	}
	events, err := f.Store.RunEvents(ctx, runID)
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	last := events[len(events)-1]
	if last.Kind != "run.cancelled" || last.Actor != "cli:4242" {
		t.Errorf("last event = %s by %q, want run.cancelled by cli:4242",
			last.Kind, last.Actor)
	}
	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations after a mid-run cancel: %+v", violations)
	}
}

// The run was frozen when it was materialised. Applying a new version while
// it runs changes nothing about what the remaining steps do.
func TestExecuteRunRunsOnTheFrozenSpecWhenTheJobChangesUnderneath(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)

	oldSpec := fmt.Sprintf(`{"name":"frozen","schema":"paceq.job.v1","steps":[
{"name":"wait","run":[%s],"shell":false},
{"name":"after","run":[%s],"shell":false}],"timeout_ms":60000}`,
		runArgv(f.fakeCmd(t), "sleep 800ms"), runArgv(f.fakeCmd(t), "exit 0"))
	version, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "frozen",
		SpecHash: "sha256:frozen-v1",
		SpecJSON: oldSpec,
	})
	if err != nil {
		t.Fatalf("record v1: %v", err)
	}
	out, err := f.Store.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "frozen"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	runID := out.Run.ID

	newSpec := strings.Replace(oldSpec, `"exit 0"`, `"exit 9"`, 1)
	if _, _, err := f.Store.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "frozen",
		SpecHash: "sha256:frozen-v2",
		SpecJSON: newSpec,
	}); err != nil {
		t.Fatalf("record v2: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := f.Engine.ExecuteRun(context.Background(), runID)
		done <- err
	}()
	waitForState(t, ctx, f.Store, runID, func(d store.RunDetail) bool {
		return d.Steps[0].State == "running"
	}, 5*time.Second)

	// The apply lands while the first step is still running: the re-upsert
	// above has by now moved the job's current version pointer to v2.
	// Nothing else may change under the run.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("execute: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the run did not finish")
	}

	detail, err := f.Store.GetRun(ctx, runID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if detail.JobVersionID != version.ID {
		t.Errorf("the run followed the new version: %s, want %s", detail.JobVersionID, version.ID)
	}
	if state := detail.Steps[1]; state.State != "succeeded" {
		t.Errorf("the second step = %s, want succeeded: under the new spec it"+
			" would have exited 9, so this proves the old argv ran", state.State)
	}
}

// A hundred runs of three steps each, back to back, on one executor: the
// write lock is taken and released hundreds of times, and SQLITE_BUSY would
// surface here as an error from any call. The wall clock total is reported so
// a regression shows up as a number, not as a flake.
func TestHundredRunsOfThreeStepsCompleteWithoutBusy(t *testing.T) {
	if testing.Short() {
		t.Skip("the throughput sweep is the long pole in -short")
	}
	ctx := context.Background()
	f := newFixture(t)

	started := time.Now()
	busiest := time.Duration(0)
	for i := 0; i < 100; i++ {
		runID := f.aQueuedRun(t, []string{"a", "b", "c"},
			[]string{"exit 0", "exit 0", "exit 0"}, nil, 60000)
		before := time.Now()
		if state := f.mustFinish(t, runID); state != "succeeded" {
			t.Fatalf("run %d ended %s", i, state)
		}
		if spent := time.Since(before); spent > busiest {
			busiest = spent
		}
	}
	total := time.Since(started)
	t.Logf("100 runs x 3 steps: total %s, slowest run %s", total, busiest)

	violations, err := f.Store.Fsck(ctx)
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found violations across the sweep: %+v", violations[:min(len(violations), 5)])
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// waitForState polls until the predicate holds, failing at the deadline. It
// exists so tests can synchronise on durable facts instead of sleeping.
func waitForState(t *testing.T, ctx context.Context, s *store.Store, runID string,
	until func(store.RunDetail) bool, within time.Duration,
) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		detail, err := s.GetRun(ctx, runID)
		if err == nil && until(detail) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach the awaited state within %s", runID, within)
}
