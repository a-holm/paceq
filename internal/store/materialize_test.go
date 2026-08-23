package store_test

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/store"
)

// The canonical documents below are written out by hand because internal/store
// does not import the spec package: it stores bytes, it never compiles them.
// Each document is exactly what spec.Compile would have produced for the same
// job, which is what makes them honest fixtures for the reader side.
const (
	singleStepSpec = `{"max_concurrent":1,"name":"nightly","schema":"paceq.job.v1",` +
		`"steps":[{"name":"build","run":["/bin/true"],"shell":false}],"timeout_ms":3600000}`

	// batchSpec is singleStepSpec with a ceiling no test here binds: the run
	// lease tests claim batches of one job and exercise fencing, not
	// admission control (#68). A binding ceiling would cap every batch at
	// its first run by design.
	batchSpec = `{"max_concurrent":200,"name":"nightly","schema":"paceq.job.v1",` +
		`"steps":[{"name":"build","run":["/bin/true"],"shell":false}],"timeout_ms":3600000}`

	diamondSpec = `{"max_concurrent":2,"name":"graph","schema":"paceq.job.v1",` +
		`"steps":[` +
		`{"name":"a","needs":["b"],"run":["/bin/true"],"shell":false},` +
		`{"name":"b","run":["/bin/true"],"shell":false,"timeout_ms":60000},` +
		`{"name":"c","retry":{"backoff":"fixed","initial_ms":30000,"jitter":"none","max":2,"max_delay_ms":60000},"run":["/bin/false"],"shell":false}` +
		`],"timeout_ms":7200000}`
)

// aCanonicalJob records one job whose spec is a real v1 document, which is
// what every materialisation path reads back. It keeps the jobs.max_concurrent
// column in step with the document, exactly as apply does (#68): admission
// branches on the frozen spec while the claim joins on the column, and a
// fixture that let them drift would prove nothing honest.
func aCanonicalJob(t *testing.T, s *store.Store, name, specJSON string) store.JobVersion {
	t.Helper()

	maxConcurrent := 0
	if m := regexp.MustCompile(`"max_concurrent":(\d+)`).FindStringSubmatch(specJSON); m != nil {
		n, err := strconv.Atoi(m[1])
		if err != nil {
			t.Fatalf("read max_concurrent out of the fixture spec: %v", err)
		}
		maxConcurrent = n
	}
	version, _, err := s.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:       name,
		SpecHash:      "sha256:" + name,
		SpecJSON:      specJSON,
		MaxConcurrent: maxConcurrent,
	})
	if err != nil {
		t.Fatalf("record job %s: %v", name, err)
	}
	return version
}

func TestMaterializeManualTriggerCreatesTheWholeChain(t *testing.T) {
	ctx := context.Background()
	s, clk := coreStore(t)
	version := aCanonicalJob(t, s, "nightly", singleStepSpec)

	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName: "nightly",
		Actor:   "cli:1000",
	})
	if err != nil {
		t.Fatalf("MaterializeManualTrigger: %v", err)
	}

	if out.Run.ID == "" || out.TickID == "" || out.TriggerID == "" {
		t.Fatalf("ids not minted: %+v", out)
	}
	for _, named := range map[string]string{"tick": out.TickID, "trigger": out.TriggerID, "run": out.Run.ID} {
		if _, err := id.Parse(named); err != nil {
			t.Errorf("%s id %q is not a ULID: %v", named, named, err)
		}
	}
	if out.Run.ID == out.TickID || out.Run.ID == out.TriggerID {
		t.Error("two objects share an id")
	}

	run := out.Run
	if run.Origin != "manual" {
		t.Errorf("origin = %q, want manual", run.Origin)
	}
	if run.State != "queued" {
		t.Errorf("state = %q, want queued", run.State)
	}
	if run.JobName != "nightly" || run.JobVersionID != version.ID || run.TriggerID != out.TriggerID {
		t.Errorf("the run does not point back at what made it: %+v", run)
	}
	if run.ParamsJSON != "{}" {
		t.Errorf("params_json = %q, want the empty object", run.ParamsJSON)
	}
	if !run.AvailableAt.Equal(clk.Now()) || !run.CreatedAt.Equal(clk.Now()) {
		t.Errorf("times not stamped from the clock: available %s created %s, clock %s",
			run.AvailableAt, run.CreatedAt, clk.Now())
	}

	// The steps come out of the frozen spec, in spec order, pending.
	detail, err := s.GetRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if len(detail.Steps) != 1 {
		t.Fatalf("steps = %d, want 1", len(detail.Steps))
	}
	step := detail.Steps[0]
	if step.Name != "build" || step.Index != 0 || step.State != "pending" || step.MaxAttempts != 1 {
		t.Errorf("step decoded wrong: %+v", step)
	}

	events, err := s.RunEvents(ctx, run.ID)
	if err != nil {
		t.Fatalf("RunEvents: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("run_events rows = %d, want exactly one", len(events))
	}
	ev := events[0]
	if ev.Kind != "run.queued" || ev.ToState != "queued" || ev.FromState != "" {
		t.Errorf("queued event wrong: %+v", ev)
	}
	if ev.Actor != "cli:1000" {
		t.Errorf("actor = %q, want cli:1000", ev.Actor)
	}
	if !ev.At.Equal(clk.Now()) {
		t.Errorf("event stamped at %s, want the clock time %s", ev.At, clk.Now())
	}
}

func TestMaterializeManualTriggerFreezesStepsAndEdges(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)

	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "graph",
		SpecHash: "sha256:diamond",
		SpecJSON: diamondSpec,
	}); err != nil {
		t.Fatalf("seed job: %v", err)
	}

	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "graph"})
	if err != nil {
		t.Fatalf("MaterializeManualTrigger: %v", err)
	}

	steps, err := s.GetRun(ctx, out.Run.ID)
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	wantOrder := []string{"a", "b", "c"}
	if len(steps.Steps) != len(wantOrder) {
		t.Fatalf("steps = %d, want %d", len(steps.Steps), len(wantOrder))
	}
	for i, name := range wantOrder {
		got := steps.Steps[i]
		if got.Name != name || got.Index != i {
			t.Errorf("step %d = %s, want %s at index %d", i, got.Name, name, i)
		}
	}

	deps, err := s.StepDeps(ctx, out.Run.ID)
	if err != nil {
		t.Fatalf("StepDeps: %v", err)
	}
	if len(deps) != 1 || deps[0].StepName != "a" || deps[0].DependsOn != "b" {
		t.Errorf("frozen edges = %+v, want one edge b -> a", deps)
	}

	// retry.max + 1 attempts, frozen at materialisation.
	c := steps.Steps[2]
	if c.MaxAttempts != 3 {
		t.Errorf("step c max_attempts = %d, want 3 (retry.max + 1)", c.MaxAttempts)
	}
	b := steps.Steps[1]
	if b.MaxAttempts != 1 {
		t.Errorf("step b max_attempts = %d, want 1", b.MaxAttempts)
	}
}

func TestMaterializeManualTriggerCarriesParams(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	aCanonicalJob(t, s, "nightly", singleStepSpec)

	out, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName:    "nightly",
		Actor:      "cli:0",
		ParamsJSON: `{"rows":"42"}`,
	})
	if err != nil {
		t.Fatalf("MaterializeManualTrigger: %v", err)
	}
	if out.Run.ParamsJSON != `{"rows":"42"}` {
		t.Errorf("params_json = %q, want it carried onto the run", out.Run.ParamsJSON)
	}
}

func TestMaterializeManualTriggerRefusesAnUnknownJob(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)

	_, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "ghost"})
	if err == nil {
		t.Fatal("an unknown job was materialised")
	}
	if !strings.Contains(err.Error(), "ghost") {
		t.Errorf("error = %v, want it to name the job", err)
	}
	nf := store.ErrNotFound
	if !errors.Is(err, nf) {
		t.Errorf("error = %v, want it to wrap ErrNotFound", err)
	}
}

// A pause stops automatic firing. A person typing pulseq run has already
// decided, so a manual trigger goes through; this case pins that reading so a
// later change is a decision rather than a drift.
func TestMaterializeManualTriggerIgnoresThePauseFlag(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	aCanonicalJob(t, s, "nightly", singleStepSpec)

	if _, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "nightly"}); err != nil {
		t.Fatalf("first trigger: %v", err)
	}
	// Pause the job directly through the one writer method that exists for it.
	if err := s.SetJobPaused(ctx, "nightly", true); err != nil {
		t.Fatalf("pause: %v", err)
	}

	if _, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "nightly"}); err != nil {
		t.Errorf("manual trigger refused while paused: %v", err)
	}
}

func TestCurrentAndVersionReadersRoundTrip(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	version := aJob(t, s, "nightly")

	current, err := s.CurrentJobVersion(ctx, "nightly")
	if err != nil {
		t.Fatalf("CurrentJobVersion: %v", err)
	}
	if current.ID != version.ID || current.SpecJSON != version.SpecJSON {
		t.Errorf("current = %+v, want the version that was applied", current)
	}

	byID, err := s.JobVersionByID(ctx, version.ID)
	if err != nil {
		t.Fatalf("JobVersionByID: %v", err)
	}
	if byID.SpecHash != version.SpecHash || byID.Version != 1 {
		t.Errorf("by id = %+v", byID)
	}

	if _, err := s.JobVersionByID(ctx, "01JNOTAREALID0000000000000000"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown version error = %v, want ErrNotFound", err)
	}
	if _, err := s.CurrentJobVersion(ctx, "ghost"); !errors.Is(err, store.ErrNotFound) {
		t.Errorf("unknown job error = %v, want ErrNotFound", err)
	}
}

// SetJobPaused exists for the pause test above; give it a tiny direct proof so
// the helper is not load bearing untested.
func TestSetJobPausedFlipsTheFlag(t *testing.T) {
	ctx := context.Background()
	s, _ := coreStore(t)
	aCanonicalJob(t, s, "nightly", singleStepSpec)

	if err := s.SetJobPaused(ctx, "nightly", true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	job, err := s.JobPaused(ctx, "nightly")
	if err != nil {
		t.Fatalf("JobPaused: %v", err)
	}
	if !job {
		t.Error("job did not report paused")
	}
	if err := s.SetJobPaused(ctx, "nightly", false); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if job, _ = s.JobPaused(ctx, "nightly"); job {
		t.Error("job still reports paused")
	}
}
