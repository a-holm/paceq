package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// status is the morning view (#30): one line per job, deviations first, an
// aggregate on top, and an exit code a monitoring script branches on. These
// tests pin the contract end to end: the JSON document, the exit-code
// matrix, the daemon-down answer, and the fold past one screen.

// statusClock is one hour after every fixture stamp, so finished runs sit
// inside the 24h confirm window while never-run jobs stay quiet.
var statusClock = testOrigin.Add(time.Hour)

// runStatusCLI runs status on the frozen fixture clock.
func runStatusCLI(t *testing.T, dir string, environ map[string]string, args ...string) result {
	t.Helper()

	var stdout, stderr strings.Builder
	env := Env{
		Stdout: &stdout,
		Stderr: &stderr,
		Dir:    dir,
		Getenv: lookup(environ),
		Clk:    clock.NewFake(statusClock),
	}
	code := run(context.Background(), env, args)
	return result{code: code, stdout: stdout.String(), stderr: stderr.String()}
}

func decodeReport(t *testing.T, out string) map[string]any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("the overview is not one JSON object: %v\n%s", err, out)
	}
	return doc
}

func summaryOf(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	s, ok := doc["summary"].(map[string]any)
	if !ok {
		t.Fatalf("the report carries no summary object: %v", doc)
	}
	return s
}

func jobsOf(t *testing.T, doc map[string]any) map[string]map[string]any {
	t.Helper()
	list, ok := doc["jobs"].([]any)
	if !ok {
		t.Fatalf("the report carries no jobs array: %v", doc)
	}
	out := make(map[string]map[string]any, len(list))
	for _, raw := range list {
		job, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("a job entry is not an object: %v", raw)
		}
		name, _ := job["name"].(string)
		out[name] = job
	}
	return out
}

// TestStatusOverviewContract pins the --json document on the mixed fixture:
// nightly succeeded, import failed inside the window. schema_version 1 is
// the whole point of the field; hint rides only the failed job; the exit
// code says 5 because a job needs attention.
func TestStatusOverviewContract(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	got := runStatusCLI(t, dir, nil, "status")
	if got.code != ExitRunFailed {
		t.Fatalf("status with a fresh failure exits %d, want %d\n%s%s",
			got.code, ExitRunFailed, got.stdout, got.stderr)
	}

	doc := decodeReport(t, got.stdout)
	if v, _ := doc["schema_version"].(float64); v != 1 {
		t.Errorf("schema_version = %v, want 1", doc["schema_version"])
	}
	if _, ok := doc["generated_at"].(string); !ok {
		t.Errorf("generated_at is missing or not a string: %v", doc["generated_at"])
	}
	daemon, _ := doc["daemon"].(map[string]any)
	if daemon == nil {
		t.Fatalf("the report carries no daemon object")
	}
	if up, _ := daemon["up"].(bool); up {
		t.Errorf("no daemon was ever started but up = true: %v", daemon)
	}

	s := summaryOf(t, doc)
	for key, want := range map[string]float64{
		"jobs": 2, "deviations": 1, "failed_24h": 1, "sla_breached": 0,
	} {
		if n, _ := s[key].(float64); n != want {
			t.Errorf("summary.%s = %v, want %v", key, s[key], want)
		}
	}

	jobs := jobsOf(t, doc)
	failed := jobs["import"]
	if state, _ := failed["state"].(string); state != "failed" {
		t.Errorf("import state = %v, want failed", failed["state"])
	}
	if hint, _ := failed["hint"].(string); hint != "paceq explain job import" {
		t.Errorf("failed job hint = %v, want the runnable explain command", failed["hint"])
	}
	lastRun, _ := failed["last_run"].(map[string]any)
	if lastRun == nil {
		t.Fatalf("the failed job carries no last_run object")
	}
	if outcome, _ := lastRun["outcome"].(string); outcome != "failed" {
		t.Errorf("last_run.outcome = %v, want failed", outcome)
	}
	if _, ok := lastRun["duration_ms"]; !ok {
		t.Errorf("last_run carries no duration_ms: %v", lastRun)
	}

	healthy := jobs["nightly"]
	if hint, _ := healthy["hint"].(string); hint != "" {
		t.Errorf("healthy job grew a hint %q", hint)
	}
	if state, _ := healthy["state"].(string); state != "ok" {
		t.Errorf("nightly state = %v, want ok", healthy["state"])
	}

	// The refusal for a name nothing matches stays exit 3.
	missing := runStatusCLI(t, dir, nil, "status", "nightl")
	if missing.code != ExitNotFound {
		t.Errorf("an unknown reference exits %d, want %d", missing.code, ExitNotFound)
	}
	if !strings.Contains(missing.stderr, `nothing named "nightl"`) {
		t.Errorf("the unknown-reference refusal does not name the miss:\n%s", missing.stderr)
	}
}

// TestStatusExitCodeMatrix pins the monitoring answers one scenario at a
// time: confirmed recovery back to 0, paused-only still 0 (the product
// decision the reference doc carries), and a missing database as exit 1
// material - paceq itself could not work.
func TestStatusExitCodeMatrix(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	// Confirm the failure by succeeding after it: exit 0 again.
	s := openFixtureStoreAt(t, filepath.Join(dir, stateDirName), clock.NewFake(statusClock))
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "import",
		SpecHash: "sha256:import-v2",
		SpecJSON: `{"steps":[{"name":"load"}]}`,
	})
	if err != nil {
		t.Fatalf("re-version import: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "import", JobVersionID: version.ID, Origin: "manual",
		Steps: []store.NewStep{{Name: "load"}},
	})
	if err != nil {
		t.Fatalf("create the confirming run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	confirmRef := store.LeaseRef{Owner: "test", Epoch: 1}
	if err := s.StartStep(ctx, run.ID, "load", confirmRef); err != nil {
		t.Fatalf("start load on the confirming run: %v", err)
	}
	zero := 0
	if err := s.RecordStepOutcome(ctx, run.ID, "load", store.StepOutcome{
		Event:      "step_succeeded",
		ReasonCode: reason.STEPSucceeded,
		ExitCode:   &zero,
	}, confirmRef); err != nil {
		t.Fatalf("succeed load on the confirming run: %v", err)
	}
	if _, err := s.FinishRun(ctx, run.ID, confirmRef,
		store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
		t.Fatalf("finish the confirming run: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if got := runStatusCLI(t, dir, nil, "status"); got.code != ExitOK {
		t.Errorf("status after confirmation exits %d, want 0\n%s%s", got.code, got.stdout, got.stderr)
	}

	// A paused job with a FAILED history still exits 0: an operator's pause
	// is not a deviation.
	dir2, _ := failedOnlyFixture(t)
	s2 := openFixtureStoreAt(t, filepath.Join(dir2, stateDirName), clock.NewFake(statusClock))
	if err := s2.SetJobPaused(ctx, "lonely", true); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if err := s2.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	paused := runStatusCLI(t, dir2, nil, "status")
	if paused.code != ExitOK {
		t.Errorf("paused job drives exit %d, want 0\n%s%s", paused.code, paused.stdout, paused.stderr)
	}
	doc := decodeReport(t, paused.stdout)
	jobs := jobsOf(t, doc)
	if state, _ := jobs["lonely"]["state"].(string); state != "paused" {
		t.Errorf("paused job state = %v, want paused", jobs["lonely"]["state"])
	}

	// The same holds when the operator names the paused job directly:
	// the block says paused and the exit stays 0.
	pausedRef := runStatusCLI(t, dir2, nil, "status", "job/lonely")
	if pausedRef.code != ExitOK {
		t.Errorf("status of a paused job by reference exits %d, want 0\n%s%s",
			pausedRef.code, pausedRef.stdout, pausedRef.stderr)
	}
	refDoc := decodeReport(t, pausedRef.stdout)
	if state, _ := refDoc["state"].(string); state != "paused" {
		t.Errorf("paused reference state = %v, want paused", refDoc["state"])
	}
	if hint, _ := refDoc["hint"].(string); hint != "" {
		t.Errorf("a paused job grew a hint %q", hint)
	}

	// paceq itself failing is exit 1 material: a database that exists but
	// cannot be opened is paceq's failure to report, not a job's.
	if err := os.WriteFile(filepath.Join(dir2, stateDirName, store.DatabaseFileName),
		[]byte("this is not a database"), 0o600); err != nil {
		t.Fatalf("corrupt the database: %v", err)
	}
	corrupt := runCLIContext(t, context.Background(), dir2, map[string]string{}, "status")
	if corrupt.code != ExitInternal {
		t.Errorf("status on a corrupt database exits %d, want 1\n%s", corrupt.code, corrupt.stderr)
	}
}

func TestStatusTextOverviewMarksDaemonDownAndHints(t *testing.T) {
	dir, _ := finishedRunsFixture(t)

	got := runStatusCLI(t, dir, nil, "status", "-o", "text")
	if got.code != ExitRunFailed {
		t.Fatalf("status text exits %d, want %d", got.code, ExitRunFailed)
	}
	for _, want := range []string{"daemon down", "1 deviations", "import"} {
		if !strings.Contains(got.stdout, want) {
			t.Errorf("the text overview lost %q:\n%s", want, got.stdout)
		}
	}
	if !strings.Contains(got.stdout, "paceq explain job import") {
		t.Errorf("the deviation carries no runnable hint:\n%s", got.stdout)
	}
	// Deviations sort first.
	if strings.Index(got.stdout, "import") > strings.Index(got.stdout, "nightly") {
		t.Errorf("deviation did not sort first:\n%s", got.stdout)
	}

	// -q keeps exactly what needs attention.
	quiet := runStatusCLI(t, dir, nil, "status", "-q", "-o", "text")
	if quiet.code != ExitRunFailed {
		t.Fatalf("quiet status exits %d, want %d", quiet.code, ExitRunFailed)
	}
	if strings.Contains(quiet.stdout, "nightly") {
		t.Errorf("-q kept the healthy row:\n%s", quiet.stdout)
	}
	if !strings.Contains(quiet.stdout, "import") {
		t.Errorf("-q dropped the deviation:\n%s", quiet.stdout)
	}
}

// TestStatusRefBlocks pins the four reference kinds end to end: each resolves,
// each block answers in JSON, and the run block names its story.
func TestStatusRefBlocks(t *testing.T) {
	dir, newestRun := finishedRunsFixture(t)

	// job
	job := runStatusCLI(t, dir, nil, "status", "job/nightly")
	if job.code != ExitOK {
		t.Fatalf("status job/nightly exits %d\n%s%s", job.code, job.stdout, job.stderr)
	}
	doc := decodeReport(t, job.stdout)
	subject, _ := doc["subject"].(map[string]any)
	if subject == nil || subject["kind"] != "job" || subject["job"] != "nightly" {
		t.Errorf("subject = %v, want job/nightly", subject)
	}
	if state, _ := doc["state"].(string); state != "ok" {
		t.Errorf("job block state = %v, want ok", doc["state"])
	}

	// bare name resolves like explain does
	bare := runStatusCLI(t, dir, nil, "status", "nightly")
	if bare.code != ExitOK {
		t.Errorf("bare job name exits %d\n%s%s", bare.code, bare.stdout, bare.stderr)
	}

	// schedule
	schedDir := t.TempDir()
	s := openFixtureStoreAt(t, filepath.Join(schedDir, stateDirName), clock.NewFake(statusClock))
	ctx := context.Background()
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "nightly",
		SpecHash: "sha256:nightly",
		SpecJSON: `{"steps":[{"name":"collect","run":["/bin/true"]}]}`,
	}); err != nil {
		t.Fatalf("record job: %v", err)
	}
	next := statusClock.Add(time.Hour).Truncate(time.Minute)
	if _, err := s.UpsertSchedule(ctx, store.ScheduleInput{
		JobName: "nightly", Name: "nightly", Kind: "cron", Expr: "0 2 * * *",
		Timezone: "UTC", NextTickAt: next,
	}); err != nil {
		t.Fatalf("plant schedule: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	sched := runStatusCLI(t, schedDir, nil, "status", "schedule/nightly.nightly")
	if sched.code != ExitOK {
		t.Fatalf("status schedule ref exits %d\n%s%s", sched.code, sched.stdout, sched.stderr)
	}
	sdoc := decodeReport(t, sched.stdout)
	sfacts, _ := sdoc["schedule"].(map[string]any)
	if sfacts == nil || sfacts["expr"] != "0 2 * * *" {
		t.Errorf("schedule facts = %v, want the cron row", sfacts)
	}

	// sensor
	sensor := runStatusCLI(t, sensorFixtureDir(t), nil, "status", "sensor/dropzone")
	if sensor.code != ExitOK {
		t.Fatalf("status sensor ref exits %d\n%s%s", sensor.code, sensor.stdout, sensor.stderr)
	}
	endoc := decodeReport(t, sensor.stdout)
	sensorFacts, _ := endoc["sensor"].(map[string]any)
	if sensorFacts == nil {
		t.Fatalf("sensor block carries no sensor object: %v", endoc)
	}

	// run, by whole id like explain resolves it
	runRef := runStatusCLI(t, dir, nil, "status", "run/"+newestRun)
	if runRef.code != ExitOK {
		t.Fatalf("status run ref exits %d\n%s%s", runRef.code, runRef.stdout, runRef.stderr)
	}
	rdoc := decodeReport(t, runRef.stdout)
	rfacts, _ := rdoc["run"].(map[string]any)
	if rfacts == nil || rfacts["id"] != newestRun {
		t.Errorf("run facts = %v, want the whole id %s", rfacts, newestRun)
	}
	if state, _ := rfacts["state"].(string); state != "succeeded" {
		t.Errorf("run state = %v, want succeeded", rfacts["state"])
	}

	// a failed run reference is a deviation: exit 5
	failedDir, failedRun := failedOnlyFixture(t)
	failed := runStatusCLI(t, failedDir, nil, "status", "run/"+failedRun)
	if failed.code != ExitRunFailed {
		t.Errorf("a failed run reference exits %d, want %d\n%s", failed.code, ExitRunFailed, failed.stderr)
	}
}

func TestStatusFoldKeepsDeviationsVisible(t *testing.T) {
	dir := t.TempDir()
	s := openFixtureStoreAt(t, filepath.Join(dir, stateDirName), clock.NewFake(statusClock))
	ctx := context.Background()

	failedVersion, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "aaa-broken",
		SpecHash: "sha256:aaa-broken",
		SpecJSON: `{"steps":[{"name":"work"}]}`,
	})
	if err != nil {
		t.Fatalf("record the broken job: %v", err)
	}
	bad, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "aaa-broken", JobVersionID: failedVersion.ID, Origin: "manual",
		Steps: []store.NewStep{{Name: "work"}},
	})
	if err != nil {
		t.Fatalf("create its run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, bad.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	one := 1
	ref := store.LeaseRef{Owner: "test", Epoch: 1}
	if err := s.StartStep(ctx, bad.ID, "work", ref); err != nil {
		t.Fatalf("start work: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, bad.ID, "work", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   &one,
	}, ref); err != nil {
		t.Fatalf("fail work: %v", err)
	}
	if _, err := s.FinishRun(ctx, bad.ID, ref, store.FinishReason{Code: reason.RUNFailedStep}); err != nil {
		t.Fatalf("finish: %v", err)
	}

	for i := 0; i < 60; i++ {
		name := fmt.Sprintf("filler-%03d", i)
		if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
			JobName:  name,
			SpecHash: "sha256:" + name,
			SpecJSON: `{"steps":[{"name":"work"}]}`,
		}); err != nil {
			t.Fatalf("record %s: %v", name, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	got := runStatusCLI(t, dir, nil, "status", "-o", "text")
	if got.code != ExitRunFailed {
		t.Fatalf("61 jobs, one failed, exit %d\n%s", got.code, got.stderr)
	}
	if !strings.Contains(got.stdout, "and 21 more (paceq status --all)") {
		t.Errorf("the fold line is wrong:\n%.700s", got.stdout)
	}
	if bb, fb := strings.Index(got.stdout, "aaa-broken"), strings.Index(got.stdout, "and 21 more"); bb < 0 || bb > fb {
		t.Errorf("the folded view lost the deviation off the top:\n%.700s", got.stdout)
	}

	all := runStatusCLI(t, dir, nil, "status", "-o", "text", "--all")
	if all.code != ExitRunFailed {
		t.Fatalf("--all exits %d", all.code)
	}
	if strings.Contains(all.stdout, "more (paceq status") {
		t.Errorf("--all still folds:\n%.400s", all.stdout)
	}
	if !strings.Contains(all.stdout, "filler-059") {
		t.Errorf("--all lost the last filler job")
	}
}

// failedOnlyFixture seeds a project with one job whose newest finished run
// failed inside the window.
func failedOnlyFixture(t *testing.T) (string, string) {
	t.Helper()

	dir := t.TempDir()
	s := openFixtureStoreAt(t, filepath.Join(dir, stateDirName), clock.NewFake(testOrigin))
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "lonely",
		SpecHash: "sha256:lonely",
		SpecJSON: `{"steps":[{"name":"work"}]}`,
	})
	if err != nil {
		t.Fatalf("record lonely: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "lonely", JobVersionID: version.ID, Origin: "manual",
		Steps: []store.NewStep{{Name: "work"}},
	})
	if err != nil {
		t.Fatalf("create the run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	one := 1
	ref := store.LeaseRef{Owner: "test", Epoch: 1}
	if err := s.StartStep(ctx, run.ID, "work", ref); err != nil {
		t.Fatalf("start work: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, run.ID, "work", store.StepOutcome{
		Event:      "step_failed",
		ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode:   &one,
	}, ref); err != nil {
		t.Fatalf("fail work: %v", err)
	}
	if _, err := s.FinishRun(ctx, run.ID, ref, store.FinishReason{Code: reason.RUNFailedStep}); err != nil {
		t.Fatalf("finish the run: %v", err)
	}
	return dir, run.ID
}

func sensorFixtureDir(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	s := openFixtureStoreAt(t, filepath.Join(dir, stateDirName), clock.NewFake(statusClock))
	defer func() { _ = s.Close() }()

	ctx := context.Background()
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "polling-job",
		SpecHash: "sha256:polling-job",
		SpecJSON: `{"steps":[{"name":"process"}]}`,
	}); err != nil {
		t.Fatalf("record job: %v", err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: "dropzone", JobName: "polling-job",
		ExecJSON: `["/bin/echo","{}"]`,
	}); err != nil {
		t.Fatalf("plant sensor: %v", err)
	}
	return dir
}
