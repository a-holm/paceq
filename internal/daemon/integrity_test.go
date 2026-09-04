package daemon

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// The integrity surface (M6-06): the hourly sweep records that it ran and
// what it found, and the health line moves with it.

type fsckStubSweeper struct {
	violations []store.Violation
	sweeps     int
	recorded   [][]store.IntegrityFinding
	stamps     []time.Time
}

func (s *fsckStubSweeper) Fsck(context.Context) ([]store.Violation, error) {
	s.sweeps++
	return s.violations, nil
}

func (s *fsckStubSweeper) RecordIntegritySweep(_ context.Context, at time.Time, findings []store.IntegrityFinding) error {
	s.stamps = append(s.stamps, at)
	s.recorded = append(s.recorded, findings)
	return nil
}

func TestHourlySweepRecordsFindingsOncePerHour(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))
	sts := newStatuses(func() time.Time { return clk.Now() })
	d := testLoops(t, logger, notify.New(), sts, clk)

	sweeper := &fsckStubSweeper{violations: []store.Violation{
		{Check: "I1", Subject: "run 01A", Detail: "no live lease"},
		{Check: "I1", Subject: "run 01B", Detail: "no live lease"},
		{Check: "reason", Subject: "run 01C", Detail: "no code"},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// The maintenance body with a nil janitor: the fsck sweep is the
		// only duty the cadence test needs. The nightly slot at hour 3 is
		// never owed at 08:00.
		_ = janitorLoop(ctx, d, 5*time.Millisecond, nil, 3, sweeper)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	if sweeper.sweeps != 1 {
		t.Fatalf("the sweep ran %d times inside one hour, want exactly 1", sweeper.sweeps)
	}
	if len(sweeper.recorded) != 1 {
		t.Fatalf("the sweep recorded %d times, want 1", len(sweeper.recorded))
	}
	findings := sweeper.recorded[0]
	if len(findings) != 2 {
		t.Fatalf("the sweep recorded %d findings, want one per broken invariant", len(findings))
	}
	if findings[0].Invariant != "I1" || findings[0].Violations != 2 || findings[0].Severity != store.Serious {
		t.Errorf("the I1 finding reads %+v", findings[0])
	}
	if got := len(findings[0].Subjects); got != 2 {
		t.Errorf("the I1 finding names %d subjects, want 2", got)
	}
	if findings[1].Invariant != "reason" || findings[1].Severity != store.Warning {
		t.Errorf("the reason finding reads %+v", findings[1])
	}

	// The health surface answers for the sweep: a /livez reader can see the
	// last time the invariants were checked.
	found := false
	for _, line := range sts.snapshot() {
		if line.Name == "fsck" {
			found = true
		}
	}
	if !found {
		t.Error("the health surface has no fsck line")
	}
}

// TestACleanHourSweepsOnceAndRecordsItOnce keeps the two promises a clean
// hour still owes. The cadence does not move with the state: one sweep an
// hour, however often the loop wakes. Neither does the write: one record per
// sweep, carrying no finding, so a clean hour costs one stamp and leaves the
// findings table alone.
func TestACleanHourSweepsOnceAndRecordsItOnce(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))
	sts := newStatuses(func() time.Time { return clk.Now() })
	d := testLoops(t, logger, notify.New(), sts, clk)

	sweeper := &fsckStubSweeper{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = janitorLoop(ctx, d, 5*time.Millisecond, nil, 3, sweeper)
	}()
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	if sweeper.sweeps != 1 {
		t.Fatalf("a clean hour swept %d times, want exactly 1", sweeper.sweeps)
	}
	if len(sweeper.recorded) != 1 {
		t.Fatalf("a clean hour recorded %d sweeps, want exactly 1: an unrecorded sweep leaves the gauges unable to tell a sound database from one nobody has looked at",
			len(sweeper.recorded))
	}
	if len(sweeper.recorded[0]) != 0 {
		t.Errorf("a clean sweep carried findings into the event log: %+v", sweeper.recorded[0])
	}
}

func TestSweepFailureSurfacesTheError(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))
	sts := newStatuses(func() time.Time { return clk.Now() })
	d := testLoops(t, logger, notify.New(), sts, clk)

	// A sweep that cannot read is a loop error, the same as every other
	// loop's: the daemon's supervision decides what a dead loop means. The
	// one thing it must never do is report a finding it did not record.
	err := sweepIntegrity(context.Background(), d, &erroringSweeper{})
	if err == nil || !strings.Contains(err.Error(), "database is busy") {
		t.Fatalf("the sweep error vanished: %v", err)
	}
}

type erroringSweeper struct{}

func (f *erroringSweeper) Fsck(context.Context) ([]store.Violation, error) {
	return nil, errors.New("database is busy")
}

func (f *erroringSweeper) RecordIntegritySweep(context.Context, time.Time, []store.IntegrityFinding) error {
	return nil
}

func TestStartupRefusalNamesTheCodeAndTheWayOut(t *testing.T) {
	err := startupRefusal("I6 (tick slot bulk@17: one evaluation slot holds more than one tick)")
	text := err.Error()
	for _, want := range []string{"PSQ-FSCK-001", "fsck --json", "--repair --confirm", "backup"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal never says %q:\n%s", want, text)
		}
	}
}

func TestStartupSweepRecordsFindingsAndRefusesCriticals(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 2, 0, 0, time.UTC))

	if got := firstCriticalSummary([]store.Violation{
		{Check: "I1", Severity: store.Serious},
		{Check: "I6", Severity: store.Critical, Subject: "tick slot x@1", Detail: "two ticks"},
	}); got == "" || !strings.HasPrefix(got, "I6") {
		t.Fatalf("the critical summary is %q", got)
	}
	if got := firstCriticalSummary([]store.Violation{
		{Check: "I1", Severity: store.Serious},
	}); got != "" {
		t.Fatalf("a serious finding read as critical: %q", got)
	}

	// The findings land in the log through the store, exactly as the hourly
	// sweep records them.
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	st, err := store.OpenState(context.Background(), dir, store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the state: %v", err)
	}
	defer func() { _ = st.Close() }()
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	violations := []store.Violation{
		{Check: "I1", Subject: "run 01A", Detail: "no live lease"},
		{Check: "I1", Subject: "run 01B", Detail: "no live lease"},
	}
	if err := recordStartupSweep(context.Background(), st, clk, logger, violations); err != nil {
		t.Fatalf("record the startup findings: %v", err)
	}
	read, err := st.MetricsIntegrityViolations(context.Background())
	if err != nil || len(read) != 1 || read[0].Invariant != "I1" || read[0].Violations != 2 {
		t.Fatalf("the event log holds %+v (err=%v)", read, err)
	}
	dirty, ok, err := st.MetricsFsckLastRun(context.Background())
	if err != nil || !ok {
		t.Fatalf("the startup findings left no sweep stamp: ok=%v err=%v", ok, err)
	}

	// A clean startup records that it ran and retires the repaired finding.
	if err := recordStartupSweep(context.Background(), st, clk, logger, nil); err != nil {
		t.Fatalf("record a clean startup: %v", err)
	}
	read2, err := st.MetricsIntegrityViolations(context.Background())
	if err != nil || len(read2) != 0 {
		t.Fatalf("a clean startup still reports the repaired invariant: %+v (err=%v)", read2, err)
	}
	clean, ok, err := st.MetricsFsckLastRun(context.Background())
	if err != nil || !ok {
		t.Fatalf("the clean startup left no sweep stamp: ok=%v err=%v", ok, err)
	}
	if !clean.After(dirty) {
		t.Errorf("the clean startup stamp %s does not move past the dirty one %s", clean, dirty)
	}
}

// TestServeRefusesAStateWithCriticalFindings is the R11 acceptance at the
// process level: a database whose uniqueness rule is already broken must not
// be served, and the refusal must carry the code, the evidence step and the
// confirm requirement.
func TestServeRefusesAStateWithCriticalFindings(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		StateDir: filepath.Join(root, "state"),
		Version:  "test",
		JobsDir:  "jobs",
		Logger:   slog.New(slog.NewTextHandler(&strings.Builder{}, nil)),
		Owner:    "serve:test",
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	ctx := context.Background()
	st, err := store.OpenState(ctx, cfg.StateDir, store.Options{})
	if err != nil {
		t.Fatalf("pre-open the state: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	version, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "dup", SpecHash: "sha256:dup", SpecJSON: `{"steps":[{"name":"build"}]}`,
	})
	if err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if _, err := st.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "dup", JobVersionID: version.ID, Origin: "manual",
		RunKey: "shared", Steps: []store.NewStep{{Name: "build"}},
	}); err != nil {
		t.Fatalf("seed the run: %v", err)
	}
	// One run claimed by two dedup identities. The trigger materialization
	// commits exactly one identity per run, so this shape only arrives by a
	// hand edit or a partial write, and it is what the sweep must catch.
	if _, err := st.InjectDuplicateRunKey(ctx); err != nil {
		t.Fatalf("plant the second dedup identity: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg, clock.System()) }()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Serve served a state with a broken uniqueness rule")
		}
		for _, want := range []string{"PSQ-FSCK-001", "fsck --json", "--repair --confirm"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("the refusal never says %q:\n%s", want, err.Error())
			}
		}
	case <-time.After(15 * time.Second):
		t.Fatal("Serve did not refuse within 15s")
	}
}

// TestServeBootsAfterADocumentedSensorReset is the #200 regression guard at
// the process level. `paceq sensors reset` raises the dedup epoch so every
// run key the sensor ever registered fires again, and the runs that follow
// share a (job_name, run_key) pair with the runs they replay. That is the
// product sentence, so the boot gate must read it as replay rather than as
// the corruption a refusal claims.
func TestServeBootsAfterADocumentedSensorReset(t *testing.T) {
	root := t.TempDir()
	cfg := Config{
		StateDir: filepath.Join(root, "state"),
		Version:  "test",
		JobsDir:  filepath.Join(root, "jobs"),
		Logger:   slog.New(slog.NewTextHandler(&strings.Builder{}, nil)),
		Owner:    "serve:test",
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	if err := os.MkdirAll(cfg.JobsDir, 0o700); err != nil {
		t.Fatalf("create the jobs directory: %v", err)
	}
	ctx := context.Background()
	st, err := store.OpenState(ctx, cfg.StateDir, store.Options{})
	if err != nil {
		t.Fatalf("pre-open the state: %v", err)
	}
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	const job = "polling-job"
	const sensorName = "poller"
	if _, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       job,
		SpecHash:      "sha256:poller",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"polling-job","steps":[{"name":"collect","run":["true"]}]}`,
		MaxConcurrent: 10,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if err := st.UpsertSensor(ctx, store.SensorSeedInput{
		Name: sensorName, JobName: job, ExecJSON: `["cat"]`,
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}

	keys := []store.SensorTrigger{{RunKey: "file:a.csv"}, {RunKey: "file:b.csv"}}
	evaluate := func(cursorBefore, cursorAfter string, epoch int64) store.SensorTickCommitResult {
		t.Helper()
		b, err := st.BeginSensorTick(ctx, store.BeginSensorTickInput{
			SensorName: sensorName, CursorBefore: cursorBefore,
		})
		if err != nil {
			t.Fatalf("begin the tick: %v", err)
		}
		out, err := st.CommitSensorTick(ctx, store.SensorTickCommitInput{
			TickID:        b.TickID,
			SensorName:    sensorName,
			JobName:       job,
			CursorVersion: b.CursorVersion,
			CursorAfter:   cursorAfter,
			DedupEpoch:    epoch,
			Triggers:      keys,
			Outcome:       store.OutcomeTriggered,
			NextEvalAt:    60000,
		})
		if err != nil {
			t.Fatalf("commit the tick: %v", err)
		}
		return out
	}

	if out := evaluate("", "a", 0); out.Accepted != len(keys) {
		t.Fatalf("the first evaluation accepted %d, want %d", out.Accepted, len(keys))
	}
	reset, err := st.ResetSensor(ctx, store.ResetSensorInput{Name: sensorName})
	if err != nil {
		t.Fatalf("reset the sensor: %v", err)
	}
	if out := evaluate("a", "a", reset.NewEpoch); out.Accepted != len(keys) {
		t.Fatalf("the replay after the reset accepted %d, want %d", out.Accepted, len(keys))
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(serveCtx, cfg, clock.System()) }()

	var serveErr error
	stopped := false
	select {
	case serveErr = <-errCh:
		stopped = true
	case <-time.After(5 * time.Second):
	}
	cancel()
	if !stopped {
		select {
		case serveErr = <-errCh:
		case <-time.After(20 * time.Second):
			t.Fatal("serve did not stop after the context was cancelled")
		}
	}
	if serveErr != nil && strings.Contains(serveErr.Error(), "PSQ-FSCK-001") {
		t.Fatalf("serve refused a database the documented reset produced:\n%v", serveErr)
	}
}
