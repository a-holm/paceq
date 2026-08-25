package obs

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// brokenSource fails every database family while its memory half stays
// healthy: the shape of a store that has gone away under a live process.
type brokenSource struct {
	stubSource
}

func errBoom() error { return errors.New("database is gone") }

func (b *brokenSource) MetricsRunsByStates(context.Context) ([]store.MetricsJobStateCount, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsLastSuccesses(context.Context) ([]store.MetricsJobStamp, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsJobSLAs(context.Context) ([]store.MetricsJobSLA, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsScheduleStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsSensorStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsTickLags(context.Context) ([]store.MetricsSourceLag, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsLastTicks(context.Context) ([]store.MetricsSourceStamp, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsOutageSeconds(context.Context) (float64, error) { return 0, errBoom() }

func (b *brokenSource) MetricsMetaValues(context.Context) (map[string]string, error) {
	return nil, errBoom()
}

func (b *brokenSource) MetricsDBBytes(context.Context) (int64, int64, error) {
	return 0, 0, errBoom()
}

// TestScrapeDegradesInsteadOfHanging is degradation test 6 of the plan: a
// database that cannot answer costs the document nothing but the state
// families - build info and the in-memory counters survive, and
// pulseq_metrics_db_error says so once.
func TestScrapeDegradesInsteadOfHanging(t *testing.T) {
	start := time.Now()
	src := &brokenSource{stubSource{
		counters:  NewCounters(),
		writeWait: 0.001,
		busy:      1,
	}}
	src.counters.ObserveTick("schedule", "nightly", store.OutcomeTriggered, "")

	c := NewCollector(src, src.counters, clock.NewFake(testOrigin),
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, "")
	out := string(c.Scrape(context.Background()))
	elapsed := time.Since(start)

	if elapsed > scrapeTimeout {
		t.Fatalf("a fully broken database cost %v; the scrape must fail fast", elapsed)
	}
	if !strings.Contains(out, "pulseq_build_info") {
		t.Error("build info must survive a database failure")
	}
	if !strings.Contains(out, `pulseq_tick_total{instigator="schedule",name="nightly",status="triggered",reason_code=""} 1`) {
		t.Error("in-memory counters must survive a database failure")
	}
	if !strings.Contains(out, "pulseq_metrics_db_error 1") {
		t.Errorf("the degraded scrape must say so once, got:\n%s", out)
	}
	if n := strings.Count(out, "\npulseq_metrics_db_error "); n != 1 {
		t.Errorf("pulseq_metrics_db_error sample appears %d times, want exactly 1", n)
	}
	for _, gone := range []string{"pulseq_last_success_timestamp_seconds", "pulseq_runs_by_state", "pulseq_outage_seconds_total"} {
		if strings.Contains(out, gone+"\n") || strings.Contains(out, gone+"{") {
			t.Errorf("%s leaked into a degraded scrape", gone)
		}
	}
}

// hangingSource answers every call by blocking until its context dies - the
// locked-database shape. The scrape deadline, not the caller's patience,
// bounds it.
type hangingSource struct {
	stubSource
}

func hang(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (h *hangingSource) MetricsRunsByStates(ctx context.Context) ([]store.MetricsJobStateCount, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsLastSuccesses(ctx context.Context) ([]store.MetricsJobStamp, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsJobSLAs(ctx context.Context) ([]store.MetricsJobSLA, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsScheduleStates(ctx context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsSensorStates(ctx context.Context) ([]store.MetricsInstigatorState, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsTickLags(ctx context.Context) ([]store.MetricsSourceLag, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsLastTicks(ctx context.Context) ([]store.MetricsSourceStamp, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsOutageSeconds(ctx context.Context) (float64, error) {
	return 0, hang(ctx)
}

func (h *hangingSource) MetricsMetaValues(ctx context.Context) (map[string]string, error) {
	return nil, hang(ctx)
}

func (h *hangingSource) MetricsDBBytes(ctx context.Context) (int64, int64, error) {
	return 0, 0, hang(ctx)
}

// TestScrapeDeadlineBoundsAHangingDatabase pins the 2 second budget: the
// answer comes back with whatever memory can say, never later than the
// deadline plus scheduling slack.
func TestScrapeDeadlineBoundsAHangingDatabase(t *testing.T) {
	src := &hangingSource{stubSource{counters: NewCounters()}}
	c := NewCollector(src, src.counters, clock.NewFake(testOrigin),
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, "")

	start := time.Now()
	out := string(c.Scrape(context.Background()))
	elapsed := time.Since(start)

	if elapsed > scrapeTimeout+250*time.Millisecond {
		t.Fatalf("scrape took %v against a %v deadline", elapsed, scrapeTimeout)
	}
	if !strings.Contains(out, "pulseq_metrics_db_error 1") {
		t.Errorf("a timed out scrape must degrade loudly, got:\n%s", out)
	}
	if !strings.Contains(out, "pulseq_build_info") {
		t.Error("build info must survive a hanging database")
	}
}

// countSeries counts sample lines: everything in the document that is not a
// HELP/TYPE comment or blank.
func countSeries(doc []byte) int {
	n := 0
	for _, ln := range strings.Split(string(doc), "\n") {
		if ln == "" || strings.HasPrefix(ln, "#") {
			continue
		}
		n++
	}
	return n
}

// TestCardinalityStaysUnderTheCap is the CI cardinality discipline (06
// section 6.4): 1000 jobs and 200 sensors - far beyond any target
// installation - must produce a bounded number of series, and the test logs
// the actual number so drift over time is visible. No ID labels exist, so
// the count grows linearly with configured entities and never with history.
func TestCardinalityStaysUnderTheCap(t *testing.T) {
	const (
		jobs      = 1000
		sensors   = 200
		seriesCap = 15000 // the fixed cap; failing here means someone added unbounded labels
	)

	now := time.Now().UTC()
	src := &stubSource{counters: NewCounters(), meta: map[string]string{}}
	for i := 0; i < jobs; i++ {
		name := fmt.Sprintf("job-%04d", i)
		src.slas = append(src.slas, store.MetricsJobSLA{Job: name, Within: time.Hour})
		src.lastSuccesses = append(src.lastSuccesses, store.MetricsJobStamp{Job: name, At: now})
		src.runs = append(src.runs,
			store.MetricsJobStateCount{Job: name, State: "queued", Count: 1},
			store.MetricsJobStateCount{Job: name, State: "succeeded", Count: 3})
	}
	for i := 0; i < sensors; i++ {
		name := fmt.Sprintf("sensor-%03d", i)
		src.sensors = append(src.sensors, store.MetricsInstigatorState{
			Name:            name,
			Cadence:         time.Minute,
			CadenceKnown:    true,
			NextTick:        now.Add(time.Minute),
			CursorUpdatedAt: now,
		})
		src.lags = append(src.lags, store.MetricsSourceLag{Kind: "sensor", Name: name})
		src.lastTicks = append(src.lastTicks, store.MetricsSourceStamp{Kind: "sensor", Name: name, At: now})
	}

	c := NewCollector(src, src.counters, clock.NewFake(testOrigin),
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, filepath.Join(t.TempDir(), "logs"))

	// The perf budget from the test plan: a full scrape over 1000 jobs must
	// answer well inside 200 ms. Measured on the second render so the first
	// one's cold maps do not describe steady state.
	_ = c.Scrape(context.Background())
	start := time.Now()
	doc := c.Scrape(context.Background())
	elapsed := time.Since(start)

	got := countSeries(doc)
	t.Logf("cardinality: %d jobs x 4 families + %d sensors x 6 families => %d series (cap %d), scrape %v",
		jobs, sensors, got, seriesCap, elapsed)
	if got >= seriesCap {
		t.Fatalf("series count %d reached the fixed cap %d; the cardinality discipline is broken", got, seriesCap)
	}
	if elapsed > 200*time.Millisecond {
		t.Fatalf("scrape with 1000 jobs took %v, budget is 200ms", elapsed)
	}
}

// realStore opens a migrated database with the hook wired, like serve does.
func realStore(t *testing.T, counters *Counters) (*store.Store, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(testOrigin)
	s, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"),
		store.Options{Clock: clk, Metrics: counters})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s, clk
}

const consistencySpec = `{"schema":"paceq.job.v1","name":"grid","expected_within_ms":93600000,` +
	`"max_concurrent":8,"steps":[{"name":"only","run":["true"],"shell":false}]}`

// seedRun creates a run in a chosen live state, or drives it all the way to
// succeeded so its finished_at becomes the last-success stamp.
func seedRun(t *testing.T, s *store.Store, state string) {
	t.Helper()
	ctx := context.Background()

	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "grid", MaxConcurrent: 8, SpecHash: "sha256:grid", SpecJSON: consistencySpec,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	run, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "grid", JobVersionID: version.ID, Origin: "manual",
		Steps: []store.NewStep{{Name: "only"}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ref := store.LeaseRef{Owner: "test", Epoch: 1}
	if _, _, err := s.ClaimRun(ctx, run.ID, store.LeaseInput{Owner: "test"}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	switch state {
	case "running":
		return
	case "queued":
		// Nothing more: claimed-but-not-started stays queued only if we
		// never start it, but ClaimRun moves it to running. Requeue by
		// finishing nothing is not available, so queued cells come from
		// separate unclaimed runs instead.
		t.Fatal("use seedQueuedRun for queued")
	default:
		if err := s.StartStep(ctx, run.ID, "only", ref); err != nil {
			t.Fatalf("start step: %v", err)
		}
		if err := s.RecordStepOutcome(ctx, run.ID, "only", store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
		}, ref); err != nil {
			t.Fatalf("record step: %v", err)
		}
		if _, err := s.FinishRun(ctx, run.ID, ref, store.FinishReason{Code: reason.RUNSucceeded}); err != nil {
			t.Fatalf("finish: %v", err)
		}
	}
}

func seedQueuedRun(t *testing.T, s *store.Store) {
	t.Helper()
	ctx := context.Background()
	version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "grid", MaxConcurrent: 8, SpecHash: "sha256:grid", SpecJSON: consistencySpec,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, err := s.CreateRunWithSteps(ctx, store.NewRun{
		JobName: "grid", JobVersionID: version.ID, Origin: "manual",
		Steps: []store.NewStep{{Name: "only"}},
	}); err != nil {
		t.Fatalf("create queued: %v", err)
	}
}

// TestScrapeMatchesKnownDatabaseState is consistency test 4: write a known
// state, scrape, assert the exact numbers. It catches gauges being read from
// the wrong source, which no amount of format testing can.
func TestScrapeMatchesKnownDatabaseState(t *testing.T) {
	counters := NewCounters()
	s, clk := realStore(t, counters)

	seedQueuedRun(t, s)
	seedQueuedRun(t, s)
	seedQueuedRun(t, s)
	seedRun(t, s, "running")
	seedRun(t, s, "running")
	seedRun(t, s, "succeeded")

	c := NewCollector(s, counters, clk,
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, "")
	out := string(c.Scrape(context.Background()))

	wants := []string{
		`pulseq_last_success_timestamp_seconds{job="grid"} 1773576000`,
		`pulseq_job_freshness_sla_seconds{job="grid"} 93600`,
		`pulseq_runs_by_state{job="grid",state="queued"} 3`,
		`pulseq_runs_by_state{job="grid",state="running"} 2`,
		`pulseq_run_total{job="grid",status="succeeded"} 1`,
		`pulseq_daemon_start_timestamp_seconds 1773576000`,
	}
	for _, want := range wants {
		if !strings.Contains(out, want+"\n") {
			t.Errorf("scrape missing %q\n--- got ---\n%s", want, out)
		}
	}
	if strings.Contains(out, "pulseq_metrics_db_error 1") {
		t.Error("a healthy database must not report a scrape error")
	}

	// The two-source rule, restart half: a new Counters set stands in for a
	// restarted daemon. Its counters are empty - resets are Prometheus's
	// business - while every gauge is immediately correct again without any
	// rebuild, because it was read fresh from the database.
	fresh := NewCounters()
	out = string(NewCollector(s, fresh, clk,
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, "").Scrape(context.Background()))
	if strings.Contains(out, "pulseq_tick_total{") {
		t.Error("a restarted daemon's tick counters must start empty")
	}
	if !strings.Contains(out, `pulseq_last_success_timestamp_seconds{job="grid"} 1773576000`) {
		t.Error("gauges must be correct immediately after a restart, without rebuild")
	}

	// The event half: one committed sensor evaluation lands in the counter
	// after its transaction, with the reason code it stored.
	if err := s.UpsertSensor(context.Background(), store.SensorSeedInput{
		Name: "watched", JobName: "grid", ExecJSON: `{"run":["true"],"shell":false}`,
	}); err != nil {
		t.Fatalf("seed sensor: %v", err)
	}
	begin, err := s.BeginSensorTick(context.Background(), store.BeginSensorTickInput{
		SensorName: "watched",
	})
	if err != nil {
		t.Fatalf("begin sensor tick: %v", err)
	}
	if _, err := s.CommitSensorTick(context.Background(), store.SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    "watched",
		Outcome:       store.OutcomeSkipped,
		ReasonCode:    reason.TICKSkippedSensor,
		CursorVersion: begin.CursorVersion,
		NextEvalAt:    clk.Now().Add(time.Minute).UnixMilli(),
	}); err != nil {
		t.Fatalf("commit sensor tick: %v", err)
	}
	out = string(NewCollector(s, counters, clk,
		Identity{Version: "v", Commit: "c", GoVersion: "go"}, "").Scrape(context.Background()))
	want := `pulseq_tick_total{instigator="sensor",name="watched",status="skipped",reason_code="TICK_SKIPPED_SENSOR"} 1`
	if !strings.Contains(out, want+"\n") {
		t.Errorf("committed tick not counted, want %q\n--- got ---\n%s", want, out)
	}
}
