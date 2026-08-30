package obs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// testOrigin is the frozen wall clock every collector render-time test uses:
// the shared instant from internal/testutil, restated here so obs keeps no
// import on a test-only package.
var testOrigin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// stubSource is a fixed dataset: the same state every render-time test feeds
// the collector so its bytes can be pinned without a database.
type stubSource struct {
	counters      *Counters
	integrity     []store.MetricsIntegrityViolation
	fsckLastRun   time.Time
	lastSuccesses []store.MetricsJobStamp
	slas          []store.MetricsJobSLA
	schedules     []store.MetricsInstigatorState
	sensors       []store.MetricsInstigatorState
	lags          []store.MetricsSourceLag
	lastTicks     []store.MetricsSourceStamp
	runs          []store.MetricsJobStateCount
	outage        float64
	meta          map[string]string
	dbBytes       int64
	walBytes      int64

	writeWait float64
	busy      uint64

	notifPending int64
	notifGivenUp int64
	delivery     store.DeliverySnapshot
}

func (s *stubSource) MetricsRunsByStates(context.Context) ([]store.MetricsJobStateCount, error) {
	return s.runs, nil
}

func (s *stubSource) MetricsLastSuccesses(context.Context) ([]store.MetricsJobStamp, error) {
	return s.lastSuccesses, nil
}

func (s *stubSource) MetricsJobSLAs(context.Context) ([]store.MetricsJobSLA, error) {
	return s.slas, nil
}

func (s *stubSource) MetricsScheduleStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return s.schedules, nil
}

func (s *stubSource) MetricsSensorStates(context.Context) ([]store.MetricsInstigatorState, error) {
	return s.sensors, nil
}

func (s *stubSource) MetricsTickLags(context.Context) ([]store.MetricsSourceLag, error) {
	return s.lags, nil
}

func (s *stubSource) MetricsLastTicks(context.Context) ([]store.MetricsSourceStamp, error) {
	return s.lastTicks, nil
}
func (s *stubSource) MetricsOutageSeconds(context.Context) (float64, error) { return s.outage, nil }
func (s *stubSource) MetricsMetaValues(context.Context) (map[string]string, error) {
	return s.meta, nil
}

func (s *stubSource) MetricsDBBytes(context.Context) (int64, int64, error) {
	return s.dbBytes, s.walBytes, nil
}
func (s *stubSource) TakeWriteWaitMax() float64 { return s.writeWait }
func (s *stubSource) BusyTotal() uint64         { return s.busy }
func (s *stubSource) MetricsNotificationsPending(context.Context) (int64, error) {
	return s.notifPending, nil
}

func (s *stubSource) MetricsIntegrityViolations(context.Context) ([]store.MetricsIntegrityViolation, error) {
	return s.integrity, nil
}

func (s *stubSource) MetricsFsckLastRun(context.Context) (time.Time, bool, error) {
	if s.fsckLastRun.IsZero() {
		return time.Time{}, false, nil
	}
	return s.fsckLastRun, true, nil
}

func (s *stubSource) MetricsNotificationsGivenUp(context.Context) (int64, error) {
	return s.notifGivenUp, nil
}
func (s *stubSource) TakeDelivery() store.DeliverySnapshot { return s.delivery }

// fixture builds the canonical small estate the golden test pins.
func fixture() (*stubSource, *clock.Fake) {
	clk := clock.NewFake(testOrigin)
	now := clk.Now()

	src := &stubSource{
		lastSuccesses: []store.MetricsJobStamp{
			{Job: "nightly-report", At: now.Add(-26 * time.Hour)},
		},
		slas: []store.MetricsJobSLA{{Job: "nightly-report", Within: 26 * time.Hour}},
		schedules: []store.MetricsInstigatorState{
			{Name: "nightly", NextTick: now.Add(12 * time.Hour)},
			{Name: "paused-one", Paused: true},
		},
		sensors: []store.MetricsInstigatorState{
			{
				Name:            "dropzone",
				Cadence:         30 * time.Second,
				CadenceKnown:    true,
				NextTick:        now.Add(20 * time.Second),
				CursorUpdatedAt: now.Add(-41*time.Second - 200*time.Millisecond),
			},
			{Name: "flaky", ConsecutiveFailures: 2, NextTick: now.Add(time.Minute)},
		},
		lags: []store.MetricsSourceLag{
			{Kind: "schedule", Name: "nightly", LagMillis: 412},
			{Kind: "sensor", Name: "dropzone", LagMillis: 1500},
		},
		lastTicks: []store.MetricsSourceStamp{
			{Kind: "schedule", Name: "nightly", At: now.Add(-12 * time.Hour)},
			{Kind: "sensor", Name: "dropzone", At: now.Add(-30 * time.Second)},
		},
		runs: []store.MetricsJobStateCount{
			{Job: "nightly-report", State: "queued", Count: 1},
			{Job: "nightly-report", State: "running", Count: 1},
			{Job: "nightly-report", State: "succeeded", Count: 410},
			{Job: "nightly-report", State: "failed", Count: 2},
			{Job: "other-job", State: "queued", Count: 3},
		},
		outage:   542.5,
		meta:     map[string]string{"backup_last_success_at_ms": "1773490400000", "backup_verified_ok": "1"},
		dbBytes:  41947136,
		walBytes: 1310720,

		writeWait: 0.019,
		busy:      3,

		notifPending: 2,
		notifGivenUp: 1,
		delivery:     store.DeliverySnapshot{SumSeconds: 0.75, TotalCount: 3},
	}

	counters := NewCounters()
	counters.ObserveTick("schedule", "nightly", store.OutcomeTriggered, "")
	counters.ObserveTick("sensor", "dropzone", store.OutcomeSkipped, "TICK_SKIPPED_SENSOR")
	counters.ObserveLeaseReclaims(7)
	counters.ObserveNotificationGaveUp()
	// Three delivery observations: two fast sends land inside the first
	// bucket, one slow one only clears the 0.5 bucket, which pins every
	// le line's shape in the golden document.
	src.delivery.BucketCounts = make([]uint64, len(store.DeliveryBuckets()))
	for i, edge := range store.DeliveryBuckets() {
		switch {
		case edge >= 0.5:
			src.delivery.BucketCounts[i] = 2
		case edge >= 0.01:
			src.delivery.BucketCounts[i] = 1
		}
	}
	src.counters = counters
	return src, clk
}

// TestGolden pins the whole document byte for byte against a frozen clock
// and a fixed dataset (test plan item 1). Any change here is a change to the
// public surface of /metrics and must say so in its commit message.
func TestGolden(t *testing.T) {
	src, clk := fixture()
	c := NewCollector(src, src.counters, clk,
		Identity{Version: "0.1.0-test", Commit: "a1b2c3d", GoVersion: "go1.27.0"}, "")

	got := c.Scrape(context.Background())
	wantPath := filepath.Join("testdata", "golden", "metrics.txt")
	want, err := os.ReadFile(wantPath)
	if err != nil {
		t.Fatalf("read golden %s: %v\n--- actual output follows ---\n%s", wantPath, err, got)
	}
	if string(got) != string(want) {
		t.Fatalf("scrape does not match %s\n--- got ---\n%s\n--- want ---\n%s",
			wantPath, got, want)
	}
}
