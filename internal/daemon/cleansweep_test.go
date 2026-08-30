package daemon

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
	"github.com/a-holm/paceq/internal/store"
)

// TestACleanHourlySweepRecordsThatItRan holds the sweep to the question the
// gauges ask. "Nothing is broken" and "nobody has looked" are different
// answers, and a sweep that writes only on findings can express only one.
func TestACleanHourlySweepRecordsThatItRan(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 10, 0, 0, 0, time.UTC))
	sts := newStatuses(func() time.Time { return clk.Now() })
	d := testLoops(t, logger, notify.New(), sts, clk)

	sweeper := &fsckStubSweeper{}
	if err := sweepIntegrity(context.Background(), d, sweeper); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(sweeper.stamps) != 1 {
		t.Fatalf("a clean sweep recorded %d sweeps, want 1", len(sweeper.stamps))
	}
	if !sweeper.stamps[0].Equal(clk.Now().UTC()) {
		t.Errorf("the clean sweep is stamped %s, want %s", sweeper.stamps[0], clk.Now().UTC())
	}
	if len(sweeper.recorded[0]) != 0 {
		t.Errorf("a clean sweep invented findings: %+v", sweeper.recorded[0])
	}
}

// TestACleanStartupSweepRecordsThatItRan is the same promise on the boot
// path: the first fact in the log must be that a sweep happened.
func TestACleanStartupSweepRecordsThatItRan(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(&strings.Builder{}, nil))
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))

	sweeper := &fsckStubSweeper{}
	if err := recordStartupSweep(context.Background(), sweeper, clk, logger, nil); err != nil {
		t.Fatalf("record the startup sweep: %v", err)
	}
	if len(sweeper.stamps) != 1 {
		t.Fatalf("a clean startup sweep recorded %d sweeps, want 1", len(sweeper.stamps))
	}
	if len(sweeper.recorded[0]) != 0 {
		t.Errorf("a clean startup sweep invented findings: %+v", sweeper.recorded[0])
	}
}

var _ = store.Violation{}
