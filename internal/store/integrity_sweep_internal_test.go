package store

import (
	"context"
	"testing"
	"time"
)

// TestACleanSweepIsRecordedAsTheCurrentState is the gauges' contract: they
// answer "what is broken now" and "when did we last look", and a sweep that
// found nothing answers both. Reading the newest sweep that wrote findings
// answers neither once a violation has been repaired.
func TestACleanSweepIsRecordedAsTheCurrentState(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)

	dirty := time.Date(2027, 1, 21, 9, 0, 0, 0, time.UTC)
	if err := s.RecordIntegritySweep(ctx, dirty, []IntegrityFinding{
		{Invariant: "I14", Severity: Warning, Violations: 1, Subjects: []string{"run 01HELD"}},
	}); err != nil {
		t.Fatalf("record the dirty sweep: %v", err)
	}

	clean := dirty.Add(time.Hour)
	if err := s.RecordIntegritySweep(ctx, clean, nil); err != nil {
		t.Fatalf("record the clean sweep: %v", err)
	}

	found, err := s.MetricsIntegrityViolations(ctx)
	if err != nil {
		t.Fatalf("read the newest sweep's findings: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("a repaired invariant still reports broken: %+v", found)
	}
	last, ok, err := s.MetricsFsckLastRun(ctx)
	if err != nil || !ok {
		t.Fatalf("the last sweep stamp is missing: ok=%v err=%v", ok, err)
	}
	if !last.Equal(clean) {
		t.Errorf("the last sweep reads %s, want the clean sweep at %s", last, clean)
	}
}

// TestANeverDirtyDatabaseStillReportsItsLastSweep covers the database that
// has never had a finding: the staleness alert needs a stamp, and silence
// cannot provide one.
func TestANeverDirtyDatabaseStillReportsItsLastSweep(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)

	if _, ok, err := s.MetricsFsckLastRun(ctx); err != nil || ok {
		t.Fatalf("a database nobody has swept reports a sweep: ok=%v err=%v", ok, err)
	}
	at := time.Date(2027, 1, 21, 10, 0, 0, 0, time.UTC)
	if err := s.RecordIntegritySweep(ctx, at, nil); err != nil {
		t.Fatalf("record the clean sweep: %v", err)
	}
	last, ok, err := s.MetricsFsckLastRun(ctx)
	if err != nil || !ok {
		t.Fatalf("the first clean sweep left no stamp: ok=%v err=%v", ok, err)
	}
	if !last.Equal(at) {
		t.Errorf("the last sweep reads %s, want %s", last, at)
	}
}

// TestTwoSweepsInOneMillisecondStayDistinct holds the identity of a sweep
// apart from the one before it. Without that, the newest sweep's findings and
// its predecessor's collapse into one reading.
func TestTwoSweepsInOneMillisecondStayDistinct(t *testing.T) {
	ctx := context.Background()
	s := internalStore(t)

	at := time.Date(2027, 1, 21, 11, 0, 0, 0, time.UTC)
	if err := s.RecordIntegritySweep(ctx, at, []IntegrityFinding{
		{Invariant: "I14", Severity: Warning, Violations: 3, Subjects: []string{"run 01HELD"}},
	}); err != nil {
		t.Fatalf("record the dirty sweep: %v", err)
	}
	if err := s.RecordIntegritySweep(ctx, at, nil); err != nil {
		t.Fatalf("record the clean sweep: %v", err)
	}
	found, err := s.MetricsIntegrityViolations(ctx)
	if err != nil {
		t.Fatalf("read the newest sweep's findings: %v", err)
	}
	if len(found) != 0 {
		t.Errorf("the clean sweep read its predecessor's findings: %+v", found)
	}
}
