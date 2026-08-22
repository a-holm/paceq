package logsink

import (
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// A frozen instant, so the date shard in every expectation below is stable.
var frozen = time.Date(2026, 9, 17, 3, 0, 1, 123000000, time.UTC)

func TestRelPathFollowsTheDateShardLayout(t *testing.T) {
	root := NewRoot("/state")
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1)
	want := filepath.Join("2026-09-17", "01K5ZQ8V3M7X", "extract.1.ndjson")
	if rel != want {
		t.Fatalf("RelFor = %q, want %q", rel, want)
	}
	abs, err := root.Abs(rel)
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	wantAbs := filepath.Join("/state", "logs", "2026-09-17", "01K5ZQ8V3M7X", "extract.1.ndjson")
	if abs != wantAbs {
		t.Fatalf("Abs = %q, want %q", abs, wantAbs)
	}
}

// The date shard comes from the day the attempt started, in UTC. A local zone
// with an offset would put the same run under two shards on two machines.
func TestRelPathDateShardIsUTC(t *testing.T) {
	root := NewRoot("/state")
	// 2026-09-17 00:30 in UTC is 2026-09-17 02:30 in Oslo but 2026-09-16 in
	// New York. The shard must be the UTC date for all of them.
	rel := root.RelFor(frozen, "01K5ZQ8V3M7X", "load", 2)
	if !strings.HasPrefix(rel, "2026-09-17"+string(filepath.Separator)) {
		t.Fatalf("RelFor = %q, want the UTC date shard 2026-09-17", rel)
	}
}

func TestAbsRefusesPathsOutsideTheRoot(t *testing.T) {
	root := NewRoot("/state")
	for _, rel := range []string{
		"../../etc/passwd",
		"2026-09-17/../../../etc/passwd",
		"/etc/passwd",
	} {
		if _, err := root.Abs(rel); err == nil {
			t.Errorf("Abs(%q) was accepted, want a refusal", rel)
		}
	}
}

func TestRelPathRefusesNamesThatCannotBePaths(t *testing.T) {
	root := NewRoot("/state")
	for _, tc := range []struct {
		runID, step string
		attempt     int
	}{
		{"", "extract", 1},
		{"01K5ZQ8V3M7X", "", 1},
		{"01K5ZQ8V3M7X", "a/b", 1},
		{"01K5ZQ8V3M7X", "..", 1},
		{"01K5ZQ8V3M7X", ".", 1},
		{"01K5ZQ8V3M7X", "extract", 0},
		{"01K5ZQ8V3M7X", "extract", -1},
	} {
		if got := root.RelFor(frozen, tc.runID, tc.step, tc.attempt); got != "" {
			t.Errorf("RelFor(%q, %q, %d) = %q, want a refusal", tc.runID, tc.step, tc.attempt, got)
		}
	}
}

// AttemptFiles lists every attempt of one step across the date shards, in
// numeric attempt order. Attempts of one run can land on different days, so the
// lookup spans the shards instead of guessing one date.
func TestAttemptFilesSpansDateShardsInNumericOrder(t *testing.T) {
	root := NewRoot(t.TempDir())
	for _, rel := range []string{
		root.RelFor(frozen, "01K5ZQ8V3M7X", "extract", 1),
		root.RelFor(frozen.Add(24*time.Hour), "01K5ZQ8V3M7X", "extract", 2),
		root.RelFor(frozen.Add(48*time.Hour), "01K5ZQ8V3M7X", "extract", 10),
		root.RelFor(frozen, "01K5ZQ8V3M7X", "load", 1),
		root.RelFor(frozen, "01K5ZQOTHER00", "extract", 1),
	} {
		f, err := root.Create(rel)
		if err != nil {
			t.Fatalf("create %s: %v", rel, err)
		}
		_ = f.Close()
	}

	files, err := root.AttemptFiles("01K5ZQ8V3M7X", "extract")
	if err != nil {
		t.Fatalf("AttemptFiles: %v", err)
	}
	if len(files) != 3 {
		t.Fatalf("AttemptFiles found %d files, want 3: %q", len(files), files)
	}
	// Numeric order, not lexical: attempt 10 sorts after attempt 2.
	wantAttempts := []int{1, 2, 10}
	for i, f := range files {
		if f.Attempt != wantAttempts[i] {
			t.Errorf("file %d is attempt %d, want %d", i, f.Attempt, wantAttempts[i])
		}
	}
}

func TestAttemptFilesOfAnUnknownStepIsEmpty(t *testing.T) {
	root := NewRoot(t.TempDir())
	files, err := root.AttemptFiles("01K5ZQ8V3M7X", "ghost")
	if err != nil {
		t.Fatalf("AttemptFiles: %v", err)
	}
	if len(files) != 0 {
		t.Fatalf("AttemptFiles found %d files, want none", len(files))
	}
}
