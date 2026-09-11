package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The read half of M3-06 (issue #11): the sensors CLI group reads live state
// straight from the RO pool, so list/show work with the daemon down (SYNTESE
// section 3.6). These tests pin the read seams the CLI calls: one sensor, all
// sensors, the coalesced tick history, and the dedup peek.

func TestGetSensorReadsOneRow(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "dropzone", "a", 0)

	row, err := s.GetSensor(context.Background(), "dropzone")
	if err != nil {
		t.Fatalf("GetSensor: %v", err)
	}
	if row.Name != "dropzone" {
		t.Errorf("Name = %q, want dropzone", row.Name)
	}
	if row.JobName != sensorJob {
		t.Errorf("JobName = %q, want %q", row.JobName, sensorJob)
	}
	if row.IntervalMS != 60000 {
		t.Errorf("IntervalMS = %d, want 60000", row.IntervalMS)
	}
	if row.Cursor == nil || *row.Cursor != "a" {
		t.Errorf("Cursor = %v, want &a", row.Cursor)
	}
	if row.DedupEpoch != 0 {
		t.Errorf("DedupEpoch = %d, want 0", row.DedupEpoch)
	}
	if row.Paused {
		t.Errorf("Paused = true, want false")
	}
}

func TestGetSensorUnknownIsNotFound(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)

	_, err := s.GetSensor(context.Background(), "missing")
	if err == nil {
		t.Fatal("GetSensor of a missing sensor succeeded, want ErrNotFound")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetSensor error = %v, want ErrNotFound", err)
	}
}

func TestListSensorsReadsAll(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "dropzone", "a", 0)
	seedSensor(t, s, "watcher", "b", 0)

	rows, err := s.ListSensors(context.Background())
	if err != nil {
		t.Fatalf("ListSensors: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("ListSensors returned %d rows, want 2", len(rows))
	}
	names := map[string]bool{}
	for _, row := range rows {
		names[row.Name] = true
	}
	if !names["dropzone"] || !names["watcher"] {
		t.Errorf("ListSensors rows = %v, want dropzone and watcher", names)
	}
}

// TestSensorTicksReadTheCoalescedHistory is the read half of the coalescing
// the schema promises. A history that folded at write time is only legible if
// the reader carries the count: without it `sensors show --limit N` silently
// spans a different stretch of time per sensor.
func TestSensorTicksReadTheCoalescedHistory(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, sensorName, "a", 0)

	base := time.UnixMilli(1_700_000_000_000).UTC()
	at := func(n int) time.Time { return base.Add(time.Duration(n) * 30 * time.Second) }
	for i := range 3 {
		runSensorEvaluation(t, s, skipAt(at(i), "no new files"))
	}
	runSensorEvaluation(t, s, sensorEvaluation{
		At: at(3), Outcome: OutcomeTriggered, Triggers: []SensorTrigger{{RunKey: "file:1"}},
	})

	ticks, err := s.SensorTicks(context.Background(), sensorName, 10)
	if err != nil {
		t.Fatalf("SensorTicks: %v", err)
	}
	if len(ticks) != 2 {
		t.Fatalf("SensorTicks returned %d ticks, want 2: three skips are one row", len(ticks))
	}
	if ticks[0].Outcome != OutcomeTriggered || ticks[0].TriggerCount != 1 {
		t.Errorf("newest tick = %s/%d, want triggered/1", ticks[0].Outcome, ticks[0].TriggerCount)
	}
	if ticks[0].RepeatCount != 1 {
		t.Errorf("the triggered tick reports repeat_count %d, want 1", ticks[0].RepeatCount)
	}
	folded := ticks[1]
	if folded.RepeatCount != 3 {
		t.Errorf("the coalesced tick reports repeat_count %d, want 3", folded.RepeatCount)
	}
	if !folded.StartedAt.Equal(at(0)) {
		t.Errorf("the coalesced tick starts at %s, want %s", folded.StartedAt, at(0))
	}
	if !folded.LastStartedAt.Equal(at(2)) {
		t.Errorf("the coalesced tick last started at %s, want %s", folded.LastStartedAt, at(2))
	}
}

func TestPeekDedupVerdicts(t *testing.T) {
	t.Parallel()
	s := migratedStore(t)
	seedSensorJob(t, s)
	seedSensor(t, s, "dropzone", "a", 0)

	// Commit one run key so the table has a seen key to report.
	ctx := context.Background()
	in := BeginSensorTickInput{SensorName: "dropzone", CursorBefore: "a"}
	r, err := s.BeginSensorTick(ctx, in)
	if err != nil {
		t.Fatalf("BeginSensorTick: %v", err)
	}
	if _, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID: r.TickID, SensorName: "dropzone", JobName: sensorJob,
		CursorVersion: r.CursorVersion, CursorAfter: "b", DedupEpoch: 0,
		Triggers: []SensorTrigger{{RunKey: "seen-key"}}, Outcome: OutcomeTriggered,
		NextEvalAt: 60000, DurationMs: 5,
	}); err != nil {
		t.Fatalf("CommitSensorTick: %v", err)
	}

	verdicts, err := s.PeekDedup(context.Background(), "dropzone", 0,
		[]string{"seen-key", "new-key"})
	if err != nil {
		t.Fatalf("PeekDedup: %v", err)
	}
	if len(verdicts) != 2 {
		t.Fatalf("PeekDedup returned %d verdicts, want 2", len(verdicts))
	}
	byKey := map[string]DedupVerdict{}
	for _, v := range verdicts {
		byKey[v.RunKey] = v
	}
	if !byKey["seen-key"].Seen {
		t.Errorf("seen-key verdict.Seen = false, want true")
	}
	if byKey["seen-key"].RunID == "" {
		t.Errorf("seen-key verdict.RunID is empty, want the run id")
	}
	if byKey["new-key"].Seen {
		t.Errorf("new-key verdict.Seen = true, want false")
	}
}
