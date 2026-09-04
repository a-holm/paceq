package daemon

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
)

// The commit is the one thing a daemon wake and a forced `paceq sensors tick`
// both go through, so every decision that must not differ between them belongs
// here rather than in either caller (#215). The trigger ceiling used to live in
// the runtime, which only the daemon runs, and the CLI committed whole batches
// no ceiling had bounded.

// oversizedSensor seeds a job and a sensor whose program elects n triggers and
// reports a cursor past all of them, placed at cursor "file:0".
func oversizedSensor(t *testing.T, st *store.Store, dir string, n int) {
	t.Helper()
	ctx := t.Context()
	if _, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       "drop",
		SpecHash:      "sha256:ceiling",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"drop","steps":[{"name":"collect","run":["true"]}]}`,
		MaxConcurrent: 1,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}

	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, `{"run_key":"k`+strconv.Itoa(i)+`"}`)
	}
	contract := `{"cursor":"file:` + strconv.Itoa(n) + `","triggers":[` + strings.Join(keys, ",") + `]}`
	script := filepath.Join(dir, "oversized.sh")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat <<'EOF'\n"+contract+"\nEOF\n"), 0o700); err != nil {
		t.Fatalf("write the sensor program: %v", err)
	}
	execJSON, err := json.Marshal(map[string]any{"run": []string{"/bin/sh", script}})
	if err != nil {
		t.Fatalf("build the exec spec: %v", err)
	}
	if err := st.UpsertSensor(ctx, store.SensorSeedInput{
		Name: "dropzone", JobName: "drop", ExecJSON: string(execJSON),
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}
	if err := st.SetSensorCursor(ctx, store.CursorInput{Name: "dropzone", Cursor: "file:0"}); err != nil {
		t.Fatalf("place the sensor at its cursor: %v", err)
	}
}

// TestTheSensorCommitHoldsTheTriggerCeiling drives one whole evaluation the way
// the runtime does and reads the rows back. max_triggers_per_tick is the
// sensor's declared ceiling, so the commit creates that many runs and leaves
// the cursor where it was: the dropped triggers are replayed into the dedup
// gate on a later evaluation instead of being lost, which is the whole of the
// no-loss guarantee. The drop count lands on the tick row, because a partial
// batch nobody recorded is indistinguishable from a complete one.
func TestTheSensorCommitHoldsTheTriggerCeiling(t *testing.T) {
	ctx := t.Context()
	dir := t.TempDir()
	st, err := store.Open(ctx, filepath.Join(dir, "state.db"), store.Options{})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	oversizedSensor(t, st, dir, 150)

	row, err := st.GetSensor(ctx, "dropzone")
	if err != nil {
		t.Fatalf("read the sensor row: %v", err)
	}
	// The seeded ceiling is what makes this a truncation at all.
	if row.MaxTriggersPerTick != 100 {
		t.Fatalf("max_triggers_per_tick = %d, want the seeded 100", row.MaxTriggersPerTick)
	}
	spec, err := SensorSpecFromRow(row)
	if err != nil {
		t.Fatalf("build the sensor spec: %v", err)
	}

	sink := sensorSink{st: st, clk: clock.System(), session: "serve:test"}
	tk, err := sink.Begin(ctx, spec)
	if err != nil {
		t.Fatalf("open the tick: %v", err)
	}
	now := time.Now().UTC()
	res := sensor.NewEvaluator(sensor.Config{}, clock.System()).Evaluate(ctx, spec, sensor.Input{
		Sensor: spec.Name, Job: spec.Job, Cursor: spec.Cursor,
		Now: now.UnixMilli(), MaxTriggers: spec.MaxTriggers,
		DeadlineMS: now.Add(spec.Timeout).UnixMilli(),
	})
	// The reproduction is worth nothing unless the sensor really answered with
	// more triggers than the ceiling allows.
	if len(res.Triggers) != 150 {
		t.Fatalf("the sensor elected %d triggers, want 150 (stderr: %s)", len(res.Triggers), res.StderrExcerpt)
	}
	if err := sink.Commit(ctx, spec, tk, res); err != nil {
		t.Fatalf("commit the evaluation: %v", err)
	}

	ticks, err := st.ExplainTicks(ctx,
		[]store.ExplainSource{{Kind: "sensor", Name: "dropzone"}}, time.UnixMilli(0), "", 10)
	if err != nil || len(ticks) == 0 {
		t.Fatalf("read the tick: %v (%d rows)", err, len(ticks))
	}
	tick := ticks[0]
	if tick.TriggerCount != 100 {
		t.Errorf("the commit created %d runs, want the declared ceiling of 100", tick.TriggerCount)
	}
	if tick.CursorAfter != "" {
		t.Errorf("the tick reports cursor_after %q, want none on a truncated batch", tick.CursorAfter)
	}

	after, err := st.GetSensor(ctx, "dropzone")
	if err != nil {
		t.Fatalf("re-read the sensor row: %v", err)
	}
	if after.Cursor == nil || *after.Cursor != "file:0" {
		t.Errorf("the cursor reads %s; a truncated batch must leave it at file:0", cursorText(after.Cursor))
	}

	var data map[string]any
	if err := json.Unmarshal([]byte(tick.ReasonData), &data); err != nil {
		t.Fatalf("the tick's reason data is %q, which is not the JSON object a truncation records: %v",
			tick.ReasonData, err)
	}
	if data["truncated"] != true || data["dropped"] != float64(50) {
		t.Errorf("reason_data = %v, want truncated=true and dropped=50", data)
	}
}
