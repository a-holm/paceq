package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// The store end of the example-sensor suite (issue #14, M3-07). The examples
// live in examples/sensors and are proven standalone in that package; this
// file proves the whole documented production path end to end with a real
// example script:
//
//	spec -> evaluator -> atomic commit -> dedup
//
// The evaluator subprocess is driven here the way the daemon drives it: an
// argv from the spec, a PACEQ_* environment, one JSON object read off stdout.
// We re-issue that contract by hand instead of importing internal/sensor
// because the architecture forbids internal/store from importing
// internal/sensor; the evaluator's own classification is thoroughly tested in
// internal/sensor, and what this test adds is the store half of the path
// (atomic commit, run_keys dedup, reset replay) against a real shell sensor.
//
// The M3-06 CLI that would drive this surface is not merged (#11), so the
// harness calls the same seam the daemon uses (BeginSensorTick,
// CommitSensorTick, ResetSensor). When the CLI lands, this stays as the lower
// level proof and the CLI adds the user-visible one.

// exRoot resolves the module root so tests find examples/sensors regardless
// of the test working directory.
func exRoot(t *testing.T) string {
	t.Helper()
	cmd := exec.Command("go", "list", "-m", "-f", "{{.Dir}}")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("resolve module root: %v", err)
	}
	return strings.TrimSpace(string(out))
}

// exSeedSensor registers a job plus a sensor row pointing at the example
// script, mirroring what M3-01 materialises at apply (not merged here, so it
// is seeded directly like the other store sensor tests do).
func exSeedSensor(t *testing.T, s *Store, name, script string) {
	t.Helper()
	seedSensorJob(t, s)
	_, err := s.w.Exec(`INSERT INTO sensors
(name, job_name, kind, exec_json, interval_ms, min_interval_ms, timeout_ms,
 max_triggers_per_tick, cursor, cursor_version, next_eval_at, created_at, updated_at)
VALUES (?, ?, 'exec', ?, 60000, 1000, 30000, 100, NULL, 0, 0, 1, 1)`,
		name, sensorJob, `["`+script+`"]`)
	if err != nil {
		t.Fatalf("seed example sensor %s: %v", name, err)
	}
}

// exMakeFiles writes n files into a fresh temp dir with deterministic mtimes
// ascending from base. Returns the dir and the max mtime.
func exMakeFiles(t *testing.T, base int64, n int) (dir string, max int64) {
	t.Helper()
	dir = t.TempDir()
	max = base + int64(n-1)
	for i := 0; i < n; i++ {
		p := filepath.Join(dir, fmt.Sprintf("file%d.txt", i))
		if err := os.WriteFile(p, []byte(p), 0o644); err != nil {
			t.Fatalf("write example fixture %s: %v", p, err)
		}
		ts := time.Unix(base+int64(i), 0)
		if err := os.Chtimes(p, ts, ts); err != nil {
			t.Fatalf("set mtime on %s: %v", p, err)
		}
	}
	return dir, max
}

// exResult is the small slice of the evaluator's result this proof needs:
// the triggers decided and the next cursor. It mirrors what internal/sensor
// hands to the commit layer.
type exResult struct {
	Triggers   []exTrigger
	CursorNext string
	CursorSet  bool
	Skipped    bool
}

type exTrigger struct {
	RunKey string
	Params string
}

// exRunScript executes the example sensor exactly as the evaluator does: an
// argv from the spec, an environment carrying the frozen PACEQ_* contract keys
// plus the sensor's own declared env, and one JSON object read off stdout. It
// returns the parsed result the commit layer would receive.
func exRunScript(t *testing.T, script, watchDir string, cursor *string) exResult {
	t.Helper()

	cmd := exec.Command(script) // #nosec G204 - the configured sensor argv is the contract, mirroring internal/sensor
	cmd.Env = append(os.Environ(),
		"PACEQ_SENSOR=dropzone",
		"PACEQ_JOB="+sensorJob,
		"PACEQ_CURSOR="+cursorOrEmptyStream(cursor),
		"PACEQ_LAST_TICK_AT=0",
		"PACEQ_MAX_TRIGGERS=100",
		"PACEQ_DEADLINE_MS="+strconv.FormatInt(time.Now().Add(5*time.Second).UnixMilli(), 10),
		"PACEQ_DRY_RUN=0",
		"WATCH_DIR="+watchDir,
	)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s failed: %v\nstderr: %s", script, err, errb.String())
	}

	var obj struct {
		Cursor   *string `json:"cursor"`
		Triggers []struct {
			RunKey string          `json:"run_key"`
			Params json.RawMessage `json:"params,omitempty"`
		} `json:"triggers"`
		SkipReason *string `json:"skip_reason"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &obj); err != nil {
		t.Fatalf("example output is not one JSON object: %v\noutput: %s", err, out.String())
	}

	res := exResult{}
	for _, tr := range obj.Triggers {
		params := string(tr.Params)
		if params == "" || params == "null" {
			params = "{}"
		}
		res.Triggers = append(res.Triggers, exTrigger{RunKey: tr.RunKey, Params: params})
	}
	if obj.Cursor != nil {
		res.CursorSet = true
		res.CursorNext = *obj.Cursor
	}
	res.Skipped = len(res.Triggers) == 0 && obj.SkipReason != nil
	return res
}

func cursorOrEmptyStream(c *string) string {
	if c == nil {
		return ""
	}
	return *c
}

// exCommit commits one evaluation through the atomic transaction. It reads a
// fresh cursor guard and epoch from the row, the way the commit path demands,
// and returns the accepted plus deduped counts.
func exCommit(t *testing.T, s *Store, name string, res exResult, cursorBefore *string) (accepted, deduped int) {
	t.Helper()
	ctx := context.Background()

	b, err := s.BeginSensorTick(ctx, BeginSensorTickInput{
		SensorName:      name,
		CursorBefore:    cursorOrEmptyStream(cursorBefore),
		DaemonSessionID: "example",
		Now:             time.Now(),
	})
	if err != nil {
		t.Fatalf("begin tick for %s: %v", name, err)
	}

	triggers := make([]SensorTrigger, 0, len(res.Triggers))
	for _, tr := range res.Triggers {
		triggers = append(triggers, SensorTrigger{RunKey: tr.RunKey, ParamsJSON: tr.Params})
	}

	outcome := OutcomeSkipped
	code := reason.TICKSkippedSensor
	if len(res.Triggers) > 0 {
		outcome = OutcomeTriggered
		code = reason.TRIGGERAccepted
	}

	var version, epoch int64
	if err := s.r.QueryRowContext(ctx,
		"SELECT cursor_version, dedup_epoch FROM sensors WHERE name = ?", name).
		Scan(&version, &epoch); err != nil {
		t.Fatalf("read guard for %s: %v", name, err)
	}

	after := ""
	if res.CursorSet {
		after = res.CursorNext
	}

	c, err := s.CommitSensorTick(ctx, SensorTickCommitInput{
		TickID:        b.TickID,
		SensorName:    name,
		JobName:       sensorJob,
		CursorVersion: version,
		CursorAfter:   after,
		DedupEpoch:    epoch,
		Triggers:      triggers,
		Outcome:       outcome,
		ReasonCode:    code,
		DurationMs:    1,
		NextEvalAt:    time.Now().Add(time.Minute).UnixMilli(),
		Now:           time.Now(),
	})
	if err != nil {
		t.Fatalf("commit tick for %s: %v", name, err)
	}
	if c.Fenced {
		t.Fatalf("commit for %s was fenced (concurrent advance), unexpected in a serial test", name)
	}
	return c.Accepted, c.Deduped
}

// TestExampleSensorProductionPath is the proof that the documented path works
// with a real example script and real rows. The shape is the product sentence:
// three files, three runs; rerun, zero new; non destructive reset, three more.
func TestExampleSensorProductionPath(t *testing.T) {
	s := openStore(t, filepath.Join(t.TempDir(), "state.db"))
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate the example store: %v", err)
	}

	const name = "dropzone"
	script := filepath.Join(exRoot(t), "examples", "sensors", "fs-watermark.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("example script is missing: %v", err)
	}
	exSeedSensor(t, s, name, script)

	dir, maxMtime := exMakeFiles(t, time.Now().Unix()-1000, 3)

	// First tick from a nil cursor: the evaluator lists every file and the
	// atomic commit creates one run per trigger. Cursor lands on max mtime.
	res := exRunScript(t, script, dir, nil)
	if len(res.Triggers) != 3 || res.Skipped {
		t.Fatalf("first eval should trigger 3 files, got %d triggers (skipped=%v)",
			len(res.Triggers), res.Skipped)
	}
	accepted, deduped := exCommit(t, s, name, res, nil)
	if accepted != 3 || deduped != 0 {
		t.Fatalf("first tick: want 3 accepted, 0 deduped; got %d/%d", accepted, deduped)
	}
	if cursor, _ := readSensor(t, s, name); cursor != strconv.FormatInt(maxMtime, 10) {
		t.Fatalf("cursor should be the max mtime %v, got %q", maxMtime, cursor)
	}
	if runs := countSensorRuns(t, s); runs != 3 {
		t.Fatalf("3 runs after first tick, got %d", runs)
	}

	// Second tick with the cursor advanced: nothing is newer, so a quiet
	// skip. Zero runs added, cursor untouched.
	res2 := exRunScript(t, script, dir, strPtr(strconv.FormatInt(maxMtime, 10)))
	if !res2.Skipped || len(res2.Triggers) != 0 {
		t.Fatalf("a second tick over the same files should skip")
	}
	accepted2, deduped2 := exCommit(t, s, name, res2, strPtr(strconv.FormatInt(maxMtime, 10)))
	if accepted2 != 0 || deduped2 != 0 {
		t.Fatalf("a quiet replay wants 0/0, got %d/%d", accepted2, deduped2)
	}
	if runs := countSensorRuns(t, s); runs != 3 {
		t.Fatalf("rerun must stay at 3 runs, got %d", runs)
	}

	// Replay the EXACT first tick (nil cursor again, same run keys). This is
	// the crash-then-resume shape. The run_keys gate folds every key into the
	// existing run: accepted 0, deduped 3, runs still 3.
	res3 := exRunScript(t, script, dir, nil)
	if len(res3.Triggers) != 3 {
		t.Fatalf("a rewind replay should still trigger, got %d triggers", len(res3.Triggers))
	}
	accepted3, deduped3 := exCommit(t, s, name, res3, nil)
	if accepted3 != 0 || deduped3 != 3 {
		t.Fatalf("a replay should fold (0 accepted, 3 deduped), got %d/%d", accepted3, deduped3)
	}
	if runs := countSensorRuns(t, s); runs != 3 {
		t.Fatalf("replay must not add runs, got %d", runs)
	}

	// Reset bumps the dedup epoch and clears the cursor, so the same files
	// are new again in a fresh epoch: three more runs, six in total.
	if _, err := s.ResetSensor(context.Background(), ResetSensorInput{Name: name}); err != nil {
		t.Fatalf("reset sensor: %v", err)
	}
	res4 := exRunScript(t, script, dir, nil)
	accepted4, _ := exCommit(t, s, name, res4, nil)
	if accepted4 != 3 {
		t.Fatalf("a reset should replay 3 new runs, got %d", accepted4)
	}
	if runs := countSensorRuns(t, s); runs != 6 {
		t.Fatalf("6 runs after first tick plus reset replay, got %d", runs)
	}
}

func strPtr(v string) *string { return &v }
