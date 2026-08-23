//go:build unix

package sensor

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
)

// inp is the canonical inbound object the contract tests run against.
func inp() Input {
	cur := "2026-08-21/03-11-02.csv"
	tick := int64(1761040800000)
	return Input{
		Sensor:      "dropzone",
		Job:         "import-file",
		Cursor:      &cur,
		LastTickAt:  &tick,
		Now:         1761040860000,
		MaxTriggers: 100,
		DeadlineMS:  1761040890000,
		DryRun:      false,
	}
}

// evalPlain runs one evaluation against the shared evaluator and returns the
// Result, failing loudly on a hang.
func evalPlain(t *testing.T, argv ...string) Result {
	cmd := append([]string{fakecmd(t)}, argv...)
	return evalBounded(t, time.Minute, context.Background(), newTestEvaluator(),
		baseSpec(t, cmd...), inp())
}

func mustRead(t *testing.T, path string) []byte {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return raw
}

// TestInputContractGolden pins the inbound JSON contract. The field names are
// the public interface a sensor author codes against, so a rename must break
// this test in plain sight.
func TestInputContractGolden(t *testing.T) {
	raw, err := json.Marshal(inp())
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	want := `{"sensor":"dropzone","job":"import-file","cursor":"2026-08-21/03-11-02.csv","last_tick_at":1761040800000,"now":1761040860000,"max_triggers":100,"deadline_ms":1761040890000,"dry_run":false}`
	if got := string(raw); got != want {
		t.Fatalf("input contract drifted:\n got %s\nwant %s", got, want)
	}
}

// TestTriggered is the first row of the outcome table: exit 0 with at least
// one trigger is a triggered tick carrying the triggers.
func TestTriggered(t *testing.T) {
	res := evalPlain(t, "sensor-ok")
	if res.Outcome != Triggered {
		t.Fatalf("outcome = %v (reason %s), want Triggered", res.Outcome, res.ReasonCode)
	}
	if len(res.Triggers) != 1 || res.Triggers[0].RunKey != "k1" {
		t.Fatalf("triggers = %+v, want one with run_key k1", res.Triggers)
	}
	if res.CursorAfter == nil || *res.CursorAfter != "c9" {
		t.Fatalf("cursor after = %v, want c9", res.CursorAfter)
	}
}

// TestSkippedByReason is the skip-with-reason row: no triggers, a skip_reason,
// the sensor's own words travel as ReasonText.
func TestSkippedByReason(t *testing.T) {
	res := evalPlain(t, "sensor-skip")
	if res.Outcome != Skipped || res.ReasonCode != reason.TICKSkippedSensor {
		t.Fatalf("outcome = %v reason = %s, want Skipped/%s", res.Outcome, res.ReasonCode, reason.TICKSkippedSensor)
	}
	if res.ReasonText != "no new files" {
		t.Fatalf("reason text = %q, want the sensor's own words", res.ReasonText)
	}
}

// TestSkippedBySilence covers silence: exit 0 with no stdout is a skip with a
// fixed explanation, not a guess.
func TestSkippedBySilence(t *testing.T) {
	res := evalPlain(t, "sensor-empty")
	if res.Outcome != Skipped || res.ReasonCode != reason.TICKSkippedSensor {
		t.Fatalf("outcome = %v reason = %s, want skipped/%s", res.Outcome, res.ReasonCode, reason.TICKSkippedSensor)
	}
	if res.ReasonText != "no output from sensor" {
		t.Fatalf("reason text = %q, want the fixed silence explanation", res.ReasonText)
	}
}

// TestTriggeredWinsOverSkip is the both-set corner: triggers win, the skip
// text rides along as the note.
func TestTriggeredWinsOverSkip(t *testing.T) {
	res := evalPlain(t, "sensor-skip-and-triggers")
	if res.Outcome != Triggered {
		t.Fatalf("outcome = %v, want Triggered even though skip_reason is set", res.Outcome)
	}
	if len(res.Triggers) != 1 || res.Triggers[0].RunKey != "k-trig" {
		t.Fatalf("triggers = %+v, want the winning trigger", res.Triggers)
	}
	if res.ReasonText != "fallback text" {
		t.Fatalf("skip note = %q, want the sensor's words preserved", res.ReasonText)
	}
}

// TestInvalidJSON is the unreadable-output row: an output error, nothing
// triggers, the cursor is never advanced.
func TestInvalidJSON(t *testing.T) {
	res := evalPlain(t, "sensor-invalid-json")
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorSensorOutput {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorSensorOutput)
	}
	if res.CursorAfter != nil {
		t.Fatalf("a failed run advanced the cursor: %v", res.CursorAfter)
	}
}

// TestStdOutOverCap is the output-limit row: more stdout than the cap is an
// errored outcome, never a truncated parse.
func TestStdOutOverCap(t *testing.T) {
	res := evalBounded(t, time.Minute, context.Background(), newTestEvaluator(),
		baseSpec(t, fakecmd(t), "spew", "4"), inp()) // 4 MiB against a 64 KiB cap
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorSensorOutput {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorSensorOutput)
	}
	if !res.OutputOverflow {
		t.Fatal("overflow flag not set")
	}
	if res.CursorAfter != nil {
		t.Fatalf("an oversized output advanced the cursor: %v", res.CursorAfter)
	}
}

// TestExit75 is the transient row: exit 75 carries the transient marker so the
// breaker layer (M3-05) can tell a busy moment from a crash.
func TestExit75(t *testing.T) {
	res := evalPlain(t, "exit", "75")
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorSensorFailed {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorSensorFailed)
	}
	if res.ReasonData["transient"] != true {
		t.Fatalf("exit 75 not marked transient: %v", res.ReasonData)
	}
}

// TestExit64 is the config row: exit 64 means the definition is wrong, not the
// run.
func TestExit64(t *testing.T) {
	res := evalPlain(t, "exit", "64")
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorConfig {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorConfig)
	}
}

// TestExitOther is the generic-failure row: any other nonzero exit.
func TestExitOther(t *testing.T) {
	res := evalPlain(t, "exit", "9")
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorSensorFailed {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorSensorFailed)
	}
	if res.ReasonData["exit_code"] != 9 {
		t.Fatalf("exit code not in reason data: %v", res.ReasonData)
	}
}

// TestStderrTail is the 4 KiB excerpt contract: the sensor's stderr is carried
// on the Result.
func TestStderrTail(t *testing.T) {
	res := evalPlain(t, "sensor-stderr-tag")
	if !strings.Contains(res.StderrExcerpt, "SENSOR_STDERR_MARKER") {
		t.Fatalf("stderr excerpt = %q, want the sensor's marker", res.StderrExcerpt)
	}
}

// TestDenyByDefaultEnv seeds a variable in the daemon's environment and proves
// it stays invisible to the sensor; the contract PACEQ_ keys are visible.
func TestDenyByDefaultEnv(t *testing.T) {
	t.Setenv("PACEQ_LEAK_PROBE", "must-not-leak")
	dir := t.TempDir()
	out := filepath.Join(dir, "env.json")
	res := evalBounded(t, time.Minute, context.Background(), newTestEvaluator(),
		baseSpec(t, fakecmd(t), "sensor-env-dump", out), inp())
	if res.Outcome == Errored {
		t.Fatalf("env dump unexpectedly failed: %v", res.ReasonData)
	}
	var env map[string]string
	if err := json.Unmarshal(mustRead(t, out), &env); err != nil {
		t.Fatalf("parse env dump: %v", err)
	}
	if _, ok := env["PACEQ_LEAK_PROBE"]; ok {
		t.Fatalf("deny by default broken: daemon var leaked into the sensor")
	}
	if env["PACEQ_SENSOR"] != "dropzone" || env["PACEQ_JOB"] != "import-file" {
		t.Fatalf("contract env missing: %+v", env)
	}
	if env["PACEQ_CURSOR"] != "2026-08-21/03-11-02.csv" {
		t.Fatalf("cursor env = %q, want the inbound cursor", env["PACEQ_CURSOR"])
	}
}

// TestTimeoutKillsTheWholeGroup proves the hard timeout ends a hanging sensor
// and its whole process group, grandchildren that ignore SIGTERM included.
func TestTimeoutKillsTheWholeGroup(t *testing.T) {
	marker := "PACEQSNSTIMEOUT_" + strconv.Itoa(int(time.Now().UnixNano()))
	ev := newTestEvaluator()
	spec := baseSpec(t, fakecmd(t), "tree", "1h", marker)
	spec.Timeout = 300 * time.Millisecond
	res := evalBounded(t, time.Minute, context.Background(), ev, spec, inp())
	if res.Outcome != Errored || res.ReasonCode != reason.TICKErrorSensorTimeout {
		t.Fatalf("outcome = %v reason = %s, want Errored/%s", res.Outcome, res.ReasonCode, reason.TICKErrorSensorTimeout)
	}
	if !res.TimedOut {
		t.Fatal("timed out flag not set")
	}
	waitForMarkerGone(t, marker, 10*time.Second)
}
