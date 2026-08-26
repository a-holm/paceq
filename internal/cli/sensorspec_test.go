package cli

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// The frozen sensor contract (docs/reference/sensor-contract.md, Environment)
// promises that a variable the sensor declared in its own definition is
// visible to the subprocess, and that workdir is where it starts. The CLI is
// the seam that turns a stored row into that program, so these tests pin the
// whole declared contract at the seam: every field the store writes
// (internal/store sensorExecJSON: run, working_dir, env) arrives in the
// evaluator's spec, and nothing else leaks through.

func TestSensorSpecFromRowCarriesTheDeclaredContract(t *testing.T) {
	spec, err := sensorSpecFromRow(sensorRowWithExec(t, map[string]any{
		"run":         []string{"./sensors/new-files.sh"},
		"working_dir": "/srv/dropzone",
		"env":         map[string]string{"WATCH_DIR": "/srv/dropzone", "FLOCK_FILE": "/tmp/l"},
	}))
	if err != nil {
		t.Fatalf("sensorSpecFromRow: %v", err)
	}
	if want := []string{"./sensors/new-files.sh"}; !reflect.DeepEqual(spec.Argv, want) {
		t.Errorf("Argv = %v, want %v", spec.Argv, want)
	}
	if spec.Workdir != "/srv/dropzone" {
		t.Errorf("Workdir = %q, want /srv/dropzone (the contract starts the sensor there)", spec.Workdir)
	}
	wantEnv := map[string]string{"WATCH_DIR": "/srv/dropzone", "FLOCK_FILE": "/tmp/l"}
	if !reflect.DeepEqual(spec.Env, wantEnv) {
		t.Errorf("Env = %v, want %v (every declared variable must reach the subprocess)", spec.Env, wantEnv)
	}
}

func TestSensorSpecFromRowBareArrayFormStaysValid(t *testing.T) {
	// Rows seeded before the object form carry a bare argv array. They have
	// no declared environment and no working directory, and stay readable.
	spec, err := sensorSpecFromRow(sensorRowWithExec(t, nil))
	if err != nil {
		t.Fatalf("sensorSpecFromRow: %v", err)
	}
	if spec.Workdir != "" || spec.Env != nil {
		t.Errorf("bare array form gained fields: Workdir=%q Env=%v", spec.Workdir, spec.Env)
	}
}

func TestSensorSpecFromRowRejectsGarbage(t *testing.T) {
	for _, garbage := range []string{``, `not json`, `{}`} {
		if _, err := sensorSpecFromRow(rowWithExecJSON(garbage)); err == nil {
			t.Errorf("exec_json %q was accepted, want an error", garbage)
		}
	}
}

// sensorRowWithExec builds a SensorSummary whose exec_json is exactly the JSON
// shape the apply path writes (internal/store sensorExecJSON marshals this
// key set), so a test here cannot drift into testing a shape apply never
// writes. A nil exec means the earlier bare-array form.
func sensorRowWithExec(t *testing.T, exec map[string]any) store.SensorSummary {
	t.Helper()
	if exec == nil {
		exec = map[string]any{"run": []string{"/bin/echo", "hi"}}
	}
	raw, err := json.Marshal(exec)
	if err != nil {
		t.Fatalf("marshal exec_json: %v", err)
	}
	return rowWithExecJSON(string(raw))
}

func rowWithExecJSON(raw string) store.SensorSummary {
	return store.SensorSummary{
		Name:               "new-files",
		JobName:            "process-file",
		Kind:               "exec",
		ExecJSON:           raw,
		IntervalMS:         30000,
		MinIntervalMS:      1000,
		TimeoutMS:          30000,
		MaxTriggersPerTick: 100,
	}
}
