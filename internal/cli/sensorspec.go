package cli

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
)

// sensorSpecFromRow turns a store row into the runnable shape the evaluator
// needs. exec_json holds the exec adapter's configuration as JSON: either the
// frozen M3 contract object {"run":[...], "working_dir":..., "env":{...}} or a
// bare argv array for rows seeded before that object form (the store tests and
// older fixtures). The evaluator owns nothing about the database, so the CLI
// is the seam that reads a row and hands the spec over.
//
// The whole declared contract travels, not just the command: the frozen sensor
// contract (docs/reference/sensor-contract.md) promises that any variable the
// sensor declared in its own definition is visible to the subprocess, and that
// workdir is where it starts. Dropping either here would make `sensors test`
// and `sensors tick` evaluate a different program than the one applied.
func sensorSpecFromRow(row store.SensorSummary) (sensor.Spec, error) {
	exec, err := parseExecJSON(row.ExecJSON)
	if err != nil {
		return sensor.Spec{}, err
	}
	timeout := time.Duration(row.TimeoutMS) * time.Millisecond
	return sensor.Spec{
		Name:        row.Name,
		Job:         row.JobName,
		Argv:        exec.Run,
		Workdir:     exec.Workdir,
		Env:         exec.Env,
		Timeout:     timeout,
		MaxTriggers: row.MaxTriggersPerTick,
		Cursor:      row.Cursor,
	}, nil
}

// execConfig is the object form of exec_json, the frozen M3 contract.
type execConfig struct {
	Run     []string          `json:"run"`
	Workdir string            `json:"working_dir"`
	Env     map[string]string `json:"env"`
}

// parseExecJSON reads the exec adapter's configuration out of exec_json. The
// object form is the frozen M3 contract; a bare argv array is the earlier
// shape, which carries no working directory and no declared environment.
func parseExecJSON(execJSON string) (execConfig, error) {
	var cfg execConfig
	if strings.TrimSpace(execJSON) == "" {
		return cfg, fmt.Errorf("sensor spec is missing")
	}
	// Object form: {"run": [...], ...}.
	if err := json.Unmarshal([]byte(execJSON), &cfg); err == nil && len(cfg.Run) > 0 {
		return cfg, nil
	}
	// Bare argv array form.
	var argv []string
	if err := json.Unmarshal([]byte(execJSON), &argv); err == nil && len(argv) > 0 {
		return execConfig{Run: argv}, nil
	}
	return execConfig{}, fmt.Errorf("sensor spec is not a valid exec command: %s", execJSON)
}
