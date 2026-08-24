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
func sensorSpecFromRow(row store.SensorSummary) (sensor.Spec, error) {
	argv, err := parseExecJSON(row.ExecJSON)
	if err != nil {
		return sensor.Spec{}, err
	}
	timeout := time.Duration(row.TimeoutMS) * time.Millisecond
	return sensor.Spec{
		Name:        row.Name,
		Job:         row.JobName,
		Argv:        argv,
		Timeout:     timeout,
		MaxTriggers: row.MaxTriggersPerTick,
		Cursor:      row.Cursor,
	}, nil
}

// parseExecJSON reads the exec adapter's argv out of exec_json. The object
// form is the frozen M3 contract; a bare array is the earlier shape.
func parseExecJSON(execJSON string) ([]string, error) {
	if strings.TrimSpace(execJSON) == "" {
		return nil, fmt.Errorf("sensor spec is missing")
	}
	// Object form: {"run": [...], ...}.
	var obj struct {
		Run []string `json:"run"`
	}
	if err := json.Unmarshal([]byte(execJSON), &obj); err == nil && len(obj.Run) > 0 {
		return obj.Run, nil
	}
	// Bare argv array form.
	var argv []string
	if err := json.Unmarshal([]byte(execJSON), &argv); err == nil {
		return argv, nil
	}
	return nil, fmt.Errorf("sensor spec is not a valid exec command: %s", execJSON)
}
