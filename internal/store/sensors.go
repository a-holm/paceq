package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/spec"
)

// Sensors are the declaration rows apply materialises from a job's sensors[]
// block. This file is the single owner of their SQL, mirroring how schedules.go
// owns the schedule rows: one task-specific method, not CRUD (11 section 6.1).
//
// The definition columns come from the spec and may be replaced by a re-apply.
// The drift columns (cursor, dedup_epoch, consecutive_failures, paused,
// last_error, breaker_opened_at, last_eval_at, next_eval_at, paused_reason)
// are owned by the evaluator and by the operator, never by a file: SyncSensors
// leaves them alone.

// SyncResult says what SyncSensors did, so a caller can tell the story of an
// apply without re-reading the table. Created and Updated both mean the row's
// definition changed; Unchanged means it was the same on arrival; Removed
// names the sensors that left the job's spec and were deleted.
type SyncResult struct {
	Created   []string
	Updated   []string
	Unchanged []string
	Removed   []string
}

// The lifecycle kinds recorded in sensor_events. Removed is the definition
// side's analogue of run_events: the act of a sensor leaving the spec.
const (
	lifecycleCreated     = "created"
	lifecycleSpecChanged = "spec_changed"
	lifecycleRemoved     = "removed"
)

// sensorExecJSON is the exec adapter's configuration: the subprocess and the
// environment it starts with. It is JSON because every sensor row shares a
// column shape with kinds that arrive later (M7-03, the file/http/sql sensors).
func sensorExecJSON(s spec.Sensor) string {
	exec := map[string]any{"run": s.Run}
	if s.Workdir != "" {
		exec["working_dir"] = s.Workdir
	}
	if len(s.Env) > 0 {
		exec["env"] = s.Env
	}
	exec["timeout_ms"] = s.Timeout.Milliseconds()
	bytes, _ := json.Marshal(exec)
	return string(bytes)
}

// sensorSpecJSON is the frozen definition form of one sensor. It is the
// idempotency guard: a re-apply that writes the same spec_json changes nothing.
// encoding/json sorts map keys, so the bytes are stable whatever order the
// fields were written in the file.
func sensorSpecJSON(s spec.Sensor) string {
	obj := map[string]any{
		"name":                  s.Name,
		"kind":                  s.Kind,
		"run":                   s.Run,
		"interval_ms":           s.Interval.Milliseconds(),
		"min_interval_ms":       s.MinInterval.Milliseconds(),
		"timeout_ms":            s.Timeout.Milliseconds(),
		"max_triggers_per_tick": s.MaxTriggersPerTick,
		"paused":                s.Paused,
	}
	if s.Workdir != "" {
		obj["workdir"] = s.Workdir
	}
	if len(s.Env) > 0 {
		obj["env"] = s.Env
	}
	if s.Description != "" {
		obj["description"] = s.Description
	}
	bytes, _ := json.Marshal(obj)
	return string(bytes)
}

// syncPlanItem is one line of work a SyncSensors call settled on before it
// takes the write lock.
type syncPlanItem struct {
	name   string
	sensor *spec.Sensor
	kind   string
}

// SyncSensors makes the sensor rows of one job identical to the job's spec,
// atomically, without touching drift state. New sensors become rows with
// cursor NULL, dedup_epoch 0, consecutive_failures 0 and next_eval_at now; a
// re-apply of an unchanged spec is a no-op against the definition columns, so
// no updated_at moves.
func (s *Store) SyncSensors(ctx context.Context, job string, sensors []spec.Sensor) (SyncResult, error) {
	now := s.clk.Now().UTC()
	at := now.UnixMilli()

	// The classification read happens before the transaction and the event ids
	// are minted there too: the write model forbids an entropy read while the
	// write lock is held, and a sync that knows what it is about to do before
	// it takes the lock keeps that rule.
	existing, err := currentSensorSpecs(ctx, s.r, job)
	if err != nil {
		return SyncResult{}, err
	}

	plan := buildSensorPlan(job, sensors, existing)
	var result SyncResult
	for _, item := range plan {
		switch item.kind {
		case lifecycleCreated:
			result.Created = append(result.Created, item.name)
		case lifecycleSpecChanged:
			result.Updated = append(result.Updated, item.name)
		case lifecycleRemoved:
			result.Removed = append(result.Removed, item.name)
		default:
			result.Unchanged = append(result.Unchanged, item.name)
		}
	}
	if len(plan) == 0 {
		return result, nil
	}

	eventIDs := make([]string, len(plan))
	for i := range plan {
		if eventIDs[i], err = id.New(now); err != nil {
			return SyncResult{}, fmt.Errorf("mint a sensor event id: %w", err)
		}
	}

	err = s.withTx(ctx, func(tx *sql.Tx) error {
		return syncSensorsTx(tx, at, job, plan, eventIDs)
	})
	if err != nil {
		return SyncResult{}, err
	}
	return result, nil
}

// currentSensorSpecs reads the names and frozen definitions of one job's
// sensor rows, outside any write transaction.
type sensorReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func currentSensorSpecs(ctx context.Context, r sensorReader, job string) (map[string]string, error) {
	rows, err := r.QueryContext(ctx, `SELECT name, spec_json FROM sensors WHERE job_name = ?`, job)
	if err != nil {
		return nil, fmt.Errorf("read the current sensors of job %s: %w", job, err)
	}
	defer func() { _ = rows.Close() }()

	existing := map[string]string{}
	for rows.Next() {
		var name, specJSON string
		if err := rows.Scan(&name, &specJSON); err != nil {
			return nil, fmt.Errorf("scan a sensor row of job %s: %w", job, err)
		}
		existing[name] = specJSON
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the current sensors of job %s: %w", job, err)
	}
	return existing, nil
}

// buildSensorPlan compares what the job wants against what the table holds
// and settles on the additions, replacements, no-ops and removals.
func buildSensorPlan(job string, sensors []spec.Sensor, existing map[string]string) []syncPlanItem {
	var plan []syncPlanItem
	want := make(map[string]bool, len(sensors))
	for i := range sensors {
		s := sensors[i]
		want[s.Name] = true
		last, wasThere := existing[s.Name]
		switch {
		case !wasThere:
			plan = append(plan, syncPlanItem{name: s.Name, sensor: &s, kind: lifecycleCreated})
		case last != sensorSpecJSON(s):
			plan = append(plan, syncPlanItem{name: s.Name, sensor: &s, kind: lifecycleSpecChanged})
		default:
			plan = append(plan, syncPlanItem{name: s.Name, kind: ""})
		}
	}
	for name := range existing {
		if !want[name] {
			plan = append(plan, syncPlanItem{name: name, kind: lifecycleRemoved})
		}
	}
	return plan
}

// syncSensorsTx performs the plan inside a transaction the caller owns. Every
// upsert and delete lands with its lifecycle event in the same transaction, so
// a reader never sees the sensor rows half way to the spec.
func syncSensorsTx(tx *sql.Tx, at int64, job string, plan []syncPlanItem, eventIDs []string) error {
	for i, item := range plan {
		switch item.kind {
		case lifecycleCreated, lifecycleSpecChanged:
			sensor := item.sensor
			if sensor == nil {
				return fmt.Errorf("sync %s: a %s plan item carries no sensor", job, item.kind)
			}
			paused := 0
			if sensor.Paused {
				paused = 1
			}
			// The drift columns are named in the INSERT only, never in the
			// conflict branch: a re-apply must not move the cursor or the
			// epoch, whatever the definition becomes.
			if _, err := tx.Exec(`INSERT INTO sensors
(name, job_name, kind, exec_json, spec_json, interval_ms, min_interval_ms,
 timeout_ms, max_triggers_per_tick, paused, cursor, dedup_epoch,
 consecutive_failures, next_eval_at, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, 0, 0, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET
    job_name              = excluded.job_name,
    kind                  = excluded.kind,
    exec_json             = excluded.exec_json,
    spec_json             = excluded.spec_json,
    interval_ms           = excluded.interval_ms,
    min_interval_ms       = excluded.min_interval_ms,
    timeout_ms            = excluded.timeout_ms,
    max_triggers_per_tick = excluded.max_triggers_per_tick,
    updated_at            = excluded.updated_at`,
				sensor.Name, job, sensor.Kind, sensorExecJSON(*sensor), sensorSpecJSON(*sensor),
				sensor.Interval.Milliseconds(), sensor.MinInterval.Milliseconds(),
				sensor.Timeout.Milliseconds(), sensor.MaxTriggersPerTick, paused,
				at, at, at); err != nil {
				return fmt.Errorf("sync sensor %s/%s: %w", job, sensor.Name, err)
			}
			if err := writeSensorEvent(tx, eventIDs[i], sensor.Name, job, item.kind, at); err != nil {
				return err
			}
		case lifecycleRemoved:
			// run_keys carries no foreign key to sensors: deleting the row
			// leaves the dedup table alone, so a re-added sensor of the same
			// name does not replay a burst of old keys.
			if _, err := tx.Exec(`DELETE FROM sensors WHERE name = ?`, item.name); err != nil {
				return fmt.Errorf("delete sensor %s: %w", item.name, err)
			}
			if err := writeSensorEvent(tx, eventIDs[i], item.name, job, lifecycleRemoved, at); err != nil {
				return err
			}
		}
	}
	return nil
}

// writeSensorEvent records one lifecycle change in sensor_events.
func writeSensorEvent(tx *sql.Tx, id, name, job, kind string, at int64) error {
	if _, err := tx.Exec(`INSERT INTO sensor_events (id, sensor_name, job_name, kind, at)
VALUES (?, ?, ?, ?, ?)`, id, name, job, kind, at); err != nil {
		return fmt.Errorf("record the %s event for sensor %s/%s: %w", kind, job, name, err)
	}
	return nil
}
