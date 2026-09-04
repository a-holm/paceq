package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/sensor"
	"github.com/a-holm/paceq/internal/store"
)

// The sensor half of the daemon: the two seams internal/sensor declares, bound
// to the store. The evaluator package promises it never writes to the
// database, and the store may not import it, so the adapters that read a row
// into a Spec and write a Result back live here, where both are in scope.
//
// The runtime owns concurrency, the per-sensor claim and the circuit breaker.
// Everything below is the database, including the trigger ceiling: it belongs
// to the commit, which is the one thing both a daemon wake and a forced
// `paceq sensors tick` go through.

// sensorStore is what the sensor wiring needs of the store.
type sensorStore interface {
	DueSensors(ctx context.Context, nowMilli int64, limit int) ([]store.SensorSummary, error)
	GetSensor(ctx context.Context, name string) (store.SensorSummary, error)
	BeginSensorTick(ctx context.Context, in store.BeginSensorTickInput) (store.BeginSensorTickResult, error)
	SensorCommitter
}

// SensorCommitter is the write half of one finished sensor evaluation.
type SensorCommitter interface {
	CommitSensorTick(ctx context.Context, in store.SensorTickCommitInput) (store.SensorTickCommitResult, error)
}

// sensorSource answers which sensors are due, out of the sensors table.
type sensorSource struct {
	st  sensorStore
	clk clock.Clock
	log *slog.Logger
}

// Due reads the due rows and turns each into a runnable spec. A row whose
// definition cannot be read is dropped rather than failing the wake: one
// broken sensor must not stop every other sensor from being evaluated.
func (s sensorSource) Due(ctx context.Context, limit int) ([]sensor.Spec, error) {
	rows, err := s.st.DueSensors(ctx, s.clk.Now().UTC().UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	specs := make([]sensor.Spec, 0, len(rows))
	for _, row := range rows {
		spec, err := SensorSpecFromRow(row)
		if err != nil {
			s.log.Warn("sensor definition is not runnable",
				"sensor", row.Name, "error", err.Error())
			continue
		}
		specs = append(specs, spec)
	}
	return specs, nil
}

// sensorSink records one evaluation: the intention row and the atomic commit,
// the pair `paceq sensors tick` performs, in the order it performs them. The
// runtime opens the tick through Begin before the sensor runs and closes it
// through Commit afterwards, so a daemon that dies mid-evaluation leaves a
// tick in 'running' for reconciliation to close, and the cursor CAS fences on
// the version read before the sensor answered.
type sensorSink struct {
	st      sensorStore
	clk     clock.Clock
	session string
}

// Begin opens the tick before the sensor runs. The version it carries is read
// here, so a reset or a cursor set that lands while the sensor is running
// bumps past it and the commit's CAS refuses the stale result.
func (s sensorSink) Begin(ctx context.Context, spec sensor.Spec) (sensor.Ticket, error) {
	begin, err := s.st.BeginSensorTick(ctx, store.BeginSensorTickInput{
		SensorName:      spec.Name,
		CursorBefore:    sensorCursorValue(spec.Cursor),
		DaemonSessionID: s.session,
		// The stamp is the instant the evaluation starts, which is now: the
		// min_interval floor counts from started_at, and the next evaluation
		// is told to look for work arriving since it.
		Now: s.clk.Now().UTC(),
	})
	if err != nil {
		return sensor.Ticket{}, err
	}
	return sensor.Ticket{ID: begin.TickID, Version: begin.CursorVersion}, nil
}

func (s sensorSink) Commit(ctx context.Context, spec sensor.Spec, tk sensor.Ticket, res sensor.Result) error {
	// The row is re-read here so the epoch, job and interval the commit
	// writes against are the current ones. It is a read, not a guard: the
	// guard is the version Begin fenced at, which this commit CASes on.
	row, err := s.st.GetSensor(ctx, spec.Name)
	if err != nil {
		return err
	}
	_, err = CommitSensorEvaluation(ctx, s.st, SensorEvaluation{
		Row:    row,
		Begin:  store.BeginSensorTickResult{TickID: tk.ID, CursorVersion: tk.Version},
		Result: res,
		Now:    s.clk.Now().UTC(),
	})
	return err
}

// SensorEvaluation is one finished evaluation, ready to be recorded: the
// sensor row the runs are written against, the intention row it is closed
// against, and what the evaluation decided.
type SensorEvaluation struct {
	Row    store.SensorSummary
	Begin  store.BeginSensorTickResult
	Result sensor.Result
	Now    time.Time
}

// SensorCommitResult is what one recorded evaluation did: the store's answer,
// plus the two facts the caller cannot recover from it. Outcome is the word
// written to ticks.outcome, and the truncation pair says whether the ceiling
// cut this batch, so a forced tick can tell the operator it was partial.
type SensorCommitResult struct {
	store.SensorTickCommitResult

	Outcome   string
	Truncated bool
	Dropped   int
}

// CommitSensorEvaluation translates one evaluation into the store's commit
// input and writes it. It is the single translation from a sensor.Result into
// tick, trigger, run and cursor rows, so a daemon evaluation and a forced
// `paceq sensors tick` can never disagree about an outcome, a reason code, a
// skip reason, the trigger ceiling or when the sensor is due again.
//
// The ceiling is applied here rather than by either caller (#215). It is the
// one point both evaluations pass through, and applying it twice would erase
// its own record: the second pass sees a batch inside the budget and reports
// nothing was dropped.
func CommitSensorEvaluation(ctx context.Context, st SensorCommitter, in SensorEvaluation) (SensorCommitResult, error) {
	res := in.Result
	truncated := sensor.ApplyLimit(&res, in.Row.MaxTriggersPerTick)
	dropped := len(in.Result.Triggers) - len(res.Triggers)

	outcome := store.OutcomeSkipped
	switch res.Outcome {
	case sensor.Triggered:
		outcome = store.OutcomeTriggered
	case sensor.Errored:
		outcome = store.OutcomeError
	}

	triggers := make([]store.SensorTrigger, 0, len(res.Triggers))
	for _, tr := range res.Triggers {
		params := ""
		if len(tr.Params) > 0 {
			params = string(tr.Params)
		}
		triggers = append(triggers, store.SensorTrigger{RunKey: tr.RunKey, ParamsJSON: params})
	}

	out, err := st.CommitSensorTick(ctx, store.SensorTickCommitInput{
		TickID:        in.Begin.TickID,
		SensorName:    in.Row.Name,
		JobName:       in.Row.JobName,
		CursorVersion: in.Begin.CursorVersion,
		CursorAfter:   sensorCursorValue(res.CursorAfter),
		DedupEpoch:    in.Row.DedupEpoch,
		Triggers:      triggers,
		Outcome:       outcome,
		ReasonCode:    res.ReasonCode,
		ReasonText:    res.ReasonText,
		ReasonData:    sensorReasonData(res.ReasonData),
		NextEvalAt:    in.Now.Add(time.Duration(in.Row.IntervalMS) * time.Millisecond).UnixMilli(),
		DurationMs:    res.DurationMS,
		Now:           in.Now,
	})
	if err != nil {
		return SensorCommitResult{}, fmt.Errorf("commit the sensor tick for %s: %w", in.Row.Name, err)
	}
	return SensorCommitResult{
		SensorTickCommitResult: out,
		Outcome:                outcome,
		Truncated:              truncated,
		Dropped:                dropped,
	}, nil
}

// sensorReasonData renders the evaluation's detail object for ticks.reason_data.
// A batch that was not cut says nothing about truncation: the marker is a fact
// about this evaluation, not a field every tick carries.
//
// An object that will not marshal is dropped rather than failing the commit:
// the tick is the record of what happened, and losing its detail is a smaller
// harm than losing the whole evaluation.
func sensorReasonData(data map[string]any) string {
	if len(data) == 0 {
		return ""
	}
	kept := make(map[string]any, len(data))
	for key, value := range data {
		if key == "truncated" && value == false {
			continue
		}
		kept[key] = value
	}
	if len(kept) == 0 {
		return ""
	}
	encoded, err := json.Marshal(kept)
	if err != nil {
		return ""
	}
	return string(encoded)
}

// SensorSpecFromRow turns a sensor row into the runnable shape the evaluator
// needs. exec_json holds the exec adapter's configuration as JSON: either the
// frozen M3 contract object {"run":[...], "working_dir":..., "env":{...}} or a
// bare argv array for rows seeded before that object form.
//
// The whole declared contract travels, not just the command: the frozen sensor
// contract (docs/reference/sensor-contract.md) promises that any variable the
// sensor declared is visible to the subprocess and that workdir is where it
// starts. Dropping either would evaluate a different program than the one
// applied.
func SensorSpecFromRow(row store.SensorSummary) (sensor.Spec, error) {
	exec, err := parseSensorExec(row.ExecJSON)
	if err != nil {
		return sensor.Spec{}, fmt.Errorf("sensor %s: %w", row.Name, err)
	}
	return sensor.Spec{
		Name:        row.Name,
		Job:         row.JobName,
		Argv:        exec.Run,
		Workdir:     exec.Workdir,
		Env:         exec.Env,
		Timeout:     time.Duration(row.TimeoutMS) * time.Millisecond,
		MaxTriggers: row.MaxTriggersPerTick,
		Cursor:      row.Cursor,
		LastTickAt:  row.LastTickAt,
	}, nil
}

// sensorExec is the object form of exec_json, the frozen M3 contract.
type sensorExec struct {
	Run     []string          `json:"run"`
	Workdir string            `json:"working_dir"`
	Env     map[string]string `json:"env"`
}

// parseSensorExec reads the exec adapter's configuration out of exec_json. The
// object form is the frozen M3 contract; a bare argv array is the earlier
// shape, which carries no working directory and no declared environment.
func parseSensorExec(execJSON string) (sensorExec, error) {
	var cfg sensorExec
	if strings.TrimSpace(execJSON) == "" {
		return cfg, fmt.Errorf("the exec spec is missing")
	}
	if err := json.Unmarshal([]byte(execJSON), &cfg); err == nil && len(cfg.Run) > 0 {
		return cfg, nil
	}
	var argv []string
	if err := json.Unmarshal([]byte(execJSON), &argv); err == nil && len(argv) > 0 {
		return sensorExec{Run: argv}, nil
	}
	return sensorExec{}, fmt.Errorf("the exec spec is not a valid command: %s", execJSON)
}

// sensorCursorValue renders an absent cursor as the empty string the store
// stores as NULL.
func sensorCursorValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
