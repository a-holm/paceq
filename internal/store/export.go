package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// RunExport is the complete evidence of one run, gathered for
// `pulseq export run` (06 section 9.4): the run row, its steps with their
// deps and artifacts, the event chain, the trigger and tick that caused it,
// and the frozen job version it executed against. Everything here is plain
// data so the archive stays readable without paceq.
type RunExport struct {
	Run        Run             `json:"run"`
	Steps      []Step          `json:"steps"`
	StepDeps   []ExportStepDep `json:"step_deps"`
	Events     []RunEvent      `json:"run_events"`
	Artifacts  []Artifact      `json:"artifacts"`
	Trigger    *ExportTrigger  `json:"trigger,omitempty"`
	Tick       *ExportTick     `json:"tick,omitempty"`
	JobVersion *ExportVersion  `json:"job_version,omitempty"`
}

// ExportStepDep is one edge of the run's step graph.
type ExportStepDep struct {
	StepName  string `json:"step_name"`
	DependsOn string `json:"depends_on"`
}

// ExportTrigger is the trigger row that produced (or refused) the run.
type ExportTrigger struct {
	ID         string    `json:"id"`
	TickID     string    `json:"tick_id"`
	JobName    string    `json:"job_name"`
	RunKey     string    `json:"run_key"`
	ParamsJSON string    `json:"params_json"`
	CreatedAt  time.Time `json:"created_at"`
	Outcome    string    `json:"outcome"`
	ReasonCode string    `json:"reason_code"`
	ReasonText string    `json:"reason_text"`
}

// ExportTick is the tick that fired the trigger. Optional fields stay
// pointers so an absent stamp exports as absent, not as the epoch.
type ExportTick struct {
	ID           string     `json:"id"`
	SourceKind   string     `json:"source_kind"`
	SourceName   string     `json:"source_name"`
	ScheduledFor *time.Time `json:"scheduled_for,omitempty"`
	StartedAt    time.Time  `json:"started_at"`
	FinishedAt   *time.Time `json:"finished_at,omitempty"`
	DurationMS   *int64     `json:"duration_ms,omitempty"`
	RepeatCount  int        `json:"repeat_count"`
	Outcome      string     `json:"outcome"`
	ReasonCode   string     `json:"reason_code"`
	ReasonText   string     `json:"reason_text"`
}

// ExportVersion is the frozen job version the run executed against: exactly
// what apply recorded, hash and all.
type ExportVersion struct {
	ID         string    `json:"id"`
	Version    int       `json:"version"`
	SpecHash   string    `json:"spec_hash"`
	SpecJSON   string    `json:"spec_json"`
	CreatedAt  time.Time `json:"created_at"`
	SourcePath string    `json:"source_path"`
}

// ExportRun gathers one run's whole evidence tree. The prefix may be any
// unambiguous prefix of the run id; ambiguity is an error, never a guess.
func (s *Store) ExportRun(ctx context.Context, idOrPrefix string) (RunExport, error) {
	detail, err := s.GetRun(ctx, idOrPrefix)
	if err != nil {
		return RunExport{}, err
	}
	runID := detail.Run.ID

	out := RunExport{
		Run:       detail.Run,
		Steps:     detail.Steps,
		StepDeps:  []ExportStepDep{},
		Events:    []RunEvent{},
		Artifacts: []Artifact{},
	}

	if err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `
SELECT step_name, depends_on FROM step_deps WHERE run_id = ? ORDER BY step_name, depends_on`, runID)
		if err != nil {
			return fmt.Errorf("read step deps of %s: %w", runID, err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var d ExportStepDep
			if err := rows.Scan(&d.StepName, &d.DependsOn); err != nil {
				return fmt.Errorf("scan a step dep of %s: %w", runID, err)
			}
			out.StepDeps = append(out.StepDeps, d)
		}
		return rows.Err()
	}); err != nil {
		return RunExport{}, err
	}

	if out.Events, err = s.RunEvents(ctx, runID); err != nil {
		return RunExport{}, err
	}
	if out.Artifacts, err = s.RunsArtifacts(ctx, runID); err != nil {
		return RunExport{}, err
	}

	if detail.Run.TriggerID != "" {
		if err := s.fillTriggerAndTick(ctx, &out); err != nil {
			return RunExport{}, err
		}
	}

	if detail.Run.JobVersionID != "" {
		jv, err := s.exportVersion(ctx, detail.Run.JobVersionID)
		if err != nil {
			return RunExport{}, err
		}
		out.JobVersion = jv
	}

	return out, nil
}

// fillTriggerAndTick reads the trigger by the run's trigger_id plus the tick
// it came from. A trigger whose tick retention already removed still exports,
// minus the tick side - retention drains run keys last for exactly this kind
// of reason.
func (s *Store) fillTriggerAndTick(ctx context.Context, out *RunExport) error {
	var tr ExportTrigger
	var tickFound bool
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		row := r.QueryRowContext(ctx, `
SELECT id, tick_id, job_name, COALESCE(run_key, ''), params_json, created_at,
       outcome, COALESCE(reason_code, ''), COALESCE(reason_text, '')
  FROM triggers WHERE id = ?`, out.Run.TriggerID)
		var created sql.NullInt64
		if err := row.Scan(&tr.ID, &tr.TickID, &tr.JobName, &tr.RunKey,
			&tr.ParamsJSON, &created, &tr.Outcome, &tr.ReasonCode, &tr.ReasonText); err != nil {
			return err
		}
		tr.CreatedAt = timeOrZero(created)

		tickRow := r.QueryRowContext(ctx, `
SELECT id, source_kind, source_name, scheduled_for, started_at, finished_at,
       duration_ms, repeat_count, outcome, COALESCE(reason_code, ''),
       COALESCE(reason_text, '')
  FROM ticks WHERE id = ?`, tr.TickID)
		tk, scanErr := scanExportTick(tickRow.Scan)
		switch {
		case scanErr == nil:
			out.Tick = &tk
			tickFound = true
		case !isNoRows(scanErr):
			return scanErr
		}
		return nil
	})
	if isNoRows(err) {
		// The trigger row vanished between reading the run and here; the
		// export still carries the run itself.
		return nil
	}
	if err != nil {
		return err
	}
	out.Trigger = &tr
	_ = tickFound
	return nil
}

func scanExportTick(scan func(dest ...any) error) (ExportTick, error) {
	var (
		tk           ExportTick
		scheduledFor sql.NullInt64
		startedAt    sql.NullInt64
		finishedAt   sql.NullInt64
		duration     sql.NullInt64
	)
	if err := scan(&tk.ID, &tk.SourceKind, &tk.SourceName, &scheduledFor,
		&startedAt, &finishedAt, &duration, &tk.RepeatCount, &tk.Outcome,
		&tk.ReasonCode, &tk.ReasonText); err != nil {
		return ExportTick{}, err
	}
	if scheduledFor.Valid {
		v := time.UnixMilli(scheduledFor.Int64).UTC()
		tk.ScheduledFor = &v
	}
	tk.StartedAt = timeOrZero(startedAt)
	if finishedAt.Valid {
		v := time.UnixMilli(finishedAt.Int64).UTC()
		tk.FinishedAt = &v
	}
	if duration.Valid {
		v := duration.Int64
		tk.DurationMS = &v
	}
	return tk, nil
}

// isNoRows reports whether err is database/sql's empty result.
func isNoRows(err error) bool {
	return err != nil && err == sql.ErrNoRows
}

// exportVersion reads the frozen job version row.
func (s *Store) exportVersion(ctx context.Context, versionID string) (*ExportVersion, error) {
	var v ExportVersion
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		row := r.QueryRowContext(ctx, `
SELECT id, version, spec_hash, spec_json, created_at, COALESCE(source_path, '')
  FROM job_versions WHERE id = ?`, versionID)
		var created sql.NullInt64
		if err := row.Scan(&v.ID, &v.Version, &v.SpecHash, &v.SpecJSON,
			&created, &v.SourcePath); err != nil {
			return err
		}
		v.CreatedAt = timeOrZero(created)
		return nil
	})
	if err != nil {
		if isNoRows(err) {
			return nil, nil
		}
		return nil, err
	}
	return &v, nil
}
