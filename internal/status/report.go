// Package status is the presentation layer behind `paceq status` (M5-03,
// issue #30): the command an operator reads fastest and a monitoring script
// runs most often. Like internal/explain it owns no SQL - internal/store
// reads, this package classifies and renders - and it answers through the
// read-only pool, so the daemon being down changes nothing.
//
// The contract this package serialises is pinned twice: schema_version on
// every JSON document, and a golden test that breaks visibly when the shape
// moves. See docs/reference/status-contract.md.
package status

import "time"

// Package-level constants...

// SchemaVersion is the version of the JSON contract this package writes.
// It exists so a consumer can tell a shape change from a data change, and so
// the golden tests break visibly when the contract moves.
const SchemaVersion = 1

// Job states, one per line of the overview. Deviations are failed, stuck and
// sla_breached: exactly the states exit code 5 reports. A paused job is an
// operator decision, not a deviation, and never moves the exit code away
// from 0.
const (
	StateOK          = "ok"
	StateIdle        = "idle" // the job exists, no run has finished yet
	StatePaused      = "paused"
	StateFailed      = "failed" // newest finished run failed, unconfirmed
	StateStuck       = "stuck"  // a running run lost its lease mid-flight
	StateSLABreached = "sla_breached"
)

// ConfirmWindow is how long a failure stays unconfirmed: a failed run with no
// successful run after it, inside this window of the moment asked about. The
// MVP has no acknowledgement mechanism, so confirmation is time-based - the
// definition the issue fixes, held here so tests and docs quote one number.
const ConfirmWindow = 24 * time.Hour

// DefaultVisibleJobs is how many lines the overview draws before it folds the
// rest into a "... and N more" line. Forty fits one screen with room for the
// aggregate line, which is the reading the command promises.
const DefaultVisibleJobs = 40

// Report is the whole answer for the project: one aggregate, one entry per
// job. The structure is the stable JSON contract external interfaces read;
// every field is self-contained.
type Report struct {
	SchemaVersion int     `json:"schema_version"`
	GeneratedAt   string  `json:"generated_at"`
	Daemon        Daemon  `json:"daemon"`
	Summary       Summary `json:"summary"`
	Jobs          []Job   `json:"jobs"`
}

// Daemon says what the command observed about the daemon. Up is a live socket
// dial, carried here so a --json consumer sees the same fact the text form
// marks. Since and Version come from the daemon's own session row and are
// absent when the daemon is down.
type Daemon struct {
	Up      bool   `json:"up"`
	Since   string `json:"since,omitempty"`
	Version string `json:"version,omitempty"`
}

// Summary is the aggregate line: the whole project's health in seven numbers.
type Summary struct {
	Jobs           int `json:"jobs"`
	Deviations     int `json:"deviations"`
	Running        int `json:"running"`
	Queued         int `json:"queued"`
	SLABreached    int `json:"sla_breached"`
	Failed24h      int `json:"failed_24h"`
	SensorsInError int `json:"sensors_in_error"`
}

// Job is one line of the overview. LastRun is absent for a job that has never
// finished a run, rather than the job being left out.
type Job struct {
	Name  string `json:"name"`
	State string `json:"state"`

	// LastRun is the newest finished run, absent for a job that has never
	// finished one.
	LastRun *LastRun `json:"last_run,omitempty"`

	// NextRunAt is the earliest pending fire-time of the job's standing
	// schedules, read pre-materialised from the scheduler.
	NextRunAt string `json:"next_run_at,omitempty"`

	// SensorIntervalMS is set when the job is sensor-driven: its next
	// chance to run is the sensor's next evaluation, not a schedule.
	SensorIntervalMS int64 `json:"sensor_interval_ms,omitempty"`

	// Hint is the runnable command that explains a deviation. Present only
	// on deviations - a permanent hint footer would be noise nobody reads.
	Hint string `json:"hint,omitempty"`
}

// LastRun is the newest finished run of one job. DurationMS is always
// present on a finished run, even when it read as zero: a run that finished
// in under a millisecond still has a duration, and an absent field would
// mean something else entirely.
type LastRun struct {
	ID         string `json:"id"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	Outcome    string `json:"outcome"`
	DurationMS int64  `json:"duration_ms"`
	ReasonCode string `json:"reason_code,omitempty"`
}

// Subject names what a reference report is about, resolved to full names.
type Subject struct {
	Kind     string `json:"kind"` // job | schedule | sensor | run
	Job      string `json:"job,omitempty"`
	Schedule string `json:"schedule,omitempty"`
	Sensor   string `json:"sensor,omitempty"`
	RunID    string `json:"run_id,omitempty"`
}

// RefReport is the status block for one reference: the same vocabulary as the
// overview, narrowed to one subject. Kind-specific facts live in their own
// objects so a job block never carries sensor fields.
type RefReport struct {
	SchemaVersion int     `json:"schema_version"`
	GeneratedAt   string  `json:"generated_at"`
	Daemon        Daemon  `json:"daemon"`
	Subject       Subject `json:"subject"`

	// State uses the job-state vocabulary for job references; a run
	// reference repeats the run's own state; schedule references are ok or
	// paused; sensor references are ok or failed while the sensor sits in
	// the failure path.
	State  string `json:"state"`
	Paused bool   `json:"paused,omitempty"`

	LastRun   *LastRun       `json:"last_run,omitempty"`
	NextRunAt string         `json:"next_run_at,omitempty"`
	Schedule  *ScheduleFacts `json:"schedule,omitempty"`
	Sensor    *SensorFacts   `json:"sensor,omitempty"`
	Run       *RunFacts      `json:"run,omitempty"`

	Hint string `json:"hint,omitempty"`
}

// ScheduleFacts is one schedule as a status block reads it back.
type ScheduleFacts struct {
	Name       string `json:"name"`
	Kind       string `json:"kind"`
	Expr       string `json:"expr"`
	Timezone   string `json:"timezone"`
	NextTickAt string `json:"next_tick_at,omitempty"`
}

// SensorFacts is one sensor as a status block reads it back: the drift state
// an operator acts on, not its whole definition.
type SensorFacts struct {
	IntervalMS          int64  `json:"interval_ms"`
	ConsecutiveFailures int    `json:"consecutive_failures"`
	BreakerOpen         bool   `json:"breaker_open"`
	LastOutcome         string `json:"last_outcome,omitempty"`
	NextEvalAt          string `json:"next_eval_at,omitempty"`
	PausedReason        string `json:"paused_reason,omitempty"`
}

// RunFacts is one run as a status block reads it back.
type RunFacts struct {
	ID         string `json:"id"`
	JobName    string `json:"job"`
	Origin     string `json:"origin"`
	State      string `json:"state"`
	ReasonCode string `json:"reason_code,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
}
