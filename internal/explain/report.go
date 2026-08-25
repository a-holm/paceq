package explain

import "time"

// SchemaVersion is the version of the JSON contract Report serialises to.
// It exists so a consumer can tell a shape change from a data change, and so
// the golden tests break visibly when the contract moves.
const SchemaVersion = 1

// readTimeout bounds every individual store call. A long-lived read snapshot
// is what starves WAL checkpointing, so explain holds none: one query at a
// time, each with its own short deadline.
const readTimeout = 5 * time.Second

// pageLimit caps how many timeline entries one report carries. The store
// pages by key underneath (ExplainPageSize rows per query), so a job with
// months of history reads as a bounded number of small queries, not one
// growing scan.
const pageLimit = 200

// Report is the whole answer for one subject: what it is, when it last did
// something, and the reverse chronological list of decisions inside the
// window. The structure is the stable contract external interfaces get; every
// field is self-contained and needs no follow-up query to be meaningful.
type Report struct {
	SchemaVersion int     `json:"schema_version"`
	Subject       Subject `json:"subject"`

	GeneratedAt int64 `json:"generated_at_ms"`
	Since       int64 `json:"since_ms"`

	// DaemonUp is what the caller observed about the daemon, carried here so
	// a consumer of --json sees the same fact the text form warns about.
	DaemonUp bool `json:"daemon_up"`

	Summary Summary `json:"summary"`
	Entries []Entry `json:"entries"`
}

// Subject names what is being explained, resolved to full names.
type Subject struct {
	Kind string `json:"kind"` // job | schedule | sensor | run
	Ref  string `json:"ref"`  // as the user typed it

	Job      string `json:"job,omitempty"`
	Schedule string `json:"schedule,omitempty"`
	Sensor   string `json:"sensor,omitempty"`

	// RunID is set on a run subject, always the whole id.
	RunID string `json:"run_id,omitempty"`
}

// Summary is the headline above the timeline: where things stand right now.
type Summary struct {
	LastSuccessAt *int64 `json:"last_success_at_ms,omitempty"`
	// LastRunAt and LastDurationMs describe the most recent run whatever its
	// outcome, which is how an all-clear report answers "is it fine?".
	LastRunAt      *int64 `json:"last_run_at_ms,omitempty"`
	LastDurationMs *int64 `json:"last_run_duration_ms,omitempty"`
	LastOutcome    string `json:"last_outcome,omitempty"`
	NextTickAt     *int64 `json:"next_tick_at_ms,omitempty"`
	FreshnessSLAMs *int64 `json:"freshness_sla_ms,omitempty"`
	FreshnessState string `json:"freshness_state"` // ok | breached | unknown
	Paused         bool   `json:"paused"`
	ActiveRuns     int    `json:"active_runs"`
	MaxConcurrent  int    `json:"max_concurrent"`
}

// Entry is one decision in the timeline. Terminal decisions - everything that
// ended in skipped, error, missed or deduped rather than in a running object -
// always carry a ReasonCode and at least one Hint.
type Entry struct {
	At          int64          `json:"at_ms"`
	Kind        string         `json:"kind"`    // tick | trigger | run | step | outage | event
	Actor       string         `json:"actor"`   // scheduler | sensor | dispatcher | executor | reaper | daemon | cli
	Ref         string         `json:"ref"`     // tick id, trigger id, run id, outage number
	Outcome     string         `json:"outcome"` // triggered | skipped | error | missed | deduped | accepted | rejected | succeeded | failed | ...
	ReasonCode  string         `json:"reason_code,omitempty"`
	ReasonText  string         `json:"reason_text,omitempty"`
	ReasonData  map[string]any `json:"reason_data,omitempty"`
	Hints       []string       `json:"hints,omitempty"`        // from internal/reason: what to do now
	RepeatCount int            `json:"repeat_count,omitempty"` // coalesced skips: one row says xN
	DurationMS  *int64         `json:"duration_ms,omitempty"`
	Children    []Entry        `json:"children,omitempty"`
}
