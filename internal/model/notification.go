package model

import "time"

// Notification topics are a closed vocabulary: the only events the outbox
// carries in 1.0 (#29, SYNTESE section 3.18). Every topic maps to one kind of
// fact whose state change wrote the row, so an audit question ("did we notify
// about this?") always has a row to answer from.
const (
	// TopicRunFailed is written with the transaction that failed a run.
	TopicRunFailed = "run.failed"
	// TopicRunSucceeded is written with the transaction that succeeded a run.
	TopicRunSucceeded = "run.succeeded"
	// TopicSLABreached is written when a job's expected_within elapses
	// without a success; one notification per breach episode.
	TopicSLABreached = "job.sla_breached"
	// TopicDiskLow is written while the disk-guard holds new runs (#44);
	// the outbox throttle collapses a lasting episode into one alert.
	TopicDiskLow = "disk.low"
	// TopicWALGrowth is written while the WAL is past its warn or error
	// level (#44); a long-lived reader is the suspected cause.
	TopicWALGrowth = "wal.growth"
)

// Notification is one alert line for the outbox. The engine (or the SLA
// checker) prepares everything except the delivery facts; the store writes
// the row inside the same IMMEDIATE transaction as the state change that
// triggered it, applies dedup on DedupKey, and collapses repeats through the
// Throttle window keyed by GroupKey.
//
// A Notification has no identity of its own beyond its fields - it is a value
// on its way into outbox, never a handle to anything live.
type Notification struct {
	// Topic names the event from the closed vocabulary above.
	Topic string

	// Subject is the job or sensor name the event is about. It is what the
	// CLI filters on and what group_by counts as "one thing".
	Subject string

	// Target names the configured notifier that should receive the event.
	Target string

	// Payload is the event as JSON, serialised once before insertion. Not
	// a template and never re-rendered: what was stored is what was sent.
	Payload string

	// DedupKey makes one logical event un-insertable twice:
	// "<topic>|<subject>|<target>|<event key>". A UNIQUE index enforces it,
	// so calling the notification path any number of times still yields one
	// row per (event, rule).
	DedupKey string

	// GroupKey identifies the throttle bucket this event belongs to within
	// its topic and target, built from the configured group_by fields, e.g.
	// "reason_code=STEP_FAILED_NONZERO_EXIT". Empty means the rule groups by
	// nothing beyond subject already covered by topic|target.
	GroupKey string

	// Throttle is the minimum distance between two deliveries for one
	// group. Events arriving inside another's window are collapsed into it
	// (counted, not lost); zero delivers every deduplicated event.
	Throttle time.Duration

	// CreatedAt stamps when the event happened (UTC). It rides in the
	// payload's at field as well; the column is what the audit trail orders by.
	CreatedAt time.Time

	// AvailableAt is the earliest delivery attempt. Insertion means due
	// now, which callers express by passing CreatedAt.
	AvailableAt time.Time
}

// NotifyDefaults are the daemon-level fallbacks a job inherits when its own
// spec says nothing about notifications (config.yaml: notify_defaults).
type NotifyDefaults struct {
	// OnFailure lists the named notifiers told about every failing run.
	OnFailure []string
	// OnSuccess lists the named notifiers told about every successful run.
	// Empty by design: nobody gets spam by default.
	OnSuccess []string
	// Throttle is the collapse window shared by grouped notifications.
	Throttle time.Duration
	// GroupBy names the payload fields that build GroupKey. The accepted
	// vocabulary is closed: job, reason_code.
	GroupBy []string
	// MaxAttempts caps delivery retries before the row is given up on and
	// kept forever (never silently deleted). Zero falls back to the
	// dispatcher's own default.
	MaxAttempts int
}
