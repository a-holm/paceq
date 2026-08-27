package store

import (
	"time"
)

// Policies are the retention configuration keys and their defaults. Every
// value in the table below is a named key an operator can set; these are the
// values used when nothing is set (06 section 9.4 merged with 07 section 6.2).
//
//	job_versions        never auto-deleted (too small to matter, history must keep meaning)
//	outages             kept forever (small and precious)
//	batch limit         200 rows per transaction (PruneBatchLimit; measured
//	                    against the 50 ms lock budget - see retention.go)
//	batch pause         50 ms between batches
//
// The batch shape is deliberately not configurable: it is the mechanism that
// keeps the write lock short, not a tuning knob.
type Policies struct {
	// LogShardDays removes whole log date shards older than this.
	// Key: retention.log_shard_days. Default 14.
	LogShardDays int

	// RunsDays deletes terminal runs older than this; children cascade.
	// Key: retention.runs_days. Default 90.
	RunsDays int

	// RunsKeepMin keeps at least this many newest finished runs per job,
	// regardless of age. Key: retention.runs_keep_min. Default 50.
	RunsKeepMin int

	// TicksSkippedDays deletes ticks with outcome 'skipped' older than this.
	// Key: retention.ticks_skipped_days. Default 7.
	TicksSkippedDays int

	// TicksDays deletes other ticks older than this.
	// Key: retention.ticks_days. Default 90.
	TicksDays int

	// TicksKeepMin keeps at least this many newest ticks per source.
	// Key: retention.ticks_keep_min_per_source. Default 200.
	TicksKeepMin int

	// RunKeysDays deletes dedup keys first seen before this horizon. It is
	// the longest one on purpose, and deleting a key means the trigger it
	// deduplicated can fire again. Key: retention.run_keys_days. Default 365.
	RunKeysDays int

	// SessionsDays deletes stopped daemon sessions older than this, except
	// the keep-minimum below and sessions outages still cite.
	// Key: retention.daemon_sessions_days. Default 90.
	SessionsDays int

	// SessionsKeepMin keeps at least this many newest daemon sessions.
	// Key: retention.daemon_sessions_keep_min. Default 50.
	SessionsKeepMin int

	// OutboxDeliveredDays deletes DELIVERED notification rows older than
	// this (#29). Delivered rows are history until then, and failed rows
	// are kept for ever regardless. Key: retention.outbox_delivered_days.
	// Default 30 (06 section 9.4 via the issue's design note; 07's seven
	// day sketch was superseded by the owner's decision).
	OutboxDeliveredDays int

	// BackupRetain is how many verified backup generations are kept.
	// Key: backup.retain. Default 14.
	BackupRetain int
}

// DefaultPolicies returns the shipped configuration defaults. Tests pin this
// table field by field: a default that drifts is a silent policy change for
// every installation that never set the key.
func DefaultPolicies() Policies {
	return Policies{
		LogShardDays:        14,
		RunsDays:            90,
		RunsKeepMin:         50,
		TicksSkippedDays:    7,
		TicksDays:           90,
		TicksKeepMin:        200,
		RunKeysDays:         365,
		SessionsDays:        90,
		SessionsKeepMin:     50,
		OutboxDeliveredDays: 30,
		BackupRetain:        14,
	}
}

// withDefaults fills zero fields from DefaultPolicies, so a caller may set
// only the keys it cares about.
func (p Policies) WithDefaults() Policies {
	d := DefaultPolicies()
	if p.LogShardDays <= 0 {
		p.LogShardDays = d.LogShardDays
	}
	if p.RunsDays <= 0 {
		p.RunsDays = d.RunsDays
	}
	// The keep-minimum floors treat zero as unset like every other field:
	// a Policies left to its zero value must never mean "protect nothing",
	// which is what letting the zero through would do.
	if p.RunsKeepMin <= 0 {
		p.RunsKeepMin = d.RunsKeepMin
	}
	if p.TicksSkippedDays <= 0 {
		p.TicksSkippedDays = d.TicksSkippedDays
	}
	if p.TicksDays <= 0 {
		p.TicksDays = d.TicksDays
	}
	if p.TicksKeepMin <= 0 {
		p.TicksKeepMin = d.TicksKeepMin
	}
	if p.RunKeysDays <= 0 {
		p.RunKeysDays = d.RunKeysDays
	}
	if p.SessionsDays <= 0 {
		p.SessionsDays = d.SessionsDays
	}
	if p.SessionsKeepMin <= 0 {
		p.SessionsKeepMin = d.SessionsKeepMin
	}
	if p.OutboxDeliveredDays <= 0 {
		p.OutboxDeliveredDays = d.OutboxDeliveredDays
	}
	if p.BackupRetain <= 0 {
		p.BackupRetain = d.BackupRetain
	}
	return p
}

// Cutoff helpers. One per rule so the estimate queries and the delete loops
// can never disagree about where the horizon is.

func runsCutoff(now time.Time, p Policies) time.Time { return now.AddDate(0, 0, -p.RunsDays) }

func skippedTicksCutoff(now time.Time, p Policies) time.Time {
	return now.AddDate(0, 0, -p.TicksSkippedDays)
}

func ticksCutoff(now time.Time, p Policies) time.Time { return now.AddDate(0, 0, -p.TicksDays) }

func outboxCutoff(now time.Time, p Policies) time.Time {
	return now.AddDate(0, 0, -p.OutboxDeliveredDays)
}

func runKeysCutoff(now time.Time, p Policies) time.Time { return now.AddDate(0, 0, -p.RunKeysDays) }

func sessionsCutoff(now time.Time, p Policies) time.Time { return now.AddDate(0, 0, -p.SessionsDays) }
