//go:generate go run ./gen

package store

import (
	"github.com/a-holm/paceq/internal/reason"
)

// The invariant catalogue: one row per named check the fsck sweep can emit,
// with the severity that decides what a finding does and the remedy an
// operator reads (M6-06, PLAN-acceptance). The sweep in fsck.go and
// quickfsck.go emits violations by these IDs and looks their metadata up
// here, so a report can never name a check the catalogue does not describe,
// and the generated reference (docs/reference/invarianter.md) cannot drift
// from the code.
//
// Severities are graded, not uniform (02 R11):
//
//   - Critical breaks the database's own uniqueness rules (I3, I6) or its
//     dependency graph (I9). They mean a hand edit or corruption; the daemon
//     refuses to start on one until an operator confirms a repair.
//   - Serious is state drift the system can reconcile out of: a lease that
//     died, a step outliving its run. Alarming, but boot continues.
//   - Warning is cosmetic or historic: rows written by an older paceq that
//     the current rules would have written differently.
//
// Every entry carries a non-empty Remedy, enforced by TestInvariantCatalogue
// the same way the reason catalogue enforces usable codes (06 section 2.1).

// Severity says how much damage a violation implies.
type Severity int

const (
	// Warning marks cosmetic or historic drift: nothing lies about what
	// happened, but the row would have been written differently today.
	Warning Severity = iota
	// Serious marks state drift the daemon reconciles out of on its own:
	// alarming, event-worthy, but not a reason to refuse startup.
	Serious
	// Critical marks a broken uniqueness rule or a cyclic graph: the state
	// is not one the code can reason about, so startup is refused until an
	// operator confirms.
	Critical
)

func (s Severity) String() string {
	switch s {
	case Warning:
		return "warning"
	case Serious:
		return "serious"
	case Critical:
		return "critical"
	default:
		return "unknown"
	}
}

// Invariant is one named rule the sweep checks.
type Invariant struct {
	// ID is the check's name in every report: "I1", "I3", "reason".
	ID string
	// Severity grades what the finding means.
	Severity Severity
	// Title is the rule in one line.
	Title string
	// Remedy is what to do about a finding. Obligatory: a report that names
	// a problem without a next step is only half a report.
	Remedy string
}

// Invariants is the catalogue, ordered by ID. The full sweep covers every
// entry; QuickFsck covers the critical subset.
var Invariants = []Invariant{
	{
		ID:       "I1",
		Severity: Serious,
		Title:    "a run in running holds a live lease",
		Remedy:   "the reaper clears this on its own within one lease TTL; paceq fsck --repair requeues the run now with epoch+1 and reason " + string(reason.RUNOrphanedReconciled),
	},
	{
		ID:       "I2",
		Severity: Serious,
		Title:    "a terminal run has no step still pending or running",
		Remedy:   "paceq fsck --repair cancels the stranded steps with a reason code, exactly as a shutdown would",
	},
	{
		ID:       "I3",
		Severity: Critical,
		Title:    "one run per (job, run_key)",
		Remedy:   "the unique rule is enforced by the schema, so a break means a hand edit or corruption; keep a copy of the state directory and restore from backup",
	},
	{
		ID:       "I5",
		Severity: Serious,
		Title:    "a step's attempt counter sits inside its budget",
		Remedy:   "a row was written behind the transition layer; report it as a bug with paceq export run <id>",
	},
	{
		ID:       "I6",
		Severity: Critical,
		Title:    "at most one tick per (source_kind, source_name, scheduled_for)",
		Remedy:   "the unique rule is enforced by the schema, so a break means a hand edit or corruption; keep a copy of the state directory and restore from backup",
	},
	{
		ID:       "I8",
		Severity: Serious,
		Title:    "a running step has every step it needs succeeded",
		Remedy:   "the claim predicate gates on the same rule, so this is drift; report it as a bug with paceq export run <id>",
	},
	{
		ID:       "I9",
		Severity: Critical,
		Title:    "the step dependency graph is acyclic and names existing steps",
		Remedy:   "a cyclic run can never finish: cancel it with paceq cancel run <id> and fix the job spec; a dangling edge means the job spec was edited mid-run",
	},
	{
		ID:       "I10",
		Severity: Serious,
		Title:    "a run's stored state is what its steps aggregate to",
		Remedy:   "paceq explain run <id> shows both stories; the reconciler repairs the aggregate on its next sweep",
	},
	{
		ID:       "I11",
		Severity: Serious,
		Title:    "the fencing token never falls",
		Remedy:   "an unfenced write happened; report it as a bug with the events of the named run (paceq export run <id>)",
	},
	{
		ID:       "I12",
		Severity: Serious,
		Title:    "no job or concurrency key runs past its ceiling",
		Remedy:   "admission control and the claim both enforce this on the way in; report it as a bug with the job name",
	},
	{
		ID:       "I13",
		Severity: Warning,
		Title:    "timestamps are monotone: created <= started <= finished, and a present stamp is a real one",
		Remedy:   "a clock jump under way; check the machine's clock sync with paceq doctor",
	},
	{
		ID:       "I14",
		Severity: Warning,
		Title:    "a run held for the future carries its defer reason",
		Remedy:   "paceq fsck --repair stamps the reason as unspecified, which the CLI then reports as held for an unknown reason",
	},
	{
		ID:       "I15",
		Severity: Warning,
		Title:    "every event's from_state is the event before it, per run and per step",
		Remedy:   "a hole in the history makes explain incomplete; report it as a bug with paceq export run <id>",
	},
	{
		ID:       "reason",
		Severity: Warning,
		Title:    "a terminal row carries a usable reason code",
		Remedy:   "rows from an older paceq; paceq fsck --repair stamps them as legacy, new rows are refused by the schema",
	},
}

// invariantByID indexes the catalogue for the sweep.
func invariantByID(id string) (Invariant, bool) {
	for _, inv := range Invariants {
		if inv.ID == id {
			return inv, true
		}
	}
	return Invariant{}, false
}

// severityOf looks a check's severity up in the catalogue. An unknown check
// reads as Serious: an unclassified finding must never silently downgrade to
// a warning, and the catalogue test keeps the case from arising.
func severityOf(id string) Severity {
	if inv, ok := invariantByID(id); ok {
		return inv.Severity
	}
	return Serious
}

// SeverityOf looks a check's severity up in the catalogue for callers outside
// the package, so the daemon's hourly sweep grades findings with the same
// table the fsck command uses.
func SeverityOf(id string) Severity {
	return severityOf(id)
}
