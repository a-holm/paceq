package reason

import "sort"

// Level is which object in the observability model the code explains. The four
// levels are the four tables a reason code can land in: ticks, triggers, runs
// and steps.
type Level string

const (
	LevelTick    Level = "tick"
	LevelTrigger Level = "trigger"
	LevelRun     Level = "run"
	LevelStep    Level = "step"
	LevelLease   Level = "lease"
)

// String returns the stored name of the level, the same one reason-codes.md
// groups by.
func (l Level) String() string { return string(l) }

// Code is one reason code. It is a plain string so it stores and ships as
// text everywhere: database column, JSON field, metric label. Nothing wraps
// or encodes it.
type Code string

// String returns the code as it is stored and printed.
func (c Code) String() string { return string(c) }

// Entry is everything the catalogue knows about one code. The anatomy is
// complete on purpose: short for lists, Explanation for `paceq error`, Remedy
// for what to do next, DataKeys for the shape of reason_data, Terminal for
// whether the code ends its object.
type Entry struct {
	Code        Code
	Level       Level
	Short       string
	Explanation string
	Remedy      []string
	DataKeys    []string
	Terminal    bool
}

// catalog is the whole catalogue, one entry per code. It is a variable only
// because Go requires that for maps at package level in this form; nothing
// writes to it after init, and the tests would catch any code that did.
var catalog = newCatalog()

// Lookup returns the entry for a code, and whether the code is in the
// catalogue at all. A false ok is the caller's sign to refuse the value, not
// to invent a fallback.
func Lookup(c Code) (Entry, bool) {
	e, ok := catalog[c]
	return e, ok
}

// IsKnown reports whether a code is in the catalogue. The empty code is not
// known: absence of an explanation is not an explanation.
func IsKnown(c Code) bool {
	_, ok := catalog[c]
	return ok
}

// All returns every entry sorted by code. The order is stable across calls,
// builds and machines, which is what makes the generated docs diffable.
func All() []Entry {
	all := make([]Entry, 0, len(catalog))
	for _, e := range catalog {
		all = append(all, e)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Code < all[j].Code })
	return all
}

// Codes returns every code as a plain string, sorted, the same order All()
// walks.
func Codes() []string {
	all := All()
	codes := make([]string, 0, len(all))
	for _, e := range all {
		codes = append(codes, string(e.Code))
	}
	return codes
}
