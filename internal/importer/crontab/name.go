package crontab

import (
	"regexp"
	"strconv"
	"strings"
)

// nameKeep mirrors internal/spec's job name alphabet; everything else in a
// derived name collapses to one dash.
var nameKeep = regexp.MustCompile(`[^a-z0-9_-]+`)

const maxNameLen = 63

// deriveName turns a program path into a job name candidate: the base name
// without its script extension, lower cased, with everything outside the
// spec's alphabet collapsed to a dash and repaired into the pattern's shape.
func deriveName(token string) string {
	base := token
	if i := strings.LastIndexAny(base, "/\\"); i >= 0 {
		base = base[i+1:]
	}
	if i := strings.IndexByte(base, '.'); i > 0 {
		base = base[:i]
	}
	name := strings.ToLower(base)
	name = nameKeep.ReplaceAllString(name, "-")
	name = strings.Trim(name, "-_")
	if name == "" || !legalOpener(name[0]) {
		name = strings.TrimRight("job-"+name, "-_")
	}
	if len(name) > maxNameLen {
		name = strings.TrimRight(name[:maxNameLen], "-_")
	}
	if name == "" {
		return "job"
	}
	return name
}

func legalOpener(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= '0' && c <= '9'
}

// uniqueName applies the prefix, then returns the first unused member of the
// family name, name-2, name-3... The loop is hard bounded: 10000 siblings of
// one basename cannot exist, and no input can spin it forever.
func uniqueName(prefix, base string, used map[string]int) string {
	prefixed := func(name string) string {
		if prefix != "" {
			return joinPrefix(sanitizePrefix(prefix), name)
		}
		return name
	}
	candidate := prefixed(base)
	// Provably bounded: when this runs, at most len(used) names exist, so
	// the family search stops within len(used)+2 attempts. No input, however
	// hostile, can spin it further.
	for attempt := 2; ; attempt++ {
		if _, taken := used[candidate]; !taken {
			used[candidate] = attempt - 1
			return candidate
		}
		candidate = prefixed(base + "-" + strconv.Itoa(attempt))
	}
}

// sanitizePrefix gives --name-prefix the same treatment as a derived name,
// so an odd flag value cannot produce a name spec would refuse.
func sanitizePrefix(p string) string {
	p = strings.ToLower(nameKeep.ReplaceAllString(p, "-"))
	p = strings.TrimLeft(p, "-_")
	if p == "" || !legalOpener(p[0]) {
		p = "x-" + p
	}
	return p
}

func joinPrefix(prefix, name string) string {
	full := prefix + name
	if len(full) > maxNameLen {
		full = strings.TrimLeft(full[len(full)-maxNameLen:], "-_")
		if full == "" || !legalOpener(full[0]) {
			full = "job-" + full
		}
	}
	return full
}
