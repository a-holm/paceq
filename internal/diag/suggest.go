package diag

import (
	"sort"
	"strings"
)

// MaxDistance is how far a typed name may sit from a known one and still be
// offered as what the user meant (03 section 8.3). Two edits covers a
// transposition, a doubled letter and a wrong plural, which is what field name
// typos actually are. Three starts suggesting "needs" for "no".
const MaxDistance = 2

// Suggest returns the known name closest to what was written, or an empty
// string when nothing is close enough. A candidate that starts with what was
// written counts however long the rest is, because a half typed name is not a
// typo, and a name that already matches one of the known ones exactly is not a
// mistake worth a suggestion.
//
// Ties break on the shortest candidate and then alphabetically, so the same
// input always produces the same suggestion. The candidate list may arrive in
// any order.
func Suggest(written string, known []string) string {
	if written == "" || len(known) == 0 {
		return ""
	}
	lowered := strings.ToLower(written)

	candidates := append([]string(nil), known...)
	sort.Strings(candidates)

	best, bestDistance := "", MaxDistance+1
	for _, candidate := range candidates {
		if candidate == written {
			return ""
		}
		name := strings.ToLower(candidate)
		distance := Distance(lowered, name)
		// A candidate that extends what was written is offered whatever the
		// distance: somebody who wrote "retr" for "retry" made no spelling
		// mistake, they stopped early.
		//
		// Only that direction. The other one, a candidate that is a prefix of
		// what was written, makes "env" beat "env_file" for "envfile", which
		// is a suggestion to delete the half of the name that was right.
		if strings.HasPrefix(name, lowered) {
			distance = 0
		}
		if distance > MaxDistance {
			continue
		}
		if distance < bestDistance || (distance == bestDistance && len(candidate) < len(best)) {
			best, bestDistance = candidate, distance
		}
	}
	return best
}

// Distance is the Levenshtein edit distance between two strings, counted in
// runes so a multi-byte character costs one edit rather than its byte length.
func Distance(a, b string) int {
	from, to := []rune(a), []rune(b)
	if len(from) == 0 {
		return len(to)
	}
	if len(to) == 0 {
		return len(from)
	}

	// Two rows rather than the full matrix: the recurrence only ever reads the
	// row above, and a field name is short enough that the allocation is the
	// expensive part.
	previous := make([]int, len(to)+1)
	current := make([]int, len(to)+1)
	for j := range previous {
		previous[j] = j
	}
	for i := 1; i <= len(from); i++ {
		current[0] = i
		for j := 1; j <= len(to); j++ {
			cost := 1
			if from[i-1] == to[j-1] {
				cost = 0
			}
			current[j] = min(previous[j]+1, current[j-1]+1, previous[j-1]+cost)
		}
		previous, current = current, previous
	}
	return previous[len(to)]
}
