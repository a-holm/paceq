package cronx

import (
	"fmt"
	"strings"
	"time"
)

// LoadZone validates a time zone name and returns it. Validation happens when
// a schedule is applied or validated, never inside the tick loop: by the time
// the iterator runs the zone must already be known good.
//
// "Local" is refused on purpose: it depends on the host's environment and
// would make schedules non deterministic across machines. Use an explicit
// IANA name or UTC.
func LoadZone(name string) (*time.Location, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return nil, fmt.Errorf("empty time zone name: use an IANA zone name such as Europe/Oslo, Asia/Kolkata or UTC")
	}
	if strings.EqualFold(trimmed, "Local") {
		return nil, fmt.Errorf("time zone name %q depends on the host environment: pass an explicit IANA zone name such as Europe/Oslo, or UTC", name)
	}
	if strings.EqualFold(trimmed, "UTC") {
		return time.UTC, nil
	}
	loc, err := time.LoadLocation(trimmed)
	if err == nil {
		return loc, nil
	}
	if suggest := suggestZone(trimmed); suggest != "" {
		return nil, fmt.Errorf("invalid time zone name %q: did you mean %q? use an IANA zone name such as Europe/Oslo, Asia/Kolkata or UTC", name, suggest)
	}
	return nil, fmt.Errorf("invalid time zone name %q: no close match in the IANA zone database; use an IANA zone name such as Europe/Oslo, Asia/Kolkata or UTC", name)
}

// maxSuggestDistance caps how far a suggestion may sit from the input. Below
// three edits, typos dominate; above them, "suggestions" become noise.
const maxSuggestDistance = 3

// suggestZone returns the closest known zone name within the edit distance
// budget, breaking ties alphabetically, or "" when nothing is close enough.
func suggestZone(name string) string {
	lower := strings.ToLower(name)
	best := ""
	bestDist := maxSuggestDistance + 1
	for _, cand := range zoneNames {
		d := editDistance(lower, strings.ToLower(cand))
		switch {
		case d < bestDist:
			best, bestDist = cand, d
		case d == bestDist && cand < best:
			best = cand
		}
	}
	if bestDist > maxSuggestDistance {
		return ""
	}
	return best
}

// editDistance is the Levenshtein distance between two strings, compared rune
// by rune so multi byte names compare correctly.
func editDistance(a, b string) int {
	ar, br := []rune(a), []rune(b)
	prev := make([]int, len(br)+1)
	cur := make([]int, len(br)+1)
	for j := range prev {
		prev[j] = j
	}
	for i := 1; i <= len(ar); i++ {
		cur[0] = i
		for j := 1; j <= len(br); j++ {
			cost := 1
			if ar[i-1] == br[j-1] {
				cost = 0
			}
			m := prev[j] + 1
			if cur[j-1]+1 < m {
				m = cur[j-1] + 1
			}
			if prev[j-1]+cost < m {
				m = prev[j-1] + cost
			}
			cur[j] = m
		}
		prev, cur = cur, prev
	}
	return prev[len(br)]
}
