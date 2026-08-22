package cronx

// The gold standard fixtures are checked in data, generated offline by
// tools/gen-cron-fixtures/gen.py with Python zoneinfo and croniter. Nothing in
// CI runs Python or reaches the network, so these tests guard the checked in
// bytes alone.
//
// Three layers live here:
//
//  1. TestGoldenFixtureCoverage: the seven zone table from the issue is fully
//     present, with both DST policies wherever a policy matters.
//  2. TestGoldenFixturesAreWellFormed: structural rules every fixture must
//     hold, independent of any scheduler code.
//  3. TestGoldenRowsClassifyAgainstTheRuntimeZone: every expected row is
//     classified again with the runtime zone database. Category errors (a row
//     marked skipped that the zone says is ordinary, or the reverse) fail hard.
//     Instant level mismatches are reported as tzdata drift warnings, not
//     errors, because historical instants legitimately move when tzdata moves.
//
// If a fixture looks wrong, do not edit it by hand. Regenerate with
// tools/gen-cron-fixtures and put a FIXTURE-CHANGE: line with the reason in
// the commit message. The CI job named fixture-change enforces that line.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

const goldenDir = "testdata/golden"

type goldenPolicy struct {
	SpringForward string `json:"spring_forward"`
	FallBack      string `json:"fall_back"`
}

type goldenRow struct {
	At         string `json:"at"`
	Local      string `json:"local"`
	Skipped    bool   `json:"skipped"`
	SkipReason string `json:"skip_reason,omitempty"`
}

type goldenFixture struct {
	ID          string       `json:"id"`
	Expr        string       `json:"expr"`
	TZ          string       `json:"tz"`
	Policy      goldenPolicy `json:"policy"`
	FromUTC     string       `json:"from_utc"`
	ToUTC       string       `json:"to_utc"`
	GeneratedBy string       `json:"generated_by"`
	Expected    []goldenRow  `json:"expected"`
}

var (
	generatedByRe = regexp.MustCompile(`^croniter \d+\.\d+\.\d+ / python \d+\.\d+\.\d+ / tzdata \S+$`)
	localWallRe   = regexp.MustCompile(`^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} [+-]\d{2}:\d{2}$`)
)

func mustParseUTC(t *testing.T, s, what string) time.Time {
	t.Helper()
	tm, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("%s: %q is not RFC3339: %v", what, s, err)
	}
	if tm.Location() != time.UTC {
		t.Fatalf("%s: %q must be UTC", what, s)
	}
	return tm
}

func mustParseLocal(t *testing.T, s, what string) (int, time.Month, int, int, int, int) {
	t.Helper()
	if !localWallRe.MatchString(s) {
		t.Fatalf("%s: %q is not a local wall string of the form 2006-01-02 15:04:05 -07:00", what, s)
	}
	var y int
	var mo int
	var d int
	var h int
	var mi int
	var se int
	day := s[:10]
	clockPart := s[11:19]
	if _, err := fmt.Sscanf(day, "%d-%d-%d", &y, &mo, &d); err != nil {
		t.Fatalf("%s: bad date in %q: %v", what, s, err)
	}
	if _, err := fmt.Sscanf(clockPart, "%d:%d:%d", &h, &mi, &se); err != nil {
		t.Fatalf("%s: bad clock in %q: %v", what, s, err)
	}
	return y, time.Month(mo), d, h, mi, se
}

func loadGoldenFixtures(t *testing.T) map[string]goldenFixture {
	t.Helper()

	paths, err := filepath.Glob(filepath.Join(goldenDir, "*.json"))
	if err != nil {
		t.Fatalf("glob %s: %v", goldenDir, err)
	}
	if len(paths) == 0 {
		t.Fatalf("no fixtures in %s: run tools/gen-cron-fixtures/gen.py and commit the output", goldenDir)
	}

	fixtures := make(map[string]goldenFixture, len(paths))
	for _, p := range paths {
		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatalf("read %s: %v", p, err)
		}
		var fx goldenFixture
		if err := json.Unmarshal(raw, &fx); err != nil {
			t.Fatalf("decode %s: %v", p, err)
		}
		base := strings.TrimSuffix(filepath.Base(p), ".json")
		if fx.ID != base {
			t.Errorf("%s: id %q does not match file name %q", p, fx.ID, base)
		}
		if _, dup := fixtures[fx.ID]; dup {
			t.Errorf("%s: duplicate fixture id %q", p, fx.ID)
			continue
		}
		fixtures[fx.ID] = fx
	}
	return fixtures
}

// fixtureWindow describes one required window: a zone, a window that contains
// a named transition, and the expressions that must appear in it.
type fixtureWindow struct {
	name    string
	tz      string
	midUTC  string // a moment inside the window, used only for reporting
	exprs   []string
	policy  func(goldenPolicy) bool
	minHits int // distinct exprs required in the window
}

func springPolicies(p goldenPolicy) bool {
	return (p.SpringForward == "skip" || p.SpringForward == "shift") &&
		(p.FallBack == "" || p.FallBack == "first")
}

func fallPolicies(p goldenPolicy) bool {
	return (p.FallBack == "first" || p.FallBack == "both") &&
		(p.SpringForward == "" || p.SpringForward == "skip")
}

func defaultOnly(p goldenPolicy) bool {
	return (p.SpringForward == "" || p.SpringForward == "skip") &&
		(p.FallBack == "" || p.FallBack == "first")
}

func TestGoldenFixtureCoverage(t *testing.T) {
	fixtures := loadGoldenFixtures(t)

	windows := []fixtureWindow{
		{
			name:    "oslo spring 2026-03-29",
			tz:      "Europe/Oslo",
			midUTC:  "2026-03-29T00:00:00Z",
			exprs:   []string{"0 2 * * *", "30 2 * * *", "*/30 * * * *", "@every 90m"},
			policy:  springPolicies,
			minHits: 4,
		},
		{
			name:    "oslo fall 2026-10-25",
			tz:      "Europe/Oslo",
			midUTC:  "2026-10-25T00:00:00Z",
			exprs:   []string{"0 2 * * *", "30 2 * * *", "*/30 * * * *", "@every 90m"},
			policy:  fallPolicies,
			minHits: 4,
		},
		{
			name:    "santiago spring, which falls in September on the southern hemisphere",
			tz:      "America/Santiago",
			midUTC:  "2026-09-06T00:00:00Z",
			exprs:   []string{"0 0 * * *", "*/30 * * * *"},
			policy:  springPolicies,
			minHits: 2,
		},
		{
			name:    "santiago fall, which falls in April on the southern hemisphere",
			tz:      "America/Santiago",
			midUTC:  "2026-04-05T00:00:00Z",
			exprs:   []string{"30 23 * * *", "*/30 * * * *"},
			policy:  fallPolicies,
			minHits: 2,
		},
		{
			name:    "lord howe spring, the 30 minute jump",
			tz:      "Australia/Lord_Howe",
			midUTC:  "2026-10-03T15:00:00Z",
			exprs:   []string{"0 2 * * *", "*/30 * * * *"},
			policy:  springPolicies,
			minHits: 2,
		},
		{
			name:    "lord howe fall, the 30 minute duplicate",
			tz:      "Australia/Lord_Howe",
			midUTC:  "2026-04-04T15:00:00Z",
			exprs:   []string{"45 1 * * *", "*/30 * * * *"},
			policy:  fallPolicies,
			minHits: 2,
		},
		{
			name:    "kolkata, half hour offset without DST",
			tz:      "Asia/Kolkata",
			midUTC:  "2026-01-17T00:00:00Z",
			exprs:   []string{"45 9 * * *", "0 12 * * *"},
			policy:  defaultOnly,
			minHits: 2,
		},
		{
			name:    "kiritimati, the lost day of 1994-12-31",
			tz:      "Pacific/Kiritimati",
			midUTC:  "1995-01-01T00:00:00Z",
			exprs:   []string{"0 6 * * *", "30 12 * * *"},
			policy:  defaultOnly,
			minHits: 2,
		},
		{
			name:    "utc reference over the oslo spring window",
			tz:      "UTC",
			midUTC:  "2026-03-29T00:00:00Z",
			exprs:   []string{"0 2 * * *", "*/30 * * * *"},
			policy:  defaultOnly,
			minHits: 2,
		},
	}

	for _, w := range windows {
		w := w
		t.Run(w.name, func(t *testing.T) {
			mid := mustParseUTC(t, w.midUTC, "window midpoint")
			hits := make(map[string]bool)
			for id, fx := range fixtures {
				if fx.TZ != w.tz {
					continue
				}
				from := mustParseUTC(t, fx.FromUTC, id+" from_utc")
				to := mustParseUTC(t, fx.ToUTC, id+" to_utc")
				if from.Before(mid) && to.After(mid) && w.policy(fx.Policy) {
					hits[fx.Expr+"|"+fx.Policy.String()] = true
				}
			}
			for _, expr := range w.exprs {
				found := false
				for key := range hits {
					if strings.HasPrefix(key, expr+"|") {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("zone %s: no fixture covers expression %q inside the window with an allowed policy", w.tz, expr)
				}
			}
			if len(hits) < w.minHits {
				t.Errorf("zone %s: want at least %d expr/policy combinations in the window, got %d (%v)", w.tz, w.minHits, len(hits), keysOf(hits))
			}
		})
	}
}

// String renders a policy the way the fixtures spell it, for messages.
func (p goldenPolicy) String() string {
	return p.SpringForward + "/" + p.FallBack
}

func keysOf(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func TestGoldenFixturesAreWellFormed(t *testing.T) {
	fixtures := loadGoldenFixtures(t)

	seenGeneratedBy := ""
	for id, fx := range fixtures {
		id := id
		t.Run(id, func(t *testing.T) {
			if fx.Expr == "" {
				t.Fatal("expr is empty")
			}
			if fx.TZ == "" {
				t.Fatal("tz is empty")
			}
			if _, err := time.LoadLocation(fx.TZ); err != nil {
				t.Fatalf("unknown zone %q: %v", fx.TZ, err)
			}
			switch fx.Policy.SpringForward {
			case "", "skip", "shift":
			default:
				t.Fatalf("policy.spring_forward %q is not one of skip, shift", fx.Policy.SpringForward)
			}
			switch fx.Policy.FallBack {
			case "", "first", "both":
			default:
				t.Fatalf("policy.fall_back %q is not one of first, both", fx.Policy.FallBack)
			}
			from := mustParseUTC(t, fx.FromUTC, "from_utc")
			to := mustParseUTC(t, fx.ToUTC, "to_utc")
			if !from.Before(to) {
				t.Fatalf("from_utc %s is not before to_utc %s", fx.FromUTC, fx.ToUTC)
			}
			if !generatedByRe.MatchString(fx.GeneratedBy) {
				t.Fatalf("generated_by %q does not read \"croniter N.N.N / python N.N.N / tzdata TOKEN\"", fx.GeneratedBy)
			}
			if seenGeneratedBy == "" {
				seenGeneratedBy = fx.GeneratedBy
			} else if seenGeneratedBy != fx.GeneratedBy {
				t.Errorf("generated_by %q differs from %q: every fixture in one commit comes from one generator run", fx.GeneratedBy, seenGeneratedBy)
			}

			isInterval := strings.HasPrefix(fx.Expr, "@every ")

			prev := time.Time{}
			for i, row := range fx.Expected {
				at := mustParseUTC(t, row.At, fmt.Sprintf("expected[%d].at", i))
				// Non decreasing, not strictly ascending: on Pacific/Kiritimati
				// the whole day of 1994-12-31 is missing, so the skipped row's
				// bookkeeping instant equals the next day's real occurrence.
				if !prev.IsZero() && at.Before(prev) {
					t.Errorf("expected[%d].at %s is before the previous row %s: rows must not descend", i, row.At, prev.Format(time.RFC3339))
				}
				prev = at
				if !(from.Before(at) || from.Equal(at)) || at.After(to) {
					t.Errorf("expected[%d].at %s lies outside the half open window (%s, %s]", i, row.At, fx.FromUTC, fx.ToUTC)
				}
				switch {
				case row.Skipped && row.SkipReason == "":
					t.Errorf("expected[%d] is skipped but carries no skip_reason", i)
				case row.Skipped && row.SkipReason != "dst_nonexistent" && row.SkipReason != "dst_duplicate":
					t.Errorf("expected[%d].skip_reason %q is not one of dst_nonexistent, dst_duplicate", i, row.SkipReason)
				case !row.Skipped && row.SkipReason != "":
					t.Errorf("expected[%d] is not skipped but carries skip_reason %q", i, row.SkipReason)
				}
				if isInterval && row.Skipped {
					t.Errorf("expected[%d] is skipped: interval schedules are pure UTC arithmetic and never skip", i)
				}
				if row.Local == "-" {
					if !row.Skipped || row.SkipReason != "dst_nonexistent" {
						t.Errorf("expected[%d] hides its local time but is not a dst_nonexistent skip", i)
					}
					continue
				}
				if !localWallRe.MatchString(row.Local) {
					t.Errorf("expected[%d].local %q is not a local wall string of the form 2006-01-02 15:04:05 -07:00", i, row.Local)
				}
			}
		})
	}
}

// offsetsAround collects the distinct UTC offsets in effect within 30 hours
// either side of a pseudo instant. Every real rendering of a wall clock near
// that moment must use one of them.
func offsetsAround(loc *time.Location, center time.Time) []time.Duration {
	seen := map[int]bool{}
	var out []time.Duration
	for h := -30; h <= 30; h++ {
		off := center.Add(time.Duration(h) * time.Hour).In(loc)
		_, sec := off.Zone()
		if !seen[sec] {
			seen[sec] = true
			out = append(out, time.Duration(sec)*time.Second)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func wallMatches(cand time.Time, loc *time.Location, y int, mo time.Month, d, h, mi, se int) bool {
	local := cand.In(loc)
	y2, mo2, d2 := local.Date()
	h2, mi2, se2 := local.Clock()
	return y2 == y && mo2 == mo && d2 == d && h2 == h && mi2 == mi && se2 == se
}

// classifyWall answers whether a wall clock reading happens once, twice, or
// never in the zone. It uses only the runtime zone database, never the
// fixtures, so it is an independent opinion about every row. A reading is real
// only when some instant renders back onto exactly that wall clock, which is
// what makes the 24 hour dateline gap of Pacific/Kiritimati and the 30 minute
// steps of Australia/Lord_Howe both fall out without special cases.
func classifyWall(t *testing.T, loc *time.Location, y int, mo time.Month, d, h, mi, se int) (kind string, first, second time.Time) {
	t.Helper()

	pseudo := time.Date(y, mo, d, h, mi, se, 0, time.UTC)
	var hits []time.Time
	seenUnix := map[int64]bool{}
	for _, off := range offsetsAround(loc, pseudo) {
		cand := pseudo.Add(-off)
		if !wallMatches(cand, loc, y, mo, d, h, mi, se) {
			continue
		}
		u := cand.Unix()
		if seenUnix[u] {
			continue
		}
		seenUnix[u] = true
		hits = append(hits, cand.UTC())
	}
	switch len(hits) {
	case 1:
		return "normal", hits[0], time.Time{}
	case 2:
		if hits[1].Before(hits[0]) {
			hits[0], hits[1] = hits[1], hits[0]
		}
		return "duplicate", hits[0], hits[1]
	case 0:
		return "nonexistent", time.Time{}, time.Time{}
	default:
		t.Fatalf("wall %04d-%02d-%02d %02d:%02d:%02d in %s renders from %d distinct instants", y, mo, d, h, mi, se, loc, len(hits))
		return "", time.Time{}, time.Time{}
	}
}

func TestGoldenRowsClassifyAgainstTheRuntimeZone(t *testing.T) {
	fixtures := loadGoldenFixtures(t)

	driftWarnings := 0
	for id, fx := range fixtures {
		fx := fx
		t.Run(id, func(t *testing.T) {
			loc, err := time.LoadLocation(fx.TZ)
			if err != nil {
				t.Fatalf("load zone: %v", err)
			}
			if strings.HasPrefix(fx.Expr, "@every ") {
				// Interval rows are pure UTC arithmetic. Their local rendering
				// is decoration and may legitimately land on a duplicated or
				// missing wall clock, so there is nothing to classify.
				return
			}
			for i, row := range fx.Expected {
				if row.Local == "-" {
					// The row claims the wall time does not exist. There is
					// nothing to map back, and the category itself is checked
					// by the generator side. Skip quietly.
					continue
				}
				what := fmt.Sprintf("expected[%d]", i)
				y, mo, d, h, mi, se := mustParseLocal(t, row.Local, what)
				at := mustParseUTC(t, row.At, what+".at")

				kind, first, second := classifyWall(t, loc, y, mo, d, h, mi, se)

				switch kind {
				case "normal":
					if row.Skipped {
						t.Errorf("%s: the zone says %s is an ordinary single instant, the row claims %s", what, row.Local, skipName(row.SkipReason))
					} else if !first.Equal(at) {
						driftWarnings++
						t.Logf("WARNING possible tzdata drift: %s maps %s to %s but the fixture says %s (generated with %s)",
							what, row.Local, first.Format(time.RFC3339), row.At, fx.GeneratedBy)
					}
				case "duplicate":
					// A duplicated wall hosts two running rows under
					// fall_back both, and one running plus one skipped row
					// under fall_back first. Which instant belongs to this
					// row therefore depends on the policy of the fixture.
					switch {
					case !row.Skipped && fx.Policy.FallBack == "both":
						if !first.Equal(at) && !second.Equal(at) {
							t.Errorf("%s: the zone says %s happens twice at %s and %s, got %s which is neither",
								what, row.Local, first.Format(time.RFC3339), second.Format(time.RFC3339), row.At)
						}
					case !row.Skipped:
						if !first.Equal(at) {
							t.Errorf("%s: the zone says %s happens twice at %s and %s, an ordinary run must take the earlier instant, got %s",
								what, row.Local, first.Format(time.RFC3339), second.Format(time.RFC3339), row.At)
						}
					case row.SkipReason == "dst_duplicate":
						if !second.Equal(at) {
							t.Errorf("%s: the zone says %s happens twice at %s and %s, the skipped duplicate is the later instant, got %s",
								what, row.Local, first.Format(time.RFC3339), second.Format(time.RFC3339), row.At)
						}
					default:
						t.Errorf("%s: the zone says %s happens twice, the row claims %s", what, row.Local, skipName(row.SkipReason))
					}
				case "nonexistent":
					if !row.Skipped || row.SkipReason != "dst_nonexistent" {
						t.Errorf("%s: the zone says %s never happens, the row claims %s", what, row.Local, skipName(row.SkipReason))
					}
				}
			}
		})
	}
	if driftWarnings > 0 {
		t.Logf("%d instant level mismatches across the corpus: regenerate with tools/gen-cron-fixtures if the runtime tzdata moved", driftWarnings)
	}
}

func skipName(reason string) string {
	if reason == "" {
		return "an ordinary run"
	}
	return reason
}
