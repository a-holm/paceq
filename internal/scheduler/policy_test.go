package scheduler

import (
	"fmt"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/reason"
)

// The acceptance matrix: daemon down for d, catchup set to p, limit l. After
// the drain (as many passes as the drip feed needs) the policy must have done
// exactly what it says: how many runs, and every owed fire-time carrying
// exactly one row with its reason.

const defaultWindowMS = 86_400_000 // 24 h, the schema default

// overdue generates the real occurrences of expr between from and now,
// exclusive of from, inclusive of now: exactly what discovery recomputes.
func overdue(t *testing.T, expr string, tz *time.Location, from, to time.Time) []cronx.Occurrence {
	t.Helper()
	sched, err := cronx.Parse(expr)
	if err != nil {
		t.Fatalf("parse %q: %v", expr, err)
	}
	occs, err := sched.Between(from, to, tz, cronx.Policy{})
	if err != nil && len(occs) == 0 {
		t.Fatalf("between [%s,%s]: %v", from, to, err)
	}
	var real []cronx.Occurrence
	for _, o := range occs {
		if !o.Skipped {
			real = append(real, o)
		}
	}
	return real
}

// drain runs applyCatchup the way the loop does, once per wake, advancing a
// fake cursor through committed attempts until nothing is owed any more. It
// returns every run attempt and every skip decision over the whole outage.
func drain(occs []cronx.Occurrence, catchup string, limit int, windowMS int64, now time.Time) ([]cronx.Occurrence, []skipDecision) {
	var allAttempts []cronx.Occurrence
	var allSkips []skipDecision
	for cursor := 0; cursor < len(occs); {
		attempts, skips := applyCatchup(occs[cursor:], catchup, limit, windowMS, now)
		allAttempts = append(allAttempts, attempts...)
		allSkips = append(allSkips, skips...)
		if len(attempts)+len(skips) == 0 {
			break // nothing attempted: the remainder stays owed forever
		}
		cursor += len(attempts) + len(skips)
	}
	return allAttempts, allSkips
}

func TestTheCatchupMatrixDoesWhatItSays(t *testing.T) {
	downtimes := map[string]time.Duration{
		"5min": 5 * time.Minute,
		"6h":   6 * time.Hour,
		"14d":  14 * 24 * time.Hour,
	}

	for downName, down := range downtimes {
		for _, catchup := range []string{"skip", "last", "all"} {
			for _, limit := range []int{1, 10} {
				name := fmt.Sprintf("%s/%s/limit%d", catchup, downName, limit)
				t.Run(name, func(t *testing.T) {
					// The restart lands two minutes after the last fire-time,
					// the way a real restart does: nothing owed is "happening
					// right now". The exact-boundary case has its own test.
					now := time.Date(2026, 3, 15, 12, 2, 0, 0, time.UTC)
					from := now.Add(-down)
					occs := overdue(t, "*/5 * * * *", time.UTC, from, now)
					if len(occs) == 0 {
						t.Fatal("the fixture generated no owed fire-times")
					}
					attempts, skips := drain(occs, catchup, limit, defaultWindowMS, now)

					var runs int
					switch catchup {
					case "skip":
						runs = 0 // everything owed is older than the fresh-now second
					case "last":
						runs = 1
					case "all":
						runs = len(occs) // 14d of */5 exceeds the 24h window only outside it
						for _, s := range skips {
							if s.Code == reason.TICKSkippedCatchupWindow {
								runs--
							}
						}
					}
					if len(attempts) != runs {
						t.Fatalf("catchup=%s downtime=%s limit=%d produced %d runs, want %d",
							catchup, downName, limit, len(attempts), runs)
					}

					// Sum invariant: triggered + skipped = owed, each exactly once.
					if len(attempts)+len(skips) != len(occs) {
						t.Fatalf("triggered(%d)+skipped(%d) != owed(%d)",
							len(attempts), len(skips), len(occs))
					}
					seen := map[string]int{}
					for _, o := range attempts {
						seen[o.At.Format(time.RFC3339)]++
					}
					for _, s := range skips {
						seen[s.Occurrence.At.Format(time.RFC3339)]++
					}
					for key, n := range seen {
						if n != 1 {
							t.Fatalf("fire-time %s carries %d rows, want exactly one", key, n)
						}
					}

					// Every skip says which rule dropped it.
					for _, s := range skips {
						switch s.Code {
						case reason.TICKSkippedCatchupDisabled,
							reason.TICKSkippedCatchupLastOnly,
							reason.TICKSkippedCatchupWindow:
						default:
							t.Fatalf("skip at %s carries code %s, not a catch-up reason",
								s.Occurrence.At.Format(time.RFC3339), s.Code)
						}
					}
				})
			}
		}
	}
}

func TestSkipPolicyRunsOnlyAFreshNowOccurrence(t *testing.T) {
	now := time.Date(2026, 3, 15, 12, 0, 30, 0, time.UTC)

	fresh := now.Add(-500 * time.Millisecond)
	stale := now.Add(-1500 * time.Millisecond)

	attempts, skips := applyCatchup(
		[]cronx.Occurrence{{At: stale}, {At: fresh}},
		"skip", 10, defaultWindowMS, now)

	if len(attempts) != 1 || !attempts[0].At.Equal(fresh) {
		t.Fatalf("the fresh-now occurrence was not kept: %+v", attempts)
	}
	if len(skips) != 1 || skips[0].Code != reason.TICKSkippedCatchupDisabled ||
		!skips[0].Occurrence.At.Equal(stale) {
		t.Fatalf("the stale occurrence did not get TICK_SKIPPED_CATCHUP_DISABLED: %+v", skips)
	}
}

func TestAllPolicyDripFeedsTheLimitPerPass(t *testing.T) {
	now := time.Date(2026, 3, 20, 12, 0, 0, 0, time.UTC)
	// A 30 day storm of five minute fires: 8640 owed instants.
	occs := overdue(t, "*/5 * * * *", time.UTC, now.Add(-30*24*time.Hour), now)
	if len(occs) < 8000 {
		t.Fatalf("storm fixture too small: %d occurrences", len(occs))
	}

	passes := 0
	var drained int
	for cursor := 0; cursor < len(occs); {
		attempts, skips := applyCatchup(occs[cursor:], "all", 10, defaultWindowMS, now)
		if len(attempts) > 10 {
			t.Fatalf("pass %d materialised %d ticks, the drip cap is 10", passes, len(attempts))
		}
		step := len(attempts) + len(skips)
		if step == 0 {
			t.Fatal("a pass attempted nothing while occurrences stayed owed")
		}
		cursor += step
		drained += step
		passes++
	}
	if drained != len(occs) {
		t.Fatalf("the storm drained %d of %d owed fire-times", drained, len(occs))
	}
	// Window first: everything older than 24h is a WINDOW skip row, never a run.
	wantWindow := 0
	for _, o := range occs {
		if o.At.Before(now.Add(-defaultWindowMS * time.Millisecond)) {
			wantWindow++
		}
	}
	_, skips := drain(occs, "all", 10, defaultWindowMS, now)
	windowSkips := 0
	var windowRuns int
	for _, s := range skips {
		if s.Code == reason.TICKSkippedCatchupWindow {
			windowSkips++
		}
	}
	attempts, _ := drain(occs, "all", 10, defaultWindowMS, now)
	windowRuns = len(attempts) - (len(occs) - wantWindow)
	if windowSkips != wantWindow || windowRuns != 0 {
		t.Fatalf("window accounting is wrong: %d WINDOW skips (want %d), %d runs inside the window budget",
			windowSkips, wantWindow, windowRuns)
	}
}

func TestWindowFilterDropsOnlyWhatIsReallyOutside(t *testing.T) {
	now := time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	window := int64(3600_000) // one hour

	outside := now.Add(-2 * time.Hour)
	edge := now.Add(-time.Hour)
	inside := now.Add(-30 * time.Minute)

	attempts, skips := applyCatchup([]cronx.Occurrence{
		{At: outside}, {At: edge}, {At: inside},
	}, "all", 10, window, now)

	if len(attempts) != 2 {
		t.Fatalf("want the edge and the inside occurrence attempted, got %+v", attempts)
	}
	if len(skips) != 1 || skips[0].Code != reason.TICKSkippedCatchupWindow ||
		!skips[0].Occurrence.At.Equal(outside) {
		t.Fatalf("only the strictly-older-than-window instant gets WINDOW: %+v", skips)
	}
}
