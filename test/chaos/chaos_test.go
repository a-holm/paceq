//go:build chaos

package chaos

import (
	"fmt"
	"testing"
	"time"
)

// The full nightly body of AC-9: five hundred runs over the shape mix while
// the real daemon is SIGKILLed on a seeded schedule, every restart waited
// out to terminal, and the whole invariant battery run over the wreckage.
// The budget is the issue's thirty minutes; the standing seed is a policy
// constant, and PACEQ_CHAOS_SEED replaces it for a replay or an exploration.
//
//	go test -tags chaos -race -count=1 ./test/chaos
func TestChaosNightlyFullSweep(t *testing.T) {
	const (
		nightlySeed  = int64(20260825) // the date the suite landed; any constant works
		nightlyRuns  = 500
		nightlyKills = 12 // about twenty minutes of worst case lease waits inside the budget
	)

	c := &chaosRun{
		Seed:   seedFromEnv(t, nightlySeed),
		Runs:   nightlyRuns,
		Kills:  nightlyKills,
		Budget: 28 * time.Minute,
	}
	c.Drive(t)
}

// The regression sweep size: big enough to walk every shape past several
// kills, small enough that a dozen rows stay minutes.
const (
	regressionRuns  = 24
	regressionKills = 5
)

// TestChaosRegressions replays every seed in regressions.txt at smoke size.
// A seed lands here only after it failed a sweep and the failure was
// understood, so each row is a product bug's fingerprint that the nightly
// must keep reproducing until the fix retires it. Small N keeps the file
// cheap forever; the shapes that exposed the bug are still in the mix.
func TestChaosRegressions(t *testing.T) {
	seeds := readRegressionSeeds(t)
	if len(seeds) == 0 {
		t.Skip("regressions.txt holds no seeds yet")
	}
	for _, seed := range seeds {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			c := &chaosRun{
				Seed:   seed,
				Runs:   regressionRuns,
				Kills:  regressionKills,
				Budget: 8 * time.Minute,
			}
			c.Drive(t)
		})
	}
}
