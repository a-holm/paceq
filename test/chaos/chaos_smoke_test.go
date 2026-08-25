package chaos

import (
	"os"
	"strconv"
	"testing"
	"time"
)

// TestChaosSmokeDeterministicSeed is the PR suite's share of AC-9: the whole
// kill, restart and assert machinery runs at small N under one fixed seed, so
// every pull request pays seconds where the nightly pays minutes. The seed is
// a constant on purpose: the PR suite must be reproducible, and PACEQ_CHAOS_SEED
// exists for whoever wants to explore other schedules at smoke size.
func TestChaosSmokeDeterministicSeed(t *testing.T) {
	const (
		smokeSeed  = 1 // policy: any constant works; one is easiest to cite
		smokeRuns  = 6 // two passes over the three shapes
		smokeKills = 3 // enough to park a run behind a dead lease twice over
	)

	seed := int64(smokeSeed)
	if v := os.Getenv("PACEQ_CHAOS_SEED"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			t.Fatalf("PACEQ_CHAOS_SEED=%q is not an integer", v)
		}
		seed = n
	}

	c := &chaosRun{
		Seed:   seed,
		Runs:   smokeRuns,
		Kills:  smokeKills,
		Budget: 8 * time.Minute,
	}
	c.Drive(t)
}
