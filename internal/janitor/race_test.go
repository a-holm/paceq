//go:build race

package janitor

// raceEnabled reports whether the test binary was built with the race
// detector. The detector multiplies every transaction's cost, so the
// lock-hold gate gets the same room under -race as the store's own
// admission-budget gate does.
const raceEnabled = true
