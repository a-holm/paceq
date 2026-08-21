//go:build race

package store

// raceEnabled reports whether the test binary was built with the race
// detector. The detector multiplies every transaction's cost, so the
// throughput gate is meaningless under it and the load window is shortened.
const raceEnabled = true
