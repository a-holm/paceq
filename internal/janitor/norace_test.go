//go:build !race

package janitor

// raceEnabled is false in an ordinary build, which is where the lock-hold
// gate measures the budget the acceptance criterion names.
const raceEnabled = false
