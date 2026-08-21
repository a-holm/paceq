//go:build !race

package store

// raceEnabled is false in an ordinary build, which is where the throughput
// gate is measured and asserted.
const raceEnabled = false
