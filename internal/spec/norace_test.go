//go:build !race

package spec_test

// raceEnabled is false in an ordinary build, which is where the parsing budget
// is measured and asserted.
const raceEnabled = false
