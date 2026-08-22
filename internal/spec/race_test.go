//go:build race

package spec_test

// raceEnabled reports whether the test binary was built with the race
// detector. The detector multiplies the cost of every read and write, so a
// budget measured under it describes the detector rather than the parser.
const raceEnabled = true
