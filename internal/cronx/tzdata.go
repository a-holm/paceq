//go:build !notzdata

package cronx

import (
	"runtime"
	_ "time/tzdata" // embed the IANA database so scratch containers resolve zones
)

// TzdataVersion stamps the timezone database this binary carries. The Go
// runtime does not publish the embedded tzdata release, so the toolchain
// version stands in for it: one toolchain embeds exactly one database, which
// makes the value stable per binary and comparable across binaries.
func TzdataVersion() string {
	return runtime.Version()
}

// TzdataChanged compares the version stored at apply time (for example in the
// meta table) with the one this binary carries. A change means zone rules may
// have moved, and the daemon should record that and recompute pending ticks.
func TzdataChanged(stored string) bool {
	return stored != TzdataVersion()
}
