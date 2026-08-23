//go:build notzdata

package cronx

import "runtime"

// Notzdata build: the binary relies on the host zone database. Same version
// stamp semantics as the default build.

func TzdataVersion() string {
	return runtime.Version()
}

func TzdataChanged(stored string) bool {
	return stored != TzdataVersion()
}
