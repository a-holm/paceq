// Package buildinfo holds the identity of the binary: the version, the commit
// it was built from, and when that commit was made. The values arrive through
// -ldflags -X at build time. The defaults are what a plain `go build` without
// stamping produces, and they are deliberately honest: a binary that claims a
// version it was not built from is worse than one that says it does not know.
//
// This package is the single source of that identity. The version command
// prints it, the Makefile stamps it, the release pipeline injects it, and the
// metrics endpoint will report the same three values, so they can never
// diverge (issue #43, design decision 5).
package buildinfo

import "runtime"

var (
	// Version is the released version ("0.1.0"), or "dev" outside a release.
	Version = "dev"
	// Commit is the full commit hash the binary was built from.
	Commit = "unknown"
	// Date is the commit date of Commit in RFC 3339, never the wall clock:
	// two builds of one commit have to stay byte identical (plan 08 §5).
	Date = "unknown"
)

// Info is the identity of one binary as users see it and as monitoring
// scrapes it. The six buildinfo field names freeze with v0.1.
type Info struct {
	Version   string `json:"version"`
	Commit    string `json:"commit"`
	Date      string `json:"date"`
	GoVersion string `json:"go_version"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// Get reports the identity of the running binary. Every consumer reads this
// one struct instead of copying the variables around, so a new consumer can
// never drift from the stamped values.
func Get() Info {
	return Info{
		Version:   Version,
		Commit:    Commit,
		Date:      Date,
		GoVersion: runtime.Version(),
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
	}
}
