//go:build unix && !linux

package reconcile

// ownGroup has no answer where process groups are not addressable through
// this build. Returning a value no real pgid takes (negative) keeps the
// self-exclusion comparison harmless.
func ownGroup() int { return -1 }
