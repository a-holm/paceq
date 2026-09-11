//go:build !unix

package daemon

// ownProcessGroup where process groups are not addressable. No real pgid is
// negative, so the self-exclusion comparison never fires.
func ownProcessGroup() int { return -1 }
