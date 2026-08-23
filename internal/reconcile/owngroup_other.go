//go:build !unix

package reconcile

// ownGroup on platforms without unix process groups: no real pgid is
// negative, so the self-exclusion comparison never fires.
func ownGroup() int { return -1 }
