//go:build !linux

package reconcile

// defaultScan is nil off Linux: there is no /proc to walk, so there is
// nothing to scan and nothing this sweep can claim to know. The decision core
// treats a nil scanner as "skip entirely", which is the documented
// degradation (issue #62, test plan item 10): reconciliation still runs, the
// lease expiry stays the only evidence of death, and OnStartup keeps working.
var defaultScan func() ([]Process, error)
