//go:build !unix

package store

// processAlive cannot be answered portably, so every holder counts as alive and
// the lock's own expiry is what releases it.
func processAlive(int) bool { return true }
