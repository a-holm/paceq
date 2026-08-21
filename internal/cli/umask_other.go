//go:build !unix

package cli

// setUmask has nothing to set where there is no umask. paceq needs flock to
// guarantee a single writer and refuses to start on such a platform anyway, so
// the state directory it would protect is never created.
func setUmask() {}
