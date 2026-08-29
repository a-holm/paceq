//go:build !unix

package cli

import "io/fs"

// fileOwner has no uid to report where files have no unix owner. paceq needs
// flock and unix sockets and refuses to run on such a platform anyway.
func fileOwner(fs.FileInfo) (int, bool) { return 0, false }
