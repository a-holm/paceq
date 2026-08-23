package api

import (
	"os"
	"syscall"
)

// sameOwner reports whether a socket file belongs to the user running this
// process. Every platform paceq cross builds for is unix, so the stat_t
// assertion holds everywhere the binary is shipped; where it cannot hold,
// the answer is "cannot tell" and the caller refuses only when it can tell
// and the owner differs.
func sameOwner(info os.FileInfo) bool {
	raw, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return true // cannot tell: the mode check above still ran
	}
	return raw.Uid == uint32(os.Geteuid())
}
