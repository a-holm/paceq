//go:build unix

package cli

import (
	"io/fs"
	"syscall"
)

// fileOwner reports the uid that owns a stat'ed file. known is false when the
// platform's FileInfo carries no uid, and the caller then refuses only on the
// rules it can still decide.
func fileOwner(info fs.FileInfo) (uid int, known bool) {
	raw, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return int(raw.Uid), true
}
