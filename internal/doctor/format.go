package doctor

import (
	"fmt"
	"io/fs"
)

// byteUnits are decimal, not binary. df and every cloud console a reader
// compares the number against are decimal too, and a report that disagrees with
// them by 7 percent invites a second look at the wrong thing.
var byteUnits = []struct {
	suffix string
	scale  uint64
}{
	{"TB", 1e12},
	{"GB", 1e9},
	{"MB", 1e6},
	{"kB", 1e3},
}

// byteText is a size a human reads. One decimal is enough to act on and few
// enough digits to scan a column of them.
func byteText(n uint64) string {
	for _, u := range byteUnits {
		if n >= u.scale {
			return fmt.Sprintf("%.1f %s", float64(n)/float64(u.scale), u.suffix)
		}
	}
	return fmt.Sprintf("%d B", n)
}

// fileSize is the size in the unit byteText takes. A negative size is not a
// size: it comes from a filesystem that does not report one, and reporting it
// as an enormous number would be worse than reporting nothing.
func fileSize(info fs.FileInfo) uint64 {
	if size := info.Size(); size > 0 {
		return uint64(size)
	}
	return 0
}
