//go:build linux

package store

import (
	"fmt"
	"syscall"
)

// Filesystem magic numbers from <linux/magic.h>. SQLite's POSIX advisory
// locking is undefined on all of them: the failure mode is not an error at
// startup but silent corruption discovered weeks later.
const (
	magicNFS    = 0x6969
	magicSMB    = 0x517B
	magicCIFS   = 0xFF534D42
	magicSMB2   = 0xFE534D42
	magicFUSE   = 0x65735546
	magic9P     = 0x01021997
	magicCeph   = 0x00C36400
	magicGFS2   = 0x01161970
	magicOCFS2  = 0x7461636F
	magicAFS    = 0x5346414F
	magicLustre = 0x0BD00BD0
)

// checkLocalFS refuses to run the database on a filesystem where SQLite locking
// does not hold.
func checkLocalFS(dir string) error {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return fmt.Errorf("statfs %s: %w", dir, err)
	}
	return classifyFSMagic(dir, magicOf(st))
}

// magicOf normalises the platform specific f_type field. Filesystem magic
// numbers are 32 bit values, while the field is signed and either 32 or 64 bits
// wide depending on the architecture, so masking to the low 32 bits both drops
// the sign extension and keeps the conversion in range.
func magicOf(st syscall.Statfs_t) uint64 {
	return uint64(int64(st.Type) & 0xFFFFFFFF)
}

func classifyFSMagic(dir string, magic uint64) error {
	switch magic {
	case magicNFS, magicSMB, magicCIFS, magicSMB2, magicFUSE,
		magic9P, magicCeph, magicGFS2, magicOCFS2, magicAFS, magicLustre:
		return fmt.Errorf("%s is on a network or FUSE filesystem (magic %#x): SQLite file "+
			"locking is undefined there and the database will corrupt without warning, weeks "+
			"after the mistake. Move the state directory to a local disk, or pass "+
			"--allow-network-fs to take that risk deliberately", dir, magic)
	}
	return nil
}
