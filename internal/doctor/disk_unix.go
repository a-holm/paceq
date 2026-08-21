//go:build unix

package doctor

import (
	"fmt"
	"syscall"
)

// freeSpace is what is left for an unprivileged process, which is what paceq
// is. Bavail excludes the blocks reserved for root, so it is the number that
// decides whether the next write succeeds.
func freeSpace(dir string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	size := st.Bsize
	if size <= 0 {
		return 0, fmt.Errorf("statfs %s reports a block size of %d", dir, size)
	}
	return uint64(st.Bavail) * uint64(size), nil
}
