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
	free, _, err := diskUsage(dir)
	return free, err
}

// diskUsage is the statfs pair the disk-guard's thresholds are computed
// from (#44): free for the unprivileged process and the whole capacity.
func diskUsage(dir string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, fmt.Errorf("statfs %s: %w", dir, err)
	}
	size := st.Bsize
	if size <= 0 {
		return 0, 0, fmt.Errorf("statfs %s reports a block size of %d", dir, size)
	}
	return uint64(st.Bavail) * uint64(size), uint64(st.Blocks) * uint64(size), nil
}
