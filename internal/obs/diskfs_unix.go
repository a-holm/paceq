//go:build unix

package obs

import "syscall"

// statfsDisk is what is left for an unprivileged process, which is what
// paceq is. Bavail excludes the blocks reserved for root, so it is the
// number that decides whether the next write succeeds - the same reading
// the doctor's disk check uses.
func statfsDisk(dir string) (free, total uint64, err error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, 0, err
	}
	size := st.Bsize
	if size <= 0 {
		return 0, 0, syscall.EINVAL
	}
	free = uint64(st.Bavail) * uint64(size)
	total = uint64(st.Blocks) * uint64(size)
	return free, total, nil
}
