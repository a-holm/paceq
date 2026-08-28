//go:build !unix

package obs

import "errors"

// statfsDisk has no portable implementation outside unix. The guard reports
// the failure per cycle and keeps the previous state, which is the honest
// behaviour for a platform where the reading does not exist.
func statfsDisk(dir string) (free, total uint64, err error) {
	return 0, 0, errors.New("statfs: not supported on this platform")
}
