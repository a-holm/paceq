//go:build !unix

package doctor

import (
	"errors"
	"fmt"
)

// freeSpace has no portable implementation outside unix. The check reports the
// failure as a warning rather than guessing a number.
func freeSpace(dir string) (uint64, error) {
	return 0, fmt.Errorf("%w: reading free space on %s", errors.ErrUnsupported, dir)
}

// diskUsage has no portable implementation outside unix, same as freeSpace.
func diskUsage(dir string) (free, total uint64, err error) {
	return 0, 0, fmt.Errorf("%w: reading the disk usage of %s", errors.ErrUnsupported, dir)
}
