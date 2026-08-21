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
