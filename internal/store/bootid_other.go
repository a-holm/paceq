//go:build !linux

package store

import (
	"errors"
	"fmt"
	"runtime"
)

// readBootID has no answer outside Linux: no other supported platform exposes a
// per boot identifier a plain file read can get at. The caller degrades to
// lease expiry rather than failing.
func readBootID() (string, error) {
	return "", fmt.Errorf("%w: %s has no per boot identifier to read", errors.ErrUnsupported, runtime.GOOS)
}
