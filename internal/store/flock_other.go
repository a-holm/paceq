//go:build !unix

package store

import (
	"errors"
	"fmt"
	"os"
	"runtime"
)

// lockExclusive has no portable implementation outside unix. paceq refuses to
// start rather than run without the guarantee that there is only one writer.
func lockExclusive(*os.File) error {
	return fmt.Errorf("%w: paceq needs flock to guarantee a single writer, and %s has no equivalent",
		errors.ErrUnsupported, runtime.GOOS)
}

func unlockFile(*os.File) error { return nil }
