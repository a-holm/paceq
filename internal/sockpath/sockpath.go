// Package sockpath holds the one fact about unix socket names every layer of
// paceq has to agree on: how long a path may be.
package sockpath

import "fmt"

// MaxLen is the longest path a named unix socket may carry. sockaddr_un's
// sun_path holds 108 bytes and a named address needs its terminator among
// them, so 107 is the last length that binds.
//
// Go refuses a longer name with EINVAL before the bind syscall runs, so the
// kernel never sees the path and the failure reads "invalid argument" without
// a word about length. Validate is what turns that into an answer.
const MaxLen = 107

// Validate refuses a socket path the kernel cannot take. The message carries
// the measured length, the limit and the path, because the error the caller
// would otherwise get carries none of the three.
func Validate(path string) error {
	if len(path) > MaxLen {
		return fmt.Errorf("socket path is %d bytes, the maximum is %d: %s", len(path), MaxLen, path)
	}
	return nil
}
