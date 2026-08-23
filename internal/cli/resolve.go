package cli

import (
	"errors"
	"net"

	"github.com/a-holm/paceq/internal/api"
)

// wireExit turns a failed socket call into the exit code the shell sees.
// The table is the whole contract: every HTTP status the daemon can send,
// plus the two transport failures, lands on exactly one documented code
// (03 section 7.2), and exitwire_test.go holds each row in place.
//
// A dial failure is deliberately absent: it is not an error here at all,
// because resolution falls back to direct mode before any of this runs.
func wireExit(err error) int {
	if err == nil {
		return ExitOK
	}
	var wire *api.WireError
	if errors.As(err, &wire) {
		return wireStatusExit(wire.Status, wire.Code)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return ExitTimeout
	}
	return ExitInternal
}

// wireStatusExit is one row per HTTP answer.
func wireStatusExit(status int, code string) int {
	switch {
	case status == 200 || status == 201:
		return ExitOK
	case status == 400:
		return ExitUsage
	case status == 404:
		return ExitNotFound
	case status == 422:
		return ExitValidation
	case status == 409 && code == "version_mismatch":
		return ExitInternal
	case status == 409:
		return ExitBusy
	case status == 504:
		return ExitTimeout
	default:
		return ExitInternal
	}
}
