package cli

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/api"
)

// TestWireExitMapsEveryHTTPAnswer walks the whole table the socket client
// and the exit code contract share. A cron migration branches on these
// numbers, so every row is asserted here and nowhere else has an opinion.
func TestWireExitMapsEveryHTTPAnswer(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		wantExit int
	}{
		{"created", &api.WireError{Status: http.StatusCreated}, ExitOK},
		{"ok", &api.WireError{Status: http.StatusOK}, ExitOK},
		{"bad request is the command line's fault", &api.WireError{Status: http.StatusBadRequest}, ExitUsage},
		{"not found", &api.WireError{Status: http.StatusNotFound}, ExitNotFound},
		{"unprocessable is validation", &api.WireError{Status: http.StatusUnprocessableEntity}, ExitValidation},
		{"a lease or busy conflict is busy", &api.WireError{Status: http.StatusConflict, Code: "conflict"}, ExitBusy},
		{"a version conflict is paceq itself", &api.WireError{Status: http.StatusConflict, Code: "version_mismatch"}, ExitInternal},
		{"gateway timeout", &api.WireError{Status: http.StatusGatewayTimeout}, ExitTimeout},
		{"any other 5xx is paceq itself", &api.WireError{Status: 502}, ExitInternal},
		{"a status with no documented row is still paceq itself", &api.WireError{Status: http.StatusTeapot}, ExitInternal},
		{"a network timeout is a timeout", netTimeoutError(), ExitTimeout},
		{"anything else is paceq itself", errors.New("mystery"), ExitInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := wireExit(tc.err); got != tc.wantExit {
				t.Fatalf("wireExit(%v) = %d, want %d", tc.err, got, tc.wantExit)
			}
		})
	}
}

// netTimeoutError builds the error shape a real dial or read timeout arrives
// in: an *net.OpError wrapping context.DeadlineExceeded.
func netTimeoutError() error {
	return &net.OpError{
		Op:  "dial",
		Net: "unix",
		Err: contextDeadline(),
	}
}

func contextDeadline() error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	<-ctx.Done()
	return ctx.Err()
}
