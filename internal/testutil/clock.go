package testutil

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// Origin is the wall clock reading every test starts from. One shared instant
// keeps expected timestamps comparable across packages and keeps golden output
// stable. It is a whole minute in UTC, so offsets in a test read as offsets.
var Origin = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)

// NewClock returns a fresh clock.Fake at Origin.
//
// There is no wrapper around testing/synctest here. synctest.Test already takes
// the test and the function, and a wrapper would only hide the rule that
// matters: inside a bubble every goroutine must reach a durable block, which
// SQLite file I/O never does. See internal/clock for which tool a test wants.
func NewClock(t *testing.T) *clock.Fake {
	t.Helper()

	return clock.NewFake(Origin)
}
