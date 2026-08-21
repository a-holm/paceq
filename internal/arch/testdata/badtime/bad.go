// Package badtime is a fixture for the time lint. It is never built: it lives
// under testdata and is parsed by the test directly. Every forbidden call
// appears once, next to the calls that must stay legal.
package badtime

import (
	"time"

	aliased "time"

	"github.com/a-holm/paceq/internal/clock"
)

func forbidden(c clock.Clock) {
	_ = time.Now()
	_ = time.Since(time.Time{})
	_ = time.Until(time.Time{})
	<-time.After(time.Second)
	_ = time.AfterFunc(time.Second, func() {})
	time.Sleep(time.Second)
	<-time.Tick(time.Second)
	_ = time.NewTimer(time.Second)
	_ = time.NewTicker(time.Second)
	_ = aliased.Now()

	// Legal: the clock method the rule points people at, a time value that
	// never reads the clock, and a local variable that shadows the import.
	_ = c.Now()
	_, _ = time.Parse(time.RFC3339, "2026-03-15T12:00:00Z")
	_ = time.Date(2026, 3, 15, 12, 0, 0, 0, time.UTC)
	shadow := struct{ Now func() int }{Now: func() int { return 0 }}
	_ = shadow.Now()
}
