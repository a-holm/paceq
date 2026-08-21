package testutil_test

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/testutil"
)

func TestNewClockStartsAtTheSharedOrigin(t *testing.T) {
	c := testutil.NewClock(t)

	if got := c.Now(); !got.Equal(testutil.Origin) {
		t.Errorf("Now() = %v, want the shared origin %v", got, testutil.Origin)
	}
	if got := testutil.NewClock(t).Now(); !got.Equal(c.Now()) {
		t.Error("two clocks from NewClock disagree: the origin must be the same in every test")
	}

	c.Advance(time.Hour)
	if got := testutil.NewClock(t).Now(); !got.Equal(testutil.Origin) {
		t.Errorf("a second clock reads %v after the first advanced: clocks must be independent", got)
	}
}

func TestOriginIsAPlainUTCInstant(t *testing.T) {
	if testutil.Origin.Location() != time.UTC {
		t.Errorf("Origin location = %v, want UTC", testutil.Origin.Location())
	}
	if testutil.Origin.Nanosecond() != 0 || testutil.Origin.Second() != 0 {
		t.Errorf("Origin = %v, want a whole minute so arithmetic in tests stays readable", testutil.Origin)
	}
}
