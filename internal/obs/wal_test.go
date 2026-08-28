package obs

import (
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// The WAL watchdog's thresholds (#44): warn at 64 MiB, error at 256 MiB,
// clear when the reader lets go. Every reading comes through the seam, so
// no test creates a 256 MiB file.

func walTestWatch(t *testing.T, size int64) (*WALWatch, *int64, *[]WALEvent, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	sizeP := &size
	var events []WALEvent
	w := NewWALWatch(WALWatchConfig{
		DBPath: "/state/state.db",
		Clock:  clk,
		SizeOf: func(string) (int64, bool) {
			if *sizeP < 0 {
				return 0, false // no WAL file at all
			}
			return *sizeP, true
		},
		Emit: func(e WALEvent) { events = append(events, e) },
	})
	return w, sizeP, &events, clk
}

func TestWALAlarmWarnsThenErrorsThenClears(t *testing.T) {
	w, size, events, clk := walTestWatch(t, 0)

	w.Step(nil)
	if len(*events) != 0 {
		t.Fatalf("a healthy WAL raised %d events, want none", len(*events))
	}

	*size = DefaultWALWarnBytes + 1 // past 64 MiB
	w.Step(nil)
	if len(*events) != 1 || (*events)[0].Level.String() != "WARN" {
		t.Fatalf("crossing the warn line produced %v, want one WARN event", *events)
	}

	*size = DefaultWALWarnBytes*4 + 1 // past 256 MiB
	w.Step(nil)
	last := (*events)[len(*events)-1]
	if len(*events) != 2 || last.Level.String() != "ERROR" || last.ErrorBytes != DefaultWALWarnBytes*4 {
		t.Fatalf("crossing the error line produced %v, want one ERROR event", *events)
	}

	// Confirming checks re-emit the same episode: the outbox collapses
	// them, so a persistent problem is one notification, not one per 30
	// seconds.
	w.Step(nil)
	if len(*events) != 3 || (*events)[2].Since != last.Since {
		t.Fatalf("a confirming check raised a new episode: %v", *events)
	}

	// The reader closes and a checkpoint shrinks the WAL: the alarm
	// clears and a later crossing starts a new episode.
	*size = 1024
	clk.Advance(2 * time.Minute)
	w.Step(nil)
	w.Step(nil)
	if len(*events) != 3 {
		t.Fatalf("the cleared state raised events: %v", *events)
	}
	*size = DefaultWALWarnBytes + 1
	clk.Advance(2 * time.Minute)
	w.Step(nil)
	if len(*events) != 4 || (*events)[3].Since == last.Since {
		t.Fatalf("a new crossing did not start a new episode: %v", *events)
	}
}

func TestWALAlarmPointsAtTheLongLivedReader(t *testing.T) {
	// The diagnostic value of the alarm is the likely cause, not the
	// number: an operator who reads "256 MiB" without "a long-lived read
	// transaction" has to rediscover 07 §6.4 mid-incident. The event's
	// level is what the outbox notification carries; the log line beside
	// it names the reader. Both derive from the same crossing, so pinning
	// the level here pins the pairing.
	w, _, events, _ := walTestWatch(t, DefaultWALWarnBytes*4+1)
	w.Step(nil)
	if len(*events) != 1 || (*events)[0].Level.String() != "ERROR" {
		t.Fatalf("a WAL past the error level produced %v, want one ERROR event", *events)
	}
	if (*events)[0].WalBytes != DefaultWALWarnBytes*4+1 {
		t.Errorf("the event does not carry the measured size: %+v", (*events)[0])
	}
}
