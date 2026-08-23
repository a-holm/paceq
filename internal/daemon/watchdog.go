package daemon

import (
	"context"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/obs/sdnotify"
)

// WatchdogNotifier is the seam for production to send watchdog pings.
type WatchdogNotifier interface {
	Send(msg string) error
}

// sdnotifyDog is the production notifier backed by sdnotify.
type sdnotifyDog struct{}

func (sdnotifyDog) Send(string) error {
	return sdnotify.Watchdog()
}

// Heart tracks the scheduler loop aliveness. Beat records each real tick.
// The watchdog loop reads the last beat time and only sends WATCHDOG=1 when
// fresh enough. Slow operations (backup, migrations, GC) must never touch
// this Heart: only the scheduler loop updates it, so a slow backup can never
// cause a watchdog timeout restart loop.
//
// The clock is injected so the Heart never calls time.Now or time.Since directly.
type Heart struct {
	clk      clock.Clock
	lastTick atomic.Int64 // unix nano of last beat
	ticks    atomic.Int64 // total number of beats since start
}

// NewHeart returns a Heart for the given clock.
func NewHeart(clk clock.Clock) *Heart {
	return &Heart{clk: clk}
}

// Beat records one scheduler tick. Called from the scheduler loop on every
// real decision (a Tick that materialises work or decides nothing to do).
func (h *Heart) Beat() {
	h.lastTick.Store(h.clk.Now().UnixNano())
	h.ticks.Add(1)
}

// LastTick returns the wall time of the last beat. Zero if never beaten.
func (h *Heart) LastTick() time.Time {
	nano := h.lastTick.Load()
	if nano == 0 {
		return time.Time{}
	}
	return time.Unix(0, nano)
}

// TickCount returns the total number of beats since start.
func (h *Heart) TickCount() int64 {
	return h.ticks.Load()
}

// SinceLastBeat returns the elapsed wall time since the last beat.
func (h *Heart) SinceLastBeat() time.Duration {
	nano := h.lastTick.Load()
	if nano == 0 {
		return time.Hour // very old if never beaten
	}
	return h.clk.Now().Sub(time.Unix(0, nano))
}

// watchdogLoop runs until ctx is done, sending WATCHDOG=1 at each ping interval
// only when the Heart has been beaten within the watchdog window. A zero
// pingInterval means the watchdog is disabled.
func watchdogLoop(ctx context.Context, clk clock.Clock, heart *Heart, pingInterval time.Duration, notifier WatchdogNotifier) {
	if pingInterval <= 0 {
		return
	}
	window := pingInterval * 2
	ticker := clk.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		since := heart.SinceLastBeat()
		if since < window {
			_ = notifier.Send("WATCHDOG=1")
		}
	}
}

// watchdogUSecFromEnv reads WATCHDOG_USEC from the environment and converts it
// to a time.Duration. Returns 0 if unset or not parseable.
func watchdogUSecFromEnv() time.Duration {
	s := os.Getenv("WATCHDOG_USEC")
	if s == "" {
		return 0
	}
	us, err := strconv.ParseInt(s, 10, 64)
	if err != nil || us <= 0 {
		return 0
	}
	return time.Duration(us) * time.Microsecond
}

// watchdogPingInterval returns half of the watchdog interval. Systemd expects
// messages at least once per WatchdogSec; sending at half ensures a single
// dropped message does not cause a restart.
func watchdogPingInterval(watchdogTimeout time.Duration) time.Duration {
	return watchdogTimeout / 2
}
