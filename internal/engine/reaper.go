package engine

import (
	"context"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// DefaultReapInterval is how often a leader sweeps for expired run leases.
// Ten seconds against a sixty second ttl: recovery is bounded by the ttl plus
// one interval plus the skew, never by the ttl alone.
const DefaultReapInterval = 10 * time.Second

// The running reaper. Every sweep takes the expired runs in one transaction
// and decides each one: cancelled if a request waited out its dead owner,
// poisoned past the crash budget, failed on a spent attempt budget, requeued
// with a backoff otherwise. The fencing token rises in every arm.
//
// The reaper is deliberately late. It waits for the expiry plus the skew
// allowance before it calls a lease dead, because its clock and the holder's
// budget are different instruments: the holder gives up early on monotonic
// time, the reaper acts late on the wall clock (02 section 5.1). Startup
// recovery runs before any loop starts and converges what it finds; by the
// time this sweep first ticks, there is nothing left from the crash to race.

// ReapExpiredRuns runs one sweep against the store with this engine's timing.
// The returned slice names every run the sweep took. The looping belongs to
// whichever process drives the sweeps; in a daemon that is the reaper loop
// under the reaper role lease, so leadership and cadence live in one place.
func (e *Engine) ReapExpiredRuns(ctx context.Context) ([]store.ReapedRun, error) {
	return e.Store.ReapExpiredRuns(ctx, store.ReapOptions{
		Skew:          e.ClockSkewAllowance,
		Backoff:       e.RequeueBackoff,
		MaxCrashCount: e.MaxCrashCount,
	})
}
