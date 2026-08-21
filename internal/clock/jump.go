package clock

import (
	"context"
	"sync"
	"time"
)

// DefaultJumpThreshold is the deviation between wall time and monotonic time
// that counts as a clock jump rather than as scheduling noise.
const DefaultJumpThreshold = 5 * time.Second

// Jump is one observed wall clock correction. Delta is positive when the wall
// clock moved forwards relative to elapsed monotonic time and negative when it
// moved backwards. At is the wall clock reading after the jump.
type Jump struct {
	At    time.Time
	Delta time.Duration
}

// Detector compares wall time against monotonic time and reports the
// difference. It reports only. What to do about a jump, pausing a reaper on a
// backwards jump or letting catch-up absorb a forwards one, is scheduler policy
// and lives with the scheduler.
type Detector struct {
	clk       Clock
	threshold time.Duration

	mu   sync.Mutex
	wall time.Time
	mark Mono
}

// NewDetector takes the first sample immediately, so the first Check measures
// against construction time. A threshold of zero or less means
// DefaultJumpThreshold.
func NewDetector(c Clock, threshold time.Duration) *Detector {
	if threshold <= 0 {
		threshold = DefaultJumpThreshold
	}
	return &Detector{clk: c, threshold: threshold, wall: c.Now(), mark: c.Mark()}
}

// Check takes a sample and reports a jump when wall time and monotonic time
// disagree by more than the threshold since the previous sample. The sample
// becomes the new baseline either way, so one jump is reported once.
func (d *Detector) Check() (Jump, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	now := d.clk.Now()
	delta := now.Sub(d.wall) - d.clk.Since(d.mark)
	d.wall, d.mark = now, d.clk.Mark()

	if delta > d.threshold || delta < -d.threshold {
		return Jump{At: now, Delta: delta}, true
	}
	return Jump{}, false
}

// Run samples every interval until ctx is cancelled, sending each jump to out.
// A receiver that is slow to read delays the next sample; jumps are rare enough
// that dropping them silently would be the worse trade.
func (d *Detector) Run(ctx context.Context, every time.Duration, out chan<- Jump) error {
	ticker := d.clk.NewTicker(every)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			jump, ok := d.Check()
			if !ok {
				continue
			}
			select {
			case <-ctx.Done():
				return nil
			case out <- jump:
			}
		}
	}
}
