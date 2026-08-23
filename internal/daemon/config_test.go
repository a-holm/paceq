package daemon

import (
	"runtime"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// TestConfigDefaultsCarryTheSketch pins the serve defaults to the numbers the
// plans and the acceptance list name: a 30 second drain, tickers every second,
// a heartbeat every ten seconds, one worker slot per CPU, and the store's own
// lease as the executor claim. The heartbeat number is acceptance criterion
// 10's "every 10 seconds"; pinning it here means changing it is a commit that
// says so, not a drift.
func TestConfigDefaultsCarryTheSketch(t *testing.T) {
	var c Config

	if got := c.drainTimeout(); got != 30*time.Second {
		t.Errorf("default drain timeout is %s, want 30s", got)
	}
	if got := c.tickInterval(); got != 1*time.Second {
		t.Errorf("default tick interval is %s, want 1s", got)
	}
	if got := c.heartbeatEvery(); got != 10*time.Second {
		t.Errorf("default heartbeat interval is %s, want 10s", got)
	}
	if got := c.workerCount(); got != runtime.NumCPU() {
		t.Errorf("default worker count is %d, want one per CPU (%d)", got, runtime.NumCPU())
	}
	if got := c.leaseTTL(); got != store.DefaultLeaseTTL {
		t.Errorf("default lease ttl is %s, want the store default (%s)", got, store.DefaultLeaseTTL)
	}

	// Every explicit value wins over its default, so an operator flag can
	// never be silently ignored.
	o := Config{
		DrainTimeout:   5 * time.Second,
		TickInterval:   2 * time.Second,
		HeartbeatEvery: 3 * time.Second,
		Workers:        7,
		LeaseTTL:       time.Minute,
	}
	if got := o.drainTimeout(); got != 5*time.Second {
		t.Errorf("drain timeout override lost: %s", got)
	}
	if got := o.tickInterval(); got != 2*time.Second {
		t.Errorf("tick interval override lost: %s", got)
	}
	if got := o.heartbeatEvery(); got != 3*time.Second {
		t.Errorf("heartbeat override lost: %s", got)
	}
	if got := o.workerCount(); got != 7 {
		t.Errorf("worker override lost: %d", got)
	}
	if got := o.leaseTTL(); got != time.Minute {
		t.Errorf("lease override lost: %s", got)
	}
}
