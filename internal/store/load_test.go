package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// loadResult is one measured run of runLoad. Latencies are kept only for calls
// that returned nil: a call cut short by the closing load window measures the
// window, not the operation.
type loadResult struct {
	ops     int
	busy    int
	failed  []error
	lat     []time.Duration
	elapsed time.Duration
}

// rate is completed operations per second over the whole window.
func (r loadResult) rate() float64 {
	if r.elapsed <= 0 {
		return 0
	}
	return float64(r.ops) / r.elapsed.Seconds()
}

// quantile returns the q quantile of the recorded latencies, q in [0,1].
func (r loadResult) quantile(q float64) time.Duration {
	if len(r.lat) == 0 {
		return 0
	}
	sorted := slices.Clone(r.lat)
	slices.Sort(sorted)
	i := int(q * float64(len(sorted)-1))
	return sorted[i]
}

func (r loadResult) summary(label string) string {
	return fmt.Sprintf("%s: ops=%d rate=%.0f/s p50=%s p99=%s max=%s over %s",
		label, r.ops, r.rate(), r.quantile(0.5).Round(time.Microsecond),
		r.quantile(0.99).Round(time.Microsecond), r.quantile(1).Round(time.Microsecond),
		r.elapsed.Round(time.Millisecond))
}

// runLoad drives workers goroutines calling work until ctx is done, and times
// every call. It is the only place the gate measures anything, which is why
// TestLoadHarnessMeasuresTheWorkItRuns calibrates it against workloads whose
// duration is known in advance.
//
// A worker stops on the first error that is neither a busy error nor the load
// window closing. Busy errors are counted rather than fatal here: the caller
// decides what a busy error means, and for this project it means the write
// model is wrong.
func runLoad(ctx context.Context, workers int, work func(ctx context.Context, worker int) error) loadResult {
	type shard struct {
		ops    int
		busy   int
		failed []error
		lat    []time.Duration
	}
	shards := make([]shard, workers)

	var wg sync.WaitGroup
	began := time.Now()
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sh := &shards[i]
			for ctx.Err() == nil {
				start := time.Now()
				err := work(ctx, i)
				took := time.Since(start)
				switch {
				case err == nil:
					sh.ops++
					sh.lat = append(sh.lat, took)
				case isBusyLeak(err):
					sh.busy++
					if len(sh.failed) == 0 {
						sh.failed = append(sh.failed, err)
					}
				case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
					return
				case ctx.Err() != nil && errors.Is(err, sql.ErrTxDone):
					// database/sql rolls a transaction back from its own
					// goroutine when the context ends, so a write in flight when
					// the window closes reports the finished transaction rather
					// than the context. It is the window closing, not a failure.
					return
				default:
					sh.failed = append(sh.failed, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(began)

	out := loadResult{elapsed: elapsed}
	for i := range shards {
		out.ops += shards[i].ops
		out.busy += shards[i].busy
		out.failed = append(out.failed, shards[i].failed...)
		out.lat = append(out.lat, shards[i].lat...)
	}
	return out
}

// isBusyLeak reports whether err is any flavour of SQLITE_BUSY reaching the
// caller. The extended result codes put SQLITE_BUSY in the low byte, so
// SQLITE_BUSY_SNAPSHOT (517) and SQLITE_BUSY_TIMEOUT (261) match too.
//
// The string fallback exists because the escape hatch driver reports its errors
// as a different concrete type. A gate that only understands one driver's error
// type would go quietly green on the other.
func isBusyLeak(err error) bool {
	if err == nil {
		return false
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) && coded.Code()&0xff == 5 {
		return true
	}
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "database is locked") || strings.Contains(text, "sqlite_busy")
}

// calibrationWindow is short on purpose: the calibration proves the harness
// measures the right thing, and that needs a handful of samples, not a load
// test.
const calibrationWindow = 300 * time.Millisecond

// TestLoadHarnessMeasuresTheWorkItRuns calibrates the measurement itself. The
// throughput gate is only worth its red build if the number it prints comes
// from the operation under test, so the harness runs two workloads whose
// answers are known in advance: one that does nothing, and one that takes a
// known 5 ms.
//
// The absolute bands are wide because a shared runner is slow and noisy. The
// bands that actually catch a harness measuring the wrong operation are the
// relative ones: a workload that is three orders of magnitude slower has to
// show up as slower.
func TestLoadHarnessMeasuresTheWorkItRuns(t *testing.T) {
	// The calibration is a statement about timing, and the race detector
	// distorts exactly that. It runs in the gate target, where the numbers it
	// calibrates are also measured.
	if raceEnabled {
		t.Skip("the race detector distorts the timings this test calibrates")
	}

	const (
		workers = 4
		step    = 5 * time.Millisecond
	)

	var calls atomic.Int64
	fastCtx, cancelFast := context.WithTimeout(context.Background(), calibrationWindow)
	defer cancelFast()
	fast := runLoad(fastCtx, workers, func(context.Context, int) error {
		calls.Add(1)
		return nil
	})
	t.Log(fast.summary("no-op workload"))

	if fast.ops != int(calls.Load()) {
		t.Errorf("harness counted %d operations, the workload ran %d times", fast.ops, calls.Load())
	}
	if len(fast.lat) != fast.ops {
		t.Errorf("harness kept %d latencies for %d operations", len(fast.lat), fast.ops)
	}
	if fast.ops == 0 {
		t.Fatal("harness completed no operations in the window")
	}
	if got := fast.quantile(0.5); got > time.Millisecond {
		t.Errorf("p50 of a no-op workload is %s, want under 1ms", got)
	}

	slowCtx, cancelSlow := context.WithTimeout(context.Background(), calibrationWindow)
	defer cancelSlow()
	slow := runLoad(slowCtx, workers, func(context.Context, int) error {
		time.Sleep(step)
		return nil
	})
	t.Log(slow.summary("5ms workload"))

	if got := slow.quantile(0.5); got < step || got > 10*step {
		t.Errorf("p50 of a %s workload is %s, want between %s and %s: the harness is not timing "+
			"the operation it runs", step, got, step, 10*step)
	}
	if want := float64(workers) / step.Seconds(); slow.rate() > want*1.2 {
		t.Errorf("measured %.0f/s for a %s workload with %d workers, which is above the %.0f/s "+
			"the workload can produce", slow.rate(), step, workers, want)
	}
	if slow.rate() >= fast.rate() {
		t.Errorf("a %s workload measured %.0f/s and a no-op workload %.0f/s: the harness does not "+
			"distinguish the two", step, slow.rate(), fast.rate())
	}
	if slow.quantile(0.5) <= fast.quantile(0.5) {
		t.Errorf("p50 is %s for a %s workload and %s for a no-op workload: the harness is timing "+
			"something other than the call", slow.quantile(0.5), step, fast.quantile(0.5))
	}
	if slow.elapsed < calibrationWindow {
		t.Errorf("window measured %s, want at least the %s it ran", slow.elapsed, calibrationWindow)
	}
}
