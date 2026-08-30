package daemon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/notify"
)

// recording log handler: captures one structured record per line so tests can
// name the exact line they hold the daemon to.
type recLog struct {
	mu      sync.Mutex
	buf     bytes.Buffer
	records []map[string]any
}

func newRecLog() (*recLog, *slog.Logger) {
	r := &recLog{}
	return r, slog.New(slog.NewJSONHandler(&writerFunc{r.append}, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))
}

func (r *recLog) append(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	var m map[string]any
	if json.Unmarshal(p, &m) == nil {
		r.records = append(r.records, m)
	}
	return r.buf.Write(p)
}

func (r *recLog) named(msg string) []map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []map[string]any
	for _, m := range r.records {
		if m["msg"] == msg {
			out = append(out, m)
		}
	}
	return out
}

// text returns everything logged so far, verbatim, for a failure that has to
// report what the daemon said about itself.
func (r *recLog) text() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.buf.String()
}

// indexes returns the record positions of every line with this msg, in the
// order they were logged. Order assertions read off this list.
func (r *recLog) indexes(msg string) []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []int
	for i, m := range r.records {
		if m["msg"] == msg {
			out = append(out, i)
		}
	}
	return out
}

type writerFunc struct {
	fn func([]byte) (int, error)
}

func (w *writerFunc) Write(p []byte) (int, error) { return w.fn(p) }

// testLoops builds the shared loop dependencies on a given logger.
func testLoops(t *testing.T, log *slog.Logger, bus *notify.Bus, st *statuses, clk clock.Clock) loops {
	t.Helper()
	return loops{clk: clk, bus: bus, status: st, log: log}
}

// TestEveryLoopWakesOnTickerAndBus is the topology proof (test plan 2): each
// loop wakes when its topic fires and when its ticker fires, in a synctest
// bubble with no database anywhere near it.
func TestEveryLoopWakesOnTickerAndBus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := notify.New()
		log, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, bus, sts, clock.System())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() { _ = schedulerLoop(ctx, d, time.Hour, nil); close(done) }()

		// Park the loop once so its subscription to the bus exists for
		// certain; synctest.Wait returns only when every goroutine in the
		// bubble is blocked.
		synctest.Wait()
		if got := sts.snapshot(); len(got) != 0 {
			t.Fatalf("the scheduler marked itself before any wake: %+v", got)
		}

		// The bus path: a wake arrives with no virtual time passing at all.
		bus.Notify(notify.TopicScheduleChanged)
		synctest.Wait()
		sched := sts.snapshot()[0]
		if sched.Name != "scheduler" || sched.Ticks != 1 {
			t.Fatalf("after one notify the scheduler shows %+v, want exactly one tick", sts.snapshot())
		}

		// The ticker path: two hours pass instantly in the bubble.
		time.Sleep(2 * time.Hour)
		cancel()
		<-done

		sched = sts.snapshot()[0]
		if sched.Ticks != 3 { // the notify, two ticks, and the exit look
			t.Fatalf("scheduler ticked %d times after two hourly ticks, want 3", sched.Ticks)
		}
		if len(log.named("loop started")) != 1 {
			t.Error("the scheduler did not log its start line")
		}
	})
}

// TestLoopsRunOnTickersAlone is --no-notify-bus: a disabled bus wakes nobody,
// and the loops still work off their tickers.
func TestLoopsRunOnTickersAlone(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		bus := notify.Disabled()
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, bus, sts, clock.System())

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		done := make(chan struct{})
		go func() { _ = sensorLoop(ctx, d, time.Minute, nil); close(done) }()

		bus.Notify(notify.TopicScheduleChanged)
		synctest.Wait()
		if got := sts.snapshot(); len(got) != 0 {
			t.Fatalf("a disabled bus woke the sensor loop: %+v", got)
		}

		time.Sleep(120 * time.Second)
		cancel()
		<-done

		if got := sts.snapshot()[0]; got.Ticks != 2 { // minute ticks at 60s and 120s
			t.Fatalf("sensor ticked %d times on the ticker alone in 120s, want 2", got.Ticks)
		}
	})
}

type fakeToucher struct {
	mu      sync.Mutex
	touched []string
}

func (f *fakeToucher) TouchSession(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, id)
	return nil
}

// TestHeartbeatTouchesEveryInterval pins the session heartbeat cadence.
func TestHeartbeatTouchesEveryInterval(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, notify.New(), sts, clock.System())
		toucher := &fakeToucher{}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			_ = heartbeatLoop(ctx, d, 10*time.Second, toucher, "sess-1")
			close(done)
		}()

		time.Sleep(35 * time.Second) // touches at 10s, 20s, 30s
		cancel()
		<-done

		toucher.mu.Lock()
		defer toucher.mu.Unlock()
		if len(toucher.touched) != 3 {
			t.Fatalf("%d heartbeats in 35s at a 10s interval, want 3", len(toucher.touched))
		}
		for _, id := range toucher.touched {
			if id != "sess-1" {
				t.Fatalf("heartbeat carried session %q, want sess-1", id)
			}
		}
	})
}

type fakeQueue struct{ ids []string }

func (f fakeQueue) ClaimableRunIDs(context.Context) ([]string, error) { return f.ids, nil }

// TestDispatcherHandsWorkOutOncePerWake proves the fast path: a queued-run
// notify reaches the pool without waiting for a tick, and a full pool leaves
// the rest queued rather than dropping them.
func TestDispatcherHandsWorkOutOncePerWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, notify.New(), sts, clock.System())
		queue := fakeQueue{ids: []string{"run-a", "run-b", "run-c"}}

		var mu sync.Mutex
		var submitted []string
		submit := func(id string) bool {
			mu.Lock()
			defer mu.Unlock()
			submitted = append(submitted, id)
			return true
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = dispatcherLoop(ctx, d, time.Hour, queue, submit); close(done) }()
		synctest.Wait() // parked in the select, subscription in place

		d.bus.Notify(notify.TopicRunQueued)
		synctest.Wait()
		cancel()
		<-done

		mu.Lock()
		defer mu.Unlock()
		if strings.Join(submitted, ",") != "run-a,run-b,run-c" {
			t.Fatalf("the dispatcher submitted %v, want all three in order", submitted)
		}
	})
}

// TestFullPoolLeavesRunsQueued: a submit refusal stops the sweep, so nothing
// is silently dropped when the executors are busy.
func TestFullPoolLeavesRunsQueued(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		_, logger := newRecLog()
		sts := newStatuses(func() time.Time { return clock.System().Now() })
		d := testLoops(t, logger, notify.New(), sts, clock.System())
		queue := fakeQueue{ids: []string{"run-a", "run-b"}}

		full := true
		var submitted []string
		submit := func(id string) bool {
			submitted = append(submitted, id)
			return !full
		}

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { _ = dispatcherLoop(ctx, d, time.Hour, queue, submit); close(done) }()
		synctest.Wait() // parked in the select, subscription in place

		d.bus.Notify(notify.TopicRunQueued)
		synctest.Wait()
		cancel()
		<-done

		if len(submitted) != 1 {
			t.Fatalf("%d runs were taken from a refusing pool, want exactly one attempt then stop",
				len(submitted))
		}
	})
}

// TestShutdownPhasesAreBounded drives the whole stop sequence on fake parts
// and holds the promise to its numbers: intake inside 100ms, everything after
// it in order, session clean, checkpoint before close.
func TestShutdownPhasesAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := newRecLog()
		clk := clock.System()
		sts := newStatuses(func() time.Time { return clk.Now() })

		intakeA := make(chan struct{})
		intakeB := make(chan struct{})
		stopIntake := func() { close(intakeA); close(intakeB) }

		execDrained := make(chan struct{})
		close(execDrained)
		loopsDone := make(chan struct{})
		close(loopsDone)

		var order []string
		sd := &shutdown{
			cfg:         Config{},
			clk:         clk,
			log:         logger,
			statuses:    sts,
			stopIntake:  stopIntake,
			intakeAcks:  []<-chan struct{}{intakeA, intakeB},
			stopExec:    func() {},
			execDrained: execDrained,
			apiStopped: func(context.Context) {
				order = append(order, "api")
			},
			loopsDrained: loopsDone,
			closeSession: func(context.Context) error {
				order = append(order, "session")
				return nil
			},
			checkpoint: func(context.Context) error {
				order = append(order, "checkpoint")
				return nil
			},
			closeStore: func() error {
				order = append(order, "close")
				return nil
			},
		}

		err := sd.run(context.Canceled)
		if err != nil {
			t.Fatalf("a cancelled stop reported %v, want clean", err)
		}
		synctest.Wait()

		got := strings.Join(order, ",")
		if got != "api,session,checkpoint,close" {
			t.Fatalf("the final writes ran %s, want api,session,checkpoint,close", got)
		}

		lines := rec.named("intake closed")
		if len(lines) != 1 {
			t.Fatalf("%d intake lines were logged, want 1", len(lines))
		}
		took, ok := lines[0]["took"].(string)
		if !ok {
			t.Fatalf("the intake line carries no duration: %v", lines[0])
		}
		d, err := time.ParseDuration(took)
		if err != nil {
			t.Fatalf("the intake duration %q does not parse: %v", took, err)
		}
		if d > intakeBudget {
			t.Errorf("closing the intake took %s, over the %s budget", took, intakeBudget)
		}
	})
}

// TestShutdownReportsTheBudgetMiss: an intake loop that never acknowledges
// cannot hold the stop hostage; the sequence moves on and says so.
func TestShutdownReportsTheBudgetMiss(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rec, logger := newRecLog()
		clk := clock.System()

		stuck := make(chan struct{}) // never closed
		sd := &shutdown{
			cfg:          Config{},
			clk:          clk,
			log:          logger,
			statuses:     newStatuses(clk.Now),
			stopIntake:   func() {},
			intakeAcks:   []<-chan struct{}{stuck},
			stopExec:     func() {},
			execDrained:  closedChan(),
			loopsDrained: closedChan(),
		}

		err := sd.run(context.Canceled)
		if err != nil {
			t.Fatalf("a cancelled stop reported %v, want clean", err)
		}
		synctest.Wait()

		if warns := rec.named("an intake loop missed its acknowledgement deadline"); len(warns) != 1 {
			t.Fatalf("the missed acknowledgement was not reported: %v", rec.records)
		}
		lines := rec.named("intake closed")
		took, _ := time.ParseDuration(lines[0]["took"].(string))
		if took < intakeBudget {
			t.Errorf("the intake reported %s although an ack was missing, want at least the budget", took)
		}
	})
}

func closedChan() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

// TestHardStopNeedsTwoSignals: the first signal copy starts nothing here, the
// second kills every live process group and ends the process through the hook.
func TestHardStopNeedsTwoSignals(t *testing.T) {
	var killed []syscall.Signal
	original := hardKill
	hardKill = func(sig syscall.Signal) { killed = append(killed, sig) }
	t.Cleanup(func() { hardKill = original })

	signals := make(chan os.Signal, 3)
	rec, logger := newRecLog()
	hardCalled := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		watchHardStop(signals, logger, func() { close(hardCalled) })
		close(watchDone)
	}()

	signals <- os.Interrupt // first request: the graceful path owns it
	select {
	case <-hardCalled:
		t.Fatal("one signal was enough for the hard stop")
	default:
	}

	signals <- os.Interrupt // second request: insist
	// The delivery is asynchronous; the only bounded way to wait for it is
	// on its own channel.
	select {
	case <-hardCalled:
	case <-time.After(10 * time.Second):
		t.Fatal("two signals did not reach the hard stop")
	}
	// Only touch the seam again once the watcher has fully returned, or the
	// cleanup restore races with its last read.
	<-watchDone
	if warns := rec.named("second stop signal: killing every process group now"); len(warns) != 1 {
		t.Fatalf("the hard stop was not announced: %v", rec.records)
	}
	if len(killed) != 1 || killed[0] != syscall.SIGKILL {
		t.Fatalf("the hard stop delivered %v, want exactly one SIGKILL", killed)
	}
}

// TestHardStopToleratesAClosedChannel: a watcher whose signal source goes
// away must not panic the process on its way down, and must not fire the hook
// on its way out.
func TestHardStopToleratesAClosedChannel(t *testing.T) {
	signals := make(chan os.Signal)
	_, logger := newRecLog()
	called := make(chan struct{})
	watchDone := make(chan struct{})
	go func() {
		watchHardStop(signals, logger, func() { close(called) })
		close(watchDone)
	}()
	close(signals)
	<-watchDone // the watcher has exited; its decision is final now
	select {
	case <-called:
		t.Fatal("the hook ran without a second signal")
	default:
	}
}

// TestStopCauseKeepsRealFailures: only a loop's own failure is reported as
// one; the caller's cancellation arrives as the cause and reads as clean
// further down, in run.
func TestStopCauseKeepsRealFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if got := stopCause(errors.New("boom"), ctx); got == nil || got.Error() != "boom" {
		t.Fatalf("a loop failure surfaced as %v, want boom", got)
	}
	if got := stopCause(nil, ctx); !errors.Is(got, context.Canceled) {
		t.Fatalf("a plain cancellation surfaced as %v, want context.Canceled", got)
	}
}
