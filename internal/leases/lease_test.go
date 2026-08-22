package leases

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The loop proofs run inside testing/synctest bubbles: the ticker, the ttl
// budget and the store all share the bubble's virtual clock, so half an hour
// of lease history passes instantly and nothing here can flake on a loaded
// machine.
//
// One property of bubbles shapes the whole harness: virtual time only moves
// forward while every goroutine in the bubble is durably parked. The test
// goroutine therefore never polls with a select and default; it parks on the
// pulse channel the stores and bodies ping, with a virtual deadline as the
// escape hatch when progress is impossible.

// parkDeadline bounds one parked wait in virtual time. Every simulated life in
// these tests spans a few minutes; ten cannot pass without either progress or
// a genuine deadlock in the loop under test.
const parkDeadline = 10 * time.Minute

// pulse carries one token per state change the harness might wait for. A ping
// never blocks its sender and never carries data: it only says look again.
type pulse struct {
	c chan struct{}
}

func newPulse() *pulse { return &pulse{c: make(chan struct{}, 1)} }

func (p *pulse) ping() {
	select {
	case p.c <- struct{}{}:
	default:
	}
}

// wait parks until cond holds, pings waking the checker between virtual time
// steps. The deadline is armed once per call, not per park: a mutant that
// keeps producing harmless pings must still run into a hard stop instead of
// sliding the deadline forward forever.
func (p *pulse) wait(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.NewTimer(parkDeadline)
	defer deadline.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-p.c:
		case <-deadline.C:
			t.Fatalf("never settled: %s", what)
		}
	}
}

// step is one scripted answer to one AcquireOrRenew call.
type step struct {
	grant store.LeaseGrant
	ok    bool
	err   error
}

func grantAt(epoch int64) step {
	return step{grant: store.LeaseGrant{Name: "scheduler", Holder: "node-a", Epoch: epoch}, ok: true}
}

func refusal() step { return step{} }

// scriptStore answers renewals from a queue, records every call in order, and
// fails a test that lets the loop outrun its script.
type scriptStore struct {
	pulse *pulse
	mu    sync.Mutex
	steps []step
	calls []string

	exhausted bool
}

func (s *scriptStore) AcquireOrRenew(_ context.Context, name, holder string, _ time.Duration) (store.LeaseGrant, bool, error) {
	s.mu.Lock()
	s.calls = append(s.calls, "acquire")
	var st step
	if len(s.steps) == 0 {
		s.exhausted = true
		s.mu.Unlock()
		s.pulse.ping()
		return store.LeaseGrant{}, false, errScriptExhausted
	}
	st = s.steps[0]
	s.steps = s.steps[1:]
	s.mu.Unlock()
	s.pulse.ping()
	if st.err != nil {
		return store.LeaseGrant{}, false, st.err
	}
	g := st.grant
	g.Name, g.Holder = name, holder
	return g, st.ok, nil
}

func (s *scriptStore) ReleaseLease(_ context.Context, _, _ string) (bool, error) {
	s.mu.Lock()
	s.calls = append(s.calls, "release")
	s.mu.Unlock()
	s.pulse.ping()
	return true, nil
}

func (s *scriptStore) AppendLeaseEvent(_ context.Context, e store.LeaseEvent) error {
	s.mu.Lock()
	s.calls = append(s.calls, "event:"+string(e.Code))
	s.mu.Unlock()
	s.pulse.ping()
	return nil
}

func (s *scriptStore) ledger() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *scriptStore) eventCodes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	codes := make([]string, 0, len(s.calls))
	for _, e := range s.calls {
		if strings.HasPrefix(e, "event:") {
			codes = append(codes, strings.TrimPrefix(e, "event:"))
		}
	}
	return codes
}

func (s *scriptStore) acquires() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if c == "acquire" {
			n++
		}
	}
	return n
}

func (s *scriptStore) ranDry() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exhausted
}

// bodySpy is a minimal leader body: it records the epochs it was handed and
// blocks until its context is cancelled, which is what a real body spends its
// life doing between transactions.
type bodySpy struct {
	pulse *pulse
	mu    sync.Mutex
	runs  []int64
	live  atomic.Bool
}

func (b *bodySpy) run(ctx context.Context, epoch int64) error {
	b.mu.Lock()
	b.runs = append(b.runs, epoch)
	b.mu.Unlock()
	b.live.Store(true)
	b.pulse.ping()
	defer func() {
		b.live.Store(false)
		b.pulse.ping()
	}()
	<-ctx.Done()
	return ctx.Err()
}

func (b *bodySpy) epochsRun() []int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]int64(nil), b.runs...)
}

func (b *bodySpy) isLive() bool { return b.live.Load() }

// logCapture collects the loop's structured lines so a test can hold the fixed
// fields against the contract. The mutex is load bearing: the loop logs from
// its own goroutine while the test reads.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logCapture) logger() *slog.Logger {
	return slog.New(slog.NewTextHandler(&lockedWriter{l, &l.buf}, &slog.HandlerOptions{Level: slog.LevelInfo}))
}

func (l *logCapture) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// lockedWriter guards the buffer with the capture's mutex, since slog writes
// from the loop goroutine.
type lockedWriter struct {
	l *logCapture
	b *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.l.mu.Lock()
	defer w.l.mu.Unlock()
	return w.b.Write(p)
}

// testOptions fixes the identity every scripted test shares.
func testOptions(st Store, log *slog.Logger) Options {
	return Options{
		Name:   "scheduler",
		Holder: "node-a",
		TTL:    30 * time.Second,
		Renew:  10 * time.Second,
		Log:    log,
	}
}

// stop cancels the loop's context and joins it, failing on any exit other than
// the requested one.
func stop(t *testing.T, cancel context.CancelFunc, done <-chan error) error {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		return err
	case <-time.After(parkDeadline):
		t.Fatal("the loop did not return after cancellation")
		return nil
	}
}

func TestTheFirstTickTakesTheLeaseAndRunsTheBodyOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPulse()
		s := &scriptStore{pulse: p, steps: []step{grantAt(1), grantAt(1), grantAt(1)}}
		var body bodySpy
		body.pulse = p
		var logs logCapture
		opt := testOptions(s, logs.logger())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- RunAsLeader(ctx, s, opt, body.run) }()

		p.wait(t, "three renew ticks with one body stint", func() bool {
			return s.acquires() >= 3 && len(body.epochsRun()) == 1
		})

		if got := body.epochsRun(); len(got) != 1 || got[0] != 1 {
			t.Errorf("the body ran %d times with epochs %v, want exactly once with epoch 1", len(got), got)
		}
		if codes := s.eventCodes(); len(codes) != 1 || codes[0] != string(reason.LEASEAcquired) {
			t.Errorf("recorded events %v, want exactly one %s", codes, reason.LEASEAcquired)
		}
		// Two steady renewals must have produced pure acquire traffic: the
		// quiet path writes one small statement and nothing else.
		want := []string{"acquire", "event:" + string(reason.LEASEAcquired), "acquire", "acquire"}
		if got := s.ledger(); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("call ledger is %v, want %v", got, want)
		}
		for _, field := range []string{"lease=scheduler", "epoch=1", "holder=node-a"} {
			if !strings.Contains(logs.text(), field) {
				t.Errorf("the structured lines are missing the fixed field %s:\n%s", field, logs.text())
			}
		}

		if err := stop(t, cancel, done); !errors.Is(err, context.Canceled) {
			t.Errorf("the loop returned %v, want context.Canceled", err)
		}
		if s.ranDry() {
			t.Error("the loop outran its script, the test under-drives it")
		}
	})
}

func TestARivalEndsLeadershipExactlyOnceAndTheRegainIsATakeover(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPulse()
		s := &scriptStore{pulse: p, steps: []step{
			grantAt(1), grantAt(1), // two confirmed renewals as leader
			refusal(), refusal(), // the rival renewed first: we are out
			grantAt(2), grantAt(2), // the rival died, we take the expired row
		}}
		var body bodySpy
		body.pulse = p
		var logs logCapture
		opt := testOptions(s, logs.logger())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- RunAsLeader(ctx, s, opt, body.run) }()

		// Wait for the consequence, not just the cause: the store pings the
		// pulse before the loop has reacted to the refusal it returned.
		p.wait(t, "the first refusal ending our body", func() bool {
			return s.acquires() >= 3 && !body.isLive()
		})
		p.wait(t, "the second refused follower tick", func() bool { return s.acquires() >= 4 })
		if body.isLive() {
			t.Error("the body came back while the rival still held the lease")
		}
		p.wait(t, "the takeover", func() bool {
			return s.acquires() >= 5 && len(body.epochsRun()) == 2
		})

		wantEvents := []string{
			string(reason.LEASEAcquired),
			string(reason.LEASELost),
			string(reason.LEASETakenOver),
		}
		if got := s.eventCodes(); strings.Join(got, ",") != strings.Join(wantEvents, ",") {
			t.Errorf("recorded events %v, want %v", got, wantEvents)
		}
		if got := body.epochsRun(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
			t.Errorf("the body ran with epochs %v, want one stint at 1 and one at 2", got)
		}
		// Between the loss and the takeover the loop is a pure follower: one
		// small acquire per tick, zero other statements.
		want := []string{
			"acquire", "event:" + string(reason.LEASEAcquired),
			"acquire",
			"acquire", "event:" + string(reason.LEASELost),
			"acquire",
			"acquire", "event:" + string(reason.LEASETakenOver),
		}
		if got := s.ledger(); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("call ledger is %v, want %v", got, want)
		}
		if n := strings.Count(logs.text(), string(reason.LEASELost)); n != 1 {
			t.Errorf("the loss line appeared %d times, want exactly once:\n%s", n, logs.text())
		}

		if err := stop(t, cancel, done); !errors.Is(err, context.Canceled) {
			t.Errorf("the loop returned %v, want context.Canceled", err)
		}
	})
}

func TestErrorsInsideTheTTLPreserveLeadershipAndPastItEndIt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPulse()
		dbErr := errors.New("database is briefly unreachable")
		s := &scriptStore{pulse: p, steps: []step{
			grantAt(1),                               // confirmed at tick 1
			{err: dbErr}, {err: dbErr}, {err: dbErr}, // three lost renewals: 30s since confirm
			grantAt(1), grantAt(1), // the database recovers, nobody took over
		}}
		var body bodySpy
		body.pulse = p
		var logs logCapture
		opt := testOptions(s, logs.logger())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- RunAsLeader(ctx, s, opt, body.run) }()

		// Consequences again: the error ticks ping before the loop has
		// decided anything about them.
		p.wait(t, "the first error tick", func() bool { return s.acquires() >= 2 })
		if !body.isLive() {
			t.Error("one missed renewal ended leadership inside the ttl budget")
		}
		p.wait(t, "the error that crosses the ttl budget", func() bool {
			return s.acquires() >= 4 && !body.isLive()
		})
		p.wait(t, "the recovery stint", func() bool {
			return s.acquires() >= 5 && len(body.epochsRun()) == 2
		})

		wantEvents := []string{
			string(reason.LEASEAcquired),
			string(reason.LEASELost),
			string(reason.LEASEAcquired),
		}
		if got := s.eventCodes(); strings.Join(got, ",") != strings.Join(wantEvents, ",") {
			t.Errorf("recorded events %v, want %v: an uncertain regain is a fresh claim, "+
				"nobody proved a takeover", got, wantEvents)
		}
		if got := body.epochsRun(); len(got) != 2 {
			t.Errorf("the body ran %d stints, want two", len(got))
		}
		// The cause travels as a structured field; its value contains spaces,
		// so slog renders it quoted. Match on the message and the value.
		if !strings.Contains(logs.text(), string(reason.LEASELost)) ||
			!strings.Contains(logs.text(), "ttl passed without a confirmed renewal") {
			t.Errorf("the loss line does not name its cause:\n%s", logs.text())
		}

		if err := stop(t, cancel, done); !errors.Is(err, context.Canceled) {
			t.Errorf("the loop returned %v, want context.Canceled", err)
		}
	})
}

func TestCleanShutdownReleasesTheLeaseAfterStoppingTheBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := newPulse()
		s := &scriptStore{pulse: p, steps: []step{grantAt(1), grantAt(1)}}
		var body bodySpy
		body.pulse = p
		var logs logCapture
		opt := testOptions(s, logs.logger())

		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- RunAsLeader(ctx, s, opt, body.run) }()

		p.wait(t, "two confirmed renewals with one body stint", func() bool {
			return s.acquires() >= 2 && len(body.epochsRun()) == 1
		})
		err := stop(t, cancel, done)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("the loop returned %v, want context.Canceled", err)
		}
		releases := 0
		for _, c := range s.ledger() {
			if c == "release" {
				releases++
			}
		}
		if releases != 1 {
			t.Errorf("shutdown released the lease %d times, want exactly once", releases)
		}
		if ledger := s.ledger(); ledger[len(ledger)-1] != "release" {
			t.Errorf("the ledger ends with %q, want the release as the last act", ledger[len(ledger)-1])
		}
		for _, code := range s.eventCodes() {
			if code == string(reason.LEASELost) {
				t.Error("a clean shutdown wrote a loss row; losing means a rival owns a live lease")
			}
		}
	})
}
