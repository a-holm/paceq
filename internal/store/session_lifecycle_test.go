package store

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
)

// TestSessionHeartbeatAndCleanStop walks one session through its whole life. The
// heartbeat is what bounds an outage later: the gap starts at the last
// heartbeat, not at the start of the run.
func TestSessionHeartbeatAndCleanStop(t *testing.T) {
	clk := clock.NewFake(sessionOrigin)
	s := sessionStore(t, clk, constantBoot("boot-one"))
	ctx := context.Background()

	sess, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}

	previous := sessionOrigin
	for beat := 1; beat <= 3; beat++ {
		clk.Advance(time.Second)
		if err := s.TouchSession(ctx, sess.ID); err != nil {
			t.Fatalf("heartbeat %d: %v", beat, err)
		}
		stored := readSession(t, s, sess.ID)
		if !stored.LastSeenAt.After(previous) {
			t.Fatalf("heartbeat %d left last_seen_at at %s, want later than %s",
				beat, stored.LastSeenAt, previous)
		}
		if !stored.StartedAt.Equal(sessionOrigin) {
			t.Errorf("heartbeat %d moved started_at to %s, want %s", beat, stored.StartedAt, sessionOrigin)
		}
		previous = stored.LastSeenAt
	}

	clk.Advance(time.Second)
	if err := s.StopSession(ctx, sess.ID); err != nil {
		t.Fatalf("stop session: %v", err)
	}

	stopped := readSession(t, s, sess.ID)
	if stopped.StopReason != "clean" {
		t.Errorf("stop_reason is %q, want %q", stopped.StopReason, "clean")
	}
	if want := sessionOrigin.Add(4 * time.Second); !stopped.StoppedAt.Equal(want) {
		t.Errorf("stopped_at is %s, want %s", stopped.StoppedAt, want)
	}
	if got := openSessions(t, s); got != 0 {
		t.Errorf("%d open sessions after a clean stop, want 0", got)
	}

	if err := s.TouchSession(ctx, sess.ID); err == nil {
		t.Error("a stopped session still accepts heartbeats, so a dead run can look alive")
	}
}

// TestOpenSessionNamesTheRunningProcess covers the lookup behind the lock
// error: whoever is refused the state lock has to learn who holds it.
func TestOpenSessionNamesTheRunningProcess(t *testing.T) {
	clk := clock.NewFake(sessionOrigin)
	s := sessionStore(t, clk, constantBoot("boot-one"))
	ctx := context.Background()

	if _, _, err := s.OpenSession(ctx); err != nil {
		t.Fatalf("read the open session of an unused database: %v", err)
	}
	if _, found, _ := s.OpenSession(ctx); found {
		t.Fatal("an unused database reports an open session")
	}

	started, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start session: %v", err)
	}

	open, found, err := s.OpenSession(ctx)
	if err != nil {
		t.Fatalf("read the open session: %v", err)
	}
	if !found {
		t.Fatal("a started session is not reported as open")
	}
	if open.ID != started.ID || open.PID != started.PID || open.Version != started.Version {
		t.Errorf("open session is %+v, want the row started as %+v", open, started)
	}
	if !open.StartedAt.Equal(sessionOrigin) {
		t.Errorf("open session started at %s, want %s", open.StartedAt, sessionOrigin)
	}

	if err := s.StopSession(ctx, started.ID); err != nil {
		t.Fatalf("stop session: %v", err)
	}
	if _, found, _ = s.OpenSession(ctx); found {
		t.Error("a stopped session is still reported as open")
	}
}

// TestStartSessionWithoutABootID is the degradation path, which is what runs on
// every platform that is not Linux. Losing the boot id costs evidence, not
// correctness, so nothing may fail and the notice is logged once.
func TestStartSessionWithoutABootID(t *testing.T) {
	logged := captureLogs(t)

	clk := clock.NewFake(sessionOrigin)
	unavailable := func() (string, error) { return "", errors.ErrUnsupported }
	s := sessionStore(t, clk, unavailable)
	ctx := context.Background()

	first, err := s.StartSession(ctx, "1.2.3")
	if err != nil {
		t.Fatalf("start a session without a boot id: %v", err)
	}
	if first.BootID != "" {
		t.Errorf("session carries boot id %q, want none", first.BootID)
	}
	if stored := readSession(t, s, first.ID); stored.BootID != "" {
		t.Errorf("stored boot id is %q, want NULL", stored.BootID)
	}
	if got := metaValue(t, s, "current_boot_id"); got != "" {
		t.Errorf("meta.current_boot_id is %q, want no row: an unknown boot id is not evidence", got)
	}

	clk.Advance(time.Minute)
	if _, err = s.StartSession(ctx, "1.2.3"); err != nil {
		t.Fatalf("start a second session without a boot id: %v", err)
	}
	if s.BootChanged() {
		t.Error("BootChanged is true without a boot id, so a restart would be claimed on no evidence")
	}

	if got := strings.Count(logged.String(), "boot id unavailable"); got != 1 {
		t.Errorf("the missing boot id was logged %d times, want exactly 1:\n%s", got, logged)
	}
}

// captureLogs points the default logger at a buffer for the duration of a test.
func captureLogs(t *testing.T) *bytes.Buffer {
	t.Helper()

	buf := &bytes.Buffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return buf
}
