package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/model"
)

// countingHook is the metrics half of a daemon, reduced to what the
// permanent-failure counter needs.
type countingHook struct {
	ticks    int
	reclaims int
	gaveUp   int
}

func (h *countingHook) ObserveTick(string, string, string, string) { h.ticks++ }
func (h *countingHook) ObserveLeaseReclaims(int)                   { h.reclaims++ }
func (h *countingHook) ObserveNotificationGaveUp()                 { h.gaveUp++ }

// TestGivingUpIsCountedOncePerDecision holds the production path to the
// counter's promise. The give-up count has to come from the moment of the
// decision: the row it writes is cleared again by the operator's retry, so a
// count over rows falls under ordinary work.
func TestGivingUpIsCountedOncePerDecision(t *testing.T) {
	ctx := context.Background()
	hook := &countingHook{}
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(ctx, path, Options{Metrics: hook})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	if err := s.RecordOpsNotifications(ctx, []model.Notification{
		aNote("run.failed", "nightly failed", "ops-webhook", "dedup-1"),
	}); err != nil {
		t.Fatalf("write the notification: %v", err)
	}
	msgs, err := s.ClaimOutbox(ctx, 10, time.UnixMilli(2_000_000).UTC(), time.Minute)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("claim the notification: %d claimed, err=%v", len(msgs), err)
	}

	now := time.UnixMilli(3_000_000).UTC()
	if err := s.MarkOutboxFailed(ctx, msgs[0].ID, now, "gave up after 8 attempts"); err != nil {
		t.Fatalf("give up: %v", err)
	}
	if hook.gaveUp != 1 {
		t.Fatalf("giving up was counted %d times, want 1", hook.gaveUp)
	}
	rows, err := s.MetricsNotificationsGivenUp(ctx)
	if err != nil || rows != 1 {
		t.Fatalf("the given-up gauge reads %d, err=%v", rows, err)
	}

	if _, err := s.RetryOutbox(ctx, msgs[0].ID); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if hook.gaveUp != 1 {
		t.Errorf("a retry moved the give-up count to %d", hook.gaveUp)
	}
	rows, err = s.MetricsNotificationsGivenUp(ctx)
	if err != nil || rows != 0 {
		t.Fatalf("the given-up gauge reads %d after a retry, want 0, err=%v", rows, err)
	}
}
