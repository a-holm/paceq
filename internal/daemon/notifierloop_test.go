package daemon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/store"
)

// The dispatcher test drives drainOutboxOnce end to end against a REAL
// subprocess relay whose script exits 1 until its call counter reaches the
// threshold in relay.calls. That gives deterministic delivery failures for
// the backoff story without fake exec seams, and the unknown-target row
// proves failed_at sealing (AC five) on the same wake.

type notifyFixture struct {
	Dir      string
	StateDir string
	Store    *store.Store
	Clock    *clock.Fake
	Service  *Notifications
	Calls    string
}

func newNotifyFixture(t *testing.T, okAfter int) *notifyFixture {
	t.Helper()
	dir := t.TempDir()
	stateDir := filepath.Join(dir, "state")
	script := filepath.Join(dir, "relay.sh")
	calls := filepath.Join(dir, "relay.calls")
	okAfterPath := filepath.Join(dir, "calls.okafter")

	if err := os.WriteFile(okAfterPath, []byte(fmtInt(okAfter)), 0o600); err != nil {
		t.Fatal(err)
	}
	body := "#!/bin/sh\n" +
		"n=$(cat \"" + okAfterPath + "\")\n" +
		"c=$(cat \"" + calls + "\" 2>/dev/null || echo 0)\n" +
		"c=$((c+1))\n" +
		"printf '%s\\n' \"$c\" > \"" + calls + "\"\n" +
		"[ \"$c\" -ge \"$n\" ] && exit 0\nexit 1\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatalf("write relay: %v", err)
	}
	configBody := "notifiers:\n" +
		"  vakt:\n" +
		"    type: exec\n" +
		"    run: [\"" + script + "\"]\n" +
		"    timeout: 30s\n" +
		"notify_defaults:\n" +
		"  on_failure: [vakt]\n" +
		"  max_attempts: 2\n"
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stateDir, NotifierFileName), []byte(configBody), 0o600); err != nil {
		t.Fatal(err)
	}

	s := openStateForNotify(t, stateDir)
	cfg, lerr := LoadNotificationConfig(stateDir, "")
	if lerr != nil || cfg == nil {
		t.Fatalf("load config: %v (%v)", cfg, lerr)
	}
	if cfg.Defaults.MaxAttempts == 0 {
		t.Fatalf("the parser lost max_attempts; the dispatcher would retry for ever")
	}
	t.Logf("parsed MaxAttempts=%d", cfg.Defaults.MaxAttempts)
	clk := clock.NewFake(time.Date(2026, 9, 17, 6, 0, 0, 0, time.UTC))
	return &notifyFixture{
		Dir:      dir,
		StateDir: stateDir,
		Store:    s,
		Clock:    clk,
		Service:  NewNotifications(s, clk, nil, cfg, os.Stderr),
		Calls:    calls,
	}
}

func openStateForNotify(t *testing.T, stateDir string) *store.Store {
	t.Helper()
	s, err := store.OpenState(context.Background(), stateDir, store.Options{})
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

func fmtInt(n int) string {
	digits := ""
	for n > 0 {
		digits = string(rune('0'+n%10)) + digits
		n /= 10
	}
	if digits == "" {
		return "0"
	}
	return digits
}

// seedRows uses the SLA transition seam as the public outbox writer: the
// first breaching verdict opens the episode and inserts the notes exactly
// like a real breach would.
// seedTwoTargets plants one row per target through a single SLA transition:
// the real relay target plus a name nobody configured.
func seedRows(t *testing.T, f *notifyFixture) {
	t.Helper()
	at := f.Clock.Now()
	notes := []model.Notification{
		{
			Topic: model.TopicRunFailed, Subject: "backup-db", Target: "vakt",
			Payload:   `{"event":"run.failed","job":"backup-db"}`,
			DedupKey:  model.TopicRunFailed + "|backup-db|vakt|k-vakt",
			CreatedAt: at,
		},
		{
			Topic: model.TopicRunFailed, Subject: "backup-db", Target: "ghost",
			Payload:   `{"event":"run.failed","job":"backup-db"}`,
			DedupKey:  model.TopicRunFailed + "|backup-db|ghost|k-ghost",
			CreatedAt: at,
		},
	}
	ch := store.SLAEpisodeChange{Job: "backup-db", Breaching: true, Notes: notes}
	if err := f.Store.ApplySLAEpisodes(context.Background(), []store.SLAEpisodeChange{ch}, at); err != nil {
		t.Fatalf("seed via episode seam: %v", err)
	}
}

func TestDispatcherDeliversOnceAfterBackoff(t *testing.T) {
	ctx := context.Background()
	f := newNotifyFixture(t, 2) // attempt 1 exits 1, attempt 2 exits 0

	seedRows(t, f) // two events, two targets, one known

	// Wake one: vakt's first attempt exits 1, ghost is unknown to any
	// configuration. Both rows are pending again with attempts raised.

	drainOutboxOnce(ctx, f.Service)

	states := rowsByIdentity(t, f.Store)
	vakt, hasVakt := states["vakt"]
	ghost, hasGhost := states["ghost"]
	if !hasVakt || !hasGhost {
		t.Fatalf("wake one lost rows: %+v", states)
	}
	if ghost.State != "pending" || ghost.Attempts != 1 {
		t.Fatalf("unknown target treated like any failing notifier first: %+v", ghost)
	}
	if vakt.State != "pending" || vakt.Attempts != 1 {
		t.Fatalf("real target after a failed attempt must be pending with attempts=1, got %+v", vakt)
	}

	// Across ten seconds of backoff, attempt two: the relay succeeds for
	// vakt, while ghost burns its LAST allowed attempt (max_attempts: 2)
	// and lands in permanent failed history right beside it.
	f.Clock.Advance(11 * time.Second)
	drainOutboxOnce(ctx, f.Service)

	states = rowsByIdentity(t, f.Store)
	vakt = states["vakt"]
	ghost = states["ghost"]
	if vakt.State != "delivered" || vakt.Delivered == nil || vakt.Attempts != 2 {
		t.Fatalf("the recovered row did not settle delivered on attempt 2: %+v", vakt)
	}
	if ghost.State != "failed" || ghost.Attempts != 2 || !strings.Contains(ghost.LastError, "unknown notifier") {
		t.Fatalf("the unconfigured target did not seal failed: %+v", ghost)
	}

	// A third immediate wake re-sends NOTHING: delivered is terminal here,
	// which keeps the send counter at exactly the attempts observed.
	drainOutboxOnce(ctx, f.Service)
	raw, err := os.ReadFile(f.Calls)
	if err != nil {
		t.Fatalf("relay calls file missing: %v", err)
	}
	if strings.TrimSpace(string(raw)) != "2" {
		t.Fatalf("relay saw %q calls, want exactly the 2 attempted sends", raw)
	}
}

func rowsByIdentity(t *testing.T, s *store.Store) map[string]store.NotificationSummary {
	t.Helper()
	rows, err := s.ListNotifications(context.Background(), store.NotificationFilter{Limit: 100})
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	out := map[string]store.NotificationSummary{}
	for _, r := range rows {
		out[r.Target] = r
	}
	return out
}
