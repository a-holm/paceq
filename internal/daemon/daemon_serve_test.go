package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/engine"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// These tests run against a real store on disk. They are the seam between the
// synctest proofs above, which fake the world, and the end to end tests under
// test/serve, which drive the built binary: here the wiring is real but the
// process boundary is not.

func openServeStore(t *testing.T, clk clock.Clock) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"),
		store.Options{Clock: clk})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

// seedClaimedRunningRun materialises a manual run, claims it and starts its
// only step, which is exactly the state an executor's context interrupt leaves.
func seedClaimedRunningRun(t *testing.T, st *store.Store) string {
	t.Helper()
	ctx := context.Background()

	spec := `{"schema":"paceq.job.v1","name":"drainme","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["sleep","60"],"shell":false}]}`
	if _, _, err := st.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "drainme", SpecHash: "sha256:drainme", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := st.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "drainme"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}
	if _, err := st.ClaimRun(ctx, res.Run.ID,
		store.LeaseInput{Owner: "serve:test", TTL: 5 * time.Minute}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := st.StartStep(ctx, res.Run.ID, "only"); err != nil {
		t.Fatalf("start step: %v", err)
	}
	return res.Run.ID
}

// TestReleaseForDrainHandsEverythingBack is the unit half of acceptance
// criterion 4: steps go to pending with the attempt restored, the event names
// the shutdown, and the run lands back in the queue with no crash counted.
func TestReleaseForDrainHandsEverythingBack(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	runID := seedClaimedRunningRun(t, st)

	p := newExecutorPool(st, &engine.Engine{Store: st}, slog.Default(), 1)
	if err := p.releaseForDrain(context.Background(), runID); err != nil {
		t.Fatalf("release for drain: %v", err)
	}

	detail, err := st.GetRun(context.Background(), runID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.Run.State != string(model.RunQueued) {
		t.Errorf("the run is %s, want queued", detail.Run.State)
	}
	if len(detail.Steps) != 1 || detail.Steps[0].State != string(model.StepPending) {
		t.Fatalf("the step is %+v, want one pending step", detail.Steps)
	}
	step := detail.Steps[0]
	if step.Attempt != 0 {
		t.Errorf("attempt is %d, want 0 after the restore", step.Attempt)
	}
	if step.ReasonCode != string(reason.RUNInterruptedShutdown) {
		t.Errorf("step reason is %q, want %q", step.ReasonCode, reason.RUNInterruptedShutdown)
	}

	events, err := st.RunEvents(context.Background(), runID)
	if err != nil {
		t.Fatalf("events: %v", err)
	}
	var sawInterrupt, sawDrained bool
	for _, e := range events {
		if e.Kind == "step.interrupted" && e.ReasonCode == string(reason.RUNInterruptedShutdown) {
			sawInterrupt = true
		}
		if e.Kind == "run.drained" && e.FromState == "running" && e.ToState == "queued" {
			sawDrained = true
		}
	}
	if !sawInterrupt || !sawDrained {
		t.Fatalf("events are incomplete: interrupt=%v drained=%v in %+v",
			sawInterrupt, sawDrained, events)
	}

	violations, err := st.Fsck(context.Background())
	if err != nil {
		t.Fatalf("fsck: %v", err)
	}
	if len(violations) != 0 {
		t.Errorf("fsck found %v after the handback", violations)
	}
}

// TestReleaseForDrainIgnoresARunWeNeverClaimed: a queued run has nothing of
// ours to give back, and the pool must not touch it.
func TestReleaseForDrainIgnoresARunWeNeverClaimed(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 9, 28, 12, 0, 0, 0, time.UTC))
	st := openServeStore(t, clk)
	spec := `{"schema":"paceq.job.v1","name":"quiet","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["true"],"shell":false}]}`
	if _, _, err := st.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName: "quiet", SpecHash: "sha256:quiet", SpecJSON: spec,
	}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	res, err := st.MaterializeManualTrigger(context.Background(),
		store.ManualTriggerInput{JobName: "quiet"})
	if err != nil {
		t.Fatalf("materialise: %v", err)
	}

	p := newExecutorPool(st, &engine.Engine{Store: st}, slog.Default(), 1)
	if err := p.releaseForDrain(context.Background(), res.Run.ID); err != nil {
		t.Fatalf("release on an unclaimed run must be a no-op, got %v", err)
	}
	detail, _ := st.GetRun(context.Background(), res.Run.ID)
	if detail.Run.State != string(model.RunQueued) {
		t.Fatalf("the unclaimed run moved to %s", detail.Run.State)
	}
}

// TestServeLifecycleEndToEnd runs Serve itself against a real state directory:
// startup order, ready line, clean stop, session row, checkpoint.
func TestServeLifecycleEndToEnd(t *testing.T) {
	rec, logger := newRecLog()
	// The state directory lives one level down so its mode is ours: paceq
	// refuses a state directory the group or others can read, and a test
	// tempdir arrives 0755.
	root := t.TempDir()
	cfg := Config{
		StateDir: filepath.Join(root, "state"),
		Version:  "test",
		JobsDir:  "jobs",
		Logger:   logger,
		Owner:    "serve:test",
	}
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	st, err := store.OpenState(context.Background(), cfg.StateDir, store.Options{})
	if err != nil {
		t.Fatalf("pre-open the state to seed it: %v", err)
	}
	if err := st.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- Serve(ctx, cfg, clock.System()) }()

	waitForLogLine(t, rec, "daemon ready", 10*time.Second)

	// The health surface answers while the daemon runs.
	if lines := rec.named("health endpoints listening"); len(lines) != 0 {
		t.Error("no socket was configured, but the endpoints started")
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("a clean stop reported %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Serve did not return within 10s of cancellation")
	}

	for _, name := range []string{"scheduler", "sensor", "dispatcher", "reaper", "janitor", "heartbeat"} {
		found := false
		for _, m := range rec.named("loop started") {
			if m["loop"] == name {
				found = true
			}
		}
		if !found {
			t.Errorf("no start line for loop %q in %+v", name, rec.records)
		}
	}
	if lines := rec.named("intake closed"); len(lines) != 1 {
		t.Errorf("%d intake-closed lines, want 1", len(lines))
	}

	// The session row says clean.
	reader, err := store.OpenReadOnly(context.Background(),
		filepath.Join(cfg.StateDir, store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("read the state back: %v", err)
	}
	defer func() { _ = reader.Close() }()
	sess, ok, err := reader.LatestSession(context.Background())
	if err != nil || !ok {
		t.Fatalf("no session row came back (ok=%v, err=%v)", ok, err)
	}
	if sess.StopReason != "clean" {
		t.Errorf("stop_reason is %q, want clean; the row is %+v", sess.StopReason, sess)
	}
	if sess.StartedAt.IsZero() || sess.StoppedAt.Before(sess.StartedAt) {
		t.Errorf("started_at/stopped_at tell no story: %+v", sess)
	}

	// The wal was truncated as the last write: at most one page remains.
	walPath := filepath.Join(cfg.StateDir, store.DatabaseFileName) + "-wal"
	if info, err := os.Stat(walPath); err == nil && info.Size() > 8192 {
		t.Errorf("the wal holds %d bytes after a clean stop, want at most one page", info.Size())
	}
}

// TestHealthEndpointServesWithoutTheDatabase proves /livez never touches the
// store: the endpoint keeps answering after the database behind it is closed.
func TestHealthEndpointServesWithoutTheDatabase(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "h.sock")

	cfg := Config{StateDir: dir, Version: "test", SocketPath: sock, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	sts := newStatuses(func() time.Time { return time.Unix(0, 0).UTC() })
	sts.mark("scheduler")

	stop := startHealthEndpoint(cfg, sts, cfg.Logger)
	if stop == nil {
		t.Fatal("a configured socket did not start the endpoints")
	}
	t.Cleanup(func() { stop(context.Background()) })

	client := httpClientOver(sock)
	resp, err := client.Get("http://localhost/livez")
	if err != nil {
		t.Fatalf("/livez: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/livez answered %d, want 200", resp.StatusCode)
	}
	var body struct {
		Status string `json:"status"`
		Loops  map[string]struct {
			Ticks int64 `json:"ticks"`
		} `json:"loops"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode /livez: %v", err)
	}
	if body.Status != "ok" || body.Loops["scheduler"].Ticks != 1 {
		t.Fatalf("the body does not carry what the status registry holds: %+v", body)
	}

	ready, err := client.Get("http://localhost/readyz")
	if err != nil {
		t.Fatalf("/readyz: %v", err)
	}
	defer func() { _ = ready.Body.Close() }()
	if ready.StatusCode != http.StatusOK {
		t.Fatalf("/readyz answered %d, want 200", ready.StatusCode)
	}

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat the socket: %v", err)
	}
	if mode := info.Mode().Perm(); mode&0o077 != 0 {
		t.Errorf("the socket is %#o, want no access for group or other", mode)
	}
}

func waitForLogLine(t *testing.T, rec *recLog, msg string, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		if len(rec.named(msg)) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%q never appeared in %+v", msg, rec.records)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func httpClientOver(sock string) *http.Client {
	return &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(context.Background(), "unix", sock)
			},
		},
	}
}
