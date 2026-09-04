package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/obs"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The daemon side of the disk-guard (#44): the health split, the outbox
// notifications with their throttle, and the config keys.

func TestReadyzDegradesWithTheDiskAndLivezDoesNot(t *testing.T) {
	dir := t.TempDir()
	sock := filepath.Join(dir, "h.sock")

	cfg := Config{StateDir: dir, Version: "test", SocketPath: sock, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	sts := newStatuses(func() time.Time { return time.Unix(0, 0).UTC() })
	sts.mark("diskguard")

	degraded := false
	stop, _, err := startHealthEndpoint(cfg, sts, cfg.Logger, nil, nil, func() bool { return degraded })
	if err != nil {
		t.Fatalf("a configured socket did not start the endpoints: %v", err)
	}
	t.Cleanup(func() { stop(context.Background()) })

	client := httpClientOver(sock)

	get := func(path string) (int, map[string]any) {
		t.Helper()
		resp, err := client.Get("http://localhost" + path)
		if err != nil {
			t.Fatalf("%s: %v", path, err)
		}
		defer func() { _ = resp.Body.Close() }()
		var body map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		return resp.StatusCode, body
	}

	code, body := get("/readyz")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("a healthy disk answered /readyz %d %v, want ready", code, body)
	}
	code, _ = get("/livez")
	if code != http.StatusOK {
		t.Fatalf("a healthy disk answered /livez %d, want 200", code)
	}

	// Under the floor: readiness degrades, liveness does not. A full disk
	// that restarted the daemon would kill the loops that are cleaning up;
	// that is the livez/readyz split the plan calls security critical.
	degraded = true
	code, body = get("/readyz")
	if code != http.StatusServiceUnavailable || body["status"] != "degraded" {
		t.Fatalf("a degraded disk answered /readyz %d %v, want 503 degraded", code, body)
	}
	code, _ = get("/livez")
	if code != http.StatusOK {
		t.Fatalf("a degraded disk answered /livez %d, want 200: the loop is still ticking", code)
	}

	degraded = false
	code, _ = get("/readyz")
	if code != http.StatusOK {
		t.Fatalf("a recovered disk answered /readyz %d, want 200", code)
	}
}

// diskTestStore opens a migrated store for the notification tests.
func diskTestStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "state.db"), store.Options{Clock: clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return st
}

// subjectForCount is the fixed subject each ops topic writes, the filter
// the count reads through.
func subjectForCount(topic string) string {
	if topic == "wal.growth" {
		return "wal"
	}
	return "disk"
}

func outboxCount(t *testing.T, st *store.Store, topic string) int {
	t.Helper()
	rows, err := st.ListNotifications(context.Background(), store.NotificationFilter{Subject: subjectForCount(topic), Limit: 100})
	if err != nil {
		t.Fatalf("list notifications: %v", err)
	}
	return len(rows)
}

func TestTwentyDegradedCyclesGiveOneDiskLowNotification(t *testing.T) {
	st := diskTestStore(t)
	clk := clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	n := &opsNotifier{
		st:       st,
		targets:  []string{"oncall"},
		throttle: 15 * time.Minute,
		clk:      clk,
		log:      slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	episode := clk.Now().UTC()
	for range 20 { // twenty consecutive degraded cycles, one per 30 seconds
		clk.Advance(30 * time.Second)
		n.emitDisk(context.Background(), obs.DiskEvent{
			Event:      "disk.low",
			Level:      10, // error
			State:      obs.DiskDegraded,
			FreeBytes:  4 << 30,
			TotalBytes: 50 << 30,
			FloorBytes: 1 << 30,
			Since:      episode,
		})
	}
	if got := outboxCount(t, st, "disk.low"); got != 1 {
		t.Fatalf("twenty degraded cycles produced %d disk.low rows, want 1", got)
	}
	rows, err := st.ListNotifications(context.Background(), store.NotificationFilter{Subject: "disk", Limit: 100})
	if err != nil || len(rows) != 1 {
		t.Fatalf("read the row: %v", err)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(rows[0].Payload), &payload); err != nil {
		t.Fatalf("the payload is not JSON: %v", err)
	}
	if payload["state"] != "degraded" || payload["doctor_cmd"] != "paceq doctor" {
		t.Errorf("the payload does not carry the degraded facts: %v", payload)
	}
	if payload["free_bytes"].(float64) != float64(4<<30) {
		t.Errorf("the payload does not carry the measurement: %v", payload["free_bytes"])
	}

	// A fresh episode inside the throttle window collapses into the first
	// one; only past the window does a new alert arrive.
	newEpisode := clk.Now().UTC()
	clk.Advance(30 * time.Second)
	n.emitDisk(context.Background(), obs.DiskEvent{Event: "disk.low", Level: 10, State: obs.DiskDegraded, Since: newEpisode})
	if got := outboxCount(t, st, "disk.low"); got != 1 {
		t.Fatalf("an episode inside the throttle window produced %d rows, want 1", got)
	}
	clk.Advance(16 * time.Minute)
	newEpisode = clk.Now().UTC()
	n.emitDisk(context.Background(), obs.DiskEvent{Event: "disk.low", Level: 10, State: obs.DiskDegraded, Since: newEpisode})
	if got := outboxCount(t, st, "disk.low"); got != 2 {
		t.Fatalf("an episode past the throttle window produced %d rows, want 2", got)
	}
}

func TestWarningsAndHealthyCyclesNotifyNobody(t *testing.T) {
	st := diskTestStore(t)
	n := &opsNotifier{
		st:      st,
		targets: []string{"oncall"},
		clk:     clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)),
		log:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	n.emitDisk(context.Background(), obs.DiskEvent{Event: "disk.low", State: obs.DiskWarning, Since: time.Now()})
	n.emitDisk(context.Background(), obs.DiskEvent{Event: "disk.low", State: obs.DiskNormal, Since: time.Now()})
	if got := outboxCount(t, st, "disk.low"); got != 0 {
		t.Fatalf("a warning or a recovery wrote %d rows, want 0: only degraded notifies", got)
	}
}

func TestWALGrowthNotificationNamesTheLikelyCause(t *testing.T) {
	st := diskTestStore(t)
	n := &opsNotifier{
		st:      st,
		targets: []string{"oncall"},
		clk:     clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)),
		log:     slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}
	n.emitWAL(context.Background(), obs.WALEvent{
		Level:     10, // error
		WalBytes:  300 << 20,
		WarnBytes: 64 << 20,
		Since:     time.UnixMilli(1234),
	})
	rows, err := st.ListNotifications(context.Background(), store.NotificationFilter{Subject: "wal", Limit: 10})
	if err != nil || len(rows) != 1 {
		t.Fatalf("the wal.growth event produced %d rows (%v), want 1", len(rows), err)
	}
	if rows[0].Subject != "wal" || rows[0].Target != "oncall" {
		t.Errorf("the row is %s/%s, want wal/oncall", rows[0].Subject, rows[0].Target)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(rows[0].Payload), &payload); err != nil {
		t.Fatalf("the payload is not JSON: %v", err)
	}
	if payload["note"] == nil || !jsonString(payload["note"]) {
		t.Fatalf("the payload carries no note: %v", payload)
	}
}

func jsonString(v any) bool {
	s, ok := v.(string)
	return ok && s != ""
}

func TestRunHoldGateRefusesOnlyWhenDegraded(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC))
	disk := &fixedDisk{free: 1 << 20, total: 50 << 30}
	guard := obs.NewGuard(obs.GuardConfig{
		StateDir: t.TempDir(),
		Clock:    clk,
		Statfs:   disk.statfs,
	})
	gate := runHoldGate(guard)
	if gate() != nil {
		t.Fatal("a guard that has not measured holds runs")
	}
	if err := guard.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	hold := gate()
	if hold == nil || hold.Code != reason.RUNRejectedDiskLow {
		t.Fatalf("a degraded guard's gate is %v, want the disk-low code", hold)
	}
	if hold.Data["min_free_bytes"] == nil {
		t.Errorf("the hold carries no measurements: %v", hold.Data)
	}
	disk.free = 40 << 30
	if err := guard.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if err := guard.Step(context.Background()); err != nil {
		t.Fatalf("step: %v", err)
	}
	if gate() != nil {
		t.Fatal("a recovered guard still holds runs")
	}
}

type fixedDisk struct{ free, total uint64 }

func (f *fixedDisk) statfs(string) (uint64, uint64, error) { return f.free, f.total, nil }

func TestParseByteSizeAndLimits(t *testing.T) {
	cases := []struct {
		raw  string
		want int64
	}{
		{"10GiB", 10 << 30},
		{"64MiB", 64 << 20},
		{"512KiB", 512 << 10},
		{"4096", 4096},
		{"1gib", 1 << 30},
	}
	for _, tc := range cases {
		got, err := parseByteSize(tc.raw)
		if err != nil || got != tc.want {
			t.Errorf("parseByteSize(%q) = %d, %v; want %d", tc.raw, got, err, tc.want)
		}
	}
	if _, err := parseByteSize("ten GiB"); err == nil {
		t.Error("parseByteSize accepted a word where a number belongs")
	}
	if _, err := parseByteSize("-5GiB"); err == nil {
		t.Error("parseByteSize accepted a negative size")
	}

	var doc limitsDoc
	doc.LogMaxBytes = "1MiB"
	doc.DiskMinFreePercent = 10
	doc.DiskMinFreeBytes = "512MiB"
	doc.WalWarnBytes = "8MiB"
	limits, err := doc.resolve()
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if limits.LogMaxBytes != 1<<20 || limits.MinFreeBytes != 512<<20 || limits.WalWarnBytes != 8<<20 || limits.MinFreePercent != 10 {
		t.Errorf("resolve produced %+v", limits)
	}

	bad := limitsDoc{LogMaxBytes: "soon"}
	if _, err := bad.resolve(); err == nil {
		t.Error("a nonsense size resolved without error")
	}
	badPct := limitsDoc{DiskMinFreePercent: 150}
	if _, err := badPct.resolve(); err == nil {
		t.Error("a percent over 100 resolved without error")
	}
}

func TestConfigYAMLLimitsAreReadAndStrict(t *testing.T) {
	dir := t.TempDir()
	cfgYAML := "notifiers:\n  stderr:\n    type: stderr\nlimits:\n  log_max_bytes: 2GiB\n  disk_min_free_percent: 12\n"
	if err := os.WriteFile(filepath.Join(dir, NotifierFileName), []byte(cfgYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadNotificationConfig(dir, "")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Limits.LogMaxBytes != 2<<30 || cfg.Limits.MinFreePercent != 12 {
		t.Fatalf("the limits section was not read: %+v", cfg.Limits)
	}

	strict := dir + "-strict"
	if err := os.MkdirAll(strict, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(strict, NotifierFileName), []byte("limits:\n  log_max_bytz: 2GiB\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadNotificationConfig(strict, ""); err == nil {
		t.Fatal("a typo'd limits key loaded without error; the config must fail loudly")
	}
}
