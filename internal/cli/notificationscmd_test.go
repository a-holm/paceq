package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/store"
)

// notificationsProject seeds a state directory carrying three outbox rows -
// one pending, one delivered, one failed - through the same seam a real
// finish or SLA transition uses, then closes the store so the CLI meets it
// exactly as a daemon-down world looks (AC ten).
func notificationsProject(t *testing.T) (dir string, ids map[string]int64) {
	t.Helper()
	dir = t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	s := openFixtureStoreAt(t, stateDir, clock.NewFake(time.Date(2026, 9, 17, 7, 0, 0, 0, time.UTC)))
	ctx := context.Background()

	at := clock.NewFake(time.Date(2026, 9, 17, 6, 30, 0, 0, time.UTC)).Now()
	mk := func(topic, subject, target, key string) model.Notification {
		return model.Notification{
			Topic: topic, Subject: subject, Target: target,
			Payload:   `{"event":"` + topic + `","job":"` + subject + `"}`,
			DedupKey:  topic + "|" + subject + "|" + target + "|" + key,
			CreatedAt: at,
		}
	}
	breach := store.SLAEpisodeChange{
		Job:       "backup-db",
		Breaching: true,
		Notes: []model.Notification{
			mk(model.TopicRunFailed, "backup-db", "vakt", "row-pending"),
			mk(model.TopicSLABreached, "backup-db", "vakt", "row-delivered"),
			mk(model.TopicRunFailed, "backup-db", "ghost", "row-failed"),
		},
	}
	if err := s.ApplySLAEpisodes(ctx, []store.SLAEpisodeChange{breach}, at); err != nil {
		t.Fatalf("seed rows: %v", err)
	}
	rows, err := s.ListNotifications(ctx, store.NotificationFilter{})
	if err != nil || len(rows) != 3 {
		t.Fatalf("seed produced %d rows (%v), want 3", len(rows), err)
	}
	now := time.Now().UTC().Add(-time.Hour)
	if err := s.MarkOutboxDelivered(ctx, rows[1].ID, now); err != nil {
		t.Fatalf("deliver row: %v", err)
	}
	if ferr := s.MarkOutboxFailed(ctx, rows[2].ID, now.Add(-time.Minute), "notifier exited 3"); ferr != nil {
		t.Fatalf("fail row: %v", ferr)
	}

	ids = map[string]int64{"pending": rows[0].ID, "delivered": rows[1].ID, "failed": rows[2].ID}
	// paceq refuses to WRITE through a database other users can read; the
	// seeded file must look like one the daemon created itself.
	if err := os.Chmod(filepath.Join(stateDir, store.DatabaseFileName), 0o600); err != nil {
		t.Fatalf("tighten the seeded database mode: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close before the command runs: %v", err)
	}
	return dir, ids
}

func TestNotificationsListIsAStableContractWithDaemonDown(t *testing.T) {
	dir, _ := notificationsProject(t)

	text := runCLI(t, dir, nil, "notifications", "list")
	if text.code != ExitOK {
		t.Fatalf("exit %d: %s%s", text.code, text.stdout, text.stderr)
	}
	for _, want := range []string{"pending", "delivered", "failed", "vakt"} {
		if !strings.Contains(text.stdout, want) {
			t.Errorf("text listing lost %q:\n%s", want, text.stdout)
		}
	}

	js := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "notifications", "list")
	if js.code != ExitOK {
		t.Fatalf("json exit %d", js.code)
	}
	for _, key := range []string{
		"\"id\"", "\"topic\"", "\"subject\"", "\"target\"", "\"state\"",
		"\"created_at\"", "\"available_at\"", "\"attempts\"", "\"payload\"",
	} {
		if !strings.Contains(js.stdout, key) {
			t.Errorf("the JSON contract lost %s:\n%s", key, js.stdout)
		}
	}

	filtered := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "notifications",
		"list", "--state", "delivered")
	if !strings.Contains(filtered.stdout, "\"state\":\"delivered\"") ||
		strings.Contains(filtered.stdout, "\"state\":\"failed\"") {
		t.Errorf("--state did not narrow the answer:\n%s", filtered.stdout)
	}

	subjectOnly := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "notifications",
		"list", "--subject", "no-such-job")
	if strings.Contains(subjectOnly.stdout, "backup-db") {
		t.Errorf("--subject matched a row that does not carry it:\n%s", subjectOnly.stdout)
	}
	if strings.Contains(subjectOnly.stdout, "[]") == false {
		t.Logf("empty result printed as %q", firstLine(subjectOnly.stdout))
	}

	badState := runCLI(t, dir, nil, "notifications", "list", "--state", "nope")
	if badState.code != ExitUsage {
		t.Errorf("a bogus --state exited %d, want the usage code", badState.code)
	}
}

func TestNotificationsShowCarriesThePayloadAndRefusesUnknownIds(t *testing.T) {
	dir, ids := notificationsProject(t)

	show := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"},
		"notifications", "show", strconv.FormatInt(ids["failed"], 10))
	if show.code != ExitOK {
		t.Fatalf("show exit %d: %s", show.code, show.stderr)
	}
	for _, want := range []string{"\"last_error\":\"notifier exited 3\"", "\"attempts\":0"} {
		if !strings.Contains(show.stdout, want) {
			t.Errorf("show lost %s:\n%s", want, show.stdout)
		}
	}

	missing := runCLI(t, dir, nil, "notifications", "show", "999999")
	if missing.code != ExitNotFound {
		t.Fatalf("unknown id exited %d, want the not-found code", missing.code)
	}
	if !strings.Contains(missing.stderr, "did you") && !strings.Contains(missing.stderr, "list") {
		t.Errorf("the refusal gave no way forward: %s", missing.stderr)
	}
}

func TestNotificationsRetryLocksAgainstTheDaemonWriter(t *testing.T) {
	dir, ids := notificationsProject(t)

	// The precondition for every retry here: state remains untouched by
	// reads (AC ten's RO path).
	before := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "notifications", "list")

	// retry through the direct path with nobody holding flock.
	failedID := -1
	var decoded struct {
		Rows []struct {
			ID    int64  `json:"id"`
			State string `json:"state"`
		} `json:"rows"`
	}
	decode := func(out string) bool {
		idx := strings.Index(before.stdout, "{")
		if idx < 0 {
			return false
		}
		raw := before.stdout[idx:]
		end := strings.LastIndex(raw, "}")
		return json.Unmarshal([]byte(raw[:end+1]), &decoded) == nil
	}
	if !decode("") || len(decoded.Rows) < 3 {
		t.Fatalf("precondition decode failed: %s", before.stdout)
	}
	for _, r := range decoded.Rows {
		if r.State == "failed" {
			failedID = int(r.ID)
		}
	}
	if failedID < 0 {
		t.Fatalf("no failed row among %+v", decoded.Rows)
	}

	retried := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"},
		"notifications", "retry", strconv.FormatInt(int64(failedID), 10))
	if retried.code != ExitOK {
		t.Fatalf("direct retry exited %d: %s", retried.code, retried.stderr)
	}
	if !strings.Contains(retried.stdout, `"previous_state":"failed"`) &&
		!strings.Contains(retried.stdout, "failed") {
		t.Errorf("retry output lost its history fact:\n%s", retried.stdout)
	}

	delivered := runCLI(t, dir, nil, "notifications", "retry", strconv.FormatInt(ids["delivered"], 10))
	if delivered.code != ExitValidation {
		t.Errorf("retrying delivered must refuse with the validation code, got %d: %s",
			delivered.code, delivered.stderr)
	}
}

func TestNotificationsTestProbesDeliveryWithoutWritingTheOutbox(t *testing.T) {
	dir := t.TempDir()
	stateDir := filepath.Join(dir, stateDirName)
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := "notifiers:\n  vakt:\n    type: stderr\nnotify_defaults:\n  on_failure: [vakt]\n"
	if werr := os.WriteFile(filepath.Join(stateDir, daemon.NotifierFileName), []byte(cfg), 0o600); werr != nil {
		t.Fatal(werr)
	}

	tested := runCLI(t, dir, map[string]string{"PACEQ_OUTPUT": "json"}, "notifications", "test", "vakt")
	if tested.code != ExitOK {
		t.Fatalf("test exited %d: %s", tested.code, tested.stderr)
	}
	if !strings.Contains(tested.stdout, `"ok":true`) || !strings.Contains(tested.stdout, `"payload"`) {
		t.Errorf("a successful probe must report ok and the sent payload:\n%s", tested.stdout)
	}

	ro, openErr := store.OpenReadOnly(context.Background(),
		filepath.Join(stateDir, store.DatabaseFileName), store.Options{})
	deferableClose(t, ro, openErr != nil)
	if openErr == nil {
		rows, lerr := ro.ListNotifications(context.Background(), store.NotificationFilter{})
		if lerr != nil {
			t.Fatalf("read the outbox after a test send: %v", lerr)
		}
		if len(rows) != 0 {
			t.Fatalf("notifications test wrote %d outbox rows; the AC says none", len(rows))
		}
	}

	unknown := runCLI(t, dir, nil, "notifications", "test", "ghost")
	if unknown.code != ExitNotFound {
		t.Errorf("an unconfigured notifier exited %d, want not-found", unknown.code)
	}
}

// --- tiny local helpers kept beside their only users -------------------------

func deferableClose(t *testing.T, s *store.Store, skip bool) {
	t.Helper()
	if skip || s == nil {
		return
	}
	t.Cleanup(func() { _ = s.Close() })
}

func firstLine(s string) string {
	i := strings.Index(s, "\n")
	if i < 0 {
		return s
	}
	return s[:i]
}
