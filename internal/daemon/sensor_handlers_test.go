package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// Issue #207: the cursor route used to decode its body into a plain string, so
// an empty object and a named cursor were the same request as far as the
// handler could tell. It answered 200 and wrote the empty string. These tests
// pin the other reading: a request that names no cursor is refused, and one
// that names a cursor stores that cursor.

// sensorRouteStore opens a store with one job and one sensor on it, which is
// the least a sensor write route needs to have something to write.
func sensorRouteStore(t *testing.T) *store.Store {
	t.Helper()
	ctx := context.Background()
	s, err := store.Open(ctx, filepath.Join(t.TempDir(), "state.db"), store.Options{})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })

	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName: "polling-job", SpecHash: "sha256:seed",
		SpecJSON: `{"schema":"paceq.job.v1","name":"polling-job","steps":[{"name":"c","run":["true"]}]}`,
	}); err != nil {
		t.Fatalf("seed the job: %v", err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: "finder", JobName: "polling-job", ExecJSON: `["/bin/true"]`,
	}); err != nil {
		t.Fatalf("seed the sensor: %v", err)
	}
	return s
}

// TestCursorRouteRefusesARequestThatNamesNoCursor is the property that makes an
// old client safe against a new daemon: a body with nothing in it can never
// destroy a cursor, because the write is refused before it happens and the
// client falls back to the path that still carries the value.
func TestCursorRouteRefusesARequestThatNamesNoCursor(t *testing.T) {
	for name, body := range map[string]string{
		"an empty object":  `{}`,
		"an empty body":    ``,
		"an explicit null": `{"cursor":null}`,
		"an empty cursor":  `{"cursor":""}`,
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			s := sensorRouteStore(t)
			if err := s.SetSensorCursor(ctx, store.CursorInput{Name: "finder", Cursor: "keep-me"}); err != nil {
				t.Fatalf("set the starting cursor: %v", err)
			}
			before, err := s.GetSensor(ctx, "finder")
			if err != nil {
				t.Fatalf("read the sensor: %v", err)
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/v1/sensors/finder/cursor", strings.NewReader(body))
			req.SetPathValue("name", "finder")
			handleSetSensorCursor(rec, req, s)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("the daemon answered %d, want 400\n%s", rec.Code, rec.Body.String())
			}
			after, err := s.GetSensor(ctx, "finder")
			if err != nil {
				t.Fatalf("read the sensor back: %v", err)
			}
			if after.Cursor == nil || *after.Cursor != "keep-me" {
				t.Errorf("the cursor reads %s after a refused request, want keep-me", cursorText(after.Cursor))
			}
			if after.CursorVersion != before.CursorVersion {
				t.Errorf("cursor_version = %d, want %d: a refused write bumped the guard",
					after.CursorVersion, before.CursorVersion)
			}
		})
	}
}

// TestCursorRouteStoresTheCursorItIsGiven is the other half: the refusal above
// must not be satisfied by refusing everything.
func TestCursorRouteStoresTheCursorItIsGiven(t *testing.T) {
	ctx := context.Background()
	s := sensorRouteStore(t)
	const value = `2026-08-21/09-20-03 "a\b".csv`

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/sensors/finder/cursor",
		strings.NewReader(`{"cursor":`+quotedJSON(value)+`}`))
	req.SetPathValue("name", "finder")
	handleSetSensorCursor(rec, req, s)

	if rec.Code != http.StatusOK {
		t.Fatalf("the daemon answered %d, want 200\n%s", rec.Code, rec.Body.String())
	}
	row, err := s.GetSensor(ctx, "finder")
	if err != nil {
		t.Fatalf("read the sensor: %v", err)
	}
	if row.Cursor == nil || *row.Cursor != value {
		t.Fatalf("cursor = %v, want %q", row.Cursor, value)
	}
}

// TestResetRouteReadsBothItsArguments: the reset body carries the two things
// the operator can ask for, and each one has to arrive. A reset that ignored
// them turned "replay from here" into a full replay and made a confirmed
// deletion a no-op.
func TestResetRouteReadsBothItsArguments(t *testing.T) {
	ctx := context.Background()
	s := sensorRouteStore(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/sensors/finder/reset",
		strings.NewReader(`{"cursor":"from-here","forget_run_keys":true}`))
	req.SetPathValue("name", "finder")
	handleResetSensor(rec, req, s)

	if rec.Code != http.StatusOK {
		t.Fatalf("the daemon answered %d, want 200\n%s", rec.Code, rec.Body.String())
	}
	row, err := s.GetSensor(ctx, "finder")
	if err != nil {
		t.Fatalf("read the sensor: %v", err)
	}
	if row.Cursor == nil || *row.Cursor != "from-here" {
		t.Errorf("cursor = %v, want %q: the reset replayed everything instead", row.Cursor, "from-here")
	}
	if row.DedupEpoch != 1 {
		t.Errorf("dedup_epoch = %d, want 1: the reset did not bump the epoch", row.DedupEpoch)
	}
}

// TestPauseRouteKeepsTheReason: the reason is the whole point of the column.
func TestPauseRouteKeepsTheReason(t *testing.T) {
	ctx := context.Background()
	s := sensorRouteStore(t)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/sensors/finder/pause",
		strings.NewReader(`{"reason":"deploying"}`))
	req.SetPathValue("name", "finder")
	handlePauseSensor(rec, req, s)

	if rec.Code != http.StatusOK {
		t.Fatalf("the daemon answered %d, want 200\n%s", rec.Code, rec.Body.String())
	}
	row, err := s.GetSensor(ctx, "finder")
	if err != nil {
		t.Fatalf("read the sensor: %v", err)
	}
	if !row.Paused || row.PausedReason != "deploying" {
		t.Fatalf("paused=%v reason=%q, want paused with the reason kept", row.Paused, row.PausedReason)
	}
}

// jsonString is the JSON spelling of one string, so an expected body can carry
// the characters that break a body pasted together instead of marshalled.
func quotedJSON(s string) string {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(out)
}

// cursorText renders a nullable cursor the way an operator reads it, so a
// failure names the value rather than a pointer.
func cursorText(cursor *string) string {
	if cursor == nil {
		return "(none)"
	}
	return strconv.Quote(*cursor)
}
