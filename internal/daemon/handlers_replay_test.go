package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/store"
)

// Issue #10, M4-04: POST /v1/runs/{id}/replay is the daemon side of the
// operator replay. It answers with the same facts the direct path reports,
// so the JSON contract does not depend on which way the write travelled.
// Unlike the reopen route it is not an allowlisted operator surface: making
// a new run touches nobody's history, so any API client may ask for one.

// postReplay posts one body to the route under test.
func postReplay(t *testing.T, st *store.Store, runID, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/runs/"+runID+"/replay", strings.NewReader(body))
	r.SetPathValue("id", runID)
	w := httptest.NewRecorder()
	handleReplayRun(w, r, st)
	var doc map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("the answer is not JSON:\n%s\n%v", w.Body.String(), err)
	}
	return w, doc
}

// TestReplayRouteMakesANewRunAndAnswersTheSameFacts drives the happy path:
// a new queued run beside the source, with the source untouched and the
// answer carrying exactly what the CLI's direct path would print.
func TestReplayRouteMakesANewRunAndAnswersTheSameFacts(t *testing.T) {
	st := retryRouteStore(t)
	failed, err := st.ListRuns(context.Background(), store.RunFilter{States: []string{"failed"}})
	if err != nil || len(failed) != 1 {
		t.Fatalf("the planted failed run is missing: %v %v", failed, err)
	}
	srcID := failed[0].ID

	w, doc := postReplay(t, st, srcID, `{"failed":true}`)

	if w.Code != http.StatusOK {
		t.Fatalf("the route answered %d:\n%s", w.Code, w.Body.String())
	}
	if doc["run_id"] == "" || doc["run_id"] == srcID {
		t.Errorf("run_id = %v, want a new id beside %s", doc["run_id"], srcID)
	}
	reused, ok := doc["reused"].([]any)
	if !ok || len(reused) != 2 || reused[0] != "a" || reused[1] != "b" {
		t.Errorf("reused = %v, want [a b], every step that succeeded", doc["reused"])
	}
	rerun, ok := doc["rerun"].([]any)
	if !ok || len(rerun) != 1 || rerun[0] != "c" {
		t.Errorf("rerun = %v, want [c]", doc["rerun"])
	}

	newID, _ := doc["run_id"].(string)
	detail, err := st.GetRun(context.Background(), newID)
	if err != nil {
		t.Fatalf("read back the replay: %v", err)
	}
	if detail.State != "queued" || detail.ReplayOf != srcID {
		t.Errorf("the replay is %s with replay_of %q, want queued naming %s",
			detail.State, detail.ReplayOf, srcID)
	}
	source, err := st.GetRun(context.Background(), srcID)
	if err != nil {
		t.Fatalf("read back the source: %v", err)
	}
	if source.State != "failed" {
		t.Errorf("the source moved to %s, want it left as history", source.State)
	}
}

// TestReplayRouteRefusesAreClassified keeps the refusals classified the way
// the CLI renders them: unknown runs are not_found, wrong shapes are
// conflicts, a GET is method not allowed.
func TestReplayRouteRefusesAreClassified(t *testing.T) {
	st := retryRouteStore(t)

	t.Run("unknown run", func(t *testing.T) {
		w, doc := postReplay(t, st, "01ZZZZZZZZZZZZZZZZZZZZZZZZZ", "{}")
		if w.Code != http.StatusNotFound {
			t.Fatalf("an unknown run answered %d, want 404", w.Code)
		}
		if doc["code"] != "not_found" {
			t.Errorf("the refusal class = %v, want not_found", doc["code"])
		}
	})

	t.Run("both reuse rules", func(t *testing.T) {
		failed, err := st.ListRuns(context.Background(), store.RunFilter{States: []string{"failed"}})
		if err != nil || len(failed) != 1 {
			t.Fatalf("the planted failed run is missing: %v %v", failed, err)
		}
		w, doc := postReplay(t, st, failed[0].ID, `{"from":"a","failed":true}`)
		if w.Code != http.StatusConflict {
			t.Fatalf("two reuse rules answered %d, want 409", w.Code)
		}
		if doc["code"] != "invalid_state" {
			t.Errorf("the refusal class = %v, want invalid_state", doc["code"])
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/runs/x/replay", nil)
		w := httptest.NewRecorder()
		handleReplayRun(w, r, st)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET answered %d, want 405", w.Code)
		}
	})
}
