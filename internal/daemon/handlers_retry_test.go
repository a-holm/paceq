package daemon

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// Issue #10, M4-04: POST /v1/runs/{id}/retry is the daemon side of the
// operator reopen. The CLI speaks for the person typing; this route speaks
// for that person's HTTP client. It is one of exactly two surfaces allowed
// to call ReopenTerminalRunByOperator (the architecture test walks every
// caller), and it answers with the same facts the direct path reports, so
// the JSON contract does not depend on which way the write travelled.

// retryRouteStore opens a store and plants one failed run on it: five steps,
// step c died with its exit code, d and e were skipped behind it.
func retryRouteStore(t *testing.T) *store.Store {
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

	const spec = `{"name":"routechain","max_concurrent":1,"timeout_ms":3600000,` +
		`"schema":"paceq.job.v1","steps":[` +
		`{"name":"a","run":["/bin/true"],"shell":false},` +
		`{"name":"b","needs":["a"],"run":["/bin/true"],"shell":false},` +
		`{"name":"c","needs":["b"],"run":["/bin/true"],"shell":false}]}`
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:  "routechain",
		SpecHash: "sha256:routechain",
		SpecJSON: spec,
	}); err != nil {
		t.Fatalf("record the job: %v", err)
	}
	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "routechain"})
	if err != nil {
		t.Fatalf("queue the run: %v", err)
	}
	if _, _, err := s.ClaimRun(ctx, queued.Run.ID, store.LeaseInput{Owner: "cli:test", TTL: time.Hour}); err != nil {
		t.Fatalf("claim: %v", err)
	}
	ref := store.LeaseRef{Owner: "cli:test", Epoch: 1}
	for _, step := range []string{"a", "b"} {
		if err := s.StartStep(ctx, queued.Run.ID, step, ref); err != nil {
			t.Fatalf("start %s: %v", step, err)
		}
		if err := s.RecordStepOutcome(ctx, queued.Run.ID, step, store.StepOutcome{
			Event: "step_succeeded", ReasonCode: reason.STEPSucceeded,
			ExitCode: new(int), FinishedAt: time.Now(),
		}, ref); err != nil {
			t.Fatalf("record %s: %v", step, err)
		}
	}
	if err := s.StartStep(ctx, queued.Run.ID, "c", ref); err != nil {
		t.Fatalf("start c: %v", err)
	}
	if err := s.RecordStepOutcome(ctx, queued.Run.ID, "c", store.StepOutcome{
		Event: "step_failed", ReasonCode: reason.STEPFailedNonzeroExit,
		ExitCode: new(int), FinishedAt: time.Now(),
	}, ref); err != nil {
		t.Fatalf("record c: %v", err)
	}
	if _, err := s.FinishRun(ctx, queued.Run.ID, ref, store.FinishReason{
		Code: reason.RUNFailedStep, Data: `{"step":"c"}`,
	}); err != nil {
		t.Fatalf("finish: %v", err)
	}
	return s
}

// postRetry posts one body to the route under test.
func postRetry(t *testing.T, st *store.Store, runID, body string) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/v1/runs/"+runID+"/retry", strings.NewReader(body))
	r.SetPathValue("id", runID)
	w := httptest.NewRecorder()
	handleRetryRun(w, r, st)
	var doc map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &doc); err != nil {
		t.Fatalf("the answer is not JSON:\n%s\n%v", w.Body.String(), err)
	}
	return w, doc
}

func TestRetryRouteReopensAndAnswersTheSameFacts(t *testing.T) {
	st := retryRouteStore(t)
	failed, err := st.ListRuns(context.Background(), store.RunFilter{States: []string{"failed"}})
	if err != nil || len(failed) != 1 {
		t.Fatalf("the planted failed run is missing: %v %v", failed, err)
	}

	w, doc := postRetry(t, st, failed[0].ID, "{}")

	if w.Code != http.StatusOK {
		t.Fatalf("the route answered %d:\n%s", w.Code, w.Body.String())
	}
	if doc["new_epoch"] != float64(2) {
		t.Errorf("new_epoch = %v, want 2", doc["new_epoch"])
	}
	reopened, ok := doc["reopened"].([]any)
	if !ok || len(reopened) != 1 || reopened[0] != "c" {
		t.Errorf("reopened = %v, want [c]", doc["reopened"])
	}
	detail, err := st.GetRun(context.Background(), failed[0].ID)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if detail.State != "queued" {
		t.Errorf("the run is %s after the route reopened it, want queued", detail.State)
	}
}

func TestRetryRouteRefusesAreClassified(t *testing.T) {
	st := retryRouteStore(t)

	t.Run("unknown run", func(t *testing.T) {
		w, doc := postRetry(t, st, "01ZZZZZZZZZZZZZZZZZZZZZZZZZ", "{}")
		if w.Code != http.StatusNotFound {
			t.Fatalf("an unknown run answered %d, want 404", w.Code)
		}
		if doc["code"] != "not_found" {
			t.Errorf("the refusal class = %v, want not_found", doc["code"])
		}
	})

	t.Run("wrong method", func(t *testing.T) {
		r := httptest.NewRequest(http.MethodGet, "/v1/runs/x/retry", nil)
		w := httptest.NewRecorder()
		handleRetryRun(w, r, st)
		if w.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET answered %d, want 405", w.Code)
		}
	})
}

// TestRetryRouteRefusesANonTerminalRunWithItsClass keeps the machine's own
// answer intact over the wire: a run still going is refused, and the class
// is the one the CLI turns into a validation exit.
func TestRetryRouteRefusesANonTerminalRunWithItsClass(t *testing.T) {
	st := retryRouteStore(t)
	ctx := context.Background()
	queued, err := st.MaterializeManualTrigger(ctx, store.ManualTriggerInput{JobName: "routechain"})
	if err != nil {
		t.Fatalf("queue a second run: %v", err)
	}
	w, doc := postRetry(t, st, queued.Run.ID, "{}")
	if w.Code != http.StatusConflict {
		t.Fatalf("a queued run answered %d, want 409\n%s", w.Code, w.Body.String())
	}
	if doc["code"] != "invalid_state" {
		t.Errorf("the refusal class = %v, want invalid_state", doc["code"])
	}
}
