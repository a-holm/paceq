package api_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/store"
)

// newTestStore opens one migrated database with the drainjob applied, the
// same shape the serve harness seeds.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dir := t.TempDir()
	st, err := store.Open(context.Background(), filepath.Join(dir, store.DatabaseFileName), store.Options{})
	if err != nil {
		t.Fatalf("open the store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	specJSON := `{"schema":"paceq.job.v1","name":"drainjob","max_concurrent":1,` +
		`"steps":[{"name":"only","run":["/bin/true"],"shell":false}]}`
	if _, _, err := st.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "drainjob",
		SpecHash: "sha256:test-drainjob",
		SpecJSON: specJSON,
	}); err != nil {
		t.Fatalf("apply drainjob: %v", err)
	}
	return st
}

// errorEnvelope is the wire shape every refusal travels in.
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func decodeError(t *testing.T, resp *http.Response) errorEnvelope {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var envelope errorEnvelope
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read the refusal: %v", err)
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("decode the refusal %q: %v", raw, err)
	}
	return envelope
}

func TestCreateRunThroughTheSocketRecordsTheAPIClientAsActor(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	resp := postJSON(t, client, "paceq/test", "/v1/runs", `{"job":"drainjob"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("POST /v1/runs answered %d, want 201", resp.StatusCode)
	}
	var document struct {
		Run struct {
			ID    string `json:"id"`
			Job   string `json:"job"`
			State string `json:"state"`
		} `json:"run"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		t.Fatalf("decode the created run: %v", err)
	}
	if document.Run.ID == "" || document.Run.Job != "drainjob" {
		t.Fatalf("the created run record is thin: %+v", document.Run)
	}

	events, err := st.RunEvents(context.Background(), document.Run.ID)
	if err != nil {
		t.Fatalf("read the events back: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("no run_events row was written")
	}
	queued := events[0]
	if queued.Kind != "run.queued" {
		t.Fatalf("first event is %q, want run.queued", queued.Kind)
	}
	if queued.Actor != "api" {
		t.Fatalf("the queued event names actor %q, want api", queued.Actor)
	}
}

func TestCreateRunNamesTheMissingJob(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	resp := postJSON(t, client, "paceq/test", "/v1/runs", `{"job":"ghost"}`)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("an unknown job answered %d, want 404", resp.StatusCode)
	}
	envelope := decodeError(t, resp)
	if envelope.Error.Code != "job_not_found" {
		t.Errorf("error code is %q, want job_not_found", envelope.Error.Code)
	}
	for _, needed := range []string{"ghost", "paceq apply"} {
		if !strings.Contains(envelope.Error.Message, needed) {
			t.Errorf("the message omits %q: %q", needed, envelope.Error.Message)
		}
	}
}

func TestCancelThroughTheSocketIsRecordedDurably(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	version, _, err := st.UpsertJobVersion(context.Background(), store.JobVersionInput{
		JobName:  "drainjob",
		SpecHash: "sha256:test-drainjob",
		SpecJSON: `{"schema":"paceq.job.v1","name":"drainjob","steps":[{"name":"only","run":["/bin/true"],"shell":false}]}`,
	})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	run, err := st.CreateRunWithSteps(context.Background(), store.NewRun{
		JobName:      "drainjob",
		JobVersionID: version.ID,
		Origin:       "manual",
		Actor:        "test",
		Steps:        []store.NewStep{{Name: "only"}},
	})
	if err != nil {
		t.Fatalf("seed the run: %v", err)
	}

	resp := postJSON(t, client, "paceq/test", "/v1/runs/"+run.ID+"/cancel", `{"reason":"operator asked"}`)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel answered %d, want 200", resp.StatusCode)
	}
	requested, by, err := st.CancelRequested(context.Background(), run.ID)
	if err != nil || !requested {
		t.Fatalf("the cancellation is not on record (requested=%v, err=%v)", requested, err)
	}
	if by != "api" {
		t.Errorf("cancel_requested_by is %q, want api", by)
	}

	missing := postJSON(t, client, "paceq/test", "/v1/runs/01AAAAAAAAAAAAAAAAAAAAAAAAAAAA/cancel", `{}`)
	if missing.StatusCode != http.StatusNotFound {
		t.Errorf("cancelling an unknown run answered %d, want 404", missing.StatusCode)
	}
	if envelope := decodeError(t, missing); envelope.Error.Code != "run_not_found" {
		t.Errorf("error code is %q, want run_not_found", envelope.Error.Code)
	}
}

func TestApplyReadsTheFilesFromTheDaemonSideDisk(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	dir := t.TempDir()
	good := filepath.Join(dir, "good.yaml")
	broken := filepath.Join(dir, "broken.yaml")
	goodYAML := "name: nice\ndescription: One step.\nsteps:\n  - name: greet\n    run: [\"/bin/echo\", \"hi\"]\n"
	if err := os.WriteFile(good, []byte(goodYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(broken, []byte("name: broken\nsteps: []\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	body := fmt.Sprintf(`{"paths":[%q,%q]}`, good, broken)
	resp := postJSON(t, client, "paceq/test", "/v1/apply", body)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("apply answered %d, want 200 with a failed list", resp.StatusCode)
	}
	var report struct {
		Applied []struct {
			Job     string `json:"job"`
			Version int    `json:"version"`
		} `json:"applied"`
		Unchanged []map[string]any `json:"unchanged"`
		Failed    []struct {
			File    string `json:"file"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"failed"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&report); err != nil {
		t.Fatalf("decode the report: %v", err)
	}
	if len(report.Applied) != 1 || report.Applied[0].Job != "nice" {
		t.Fatalf("the good file did not land: %+v", report.Applied)
	}
	if len(report.Failed) != 1 || report.Failed[0].Code == "" || report.Failed[0].Message == "" {
		t.Fatalf("the broken file is not named with a code and a message: %+v", report.Failed)
	}

	again := postJSON(t, client, "paceq/test", "/v1/apply", fmt.Sprintf(`{"paths":[%q]}`, good))
	defer func() { _ = again.Body.Close() }()
	var repeat struct {
		Unchanged []map[string]any `json:"unchanged"`
	}
	if err := json.NewDecoder(again.Body).Decode(&repeat); err != nil {
		t.Fatalf("decode the second report: %v", err)
	}
	if len(repeat.Unchanged) != 1 {
		t.Fatalf("applying twice must be safe and say unchanged: %+v", repeat)
	}
}

func TestApplyWithoutPathsIsAUsageRefusal(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	resp := postJSON(t, client, "paceq/test", "/v1/apply", `{"paths":[]}`)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty apply answered %d, want 400", resp.StatusCode)
	}
	if envelope := decodeError(t, resp); envelope.Error.Code != "invalid_request" {
		t.Errorf("error code is %q, want invalid_request", envelope.Error.Code)
	}
}

func TestConcurrentClientsEachGetTheirOwnRun(t *testing.T) {
	st := newTestStore(t)
	_, client := startServer(t, api.Deps{Version: "test", Store: st})

	const writers = 8
	ids := make([]string, writers)
	errs := make([]error, writers)
	var wg sync.WaitGroup
	for i := range writers {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			resp, err := client.Post("http://localhost/v1/runs", "application/json",
				strings.NewReader(`{"job":"drainjob"}`))
			if err != nil {
				errs[slot] = err
				return
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusCreated {
				errs[slot] = fmt.Errorf("status %d", resp.StatusCode)
				return
			}
			var document struct {
				Run struct {
					ID string `json:"id"`
				} `json:"run"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
				errs[slot] = err
				return
			}
			ids[slot] = document.Run.ID
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("client %d failed: %v", i, err)
		}
	}
	seen := map[string]bool{}
	for _, id := range ids {
		if id == "" || seen[id] {
			t.Fatalf("run ids are not distinct: %v", ids)
		}
		seen[id] = true
	}
	rows, err := st.ListRuns(context.Background(), store.RunFilter{})
	if err != nil {
		t.Fatalf("list the runs: %v", err)
	}
	if len(rows) != writers {
		t.Fatalf("%d runs in the database, want %d", len(rows), writers)
	}
}

// startHandler serves one handler over a socket without going through
// Serve's filesystem half; the server tests cover that half.
