package api_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/api"
)

// unixClient speaks HTTP to a unix socket the way the real client does.
func unixClient(t *testing.T, path string) *http.Client {
	t.Helper()
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return net.Dial("unix", path)
		},
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}
}

// get issues one GET against the socket and hands back the response.
func get(t *testing.T, client *http.Client, target string) *http.Response {
	t.Helper()
	resp, err := client.Get("http://localhost" + target)
	if err != nil {
		t.Fatalf("%s: %v", target, err)
	}
	return resp
}

// postJSON issues one POST with a JSON body and the client version header.
func postJSON(t *testing.T, client *http.Client, headerVersion, target, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://localhost"+target,
		strings.NewReader(body))
	if err != nil {
		t.Fatalf("build %s: %v", target, err)
	}
	req.Header.Set("Content-Type", "application/json")
	if headerVersion != "" {
		req.Header.Set("X-Pulseq-Client", headerVersion)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", target, err)
	}
	return resp
}

// decodeJSON folds a response body into a map and closes it.
func decodeJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer func() { _ = resp.Body.Close() }()
	var document map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	return document
}

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// startServer brings one server up on a socket inside its own new directory,
// so the directory permission assertion has something of ours to read.
func startServer(t *testing.T, deps api.Deps) (sockPath string, client *http.Client) {
	t.Helper()
	root := t.TempDir()
	sockPath = filepath.Join(root, "run", "paceq.sock")
	stop, err := api.Serve(context.Background(), api.Config{
		Path: sockPath,
		Deps: deps,
		Log:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(func() { stop(context.Background()) })
	return sockPath, unixClient(t, sockPath)
}

func TestServeAnswersHealthOverTheSocket(t *testing.T) {
	sock, client := startServer(t, api.Deps{Version: "v9.1.2-test"})

	live := get(t, client, "/livez")
	if live.StatusCode != http.StatusOK {
		t.Fatalf("/livez answered %d, want 200", live.StatusCode)
	}
	if body := decodeJSON(t, live); body["status"] != "ok" {
		t.Fatalf("/livez body does not say ok: %v", body)
	}

	ready := get(t, client, "/v1/healthz")
	if ready.StatusCode != http.StatusOK {
		t.Fatalf("/v1/healthz answered %d, want 200", ready.StatusCode)
	}
	body := decodeJSON(t, ready)
	if body["status"] != "ready" || body["version"] != "v9.1.2-test" {
		t.Fatalf("/v1/healthz does not carry readiness facts: %v", body)
	}

	info, err := os.Stat(sock)
	if err != nil {
		t.Fatalf("stat the socket: %v", err)
	}
	if got := info.Mode().Perm(); got != 0o660 {
		t.Errorf("the socket is %#o, want 0660 exactly: the mode serves group collaboration and must not depend on the umask", got)
	}
	dirInfo, err := os.Stat(filepath.Dir(sock))
	if err != nil {
		t.Fatalf("stat the socket directory: %v", err)
	}
	if got := dirInfo.Mode().Perm(); got != 0o700 {
		t.Errorf("the socket directory is %#o, want 0700", got)
	}
}

func TestServeReplacesASocketFileLeftByACrash(t *testing.T) {
	root := t.TempDir()
	sock := filepath.Join(root, "paceq.sock")
	// The litter a killed daemon leaves: a plain file where the socket was.
	if err := os.WriteFile(sock, []byte("stale"), 0o600); err != nil {
		t.Fatalf("plant the stale file: %v", err)
	}

	stop, err := api.Serve(context.Background(), api.Config{
		Path: sock,
		Deps: api.Deps{Version: "test"},
		Log:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("a stale socket file must never block startup: %v", err)
	}
	t.Cleanup(func() { stop(context.Background()) })

	live := get(t, unixClient(t, sock), "/livez")
	if live.StatusCode != http.StatusOK {
		t.Fatalf("/livez answered %d behind a replaced socket, want 200", live.StatusCode)
	}
}

func TestServeRemovesTheSocketWhenItStops(t *testing.T) {
	root := t.TempDir()
	sock := filepath.Join(root, "paceq.sock")
	stop, err := api.Serve(context.Background(), api.Config{
		Path: sock,
		Deps: api.Deps{Version: "test"},
		Log:  discardLogger(),
	})
	if err != nil {
		t.Fatalf("serve: %v", err)
	}

	done := make(chan struct{})
	go func() { defer close(done); stop(context.Background()) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("stop did not return within 10s")
	}

	if _, err := os.Stat(sock); !os.IsNotExist(err) {
		t.Errorf("the socket file survived a clean stop (stat err=%v)", err)
	}
}

// getWithHeader issues one GET carrying one request header.
func getWithHeader(t *testing.T, client *http.Client, headerVersion, target string) *http.Response {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, "http://localhost"+target, nil)
	if err != nil {
		t.Fatalf("build %s: %v", target, err)
	}
	if headerVersion != "" {
		req.Header.Set("X-Pulseq-Client", headerVersion)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s: %v", target, err)
	}
	return resp
}

func TestTheVersionGateRejectsAForeignMajor(t *testing.T) {
	_, client := startServer(t, api.Deps{Version: "v9.1.2-test"})

	cases := []struct {
		name       string
		header     string
		wantStatus int
	}{
		{"no header at all is a debugging client", "", http.StatusOK},
		{"a dev build parses as compatible", "paceq/dev", http.StatusOK},
		{"the same major passes", "paceq/9.0.0", http.StatusOK},
		{"a newer major is refused", "paceq/10.0.0", http.StatusConflict},
		{"an older major is refused", "paceq/8.3.0", http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := getWithHeader(t, client, tc.header, "/v1/healthz")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("X-Pulseq-Client %q answered %d, want %d", tc.header, resp.StatusCode, tc.wantStatus)
			}
			if tc.wantStatus != http.StatusConflict {
				return
			}
			document := decodeJSON(t, resp)
			errBody, _ := document["error"].(map[string]any)
			if errBody == nil {
				t.Fatalf("the refusal carries no error envelope: %v", document)
			}
			if errBody["code"] != "version_mismatch" {
				t.Errorf("error code is %v, want version_mismatch", errBody["code"])
			}
			message, _ := errBody["message"].(string)
			for _, needed := range []string{"--socket none", "restart"} {
				if !strings.Contains(message, needed) {
					t.Errorf("the message does not tell the operator to %s: %q", needed, message)
				}
			}
		})
	}
}

func TestTheRouteTableIsTheWholeSurface(t *testing.T) {
	registered := map[string]bool{}
	for _, route := range api.Routes() {
		if !route.Registered {
			continue
		}
		registered[route.Method+" "+route.Pattern] = true
		if route.WritesSpecs {
			t.Errorf("route %s %s writes job specifications; definitions enter through files or they do not enter",
				route.Method, route.Pattern)
		}
	}

	want := map[string]bool{
		"POST /v1/runs":             true,
		"POST /v1/runs/{id}/cancel": true,
		"POST /v1/apply":            true,
		"GET /v1/healthz":           true,
		"GET /livez":                true,
	}
	for pattern := range want {
		if !registered[pattern] {
			t.Errorf("route %s is missing from the surface", pattern)
		}
	}
	for pattern := range registered {
		if !want[pattern] {
			t.Errorf("route %s is registered but nobody documented it", pattern)
		}
	}

	// The schedule pause/resume endpoints stay absent until their semantics
	// exist (M2-05/M2-10); the table says so instead of forgetting them.
	blocked := map[string]string{}
	for _, route := range api.Routes() {
		if !route.Registered {
			blocked[route.Method+" "+route.Pattern] = route.BlockedBy
		}
	}
	for _, pattern := range []string{
		"POST /v1/schedules/{name}/pause",
		"POST /v1/schedules/{name}/resume",
	} {
		reason, ok := blocked[pattern]
		if !ok {
			t.Errorf("route %s is neither registered nor recorded as absent by design", pattern)
			continue
		}
		if !strings.Contains(reason, "M2") {
			t.Errorf("route %s names no blocking milestone: %q", pattern, reason)
		}
	}
}
