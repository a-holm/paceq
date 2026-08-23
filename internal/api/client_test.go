package api_test

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/api"
)

func TestProbeFindsADaemonAndItsAbsence(t *testing.T) {
	sock, _ := startServer(t, api.Deps{Version: "v9.0.0-test"})

	if err := api.Probe(sock, "v9.1.0"); err != nil {
		t.Fatalf("a live daemon was not found through %s: %v", sock, err)
	}

	silent := filepath.Join(t.TempDir(), "missing.sock")
	err := api.Probe(silent, "v9.1.0")
	if !errors.Is(err, api.ErrNoDaemon) {
		t.Fatalf("an absent daemon answered %v, want ErrNoDaemon", err)
	}
}

// A world-writable socket means somebody invited the whole machine. The
// client refuses such a path loudly instead of treating it as absent:
// falling back to direct writes would hide the anomaly this reports.
func TestProbeFailsClosedOnAHostileSocketFile(t *testing.T) {
	sock, _ := startServer(t, api.Deps{Version: "test"})

	if err := os.Chmod(sock, 0o666); err != nil {
		t.Fatalf("widen the socket: %v", err)
	}
	err := api.Probe(sock, "v9")
	var refused *api.SocketRefused
	switch {
	case errors.As(err, &refused):
		if refused.Path != sock {
			t.Errorf("the refusal names %q, want %q", refused.Path, sock)
		}
	case errors.Is(err, api.ErrNoDaemon):
		t.Fatal("a world-writable socket was treated as simply absent; it must be refused loudly")
	default:
		t.Fatalf("a world-writable socket answered %v, want a loud refusal", err)
	}
}

func TestTheClientSendsItsVersionOnEveryCall(t *testing.T) {
	sock, _ := startServer(t, api.Deps{Version: "v9.0.0-test", Store: newTestStore(t)})
	client, err := api.Dial(sock, "v9.9.9-client")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	id, err := client.CreateRun(context.Background(), "drainjob", nil)
	if err != nil {
		t.Fatalf("the same major was gated off or failed: %v", err)
	}
	if id == "" {
		t.Fatal("no run id came back")
	}
}

func TestACallAcrossAForeignMajorSurfacesTheWholeRefusal(t *testing.T) {
	sock, _ := startServer(t, api.Deps{Version: "v9.0.0-test"})
	client, err := api.Dial(sock, "v10.0.0-client")
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	_, err = client.CreateRun(context.Background(), "drainjob", nil)
	var wireErr *api.WireError
	if !errors.As(err, &wireErr) {
		t.Fatalf("the error is not a WireError: %v", err)
	}
	if wireErr.Status != http.StatusConflict || wireErr.Code != "version_mismatch" {
		t.Fatalf("the refusal lost parts: %+v", wireErr)
	}
	for _, needed := range []string{"--socket none", "restart"} {
		if !strings.Contains(wireErr.Message, needed) {
			t.Errorf("the message omits %q: %q", needed, wireErr.Message)
		}
	}
}
