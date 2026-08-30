package cli

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Issue #207: a sensor write sent over the daemon socket has to carry the
// arguments the operator typed. The direct path always did; the socket path
// sent an empty object for every route, so a cursor move stored the empty
// string, a --cursor reset became a full replay, and a --forget-run-keys
// confirmed by typing the sensor name deleted nothing while printing success.
//
// The defect was invisible because the tests drove one route only. This one
// drives the other, and it takes its list of routes from the daemon's own
// registrations rather than from a list kept here: a route added there with
// no case below fails this test instead of shipping with its arguments lost.

// sensorSocketCall is one request the planted daemon was sent.
type sensorSocketCall struct {
	method string
	path   string
	body   string
}

// sensorSocketRecorder is a daemon that only listens. It answers every request
// with 200 and keeps what it was sent, which is the whole of the CLI's half of
// the contract: which route, and what body.
type sensorSocketRecorder struct {
	mu    sync.Mutex
	calls []sensorSocketCall
}

func (rec *sensorSocketRecorder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	rec.mu.Lock()
	rec.calls = append(rec.calls, sensorSocketCall{
		method: r.Method, path: r.URL.Path, body: string(body),
	})
	rec.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (rec *sensorSocketRecorder) recorded() []sensorSocketCall {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]sensorSocketCall(nil), rec.calls...)
}

// plantSensorSocket puts a listening socket where the resolution order leaves
// it for a project with no other socket named, at the mode the client check
// accepts.
func plantSensorSocket(t *testing.T, dir string) *sensorSocketRecorder {
	t.Helper()

	path := filepath.Join(dir, stateDirName, socketName)
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	rec := &sensorSocketRecorder{}
	srv := &http.Server{Handler: rec, ReadHeaderTimeout: time.Second}
	go func() { _ = srv.Serve(listener) }()
	t.Cleanup(func() { _ = srv.Close() })
	return rec
}

// daemonSensorRoutes reads the routes the daemon registers under
// /v1/sensors/{name}/. That file is where a sensor route has to appear for it
// to exist at all, so reading it is what makes the coverage check below
// impossible to satisfy by forgetting.
func daemonSensorRoutes(t *testing.T) []string {
	t.Helper()

	source := filepath.Join("..", "daemon", "api.go")
	body, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("read %s: %v", source, err)
	}
	found := regexp.MustCompile(`POST /v1/sensors/\{name\}/([a-z_]+)`).
		FindAllStringSubmatch(string(body), -1)
	var routes []string
	for _, match := range found {
		routes = append(routes, match[1])
	}
	if len(routes) == 0 {
		t.Fatalf("no sensor routes found in %s, so this test proves nothing", source)
	}
	sort.Strings(routes)
	return routes
}

// TestEverySensorSocketRouteCarriesItsArguments is the guard the defect got
// past. Each route is driven through the real command line against a listening
// socket, and the body the daemon receives has to say what the operator typed.
func TestEverySensorSocketRouteCarriesItsArguments(t *testing.T) {
	// A cursor value carrying the characters that break a body pasted
	// together instead of marshalled.
	const trickyCursor = `2026-08-21/09-20-03 "a\b".csv`

	cases := map[string]struct {
		argv []string
		want string
	}{
		"pause": {
			argv: []string{"sensors", "pause", "finder", "--reason", `deploying "v2"`},
			want: `{"reason":"deploying \"v2\""}`,
		},
		"resume": {
			argv: []string{"sensors", "resume", "finder"},
			want: `{}`,
		},
		"reset": {
			argv: []string{"sensors", "reset", "finder", "--cursor", trickyCursor, "--forget-run-keys", "--yes"},
			want: `{"cursor":` + quoted(trickyCursor) + `,"forget_run_keys":true}`,
		},
		"cursor": {
			argv: []string{"sensors", "cursor", "set", "finder", trickyCursor},
			want: `{"cursor":` + quoted(trickyCursor) + `}`,
		},
		"tick": {
			argv: []string{"sensors", "tick", "finder"},
			want: `{}`,
		},
	}

	for _, route := range daemonSensorRoutes(t) {
		tc, ok := cases[route]
		if !ok {
			t.Errorf("the daemon serves /v1/sensors/{name}/%s and nothing here proves "+
				"its arguments reach it; add a case", route)
			continue
		}
		t.Run(route, func(t *testing.T) {
			dir := t.TempDir()
			if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
				t.Fatalf("init = %d\n%s%s", got.code, got.stdout, got.stderr)
			}
			seedSensorCLI(t, dir, "finder", `["/bin/true"]`)
			rec := plantSensorSocket(t, dir)

			got := runCLI(t, dir, nil, tc.argv...)
			if got.code != ExitOK {
				t.Fatalf("paceq %s = %d\n%s%s", strings.Join(tc.argv, " "), got.code, got.stdout, got.stderr)
			}

			calls := rec.recorded()
			if len(calls) != 1 {
				t.Fatalf("the daemon was sent %d requests, want 1: %+v", len(calls), calls)
			}
			call := calls[0]
			if wantPath := "/v1/sensors/finder/" + route; call.path != wantPath {
				t.Errorf("the command dialled %s, want %s", call.path, wantPath)
			}
			if call.method != http.MethodPost {
				t.Errorf("the command sent %s, want POST", call.method)
			}
			if !sameJSON(t, call.body, tc.want) {
				t.Errorf("the daemon was sent\n\t%s\nand the operator asked for\n\t%s",
					call.body, tc.want)
			}
		})
	}
}

// TestTheSensorRouteListMatchesTheDaemon keeps the CLI's idea of the sensor
// routes and the daemon's registrations from drifting apart. A route the
// daemon serves that the CLI cannot name is a command that will be written
// against nothing; a route the CLI names that the daemon does not serve is a
// command that silently falls back to writing directly.
func TestTheSensorRouteListMatchesTheDaemon(t *testing.T) {
	var mine []string
	for _, route := range sensorRoutes {
		mine = append(mine, string(route))
	}
	sort.Strings(mine)

	theirs := daemonSensorRoutes(t)
	if !reflect.DeepEqual(mine, theirs) {
		t.Fatalf("the CLI dials %v and the daemon serves %v", mine, theirs)
	}
}

// TestCursorSetRefusesAnEmptyValue: "move the cursor to nothing" is what
// reset means, and an empty argument is a shell mistake far more often than an
// intention. Refusing it is also what keeps the empty string out of a column
// where NULL is the no-cursor form, so cursor get has one answer for it.
func TestCursorSetRefusesAnEmptyValue(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	seedSensorCLI(t, dir, "finder", `["/bin/true"]`)

	fresh := runCLI(t, dir, nil, "sensors", "cursor", "get", "finder")

	got := runCLI(t, dir, nil, "sensors", "cursor", "set", "finder", "")
	if got.code != ExitUsage {
		t.Fatalf("exit %d, want %d\n%s%s", got.code, ExitUsage, got.stdout, got.stderr)
	}

	after := runCLI(t, dir, nil, "sensors", "cursor", "get", "finder")
	if after.stdout != fresh.stdout {
		t.Errorf("the sensor no longer reads as one that has not been anywhere\nbefore: %s\nafter:  %s",
			fresh.stdout, after.stdout)
	}
}

// quoted is the JSON spelling of one string, for building an expected body
// without pasting escapes by hand.
func quoted(s string) string {
	out, err := json.Marshal(s)
	if err != nil {
		panic(err)
	}
	return string(out)
}

// sameJSON compares two bodies as documents rather than as text, so field
// order is not part of the contract.
func sameJSON(t *testing.T, got, want string) bool {
	t.Helper()

	var gotDoc, wantDoc any
	if err := json.Unmarshal([]byte(want), &wantDoc); err != nil {
		t.Fatalf("the expected body is not JSON: %v\n%s", err, want)
	}
	if err := json.Unmarshal([]byte(got), &gotDoc); err != nil {
		return false
	}
	return reflect.DeepEqual(gotDoc, wantDoc)
}
