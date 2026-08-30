package cli

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// The direct path is the command doing the daemon's work itself when no daemon
// answers. It is the same decision made twice, and the two copies drifted
// (#215): the forced tick committed a batch no ceiling had bounded, dropped the
// sensor's own skip reason, and every surface that asked whether a daemon was
// there asked a socket file the daemon does not maintain.
//
// These tests read rows and findings rather than stdout. Both routes print OK
// and exit 0 for every defect below, so the output is the one place the
// disagreement cannot be seen.

// seedTickSensor records a job and a sensor whose program prints contract on
// stdout, placed at cursor so a test can say whether the tick moved it.
func seedTickSensor(t *testing.T, dir, name, contract, cursor string) {
	t.Helper()
	execJSON, err := json.Marshal([]string{"/bin/echo", contract})
	if err != nil {
		t.Fatalf("build the exec spec: %v", err)
	}
	seedSensorCLI(t, dir, name, string(execJSON))
	if cursor == "" {
		return
	}
	s, err := store.OpenState(t.Context(), filepath.Join(dir, stateDirName), store.Options{})
	if err != nil {
		t.Fatalf("open state to place the cursor: %v", err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SetSensorCursor(t.Context(), store.CursorInput{Name: name, Cursor: cursor}); err != nil {
		t.Fatalf("place the sensor at its cursor: %v", err)
	}
}

// triggerContract is one sensor answer electing n triggers and reporting a
// cursor past all of them.
func triggerContract(n int) string {
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		keys = append(keys, fmt.Sprintf(`{"run_key":"k%04d"}`, i))
	}
	return fmt.Sprintf(`{"cursor":"file:%d","triggers":[%s]}`, n, strings.Join(keys, ","))
}

// lastTick reads back the tick the command just wrote.
func lastTick(t *testing.T, dir, sensor string) store.ExplainTick {
	t.Helper()
	s := openStoreRead(t, dir)
	defer func() { _ = s.Close() }()
	ticks, err := s.ExplainTicks(t.Context(),
		[]store.ExplainSource{{Kind: "sensor", Name: sensor}}, time.UnixMilli(0), "", 10)
	if err != nil {
		t.Fatalf("read the ticks of %s: %v", sensor, err)
	}
	if len(ticks) == 0 {
		t.Fatalf("sensor %s has no tick row", sensor)
	}
	return ticks[0]
}

// sensorCursor is where the sensor stands now, as a printable word.
func sensorCursor(t *testing.T, dir, sensor string) string {
	t.Helper()
	s := openStoreRead(t, dir)
	defer func() { _ = s.Close() }()
	row, err := s.GetSensor(t.Context(), sensor)
	if err != nil {
		t.Fatalf("read sensor %s: %v", sensor, err)
	}
	if row.Cursor == nil {
		return "(none)"
	}
	return *row.Cursor
}

// TestSensorsTickHoldsTheTriggerCeiling: max_triggers_per_tick is the number
// `paceq sensors show` prints, and a forced tick has to honour it. The daemon
// cuts the batch and leaves the cursor where it was, so the dropped triggers
// are replayed into the dedup gate instead of becoming twins. The CLI's own
// commit had no ceiling at all: it queued the whole batch in one transaction
// and stepped the cursor past every trigger it had just created.
func TestSensorsTickHoldsTheTriggerCeiling(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	// The seeded ceiling is 100; the sensor answers with half again as many.
	seedTickSensor(t, dir, "finder", triggerContract(150), "file:0")

	if got := runCLI(t, dir, nil, "sensors", "tick", "finder"); got.code != ExitOK {
		t.Fatalf("tick = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	tick := lastTick(t, dir, "finder")
	if tick.TriggerCount != 100 {
		t.Errorf("the tick created %d runs, want the declared ceiling of 100", tick.TriggerCount)
	}
	if got := sensorCursor(t, dir, "finder"); got != "file:0" {
		t.Errorf("the cursor moved to %q; a truncated batch must leave it at file:0, "+
			"or the dropped triggers are lost", got)
	}
	if tick.CursorAfter != "" {
		t.Errorf("the tick reports cursor_after %q, want none on a truncated batch", tick.CursorAfter)
	}
}

// TestSensorsTickRecordsThatItWasTruncated: a forced tick that quietly dropped
// most of its batch is worse than one that says so. The drop count belongs on
// the tick row, where explain reads it back.
func TestSensorsTickRecordsThatItWasTruncated(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedTickSensor(t, dir, "finder", triggerContract(150), "file:0")

	if got := runCLI(t, dir, nil, "sensors", "tick", "finder"); got.code != ExitOK {
		t.Fatalf("tick = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	tick := lastTick(t, dir, "finder")
	var data map[string]any
	if err := json.Unmarshal([]byte(tick.ReasonData), &data); err != nil {
		t.Fatalf("the tick's reason data is %q, which is not the JSON object a truncation records: %v",
			tick.ReasonData, err)
	}
	if data["truncated"] != true {
		t.Errorf("reason_data[truncated] = %v, want true", data["truncated"])
	}
	if data["dropped"] != float64(50) {
		t.Errorf("reason_data[dropped] = %v, want 50", data["dropped"])
	}
}

// TestSensorsTickKeepsTheSensorsOwnSkipReason: only the sensor knows what its
// skip meant, so the store writes reasonText verbatim and the daemon hands it
// over. The CLI's commit input never carried the field, so a forced evaluation
// stored the empty string and explain showed the bare catalogue code.
func TestSensorsTickKeepsTheSensorsOwnSkipReason(t *testing.T) {
	const reason = "no new files in /inbox"
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedTickSensor(t, dir, "finder", `{"skip_reason":"`+reason+`"}`, "")

	if got := runCLI(t, dir, nil, "sensors", "tick", "finder"); got.code != ExitOK {
		t.Fatalf("tick = %d\n%s%s", got.code, got.stdout, got.stderr)
	}

	tick := lastTick(t, dir, "finder")
	if tick.Outcome != "skipped" {
		t.Fatalf("outcome = %q, want skipped", tick.Outcome)
	}
	if tick.ReasonText != reason {
		t.Errorf("ticks.reason_text = %q, want the sensor's own words %q", tick.ReasonText, reason)
	}
}

// holdStateAsDaemon makes this state directory look exactly like one the
// shipped systemd unit serves: an open session row naming a live process, the
// state lock held, and no socket file anywhere, because `paceq serve` without
// --socket exposes none.
//
// The lock is held on a second open file description, which is what a second
// paceq would take, so doctor's own attempt is refused the same way.
func holdStateAsDaemon(t *testing.T, dir, version string) store.Session {
	t.Helper()
	s, err := store.OpenState(t.Context(), filepath.Join(dir, stateDirName), store.Options{})
	if err != nil {
		t.Fatalf("hold the state: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	session, err := s.StartSession(t.Context(), version)
	if err != nil {
		t.Fatalf("open the session row: %v", err)
	}
	return session
}

// findings indexes a doctor report by title.
func findings(t *testing.T, doc map[string]any) map[string]map[string]any {
	t.Helper()
	list, ok := doc["findings"].([]any)
	if !ok {
		t.Fatalf("the report has no findings: %v", doc)
	}
	out := map[string]map[string]any{}
	for _, item := range list {
		finding, ok := item.(map[string]any)
		if !ok {
			t.Fatalf("a finding is not an object: %v", item)
		}
		title, _ := finding["title"].(string)
		out[title] = finding
	}
	return out
}

// TestDoctorDoesNotDenyTheDaemonItJustNamed: one report, one answer. Liveness
// was decided twice, from different evidence: the write lock check read the
// open session row and named the pid holding it, while the daemon version
// check stat-ed a socket file. The shipped unit starts `paceq serve` with no
// --socket, so the same run printed the daemon's pid and "no daemon is
// running" two lines apart.
func TestDoctorDoesNotDenyTheDaemonItJustNamed(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	session := holdStateAsDaemon(t, dir, "0.9.0")

	got := runCLI(t, dir, nil, "doctor")
	report := findings(t, got.json(t))

	lock, ok := report["write lock"]
	if !ok {
		t.Fatalf("the report has no write lock finding:\n%s", got.stdout)
	}
	// The reproduction is worth nothing unless the lock finding really did
	// name the daemon this test is pretending to be.
	if detail, _ := lock["detail"].(string); !strings.Contains(detail, fmt.Sprintf("pid %d", session.PID)) {
		t.Fatalf("the write lock finding does not name pid %d: %q", session.PID, detail)
	}

	daemon, ok := report["daemon version"]
	if !ok {
		t.Fatalf("the report has no daemon version finding:\n%s", got.stdout)
	}
	detail, _ := daemon["detail"].(string)
	if strings.Contains(detail, "no daemon is running") {
		t.Fatalf("the report says %q about the daemon whose pid it just named in %q",
			detail, lock["detail"])
	}
	if !strings.Contains(detail, fmt.Sprintf("pid %d", session.PID)) {
		t.Errorf("the daemon version finding does not name the daemon holding the state: %q", detail)
	}
	if !strings.Contains(detail, "socket") {
		t.Errorf("the finding does not say the daemon cannot be reached: %q", detail)
	}
}

// TestStatusReportsADaemonThatHasNoSocketAsUp: status decided the daemon was
// down by dialling a socket file, so a correctly installed daemon read as
// down and the version and uptime it could have printed from the session row
// were never read.
func TestStatusReportsADaemonThatHasNoSocketAsUp(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	holdStateAsDaemon(t, dir, "0.9.0")

	got := runCLI(t, dir, nil, "status")
	if got.code != ExitOK {
		t.Fatalf("status = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	doc := got.json(t)
	block, ok := doc["daemon"].(map[string]any)
	if !ok {
		t.Fatalf("the report has no daemon block: %v", doc)
	}
	if block["up"] != true {
		t.Fatalf("status reports the daemon as down while its session row is open: %v", block)
	}
	if block["version"] != "0.9.0" {
		t.Errorf("daemon.version = %v, want the session row's 0.9.0", block["version"])
	}
	if since, _ := block["since"].(string); since == "" {
		t.Errorf("daemon.since is empty; the session row records when it started")
	}
}

// TestSensorWriteNamesTheDaemonItCannotReach: a write that cannot reach the
// daemon holding the state must say so. The fallback took the flock branch and
// handed the operator a refusal naming a lock file, which does not mention the
// flag that would have let the write through.
func TestSensorWriteNamesTheDaemonItCannotReach(t *testing.T) {
	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d", got.code)
	}
	seedTickSensor(t, dir, "finder", `{"skip_reason":"idle"}`, "")
	holdStateAsDaemon(t, dir, "0.9.0")

	got := runCLI(t, dir, nil, "sensors", "pause", "finder", "--reason", "deploying")

	if got.code != ExitBusy {
		t.Fatalf("pause against an unreachable daemon = %d, want %d\n%s", got.code, ExitBusy, got.stderr)
	}
	if !strings.Contains(got.stderr, "socket") {
		t.Errorf("the refusal never names the missing socket:\n%s", got.stderr)
	}
}

// TestEveryDirectWriteAsksWhetherADaemonHoldsTheState is the guard that keeps
// the two questions apart. Which socket to dial and whether a daemon is there
// were answered by one stat, so a command that found no socket file wrote
// behind a running daemon and reported the flock as the reason. Any function
// that resolves the socket and then opens the state for writing has to ask the
// session row first.
//
// It reads the package's own source, so the rule holds for a write route
// nobody has thought of yet.
func TestEveryDirectWriteAsksWhetherADaemonHoldsTheState(t *testing.T) {
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, ".", func(f os.FileInfo) bool {
		return !strings.HasSuffix(f.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse the package: %v", err)
	}
	pkg, ok := pkgs["cli"]
	if !ok {
		t.Fatal("the cli package did not parse")
	}

	checked := 0
	for _, file := range pkg.Files {
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			called := calledNames(fn.Body)
			if !called["daemonSocket"] || !called["store.OpenState"] {
				continue
			}
			checked++
			if !called["daemonHoldsState"] {
				t.Errorf("%s takes the direct write path on an absent socket without asking "+
					"whether a daemon holds the state (%s)",
					fn.Name.Name, fset.Position(fn.Pos()))
			}
		}
	}
	// A guard that matched nothing would pass forever.
	if checked == 0 {
		t.Fatal("no function resolves the socket and then opens the state; the guard matched nothing")
	}
}

// calledNames collects the function names called inside a body, with a
// package-qualified call spelled "pkg.Name".
func calledNames(body *ast.BlockStmt) map[string]bool {
	out := map[string]bool{}
	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch fun := call.Fun.(type) {
		case *ast.Ident:
			out[fun.Name] = true
		case *ast.SelectorExpr:
			if ident, ok := fun.X.(*ast.Ident); ok {
				out[ident.Name+"."+fun.Sel.Name] = true
			}
		}
		return true
	})
	return out
}
