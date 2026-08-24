// The standalone half of the example-sensor suite (issue #14, M3-07): it
// proves each example behaves as a sensor from outside, the way a user runs
// it, with no access to internal Go code. Every script reads its cursor from
// PACEQ_* environment and writes one JSON object on stdout; this test drives
// that contract and validates the JSON shape, including the empty-cursor
// first run and the skip on no change.
//
// The store end of the suite (spec -> evaluator -> atomic commit -> dedup)
// lives in internal/store/example_sensors_test.go, where the sensor seam the
// M3-06 CLI would later drive is exercised directly.
package sensors_test

import (
	"bytes"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

// contract is the frozen output shape, the same field set the evaluator parser
// accepts and rejects unknown fields for (sensor-contract.md, M3-02).
type contract struct {
	Cursor     *string   `json:"cursor"`
	Triggers   []trigger `json:"triggers"`
	SkipReason *string   `json:"skip_reason"`
}

type trigger struct {
	RunKey string `json:"run_key"`
	Params any    `json:"params,omitempty"`
}

// runScript executes one example with the given env and returns stdout.
func runScript(t *testing.T, script string, env []string) []byte {
	t.Helper()
	cmd := exec.Command("sh", script)
	cmd.Env = append(os.Environ(), env...)
	var out, errb bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errb
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s failed: %v\nstderr: %s", script, err, errb.String())
	}
	return out.Bytes()
}

// setMtime pins a file's mtime to a fixed past value so the test is
// deterministic and never depends on the current clock (no sleep, no real
// mtime).
func setMtime(t *testing.T, path string, unix int64) {
	t.Helper()
	ts := time.Unix(unix, 0)
	if err := os.Chtimes(path, ts, ts); err != nil {
		t.Fatalf("set mtime on %s: %v", path, err)
	}
}

// makeFixtures writes files into dir with deterministic mtimes ascending from
// base. Returns the file paths in mtime order.
func makeFiles(t *testing.T, dir string, base int64, names []string) []string {
	t.Helper()
	files := make([]string, 0, len(names))
	for i, name := range names {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(name), 0o644); err != nil {
			t.Fatalf("write fixture %s: %v", p, err)
		}
		setMtime(t, p, base+int64(i))
		files = append(files, p)
	}
	return files
}

// validOutput requires one JSON object and validates the frozen shape.
func validOutput(t *testing.T, out []byte) contract {
	t.Helper()
	out = bytes.TrimSpace(out)
	if len(out) == 0 {
		t.Fatal("sensor wrote nothing on stdout")
	}
	var c contract
	if err := json.Unmarshal(out, &c); err != nil {
		t.Fatalf("output is not one JSON object: %v\noutput: %s", err, out)
	}
	if c.Cursor == nil && len(c.Triggers) == 0 && c.SkipReason == nil {
		t.Fatalf("output has neither cursor, trigger nor skip_reason: %s", out)
	}
	for _, tr := range c.Triggers {
		if tr.RunKey == "" {
			t.Fatalf("a trigger has an empty run_key: %s", out)
		}
	}
	return c
}

// wenv builds a sensor env: neutral PACEQ_ contract keys plus the supplied
// WATCH_/test config and cursor.
func wenv(pairs map[string]string) []string {
	out := []string{}
	for _, k := range []string{
		"PACEQ_SENSOR", "PACEQ_JOB", "PACEQ_LAST_TICK_AT",
		"PACEQ_NOW", "PACEQ_MAX_TRIGGERS", "PACEQ_DEADLINE_MS", "PACEQ_DRY_RUN",
	} {
		out = append(out, k+"=")
	}
	for k, v := range pairs {
		out = append(out, k+"="+v)
	}
	return out
}

// withBin prepends examples/sensors/bin to PATH so the aws and sqlite3 shims
// are found when the script calls them, exactly as paceq does by putting the
// sensor's declared env on top of its fixed PATH.
func withBin(t *testing.T) string {
	t.Helper()
	abs, err := filepath.Abs("bin")
	if err != nil {
		t.Fatalf("resolve bin dir: %v", err)
	}
	return abs + ":" + os.Getenv("PATH")
}

func TestMalReportsNothingNewAndStableCursor(t *testing.T) {
	out := validOutput(t, runScript(t, "mal.sh", wenv(map[string]string{"PACEQ_CURSOR": "5"})))
	if c := *out.Cursor; c != "5" {
		t.Fatalf("a skip must not move the cursor, got %q want 5", c)
	}
	if len(out.Triggers) != 0 {
		t.Fatalf("mal reports no triggers, got %d", len(out.Triggers))
	}
	if out.SkipReason == nil {
		t.Fatal("a quiet run carries a skip_reason")
	}
}

func TestFsWatermarkFirstRunListsThenSkips(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Unix() - 1000
	files := makeFiles(t, dir, base, []string{"a.txt", "b.txt", "c.txt"})

	// First run from an empty cursor: every file is new, cursor = max mtime.
	first := validOutput(t, runScript(t, "fs-watermark.sh",
		wenv(map[string]string{"WATCH_DIR": dir, "PACEQ_CURSOR": ""})))
	if len(first.Triggers) != len(files) {
		t.Fatalf("first run should trigger %d files, got %d", len(files), len(first.Triggers))
	}
	if c := *first.Cursor; c != strconv.FormatInt(base+2, 10) {
		t.Fatalf("cursor should be the max mtime %d, got %q", base+2, c)
	}

	// Second run from that cursor: nothing newer, so a quiet skip.
	second := validOutput(t, runScript(t, "fs-watermark.sh",
		wenv(map[string]string{"WATCH_DIR": dir, "PACEQ_CURSOR": *first.Cursor})))
	if len(second.Triggers) != 0 {
		t.Fatalf("nothing newer than the cursor should trigger 0, got %d", len(second.Triggers))
	}
}

func TestFsWatermarkAcceptsACap(t *testing.T) {
	dir := t.TempDir()
	base := time.Now().Unix() - 1000
	makeFiles(t, dir, base, []string{"a.txt", "b.txt", "c.txt", "d.txt"})

	out := validOutput(t, runScript(t, "fs-watermark.sh",
		wenv(map[string]string{"WATCH_DIR": dir, "PACEQ_CURSOR": "", "PACEQ_MAX_TRIGGERS": "2"})))
	if len(out.Triggers) != 2 {
		t.Fatalf("PACEQ_MAX_TRIGGERS=2 should cap triggers at 2, got %d", len(out.Triggers))
	}
}

func TestHttpETagTriggersOnChangeAndSkipsWhenUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.Header().Set("ETag", `"v7"`)
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("ETag", `"v7"`)
		_, _ = w.Write([]byte("release body"))
	}))
	defer srv.Close()

	first := validOutput(t, runScript(t, "http-etag.sh",
		wenv(map[string]string{"WATCH_URL": srv.URL, "PACEQ_CURSOR": ""})))
	if len(first.Triggers) != 1 {
		t.Fatalf("a changed URL should trigger once, got %d", len(first.Triggers))
	}
	if *first.Cursor != `v7` {
		t.Fatalf("the ETag is the cursor, got %q", *first.Cursor)
	}

	unchanged := validOutput(t, runScript(t, "http-etag.sh",
		wenv(map[string]string{"WATCH_URL": srv.URL, "PACEQ_CURSOR": *first.Cursor})))
	if len(unchanged.Triggers) != 0 {
		t.Fatalf("an unchanged ETag should trigger 0, got %d", len(unchanged.Triggers))
	}
}

func TestHTTPETagExit75OnUnreachable(t *testing.T) {
	// A port that accepts no connection: curl exits nonzero and the sensor
	// must map it to exit 75 (EX_TEMPFAIL) so the breaker treats it as
	// transient rather than a fault.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve a loopback address: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens here

	cmd := exec.Command("sh", "http-etag.sh")
	cmd.Env = append(os.Environ(), wenv(map[string]string{
		"WATCH_URL": "http://" + addr + "/", "PACEQ_CURSOR": "",
	})...)
	err = cmd.Run()
	if ee, ok := err.(*exec.ExitError); !ok || ee.ExitCode() != 75 {
		t.Fatalf("an unreachable URL must exit 75 (transient), got %v", err)
	}
}

func TestSqlWatermarkTriggersByCursorAndSkips(t *testing.T) {
	db := filepath.Join("testdata", "sql", "ids.txt")
	env := func(cursor string) []string {
		return wenv(map[string]string{
			"WATCH_DB": db, "WATCH_TABLE": "events", "PACEQ_CURSOR": cursor,
			"SQLITE_STUB_DB": db, "PATH": withBin(t),
		})
	}
	first := validOutput(t, runScript(t, "sql-watermark.sh", env("")))
	if len(first.Triggers) != 4 {
		t.Fatalf("four ids newer than the empty cursor, got %d", len(first.Triggers))
	}
	mid := validOutput(t, runScript(t, "sql-watermark.sh", env("20")))
	if len(mid.Triggers) != 2 { // ids 30, 40
		t.Fatalf("two ids newer than 20, got %d", len(mid.Triggers))
	}
	last := validOutput(t, runScript(t, "sql-watermark.sh", env("40")))
	if len(last.Triggers) != 0 {
		t.Fatalf("nothing newer than 40, got %d", len(last.Triggers))
	}
}

func TestS3ListingTriggersKeysInLexicalOrder(t *testing.T) {
	root := filepath.Join("testdata", "s3")
	env := func(cursor string) []string {
		return wenv(map[string]string{
			"WATCH_BUCKET": "bucket01", "PACEQ_CURSOR": cursor,
			"S3_STUB_ROOT": root, "PATH": withBin(t),
		})
	}
	first := validOutput(t, runScript(t, "s3-listing.sh", env("")))
	if len(first.Triggers) != 2 {
		t.Fatalf("two keys in bucket01, got %d", len(first.Triggers))
	}
	if first.Triggers[0].RunKey != "alpha.txt" {
		t.Fatalf("keys must come back lexicographically, first is %q", first.Triggers[0].RunKey)
	}
	after := validOutput(t, runScript(t, "s3-listing.sh", env("alpha.txt")))
	if len(after.Triggers) != 1 {
		t.Fatalf("one key after alpha.txt, got %d", len(after.Triggers))
	}
}
