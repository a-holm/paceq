package cli

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// TestEveryErrorCarriesAllThreeParts is 03 section 8.1 and 09 section 6.4 as a
// test: what went wrong, where, and what to do now. A message without the third
// part is a bug, so every error the package can produce is built here and
// checked, and the completeness of this table is checked in turn against the
// source.
func TestEveryErrorCarriesAllThreeParts(t *testing.T) {
	ctx := context.Background()
	locked := &store.LockedError{Path: "/tmp/x/.paceq/paceq.lock"}
	perm := &store.PermissionError{Path: "/tmp/x/.paceq", Got: 0o755, Want: store.DirMode}
	refused := peerVerdict("/run/user/1000/paceq.sock", 0, true, 1000)
	held, _ := daemonHoldsStateRefusal(t, "finder was not paused")
	stolen, _, _ := sensorNameTakenRefusal()

	built := map[string]*Error{
		"usageError":       usageError("the flag --nope is not a paceq flag", "paceq --help lists every flag"),
		"notFoundError":    notFoundError("no error code PQ9999", "the catalogue in this build", "paceq doctor"),
		"validationError":  validationError(perm.Error(), perm, "chmod 0700 /tmp/x/.paceq"),
		"busyError":        busyError(locked),
		"timeoutError":     timeoutError("opening the state took longer than allowed", context.DeadlineExceeded),
		"interruptedError": interruptedError(context.Canceled),
		"internalError":    internalError("could not write paceq.yaml", errors.New("read only file system")),
		"cutoverFailure": cutoverFailure("the crontab was not changed: the write failed after the backup was taken", errors.New("disk full"),
			"the state before the cutover is in .paceq/crontab.backup.2027-01-12T09-14-03",
			"restore it with:  paceq cutover --rollback --from .paceq/crontab.backup.2027-01-12T09-14-03"),
		"pathError":            pathError(validateCatalog, "jobs/nightly.yaml", fs.ErrNotExist),
		"sensorNameTakenError": stolen,

		"socketRefusedError": socketRefusedError(refused),
		"stopOnRefusal":      stopOnRefusal(fmt.Errorf("dial the daemon: %w", refused)),
		"daemonHoldsState":   held,

		"repairConfirmError": repairConfirmError(&store.RepairConfirmError{Critical: []store.Violation{
			{
				Check: "I3", Severity: store.Critical, Subject: "run 01J0RUN",
				Detail: "the run is claimed by more than one dedup identity (sensor-a/1/k, sensor-b/1/k)",
			},
		}}, Env{}, "/tmp/x/.paceq"),

		"classify": classify(ctx, errors.New("something nobody classified")),
	}

	for name, err := range built {
		t.Run(name, func(t *testing.T) {
			if err == nil {
				t.Fatal("the constructor returned nil, so nothing is reported to the user")
			}
			message := err.Error()
			if strings.TrimSpace(message) == "" {
				t.Fatal("the error says nothing about what went wrong")
			}
			if len(err.next) == 0 {
				t.Errorf("the error offers no next step, which 09 section 6.4 calls a bug: %q", message)
			}
			if !strings.Contains(message, "\n  ") {
				t.Errorf("the message has no indented next step line:\n%s", message)
			}
			if !slices.ContainsFunc(exitCodes, func(c exitCode) bool { return c.Code == err.code }) {
				t.Errorf("exit code %d is not in the documented table", err.code)
			}
			if err.code == ExitOK {
				t.Error("an error exits 0, so a script cannot tell it happened")
			}
		})
	}

	for _, name := range errorConstructors(t) {
		if _, ok := built[name]; !ok {
			t.Errorf("%s returns an *Error but no test builds one, so its message is unchecked", name)
		}
	}
}

// daemonHoldsStateRefusal builds the refusal a direct write meets when a
// daemon owns this state directory, in the shape the shipped unit runs: an
// open session row naming a live process and no socket file anywhere. It
// returns the session too, so a test asserts against the daemon that is really
// there rather than against words copied out of the message.
func daemonHoldsStateRefusal(t *testing.T, what string) (*Error, store.Session) {
	t.Helper()

	dir := t.TempDir()
	if got := runCLI(t, dir, nil, "init"); got.code != ExitOK {
		t.Fatalf("init = %d\n%s%s", got.code, got.stdout, got.stderr)
	}
	session := holdStateAsDaemon(t, dir, "0.9.0")

	env := Env{Dir: dir, Getenv: lookup(nil)}
	held := daemonHoldsState(t.Context(), env, &globals{}, what)
	if held == nil {
		t.Fatal("a daemon holds the state and exposes no socket, and the direct write was let through")
	}
	return held, session
}

// TestDaemonHoldsStateNamesTheDaemonAndTheWayIn reads the message an operator
// meets when a write cannot be routed. Telling them the write did not happen
// is not enough: the refusal has to name the process that holds the state and
// both ways past it, because the alternative is the flock's refusal, which
// names a lock file and a pid and no flag (#215).
func TestDaemonHoldsStateNamesTheDaemonAndTheWayIn(t *testing.T) {
	held, session := daemonHoldsStateRefusal(t, "finder was not paused")
	message := held.Error()

	if held.code != ExitBusy {
		t.Errorf("the refusal exits %d, want %d: the state is held, nothing is broken", held.code, ExitBusy)
	}

	parts := []struct{ want, why string }{
		{"finder was not paused", "which write did not happen"},
		{fmt.Sprintf("pid %d", session.PID), "the process holding the state"},
		{fmt.Sprintf("paceq %s", session.Version), "which build holds it"},
		{session.StartedAt.Format(time.RFC3339), "since when"},
		{"exposes no socket", "why the write could not be routed to that daemon"},
		{"--socket", "the flag that routes the next write instead of refusing it"},
		{fmt.Sprintf("kill %d", session.PID), "the other way out, naming the same process"},
	}
	for _, part := range parts {
		if !strings.Contains(message, part.want) {
			t.Errorf("the refusal never says %q (%s):\n%s", part.want, part.why, message)
		}
	}
}

// TestClassifyMapsStoreFailuresToTheirOwnCode keeps the difference between "the
// tool broke", "somebody else has the state" and "the state is exposed" visible
// to a script, which is the whole point of the table.
func TestClassifyMapsStoreFailuresToTheirOwnCode(t *testing.T) {
	ctx := context.Background()
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	expired, stop := context.WithDeadline(ctx, time.Unix(0, 0))
	defer stop()

	cases := map[string]struct {
		ctx  context.Context
		err  error
		want int
	}{
		"a held state directory":   {ctx: ctx, err: &store.LockedError{Path: "/tmp/x"}, want: ExitBusy},
		"an exposed database":      {ctx: ctx, err: &store.PermissionError{Path: "/tmp/x", Got: 0o644, Want: store.DatabaseMode}, want: ExitValidation},
		"a wrapped refusal":        {ctx: ctx, err: fmt.Errorf("open the state: %w", &store.LockedError{Path: "/tmp/x"}), want: ExitBusy},
		"an interrupted command":   {ctx: cancelled, err: context.Canceled, want: ExitInterrupted},
		"a command that timed out": {ctx: expired, err: context.DeadlineExceeded, want: ExitTimeout},
		"anything else":            {ctx: ctx, err: errors.New("disk on fire"), want: ExitInternal},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			got := classify(c.ctx, c.err)
			if got.code != c.want {
				t.Errorf("classify(%v) exits %d, want %d", c.err, got.code, c.want)
			}
			if !strings.Contains(got.Error(), "\n  ") {
				t.Errorf("the message has no indented next step:\n%s", got.Error())
			}
		})
	}
}

// errorConstructors is every function in the package that returns an *Error.
// The list comes from the source rather than a hand written table, so a new
// error type cannot be added without the test above noticing.
func errorConstructors(t *testing.T) []string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("read the working directory: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}

	fset := token.NewFileSet()
	var found []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
		if err != nil {
			t.Fatalf("parse %s: %v", name, err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv != nil || fn.Type.Results == nil {
				continue
			}
			for _, result := range fn.Type.Results.List {
				star, ok := result.Type.(*ast.StarExpr)
				if !ok {
					continue
				}
				if ident, ok := star.X.(*ast.Ident); ok && ident.Name == "Error" {
					found = append(found, fn.Name.Name)
				}
			}
		}
	}
	if len(found) == 0 {
		t.Fatal("no error constructors found, the completeness check would pass vacuously")
	}
	return found
}

// sensorNameTakenRefusal builds the refusal an apply meets when the state
// database already holds a sensor name under another job: two job files in one
// batch, the name owned by the first and declared by the second. It returns
// the store refusal and the file the declaring job was read from, so a test
// asserts against the conflict that is really there rather than against words
// copied out of the message.
func sensorNameTakenRefusal() (*Error, *store.SensorNameTakenError, string) {
	taken := &store.SensorNameTakenError{Sensor: "dropzone", Owner: "alpha", Taker: "beta"}
	loaded := []loadedSpec{
		{display: "jobs/a.yaml", input: store.JobVersionInput{JobName: taken.Owner}},
		{display: "jobs/b.yaml", input: store.JobVersionInput{JobName: taken.Taker}},
	}
	return sensorNameTakenError(loaded, taken), taken, loaded[1].display
}

// TestSensorNameTakenNamesTheOwnerAndTheFileToEdit reads the message an
// operator meets when apply refuses a sensor name the database holds under
// another job. Saying the name is taken is not enough: the refusal has to
// separate the job whose row owns the name, which is the job that has to give
// it up, from the file that declares it, which is the file to open. It also
// has to say the batch wrote nothing, because the alternative is an operator
// hunting for a half applied job (#206).
func TestSensorNameTakenNamesTheOwnerAndTheFileToEdit(t *testing.T) {
	refusal, taken, file := sensorNameTakenRefusal()
	message := refusal.Error()

	if refusal.code != ExitValidation {
		t.Errorf("the refusal exits %d, want %d: a spec has to be corrected, nothing is broken", refusal.code, ExitValidation)
	}

	parts := []struct{ want, why string }{
		{
			fmt.Sprintf("%s: the sensor %s is already owned by the job %s", spec.CodeSensorNameTaken, taken.Sensor, taken.Owner),
			"the code to look up, the contested name, and the job that owns it today",
		},
		{"\n  at " + file, "the file declaring the name, which is the file to edit"},
		{"the primary key its row lives under", "why no way out leaves both jobs owning the name"},
		{"take it out of " + taken.Owner + " first", "the way out, naming the job that has to give the name up"},
		{"nothing was written", "whether a half applied batch has to be cleaned up"},
		{"paceq error " + spec.CodeSensorNameTaken, "where that code is explained in full"},
	}
	for _, part := range parts {
		if !strings.Contains(message, part.want) {
			t.Errorf("the refusal never says %q (%s):\n%s", part.want, part.why, message)
		}
	}
}
