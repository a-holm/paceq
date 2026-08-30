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
		"pathError": pathError("jobs/nightly.yaml", fs.ErrNotExist),

		"socketRefusedError": socketRefusedError(refused),
		"stopOnRefusal":      stopOnRefusal(fmt.Errorf("dial the daemon: %w", refused)),

		"repairConfirmError": repairConfirmError(&store.RepairConfirmError{Critical: []store.Violation{
			{
				Check: "I3", Severity: store.Critical, Subject: "job x run_key k",
				Detail: "the run key names more than one run",
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
