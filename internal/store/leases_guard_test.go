package store

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The guards in this file hold the two structural promises of the role lease
// port that plain behaviour tests cannot see: admission is one INSERT first
// statement with no read before it, and no store source ever reads the leases
// table to decide something. The issue spells both out as architecture tests,
// because a check then act split survives every black box test right up until
// two processes race through it.

// TestLeaseMethodsTakeNoNowParameter is the compiling half of the no now rule
// (11 section 4.5): lease time belongs to the process that owns the statement,
// so these signatures carry a ttl and never a now. Adding a now parameter to
// any of them fails this file to build, which is the review stopper working as
// intended.
func TestLeaseMethodsTakeNoNowParameter(t *testing.T) {
	s := &Store{}

	var _ func(context.Context, string, string, time.Duration) (LeaseGrant, bool, error) = s.AcquireOrRenew
	var _ func(context.Context, string, string) (bool, error) = s.ReleaseLease
}

// TestLeaseAdmissionStaysOneInsertFirstStatement pins the shape of the one
// statement: it starts from INSERT, never contains a SELECT, and carries the
// conflict arm plus the guard that keeps another holder's live row untouched.
// Splitting it into a SELECT then an UPDATE would pass every behaviour test in
// the happy path and reopen the TOCTOU hole the design closed.
func TestLeaseAdmissionStaysOneInsertFirstStatement(t *testing.T) {
	upper := strings.ToUpper(acquireOrRenewSQL)

	if strings.Contains(upper, "SELECT") {
		t.Error("the admission statement reads before it writes; admission must be one INSERT ON CONFLICT statement")
	}
	if !strings.HasPrefix(upper, "INSERT INTO LEASES") {
		t.Errorf("admission starts %q, want an INSERT INTO leases statement", firstLine(acquireOrRenewSQL))
	}
	if strings.Count(acquireOrRenewSQL, ";") != 0 {
		t.Error("the admission constant holds more than one SQL statement")
	}
	for _, piece := range []string{"ON CONFLICT", "RETURNING", "leases.holder = excluded.holder", "leases.expires_at <="} {
		if !strings.Contains(upper, strings.ToUpper(piece)) {
			t.Errorf("the admission statement lost its %q part", piece)
		}
	}
}

// TestNothingElseReadsTheLeasesTableToDecide walks the non test sources of the
// package and holds the read count of the leases table at exactly the two
// sanctioned statements inside leases.go: the status SELECT in LeaseHolder and
// the own row DELETE in ReleaseLease. Any further read means somebody put a
// lookup in front of a write decision again, which is the pattern the single
// statement exists to kill. Prose in comments avoids the literal "FROM leases"
// so this scan stays exact.
func TestNothingElseReadsTheLeasesTableToDecide(t *testing.T) {
	const sanctioned = 2

	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("list package sources: %v", err)
	}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(".", name))
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		hits := strings.Count(string(raw), "FROM leases")
		if name == "leases.go" {
			if hits != sanctioned {
				t.Errorf("%s touches the leases table %d times, want exactly the %d sanctioned statements", name, hits, sanctioned)
			}
			continue
		}
		if hits != 0 {
			t.Errorf("%s reads or deletes from the leases table outside internal/store/leases.go; "+
				"lease decisions belong behind the single admission statement, not beside it", name)
		}
	}
}

func firstLine(s string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(s), "\n")
	return line
}
