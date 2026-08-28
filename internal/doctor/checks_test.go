package doctor_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/store"
)

// The M6-06 checks: what fsck cannot see. Every test pins one check to a
// deterministic answer through an injected reader, and every non-doctor.OK finding
// must carry a next step, because the issue's acceptance says a finding
// without a remedy is a defect the build refuses.

func TestCheckNTPGradesTheMachinesClock(t *testing.T) {
	ok := doctor.CheckNTP(func() (bool, bool) { return true, true })
	if ok.Level != doctor.OK {
		t.Errorf("a synchronized clock warns: %+v", ok)
	}
	drift := doctor.CheckNTP(func() (bool, bool) { return false, true })
	if drift.Level != doctor.Warn {
		t.Fatalf("an undisciplined clock is a warning: %+v", drift)
	}
	if len(drift.Next) == 0 {
		t.Error("the warning carries no next step")
	}
	unknown := doctor.CheckNTP(func() (bool, bool) { return false, false })
	if unknown.Level != doctor.OK {
		t.Errorf("a machine with no answer is not broken: %+v", unknown)
	}
	if !strings.Contains(unknown.Detail, "unknown") {
		t.Errorf("an unknown answer must say so: %+v", unknown)
	}
}

func TestCheckTzdataVersionNamesTheDrift(t *testing.T) {
	current := doctor.CheckTzdataVersion(func() (string, bool) { return "2027a", true }, "2027a")
	if current.Level != doctor.OK {
		t.Errorf("matching versions warn: %+v", current)
	}
	stale := doctor.CheckTzdataVersion(func() (string, bool) { return "2027a", true }, "2026c")
	if stale.Level != doctor.Warn {
		t.Fatalf("an older embedded database is a warning: %+v", stale)
	}
	if !strings.Contains(stale.Detail, "2026c") || !strings.Contains(stale.Detail, "2027a") {
		t.Errorf("the warning never names the two versions: %+v", stale)
	}
	newer := doctor.CheckTzdataVersion(func() (string, bool) { return "2026c", true }, "2027a")
	if newer.Level != doctor.OK {
		t.Errorf("a newer binary over an older system is not drift: %+v", newer)
	}
	noversion := doctor.CheckTzdataVersion(func() (string, bool) { return "", false }, "2027a")
	if noversion.Level != doctor.OK {
		t.Errorf("a system without a stamp is not broken: %+v", noversion)
	}
}

func TestCheckSpoolBacklogCountsUnconsumedResults(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))

	empty := doctor.CheckSpoolBacklog(dir, clk)
	if empty.Level != doctor.OK {
		t.Fatalf("no spool at all is the healthy case: %+v", empty)
	}

	spool := filepath.Join(dir, "spool", "attempts")
	if err := os.MkdirAll(spool, 0o700); err != nil {
		t.Fatal(err)
	}
	fresh := doctor.CheckSpoolBacklog(dir, clk)
	if fresh.Level != doctor.OK {
		t.Fatalf("an empty spool is the healthy case: %+v", fresh)
	}

	if err := os.WriteFile(filepath.Join(spool, "01 RESULT.json"), []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(spool, "01 RESULT.json"), old, old); err != nil {
		t.Fatal(err)
	}
	backlog := doctor.CheckSpoolBacklog(dir, clk)
	if backlog.Level != doctor.Warn {
		t.Fatalf("a backlog is a warning: %+v", backlog)
	}
	if !strings.Contains(backlog.Detail, "1 unconsumed result file") {
		t.Errorf("the finding never counts the backlog: %+v", backlog.Detail)
	}
	if len(backlog.Next) == 0 {
		t.Error("the warning carries no next step")
	}
}

func TestCheckOrphanedProcessesNamesTheLeftovers(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{fsck: nil}
	db.active = []store.AttemptProcess{{RunID: "01ALIVE", Step: "build", PID: 10}}

	lister := func() ([]reconcile.Process, error) {
		return []reconcile.Process{
			{PID: 11, RunID: "01ALIVE"},
			{PID: 12, RunID: "01DEAD"},
		}, nil
	}
	found := doctor.CheckOrphanedProcesses(ctx, db, lister)
	if found.Level != doctor.Warn {
		t.Fatalf("an orphan is a warning: %+v", found)
	}
	if !strings.Contains(found.Detail, "pid 12 (run 01DEAD)") {
		t.Errorf("the finding never names the orphan: %+v", found.Detail)
	}
	if strings.Contains(found.Detail, "pid 11") {
		t.Errorf("a live process was called an orphan: %+v", found.Detail)
	}

	quiet := doctor.CheckOrphanedProcesses(ctx, db, func() ([]reconcile.Process, error) {
		return []reconcile.Process{{PID: 11, RunID: "01ALIVE"}}, nil
	})
	if quiet.Level != doctor.OK {
		t.Fatalf("every process claimed by a run is healthy: %+v", quiet)
	}
}

func TestCheckFreshnessSLANamesTheUnpolicedJobs(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{unpoliced: []string{"cleanup-tmp", "legacy-etl", "rapport-uke", "sync-files", "fifth"}}
	finding := doctor.CheckFreshnessSLA(ctx, db)
	if finding.Level != doctor.Warn {
		t.Fatalf("jobs nobody can alarm on is a warning: %+v", finding)
	}
	if !strings.Contains(finding.Detail, "5 of the jobs") {
		t.Errorf("the finding never counts the jobs: %+v", finding.Detail)
	}
	if strings.Contains(finding.Detail, "fifth") {
		t.Errorf("the finding names every job instead of a sample: %+v", finding.Detail)
	}
	db.unpoliced = nil
	if ok := doctor.CheckFreshnessSLA(ctx, db); ok.Level != doctor.OK {
		t.Fatalf("a fully policed installation warns: %+v", ok)
	}
}

func TestCheckFilesystemWarnsOnNetworkMagic(t *testing.T) {
	local := doctor.CheckFilesystem("/state", func(string) (uint64, error) { return 0xEF53, nil })
	if local.Level != doctor.OK {
		t.Fatalf("a local filesystem warns: %+v", local)
	}
	network := doctor.CheckFilesystem("/state", func(string) (uint64, error) { return 0x6969, nil })
	if network.Level != doctor.Warn {
		t.Fatalf("NFS is a warning: %+v", network)
	}
	if !strings.Contains(network.Detail, "network filesystem") {
		t.Errorf("the warning never names the risk: %+v", network.Detail)
	}
}

func TestCheckCriticalInvariantsFailsOnAQuickFsckFinding(t *testing.T) {
	ctx := context.Background()
	clean := &fakeDB{}
	if ok := doctor.CheckCriticalInvariants(ctx, clean); ok.Level != doctor.OK {
		t.Fatalf("a clean sweep is the healthy case: %+v", ok)
	}
	broken := &fakeDB{fsck: []store.Violation{
		{
			Check: "I6", Severity: store.Critical, Subject: "tick slot x@1",
			Detail: "one evaluation slot holds more than one tick",
		},
	}}
	failing := doctor.CheckCriticalInvariants(ctx, broken)
	if failing.Level != doctor.Fail {
		t.Fatalf("a critical invariant must fail the report: %+v", failing)
	}
	for _, step := range failing.Next {
		if strings.Contains(step, "--confirm") {
			return
		}
	}
	t.Errorf("the remedy never names the confirm requirement: %+v", failing.Next)
}

func TestCheckPragmasGradesTheConnectionDiscipline(t *testing.T) {
	ctx := context.Background()
	healthy := &fakeDB{}
	for _, f := range doctor.CheckPragmas(ctx, healthy) {
		if f.Level != doctor.OK {
			t.Fatalf("the pinned pragmas warn: %+v", f)
		}
	}
	// foreignKeys names the breakage in the stub: true means the pragma
	// answers off.
	off := &fakeDB{foreignKeys: true, sync: "0"}
	findings := doctor.CheckPragmas(ctx, off)
	if len(findings) != 2 {
		t.Fatalf("two broken pragmas produced %d findings: %+v", len(findings), findings)
	}
	for _, f := range findings {
		if f.Level != doctor.Fail {
			t.Errorf("synchronous OFF and foreign keys OFF are failures: %+v", f)
		}
	}
}

// TestEveryNonOKFindingCarriesANextStep is the PLAN acceptance, held against
// every check in this file at once: a finding without a remedy is a defect
// the build refuses, whoever adds the check and whenever.
func TestEveryNonOKFindingCarriesANextStep(t *testing.T) {
	clk := clock.NewFake(time.Date(2027, 1, 21, 8, 0, 0, 0, time.UTC))
	ctx := context.Background()
	db := &fakeDB{}

	findings := []doctor.Finding{
		doctor.CheckNTP(func() (bool, bool) { return false, true }),
		doctor.CheckTzdataVersion(func() (string, bool) { return "2027a", true }, "2026c"),
		doctor.CheckSpoolBacklog(t.TempDir(), clk),
		doctor.CheckOrphanedProcesses(ctx, db, func() ([]reconcile.Process, error) {
			return []reconcile.Process{{PID: 12, RunID: "01DEAD"}}, nil
		}),
		doctor.CheckFreshnessSLA(ctx, db),
		doctor.CheckFilesystem("/state", func(string) (uint64, error) { return 0x6969, nil }),
		doctor.CheckCriticalInvariants(ctx, &fakeDB{fsck: []store.Violation{
			{Check: "I6", Severity: store.Critical, Subject: "tick slot x@1"},
		}}),
	}
	findings = append(findings, doctor.CheckPragmas(ctx, &fakeDB{foreignKeys: false, sync: "0"})...)
	findings = append(findings, doctor.CheckAutoVacuum(ctx, db))

	for _, f := range findings {
		if f.Level == doctor.OK {
			continue
		}
		if len(f.Next) == 0 {
			t.Errorf("%s (%s) offers no next step: %+v", f.Title, f.Level, f)
		}
	}
}
