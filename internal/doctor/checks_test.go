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

// scanned is the process /proc reports for a baseline row: the same pid, run
// and start ticks. A test that wants a foreign or a recycled process changes
// one of the three.
func scanned(a store.AttemptProcess) reconcile.Process {
	return reconcile.Process{PID: a.PID, PGID: a.PID, RunID: a.RunID, StartTicks: a.StartTicks, TicksOK: true}
}

func listing(procs ...reconcile.Process) doctor.ProcLister {
	return func() ([]reconcile.Process, error) { return procs, nil }
}

func TestCheckOrphanedProcessesNamesTheLeftovers(t *testing.T) {
	ctx := context.Background()
	worker := store.AttemptProcess{RunID: "01ALIVE", Step: "build", PID: 11, StartTicks: 100}
	leftover := store.AttemptProcess{RunID: "01DEAD", Step: "build", PID: 12, StartTicks: 200}
	db := &fakeDB{
		known:  []store.AttemptProcess{worker, leftover},
		active: []store.AttemptProcess{worker},
	}

	found := doctor.CheckOrphanedProcesses(ctx, db, listing(scanned(worker), scanned(leftover)))
	if found.Level != doctor.Warn {
		t.Fatalf("an orphan is a warning: %+v", found)
	}
	if !strings.Contains(found.Detail, "pid 12 (run 01DEAD)") {
		t.Errorf("the finding never names the orphan: %+v", found.Detail)
	}
	if strings.Contains(found.Detail, "pid 11") {
		t.Errorf("a live process was called an orphan: %+v", found.Detail)
	}
	if len(found.Next) == 0 {
		t.Error("the warning carries no next step")
	}

	quiet := doctor.CheckOrphanedProcesses(ctx, db, listing(scanned(worker)))
	if quiet.Level != doctor.OK {
		t.Fatalf("every process claimed by an active attempt is healthy: %+v", quiet)
	}
}

// TestCheckOrphanedProcessesLeavesAnotherInstallationAlone is #189: the scan is
// machine wide, so a second paceq's healthy job processes turn up in it. They
// are none of this report's business, and advising a kill on one would send an
// operator after work the sweep itself refuses to signal.
func TestCheckOrphanedProcessesLeavesAnotherInstallationAlone(t *testing.T) {
	ctx := context.Background()
	db := &fakeDB{} // a fresh paceq init: no baselines at all

	finding := doctor.CheckOrphanedProcesses(ctx, db, listing(
		reconcile.Process{PID: 2107018, RunID: "01M17NNQ5Y3EXTKHX9TFCNBZ2J", StartTicks: 900, TicksOK: true},
		reconcile.Process{PID: 2107029, RunID: "01M17NNQ5Y3EXTKHX9TFCNBZ2J", StartTicks: 901, TicksOK: true},
	))

	if finding.Level != doctor.OK {
		t.Fatalf("another installation's processes are not this installation's problem: %+v", finding)
	}
	if strings.Contains(finding.Detail, "orphan") {
		t.Errorf("a foreign process was called an orphan: %q", finding.Detail)
	}
	if len(finding.Next) != 0 {
		t.Errorf("the healthy finding advises action on a process paceq must not touch: %v", finding.Next)
	}
	if !strings.Contains(finding.Detail, "2") || !strings.Contains(finding.Detail, "another installation") {
		t.Errorf("the finding never accounts for the two foreign processes: %q", finding.Detail)
	}
}

func TestCheckOrphanedProcessesCountsWhatBelongsElsewhere(t *testing.T) {
	ctx := context.Background()
	worker := store.AttemptProcess{RunID: "01ALIVE", Step: "build", PID: 11, StartTicks: 100}
	foreign := func(pid int) reconcile.Process {
		return reconcile.Process{PID: pid, RunID: "01ELSEWHERE", StartTicks: 900, TicksOK: true}
	}

	cases := []struct {
		name  string
		procs []reconcile.Process
		want  string
	}{
		{
			name:  "none",
			procs: []reconcile.Process{scanned(worker)},
			want:  "1 job processes, all named by active attempts",
		},
		{
			name:  "one",
			procs: []reconcile.Process{scanned(worker), foreign(20)},
			want:  "1 job processes, all named by active attempts; 1 more belong to another installation",
		},
		{
			name:  "several, and none of ours",
			procs: []reconcile.Process{foreign(20), foreign(21), foreign(22)},
			want:  "no job processes of this installation; 3 belong to another installation",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db := &fakeDB{
				known:  []store.AttemptProcess{worker},
				active: []store.AttemptProcess{worker},
			}
			finding := doctor.CheckOrphanedProcesses(ctx, db, listing(c.procs...))
			if finding.Level != doctor.OK {
				t.Fatalf("nothing here is an orphan: %+v", finding)
			}
			if finding.Detail != c.want {
				t.Errorf("detail is %q, want %q", finding.Detail, c.want)
			}
		})
	}
}

// TestCheckOrphanedProcessesRefusesARecycledPid is the sweep's tick comparison
// read from the doctor side: a pid our baseline holds whose /proc identity has
// changed is somebody else's process now, and the sweep would refuse to signal
// it, so the report must not name it either.
func TestCheckOrphanedProcessesRefusesARecycledPid(t *testing.T) {
	ctx := context.Background()
	gone := store.AttemptProcess{RunID: "01DEAD", Step: "build", PID: 12, StartTicks: 200}
	db := &fakeDB{known: []store.AttemptProcess{gone}}

	recycled := scanned(gone)
	recycled.StartTicks = gone.StartTicks + 1

	finding := doctor.CheckOrphanedProcesses(ctx, db, listing(recycled))
	if finding.Level != doctor.OK {
		t.Fatalf("a pid whose identity no longer matches the baseline is not our orphan: %+v", finding)
	}
	if strings.Contains(finding.Detail, "pid 12") {
		t.Errorf("the finding names a pid the sweep would refuse to signal: %q", finding.Detail)
	}

	unreadable := scanned(gone)
	unreadable.TicksOK = false
	if quiet := doctor.CheckOrphanedProcesses(ctx, db, listing(unreadable)); quiet.Level != doctor.OK {
		t.Errorf("a process whose start ticks could not be read is not our orphan: %+v", quiet)
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
	orphan := store.AttemptProcess{RunID: "01DEAD", Step: "build", PID: 12, StartTicks: 200}

	findings := []doctor.Finding{
		doctor.CheckNTP(func() (bool, bool) { return false, true }),
		doctor.CheckTzdataVersion(func() (string, bool) { return "2027a", true }, "2026c"),
		doctor.CheckSpoolBacklog(t.TempDir(), clk),
		doctor.CheckOrphanedProcesses(ctx,
			&fakeDB{known: []store.AttemptProcess{orphan}}, listing(scanned(orphan))),
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
