package doctor

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/reconcile"
	"github.com/a-holm/paceq/internal/store"
)

// The M6-06 checks: what fsck cannot see, because the database is consistent
// and the world around it is still wrong. Every check here follows the house
// rule the catalogue tests enforce: a finding without a next step is half a
// report.

// NTPReader answers whether the machine's clock is disciplined. known is
// false when the answer cannot be had at all, which is its own honest result.
type NTPReader func() (synced, known bool)

// TzVersionReader reads the system zone database's version. ok is false when
// the system carries no version stamp.
type TzVersionReader func() (version string, ok bool)

// ProcLister scans for paceq job processes the way the startup sweep does.
type ProcLister func() ([]reconcile.Process, error)

// MagicProber reports the filesystem magic of a directory, 0 when unknown.
type MagicProber func(dir string) (uint64, error)

// spoolDirName and attemptsDirName are the exec-shim's result spool (M6-04):
// result files land here, fsynced, and the reconciler consumes them. A
// backlog here is work the daemon has not accounted for.
const (
	spoolDirName    = "spool"
	attemptsDirName = "attempts"
)

// CheckPragmas reads the connection discipline back from the database. The
// DSN pins synchronous and foreign keys on every connection; this check is
// what proves the pin took, because a pragma set in config and ignored in
// fact is the kind of drift only a readback catches.
func CheckPragmas(ctx context.Context, db DB) []Finding {
	var out []Finding
	sync, err := db.SynchronousMode(ctx)
	if err != nil {
		out = append(out, Finding{
			Level:  Fail,
			Title:  "pragmas",
			Detail: fmt.Sprintf("could not read the synchronous setting: %v", err),
			Next:   []string{"run paceq doctor again, and check the disk for I/O errors"},
		})
	} else {
		detail := "synchronous " + sync
		switch sync {
		case "1", "2": // NORMAL, FULL: both are settings the DSN may pin.
			out = append(out, Finding{Level: OK, Title: "pragmas", Detail: detail + ", foreign keys on"})
		case "0":
			out = append(out, Finding{
				Level: Fail,
				Title: "pragmas",
				Detail: detail + ": the database runs synchronous OFF, so a power cut can " +
					"corrupt a database that was never touched by a bug",
				Next: []string{
					"check the DSN in the service definition and every tool that opens the file",
					"paceq's own opens always request NORMAL or FULL",
				},
			})
		default:
			out = append(out, Finding{
				Level:  Warn,
				Title:  "pragmas",
				Detail: detail + " is not a value paceq sets: something else configured this file",
				Next:   []string{"check the DSN: paceq pins synchronous to NORMAL or FULL"},
			})
		}
	}
	on, err := db.ForeignKeys(ctx)
	if err == nil && !on {
		out = append(out, Finding{
			Level: Fail,
			Title: "pragmas",
			Detail: "foreign keys are off, so rows can point at rows that do not exist: " +
				"fsck would read the damage as drift",
			Next: []string{"check the DSN: paceq's own opens always set foreign_keys = ON"},
		})
	}
	return out
}

// CheckNTP reports whether the clock is disciplined. A daemon that jumps an
// hour schedules that hour twice or not at all, so an undisciplined clock is
// worth naming on every report, not only after the first weird schedule.
func CheckNTP(read NTPReader) Finding {
	const title = "clock"
	synced, known := read()
	if !known {
		return Finding{
			Level:  OK,
			Title:  title,
			Detail: "clock discipline unknown: no timedatectl on this machine",
			Next:   []string{"if this machine keeps time badly, install NTP; paceq schedules by wall time"},
		}
	}
	if !synced {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: "the clock is not NTP disciplined: a jump makes schedules fire twice " +
				"or not at all, and fsck reads the fallout as timestamp drift",
			Next: []string{
				"enable a time sync service: timedatectl set-ntp true, or install chrony",
				"the drift so far is visible in fsck's I13 findings, if there are any",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: "NTP synchronized"}
}

// CheckTzdataVersion reports the system zone database's version, and warns
// when the binary carries an older embedded copy: schedules in named zones
// then resolve by rules that predate the system's, which shows up as runs an
// hour off exactly at the twice-a-year seams.
func CheckTzdataVersion(system TzVersionReader, embedded string) Finding {
	const title = "tzdata"
	version, ok := system()
	if !ok {
		return Finding{
			Level:  OK,
			Title:  title,
			Detail: "the system zone database carries no version stamp",
		}
	}
	if embedded == "" {
		return Finding{Level: OK, Title: title, Detail: "system " + version + ", the binary resolves zones from it"}
	}
	if olderZoneVersion(embedded, version) {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("the binary carries %s, the system has %s: schedules in named "+
				"zones resolve by the older rules until the binary is upgraded", embedded, version),
			Next: []string{
				"upgrade paceq to a build carrying the newer zone database",
				"the next DST seam is where the difference becomes a wrong hour",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: "system " + version}
}

// olderZoneVersion compares the tzdata version stamp the releases use
// (year plus letter, 2026c < 2027a). An unreadable shape reads as not older:
// the check must not warn on a version format it does not know.
func olderZoneVersion(have, system string) bool {
	haveYear, haveLetter, haveOK := splitZoneVersion(have)
	sysYear, sysLetter, sysOK := splitZoneVersion(system)
	if !haveOK || !sysOK {
		return false
	}
	if haveYear != sysYear {
		return haveYear < sysYear
	}
	return haveLetter < sysLetter
}

func splitZoneVersion(v string) (int, byte, bool) {
	if len(v) < 5 {
		return 0, 0, false
	}
	year, err := strconv.Atoi(v[:4])
	if err != nil {
		return 0, 0, false
	}
	letter := v[4]
	if letter < 'a' || letter > 'z' {
		return 0, 0, false
	}
	return year, letter, true
}

// CheckSpoolBacklog counts the exec-shim result files the daemon has not
// consumed (M6-04's spool, read here from the doctor side). Result files are
// crash insurance: they exist so a daemon that died mid-commit can finish the
// bookkeeping on restart. A backlog that is not shrinking is work the daemon
// does not know it owes.
func CheckSpoolBacklog(dir string, clk clock.Clock) Finding {
	const title = "spool"
	spool := filepath.Join(dir, spoolDirName, attemptsDirName)
	entries, err := os.ReadDir(spool)
	if err != nil {
		return Finding{Level: OK, Title: title, Detail: "empty: nothing is waiting to be reconciled"}
	}
	if len(entries) == 0 {
		return Finding{Level: OK, Title: title, Detail: "empty: nothing is waiting to be reconciled"}
	}
	oldest := time.Time{}
	for _, e := range entries {
		info, err := e.Info()
		if err != nil {
			continue
		}
		if oldest.IsZero() || info.ModTime().Before(oldest) {
			oldest = info.ModTime()
		}
	}
	detail := fmt.Sprintf("%d unconsumed result files", len(entries))
	if !oldest.IsZero() {
		detail += fmt.Sprintf(", oldest written %s ago", clk.Now().UTC().Sub(oldest).Round(time.Second))
	}
	return Finding{
		Level:  Warn,
		Title:  title,
		Detail: detail + ": steps may re-run that already finished",
		Next: []string{
			"check the daemon is running: the reconciler consumes the spool at startup",
			"paceq doctor  again after a restart; a backlog that still grows is a bug",
		},
	}
}

// CheckOrphanedProcesses names the job processes this installation started
// that no active attempt still owns: the processes nobody will ever reap.
//
// The /proc scan is machine wide, so ownership is decided afterwards through
// reconcile.Ownership, the predicate the startup sweep itself acts on. That is
// what keeps the finding's next steps true: the sweep clears exactly what this
// check calls an orphan, and another installation's processes, which the sweep
// will never signal, are counted rather than accused.
func CheckOrphanedProcesses(ctx context.Context, db DB, list ProcLister) Finding {
	const title = "processes"
	if list == nil {
		return Finding{Level: OK, Title: title, Detail: "not scannable on this platform"}
	}
	procs, err := list()
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not scan: %v", err),
			Next:   []string{"check for leftover job processes by hand: ps -ef | grep paceq"},
		}
	}
	if len(procs) == 0 {
		return Finding{Level: OK, Title: title, Detail: "no job processes running"}
	}
	known, err := db.KnownAttempts(ctx)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not read the attempt baselines: %v", err),
			Next:   []string{"run paceq doctor again once the database answers"},
		}
	}
	active, err := db.ActiveAttempts(ctx)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not read the active attempts: %v", err),
			Next:   []string{"run paceq doctor again once the database answers"},
		}
	}

	own := reconcile.NewOwnership(known, active)
	var (
		orphans         []string
		mine, elsewhere int
	)
	for _, p := range procs {
		claim, _ := own.Classify(p)
		switch claim {
		case reconcile.ClaimOrphan:
			orphans = append(orphans, fmt.Sprintf("pid %d (run %s)", p.PID, p.RunID))
		case reconcile.ClaimRunning:
			mine++
		default:
			elsewhere++
		}
	}
	if len(orphans) == 0 {
		return Finding{Level: OK, Title: title, Detail: ownedProcessDetail(mine, elsewhere)}
	}
	sort.Strings(orphans)
	return Finding{
		Level: Warn,
		Title: title,
		Detail: fmt.Sprintf("%d orphaned job processes: %s",
			len(orphans), strings.Join(orphans, ", ")),
		Next: []string{
			"the daemon's process sweep clears these at startup",
			"kill one by hand only after reading its run: paceq explain run <id>",
		},
	}
}

// ownedProcessDetail is the healthy line. Another installation's processes are
// counted rather than hidden, because an operator who can see them in ps needs
// the report to account for them, and a bare count carries no advice to act on
// a process this paceq has no business touching.
func ownedProcessDetail(mine, elsewhere int) string {
	detail := "no job processes of this installation"
	more := ""
	if mine > 0 {
		detail = fmt.Sprintf("%d job processes, all named by active attempts", mine)
		more = "more "
	}
	if elsewhere > 0 {
		detail += fmt.Sprintf("; %d %sbelong to another installation", elsewhere, more)
	}
	return detail
}

// CheckFreshnessSLA names the jobs monitoring cannot alarm on (06 SLO 6): a
// job that declares no expected_within can stop succeeding today and nothing
// will say so until a human looks.
func CheckFreshnessSLA(ctx context.Context, db DB) Finding {
	const title = "freshness"
	jobs, err := db.JobsWithoutFreshnessSLA(ctx)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not read the job expectations: %v", err),
			Next:   []string{"run paceq doctor again once the database answers"},
		}
	}
	if len(jobs) == 0 {
		return Finding{Level: OK, Title: title, Detail: "every job declares expected_within"}
	}
	shown := jobs
	if len(shown) > 4 {
		shown = shown[:4]
	}
	return Finding{
		Level: Warn,
		Title: title,
		Detail: fmt.Sprintf("%d of the jobs cannot be alarmed on: %s",
			len(jobs), strings.Join(shown, ", ")),
		Next: []string{
			"set expected_within on them, the freshness the notifier polices",
			"a job without it can silently stop succeeding; monitoring has nothing to watch",
		},
	}
}

// CheckFilesystem reports whether the state directory sits on a filesystem
// where SQLite locking is undefined. The open path refuses outright; the
// report exists for the instance that chose the risk deliberately, and for
// the machine that drifted onto a mount after the fact.
func CheckFilesystem(dir string, probe MagicProber) Finding {
	const title = "filesystem"
	if probe == nil {
		return Finding{Level: OK, Title: title, Detail: "not known on this platform"}
	}
	magic, err := probe(dir)
	if err != nil {
		return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("could not probe: %v", err)}
	}
	if magic == 0 {
		return Finding{Level: OK, Title: title, Detail: "local disk"}
	}
	if !store.IsNetworkFSMagic(magic) {
		return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("local disk (magic %#x)", magic)}
	}
	return Finding{
		Level: Warn,
		Title: title,
		Detail: fmt.Sprintf("the state directory sits on a network filesystem (magic %#x): "+
			"SQLite's locking is undefined there and corruption arrives weeks later, quietly", magic),
		Next: []string{
			"move the state directory to a local disk",
			"--allow-network-fs exists for the deliberate risk; anything else is a mistake waiting",
		},
	}
}

// CheckCriticalInvariants is the fsck line in the report: the critical subset
// of the sweep, the one that refuses a startup. Doctor exits 1 on it, which
// is the whole point: the same fact that stops the daemon should stop the
// script that runs doctor.
func CheckCriticalInvariants(ctx context.Context, db DB) Finding {
	const title = "fsck"
	violations, err := db.QuickFsck(ctx)
	if err != nil {
		return Finding{
			Level:  Fail,
			Title:  title,
			Detail: fmt.Sprintf("the sweep failed: %v", err),
			Next:   []string{"paceq fsck  runs the full sweep and names every finding"},
		}
	}
	if len(violations) == 0 {
		return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%d invariants, no critical findings", len(store.Invariants))}
	}
	names := make([]string, 0, len(violations))
	for _, v := range violations {
		names = append(names, v.Check+" "+v.Subject)
	}
	return Finding{
		Level:  Fail,
		Title:  title,
		Detail: fmt.Sprintf("%d critical findings: %s", len(violations), strings.Join(names, ", ")),
		Next: []string{
			"startup is refused while these stand",
			"paceq fsck --json  keeps the evidence",
			"paceq fsck --repair --confirm  repairs what is safely repairable, after you confirm",
		},
	}
}

// readSystemTzdataVersion is the production reader: the zone database's own
// stamp, from the header of the compiled source or the release file beside it.
func readSystemTzdataVersion() (string, bool) {
	zoneinfo, err := os.OpenRoot("/usr/share/zoneinfo")
	if err != nil {
		return "", false
	}
	defer func() { _ = zoneinfo.Close() }()
	for _, name := range []string{"tzdata.zi", "+VERSION"} {
		raw, err := zoneinfo.ReadFile(name)
		if err != nil {
			continue
		}
		line := strings.TrimSpace(strings.SplitN(string(raw), "\n", 2)[0])
		line = strings.TrimPrefix(line, "# version ")
		if line != "" {
			return line, true
		}
	}
	return "", false
}

// readNTP is the production reader: systemd's answer, best effort. A machine
// without timedatectl gets "unknown", which the check reports as its own
// honest finding rather than pretending.
func readNTP() (synced, known bool) {
	out, err := exec.Command("timedatectl", // #nosec G204 - fixed argv, no input
		"show", "-p", "NTPSynchronized", "--value").Output()
	if err != nil {
		return false, false
	}
	return strings.TrimSpace(string(out)) == "yes", true
}

// scanProcs is the production lister: the same /proc walk the startup sweep
// runs, so doctor and reconciliation agree on what a live process is.
func scanProcs() ([]reconcile.Process, error) {
	return reconcile.ScanProcs()
}

// probeMagic is the production prober, the open path's own classification.
func probeMagic(dir string) (uint64, error) {
	return store.FSMagic(dir)
}
