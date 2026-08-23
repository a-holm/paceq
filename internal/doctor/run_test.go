package doctor_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/store"
)

// fakeDB answers the four questions doctor asks a database, so every branch of
// the report can be reproduced without a file in that state.
type fakeDB struct {
	path    string
	schema  int
	journal string
	mode    store.AutoVacuumMode
	err     error
	closed  bool
}

func (f *fakeDB) Path() string { return f.path }

func (f *fakeDB) SchemaVersion(context.Context) (int, error) { return f.schema, f.err }

func (f *fakeDB) JournalMode(context.Context) (string, error) { return f.journal, f.err }

func (f *fakeDB) AutoVacuum(context.Context) (store.AutoVacuumMode, error) {
	return f.mode, f.err
}

func (f *fakeDB) Close() error {
	f.closed = true
	return nil
}

// healthy is a database in the state paceq creates.
func healthy(dir string) *fakeDB {
	known, err := store.KnownSchemaVersion()
	if err != nil {
		panic(err)
	}
	return &fakeDB{
		path:    filepath.Join(dir, "state.db"),
		schema:  known,
		journal: "wal",
		mode:    store.AutoVacuumIncremental,
	}
}

// stateDir is a state directory with the mode and the database file paceq
// creates, without opening SQLite.
func stateDir(t *testing.T) string {
	t.Helper()

	dir := filepath.Join(t.TempDir(), ".paceq")
	if err := os.Mkdir(dir, store.DirMode); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.db"), []byte("SQLite format 3\x00"), store.DatabaseMode); err != nil {
		t.Fatalf("write the database file: %v", err)
	}
	return dir
}

func opening(db doctor.DB, err error) doctor.Opener {
	return func(context.Context, string) (doctor.DB, error) {
		if err != nil {
			return nil, err
		}
		return db, nil
	}
}

// options carries the injectable edges with values that are healthy unless a
// test changes one, so each test states only what it is about.
func options(open doctor.Opener) doctor.Options {
	return doctor.Options{
		Open:   open,
		Zones:  func(string) (*time.Location, error) { return time.UTC, nil },
		Status: sandboxedStatus(),
		Local:  "Europe/Oslo",
		Free:   func(string) (uint64, error) { return 40 << 30, nil },
	}
}

// sandboxedStatus is a process running under the hardened systemd unit: the
// state doctor approves of on a machine that sandboxes the daemon.
func sandboxedStatus() doctor.StatusReader {
	return func() (doctor.ProcessStatus, error) {
		return doctor.ProcessStatus{
			NoNewPrivs: 1,
			Seccomp:    2,
			CapEff:     "0000000000000000",
		}, nil
	}
}

// find is the finding with that title, and a failure when the report has none.
func find(t *testing.T, r doctor.Report, title string) doctor.Finding {
	t.Helper()

	for _, f := range r.Findings {
		if f.Title == title {
			return f
		}
	}
	t.Fatalf("report has no %q finding, only %v", title, titles(r))
	return doctor.Finding{}
}

func absent(t *testing.T, r doctor.Report, title string) {
	t.Helper()

	for _, f := range r.Findings {
		if f.Title == title {
			t.Fatalf("report has a %q finding, which cannot be answered here: %s", title, f.Detail)
		}
	}
}

func titles(r doctor.Report) []string {
	var out []string
	for _, f := range r.Findings {
		out = append(out, f.Title)
	}
	return out
}

// TestHealthyInstallationHasNoFailures is the state paceq init leaves behind.
func TestHealthyInstallationHasNoFailures(t *testing.T) {
	dir := stateDir(t)
	db := healthy(dir)

	report := doctor.Run(context.Background(), dir, options(opening(db, nil)))

	if report.Worst() != doctor.OK {
		for _, f := range report.Findings {
			if f.Level != doctor.OK {
				t.Errorf("%s: %s: %s", f.Level, f.Title, f.Detail)
			}
		}
	}
	for _, want := range []string{
		"state directory", "disk space", "sandbox", "database", "write lock",
		"journal mode", "schema version", "auto_vacuum", "time zone",
	} {
		f := find(t, report, want)
		if f.Detail == "" {
			t.Errorf("%s reports no detail", want)
		}
	}
	if !db.closed {
		t.Error("Run left the database open")
	}
	if got := find(t, report, "state directory").Detail; !strings.Contains(got, dir) {
		t.Errorf("state directory detail %q does not name %s", got, dir)
	}
	if got := find(t, report, "database").Detail; !strings.Contains(got, db.Path()) {
		t.Errorf("database detail %q does not name %s", got, db.Path())
	}
}

// broken is the database paceq creates with one answer changed, so a case
// fails for the reason it is named after and nothing else. A fake left at
// schema 0 would warn about the schema in every case and hide whatever the
// case is really about.
func broken(dir string, change func(*fakeDB)) *fakeDB {
	db := healthy(dir)
	change(db)
	return db
}

// TestEveryFindingIsTheOneTheStateCallsFor is the error anatomy rule applied to
// the report, and the check that each state produces its own finding: a warning
// nobody can act on is a bug, and so is a broken check that stays quiet because
// another finding happens to carry the level.
func TestEveryFindingIsTheOneTheStateCallsFor(t *testing.T) {
	dir := stateDir(t)
	cases := []struct {
		name   string
		report doctor.Report
		title  string
		level  doctor.Level
	}{
		{
			name:   "missing state directory",
			report: doctor.Run(context.Background(), filepath.Join(t.TempDir(), ".paceq"), options(opening(nil, os.ErrNotExist))),
			title:  "state directory",
			level:  doctor.Warn,
		},
		{
			name:   "wide state directory",
			report: doctor.Run(context.Background(), widened(t, stateDir(t), 0o755), options(opening(nil, nil))),
			title:  "state directory",
			level:  doctor.Warn,
		},
		{
			name: "locked",
			report: doctor.Run(context.Background(), dir, options(opening(nil, &store.LockedError{
				Path:  filepath.Join(dir, "paceq.lock"),
				Owner: &store.Session{PID: 4711, Version: "0.1.0", StartedAt: time.Unix(0, 0).UTC()},
			}))),
			title: "write lock",
			level: doctor.Warn,
		},
		{
			name:   "unreadable database",
			report: doctor.Run(context.Background(), dir, options(opening(nil, errors.New("file is not a database")))),
			title:  "database contents",
			level:  doctor.Fail,
		},
		{
			name:   "old schema",
			report: doctor.Run(context.Background(), dir, options(opening(broken(dir, func(db *fakeDB) { db.schema = 0 }), nil))),
			title:  "schema version",
			level:  doctor.Warn,
		},
		{
			name:   "newer schema",
			report: doctor.Run(context.Background(), dir, options(opening(broken(dir, func(db *fakeDB) { db.schema = 9999 }), nil))),
			title:  "schema version",
			level:  doctor.Fail,
		},
		{
			name:   "not wal",
			report: doctor.Run(context.Background(), dir, options(opening(broken(dir, func(db *fakeDB) { db.journal = "delete" }), nil))),
			title:  "journal mode",
			level:  doctor.Fail,
		},
		{
			name:   "auto_vacuum none",
			report: doctor.Run(context.Background(), dir, options(opening(broken(dir, func(db *fakeDB) { db.mode = store.AutoVacuumNone }), nil))),
			title:  "auto_vacuum",
			level:  doctor.Warn,
		},
		{
			name:   "low disk",
			report: doctor.Run(context.Background(), dir, lowDisk(opening(healthy(dir), nil))),
			title:  "disk space",
			level:  doctor.Warn,
		},
		{
			name:   "no tzdata",
			report: doctor.Run(context.Background(), dir, noZones(opening(healthy(dir), nil))),
			title:  "time zone",
			level:  doctor.Warn,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			found := find(t, c.report, c.title)
			if found.Level != c.level {
				t.Errorf("%s is %s, want %s: %s", c.title, found.Level, c.level, found.Detail)
			}
			for _, f := range c.report.Findings {
				if f.Level != doctor.OK && len(f.Next) == 0 {
					t.Errorf("%s finding %q has no next step: %s", f.Level, f.Title, f.Detail)
				}
			}
		})
	}
}

func widened(t *testing.T, dir string, mode os.FileMode) string {
	t.Helper()

	if err := os.Chmod(dir, mode); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	return dir
}

func lowDisk(open doctor.Opener) doctor.Options {
	opt := options(open)
	opt.Free = func(string) (uint64, error) { return 64 << 20, nil }
	return opt
}

func noZones(open doctor.Opener) doctor.Options {
	opt := options(open)
	opt.Zones = func(name string) (*time.Location, error) {
		return nil, fmt.Errorf("open %s: no such file or directory", name)
	}
	return opt
}

// TestMissingStateDirectoryIsAWarningNotAFailure keeps doctor usable in a motd:
// a machine that has never run paceq init is not broken.
func TestMissingStateDirectoryIsAWarningNotAFailure(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".paceq")

	report := doctor.Run(context.Background(), dir, options(opening(nil, os.ErrNotExist)))

	if report.Worst() != doctor.Warn {
		t.Errorf("worst level is %s, want %s", report.Worst(), doctor.Warn)
	}
	if got := find(t, report, "state directory"); !strings.Contains(strings.Join(got.Next, " "), "paceq init") {
		t.Errorf("the finding does not point at paceq init: %v", got.Next)
	}
	absent(t, report, "journal mode")
	absent(t, report, "write lock")
}

// TestWidePermissionsWarnAndStopTheInspection is the split from 08 section 3.9:
// a command that writes refuses, doctor warns. The database is deliberately not
// opened, because opening it is the refusal doctor exists to explain.
func TestWidePermissionsWarnAndStopTheInspection(t *testing.T) {
	dir := widened(t, stateDir(t), 0o755)
	opened := false
	opt := options(func(context.Context, string) (doctor.DB, error) {
		opened = true
		return healthy(dir), nil
	})

	report := doctor.Run(context.Background(), dir, opt)

	if opened {
		t.Error("doctor opened a state directory other users can read")
	}
	if report.Worst() != doctor.Warn {
		t.Errorf("worst level is %s, want %s", report.Worst(), doctor.Warn)
	}
	found := find(t, report, "state directory")
	if !strings.Contains(found.Detail, "0755") || !strings.Contains(found.Detail, "0700") {
		t.Errorf("detail %q does not name both the mode found and the mode required", found.Detail)
	}
	if !strings.Contains(strings.Join(found.Next, " "), "chmod 0700") {
		t.Errorf("next steps %v do not offer the command that fixes it", found.Next)
	}
}

// TestWideDatabaseWarns covers the file rather than the directory, which is the
// case a backup or a restore produces.
func TestWideDatabaseWarns(t *testing.T) {
	dir := stateDir(t)
	dbPath := filepath.Join(dir, "state.db")
	if err := os.Chmod(dbPath, 0o644); err != nil {
		t.Fatalf("chmod %s: %v", dbPath, err)
	}

	report := doctor.Run(context.Background(), dir, options(opening(healthy(dir), nil)))

	found := find(t, report, "database")
	if found.Level != doctor.Warn {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Warn, found.Detail)
	}
	if !strings.Contains(found.Detail, "0644") {
		t.Errorf("detail %q does not name the mode found", found.Detail)
	}
	if !strings.Contains(strings.Join(found.Next, " "), "chmod 0600") {
		t.Errorf("next steps %v do not offer the command that fixes it", found.Next)
	}
}

// TestLockedStateNamesTheHolder is what turns "doctor says nothing" into an
// answer while a daemon or another command holds the state.
func TestLockedStateNamesTheHolder(t *testing.T) {
	dir := stateDir(t)
	locked := &store.LockedError{
		Path:  filepath.Join(dir, "paceq.lock"),
		Owner: &store.Session{PID: 4711, Version: "0.1.0", StartedAt: time.Unix(0, 0).UTC()},
	}

	report := doctor.Run(context.Background(), dir, options(opening(nil, locked)))

	found := find(t, report, "write lock")
	if found.Level != doctor.Warn {
		t.Errorf("level is %s, want %s", found.Level, doctor.Warn)
	}
	if !strings.Contains(found.Detail, "4711") {
		t.Errorf("detail %q does not name the holder", found.Detail)
	}
	if report.Worst() != doctor.Warn {
		t.Errorf("worst level is %s, want %s: another paceq running is not a broken installation", report.Worst(), doctor.Warn)
	}
	absent(t, report, "schema version")
}

// TestUnreadableDatabaseFails separates a state paceq cannot use from a state
// that merely needs attention. A file that is not a database is the case the
// report has to survive without panicking.
func TestUnreadableDatabaseFails(t *testing.T) {
	dir := stateDir(t)

	report := doctor.Run(context.Background(), dir, options(opening(nil, errors.New("file is not a database"))))

	if report.Worst() != doctor.Fail {
		t.Errorf("worst level is %s, want %s", report.Worst(), doctor.Fail)
	}
	joined := strings.Join(titles(report), " ")
	if !strings.Contains(joined, "database contents") {
		t.Errorf("report does not say the contents went unread: %v", titles(report))
	}
	found := find(t, report, "database contents")
	if !strings.Contains(found.Detail, "file is not a database") {
		t.Errorf("detail %q loses the underlying error", found.Detail)
	}
}

// TestNewerSchemaFails is the fence: a database written by a later paceq is not
// something this build may write to.
func TestNewerSchemaFails(t *testing.T) {
	dir := stateDir(t)
	db := healthy(dir)
	db.schema = 9999

	report := doctor.Run(context.Background(), dir, options(opening(db, nil)))

	found := find(t, report, "schema version")
	if found.Level != doctor.Fail {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Fail, found.Detail)
	}
	if !strings.Contains(found.Detail, "9999") {
		t.Errorf("detail %q does not name the version found", found.Detail)
	}
}

// TestNonWalJournalModeFails is the check that keeps the concurrency promise
// honest: in any other journal mode a reader and the writer block each other,
// so a database that is not in WAL is a broken installation rather than a
// preference.
func TestNonWalJournalModeFails(t *testing.T) {
	dir := stateDir(t)
	db := healthy(dir)
	db.journal = "delete"

	report := doctor.Run(context.Background(), dir, options(opening(db, nil)))

	found := find(t, report, "journal mode")
	if found.Level != doctor.Fail {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Fail, found.Detail)
	}
	if !strings.Contains(found.Detail, "delete") {
		t.Errorf("detail %q does not name the mode found", found.Detail)
	}
	if !strings.Contains(strings.Join(found.Next, " "), "journal_mode = WAL") {
		t.Errorf("next steps %v do not offer the command that fixes it", found.Next)
	}
	if report.Worst() != doctor.Fail {
		t.Errorf("worst level is %s, want %s", report.Worst(), doctor.Fail)
	}
}

// TestDiskSpaceWarnsBeforeItIsTooLate. A database that cannot extend its WAL
// fails writes, so the warning has to arrive while there is room to act.
func TestDiskSpaceWarnsBeforeItIsTooLate(t *testing.T) {
	dir := stateDir(t)

	report := doctor.Run(context.Background(), dir, lowDisk(opening(healthy(dir), nil)))

	found := find(t, report, "disk space")
	if found.Level != doctor.Warn {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Warn, found.Detail)
	}
	if !strings.Contains(found.Detail, "MB") {
		t.Errorf("detail %q does not report the free space in units a human reads", found.Detail)
	}
}

// TestTimeZoneWithoutTzdataWarns. Schedules carry a zone, and a binary that
// cannot load one silently runs jobs in the wrong hour.
func TestTimeZoneWithoutTzdataWarns(t *testing.T) {
	dir := stateDir(t)

	report := doctor.Run(context.Background(), dir, noZones(opening(healthy(dir), nil)))

	found := find(t, report, "time zone")
	if found.Level != doctor.Warn {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Warn, found.Detail)
	}
	if !strings.Contains(strings.Join(found.Next, " "), "tzdata") {
		t.Errorf("next steps %v do not say what is missing", found.Next)
	}
}

// unrestricted is a process running with no sandbox at all: NoNewPrivs off and
// no seccomp filter, which is what a paceq started without the hardened systemd
// unit looks like. Doctor must warn about it and say how to fix it, not report
// the sandbox as OK.
func unrestricted() doctor.StatusReader {
	return func() (doctor.ProcessStatus, error) {
		return doctor.ProcessStatus{
			NoNewPrivs: 0,
			Seccomp:    0,
			CapEff:     "0000000000000000",
		}, nil
	}
}

// TestUnrestrictedSandboxWarnsAndNamesTheRemedy pins the doctor sandbox check
// to the behaviour an operator actually needs: running without the hardened
// unit is an attention-level problem with a named fix, never an OK. The mutant
// this test kills is the one that would let an unrestricted run report OK.
func TestUnrestrictedSandboxWarnsAndNamesTheRemedy(t *testing.T) {
	dir := stateDir(t)
	opt := options(opening(healthy(dir), nil))
	opt.Status = unrestricted()

	report := doctor.Run(context.Background(), dir, opt)

	found := find(t, report, "sandbox")
	if found.Level != doctor.Warn && found.Level != doctor.Fail {
		t.Fatalf("an unrestricted sandbox reported %s, want %s: %s",
			found.Level, doctor.Warn, found.Detail)
	}
	next := strings.Join(found.Next, " ")
	if !strings.Contains(found.Detail, "hardened") &&
		!strings.Contains(next, "install-service") {
		t.Fatalf("the warning does not name the remedy (hardened unit, paceq install-service):\n%s\nsee: %v",
			found.Detail, found.Next)
	}
	if !strings.Contains(next, "install-service") {
		t.Errorf("the next step does not say install-service: %v", found.Next)
	}
}

// TestHardenedSandboxStaysOK pins the healthy side: a process under the
// hardened unit (NoNewPrivs on, seccomp filter) passes, so doctor does not
// nag a correctly deployed daemon.
func TestHardenedSandboxStaysOK(t *testing.T) {
	dir := stateDir(t)

	report := doctor.Run(context.Background(), dir, options(opening(healthy(dir), nil)))

	found := find(t, report, "sandbox")
	if found.Level != doctor.OK {
		t.Errorf("a hardened daemon reported sandbox %s, want %s: %s",
			found.Level, doctor.OK, found.Detail)
	}
}

// TestRunAgainstARealStateDirectory wires the defaults: the store opens the
// database, the disk is read through the kernel and the zone through the
// runtime. Without it every branch above would be proven against fakes only.
func TestRunAgainstARealStateDirectory(t *testing.T) {
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), ".paceq")
	if err := os.Mkdir(dir, store.DirMode); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}

	s, err := store.OpenState(ctx, dir, store.Options{})
	if err != nil {
		t.Fatalf("open the state directory: %v", err)
	}
	if err := s.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	report := doctor.Run(ctx, dir, doctor.Options{})

	for _, f := range report.Findings {
		if f.Level == doctor.Fail {
			t.Errorf("%s: %s\n%s", f.Title, f.Detail, strings.Join(f.Next, "\n"))
		}
	}
	known, err := store.KnownSchemaVersion()
	if err != nil {
		t.Fatalf("read the known schema version: %v", err)
	}
	if got := find(t, report, "schema version").Detail; !strings.Contains(got, fmt.Sprint(known)) {
		t.Errorf("schema version detail %q does not name version %d", got, known)
	}
	if got := find(t, report, "journal mode").Detail; !strings.Contains(strings.ToLower(got), "wal") {
		t.Errorf("journal mode detail %q does not say wal", got)
	}
	if got := find(t, report, "write lock"); got.Level != doctor.OK {
		t.Errorf("write lock is %s, want %s: %s", got.Level, doctor.OK, got.Detail)
	}
}

// TestMissingDatabaseIsNotCreated is the rule that separates doctor from every
// other command: a report never materialises the state it reports on.
func TestMissingDatabaseIsNotCreated(t *testing.T) {
	dir := filepath.Join(t.TempDir(), ".paceq")
	if err := os.Mkdir(dir, store.DirMode); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	opt := options(func(context.Context, string) (doctor.DB, error) {
		t.Error("doctor opened a state directory that has no database, which would create one")
		return nil, errors.New("must not be called")
	})

	report := doctor.Run(context.Background(), dir, opt)

	found := find(t, report, "database")
	if found.Level != doctor.Warn {
		t.Errorf("level is %s, want %s: %s", found.Level, doctor.Warn, found.Detail)
	}
	if !strings.Contains(strings.Join(found.Next, " "), "paceq init") {
		t.Errorf("next steps %v do not point at paceq init", found.Next)
	}
	if _, err := os.Stat(filepath.Join(dir, store.DatabaseFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("doctor created a database: %v", err)
	}
	absent(t, report, "schema version")
}

// TestLocalZoneIsNamed. A report that says "Local" answers nothing, and the
// zone a schedule without an explicit one runs in is the fact an operator is
// checking for.
func TestLocalZoneIsNamed(t *testing.T) {
	t.Setenv("TZ", "Europe/Oslo")
	dir := stateDir(t)
	opt := options(opening(healthy(dir), nil))
	opt.Local = ""

	report := doctor.Run(context.Background(), dir, opt)

	found := find(t, report, "time zone")
	if !strings.Contains(found.Detail, "Europe/Oslo") {
		t.Errorf("detail %q does not name the local zone", found.Detail)
	}
}
