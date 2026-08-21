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
		Open:  open,
		Zones: func(string) (*time.Location, error) { return time.UTC, nil },
		Local: "Europe/Oslo",
		Free:  func(string) (uint64, error) { return 40 << 30, nil },
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
		"state directory", "disk space", "database", "write lock",
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

// TestEveryFindingThatIsNotOKCarriesANextStep is the error anatomy rule applied
// to the report: a warning nobody can act on is a bug.
func TestEveryFindingThatIsNotOKCarriesANextStep(t *testing.T) {
	dir := stateDir(t)
	reports := map[string]doctor.Report{
		"missing state directory": doctor.Run(context.Background(), filepath.Join(t.TempDir(), ".paceq"), options(opening(nil, os.ErrNotExist))),
		"wide state directory":    doctor.Run(context.Background(), widened(t, stateDir(t), 0o755), options(opening(nil, nil))),
		"locked": doctor.Run(context.Background(), dir, options(opening(nil, &store.LockedError{
			Path:  filepath.Join(dir, "paceq.lock"),
			Owner: &store.Session{PID: 4711, Version: "0.1.0", StartedAt: time.Unix(0, 0).UTC()},
		}))),
		"unreadable database": doctor.Run(context.Background(), dir, options(opening(nil, errors.New("file is not a database")))),
		"old schema":          doctor.Run(context.Background(), dir, options(opening(&fakeDB{path: filepath.Join(dir, "state.db"), schema: 0, journal: "wal", mode: store.AutoVacuumIncremental}, nil))),
		"newer schema":        doctor.Run(context.Background(), dir, options(opening(&fakeDB{path: filepath.Join(dir, "state.db"), schema: 9999, journal: "wal", mode: store.AutoVacuumIncremental}, nil))),
		"not wal":             doctor.Run(context.Background(), dir, options(opening(&fakeDB{path: filepath.Join(dir, "state.db"), journal: "delete", mode: store.AutoVacuumIncremental}, nil))),
		"auto_vacuum none":    doctor.Run(context.Background(), dir, options(opening(&fakeDB{path: filepath.Join(dir, "state.db"), journal: "wal", mode: store.AutoVacuumNone}, nil))),
		"low disk":            doctor.Run(context.Background(), dir, lowDisk(opening(healthy(dir), nil))),
		"no tzdata":           doctor.Run(context.Background(), dir, noZones(opening(healthy(dir), nil))),
	}

	for name, report := range reports {
		t.Run(name, func(t *testing.T) {
			worst := doctor.OK
			for _, f := range report.Findings {
				if f.Level == doctor.OK {
					continue
				}
				if f.Level > worst {
					worst = f.Level
				}
				if len(f.Next) == 0 {
					t.Errorf("%s finding %q has no next step: %s", f.Level, f.Title, f.Detail)
				}
			}
			if worst == doctor.OK {
				t.Errorf("this state is not healthy, but every finding is ok: %v", titles(report))
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
