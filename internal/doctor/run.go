package doctor

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// DB is what a report asks a database. It is the store's surface narrowed to
// the four questions doctor answers from, so a test can reproduce a database in
// a state no test could create on disk, such as one written by a later paceq.
type DB interface {
	Path() string
	SchemaVersion(ctx context.Context) (int, error)
	JournalMode(ctx context.Context) (string, error)
	AutoVacuum(ctx context.Context) (store.AutoVacuumMode, error)
	Close() error
}

// Opener claims a state directory and opens the database in it. It never
// creates one: doctor reports on an installation, it does not make one.
type Opener func(ctx context.Context, dir string) (DB, error)

// ZoneLoader loads a named time zone.
type ZoneLoader func(name string) (*time.Location, error)

// FreeSpace is the space left on the filesystem holding dir, in bytes.
type FreeSpace func(dir string) (uint64, error)

// Options are the edges of a report: everything that reads the machine rather
// than the state directory. The zero value uses the real ones.
type Options struct {
	Open  Opener
	Zones ZoneLoader
	Free  FreeSpace
	// Local is the time zone name to report. Empty means the process one.
	Local string
}

// Report is one doctor run.
type Report struct {
	Findings []Finding
}

// Worst is the highest level in the report, and OK for an empty one. It is what
// an exit code is derived from.
func (r Report) Worst() Level {
	worst := OK
	for _, f := range r.Findings {
		if f.Level > worst {
			worst = f.Level
		}
	}
	return worst
}

func (r *Report) add(f Finding) { r.Findings = append(r.Findings, f) }

// probeZone is the zone a report loads to prove the time zone database is
// reachable. UTC would not do: the runtime answers for it without any database
// at all, so it would pass on a container that has none.
const probeZone = "Europe/Oslo"

// localTimeFile and zoneinfoDir are where a unix system records which zone is
// local: a symlink into the zone database, whose target names the zone.
const (
	localTimeFile = "/etc/localtime"
	zoneinfoDir   = "zoneinfo/"
)

// lowDisk is where free space becomes a warning. The database, its WAL and the
// job logs share the filesystem, and a write that runs out of room fails a run
// rather than degrading.
const lowDisk = 1 << 30

// Run performs every check against a state directory and reports what it found.
// It never repairs and never creates: a report is safe to run from a motd, from
// cron, and while another paceq holds the state.
func Run(ctx context.Context, dir string, opt Options) Report {
	opt = opt.withDefaults()
	dbPath := filepath.Join(dir, store.DatabaseFileName)

	var r Report
	dirFinding, dirState := checkStateDir(dir)
	r.add(dirFinding)
	r.add(checkDiskSpace(nearestExisting(dir), opt.Free))

	r.add(CheckSandbox())

	switch dirState {
	case stateMissing:
		// Nothing else can be answered, and every answer would repeat the one
		// next step there is.
	case stateWide:
		dbFinding, _ := checkDatabaseFile(dbPath)
		r.add(dbFinding)
		r.add(skipped("the state directory is readable by other users, and paceq "+
			"does not open state it would refuse to write to", dirFinding.Next))
	case stateReady:
		dbFinding, dbUsable := checkDatabaseFile(dbPath)
		r.add(dbFinding)
		if dbUsable {
			r.Findings = append(r.Findings, inspect(ctx, dir, opt.Open)...)
		} else if len(dbFinding.Next) > 0 {
			r.add(skipped("the database was not read", dbFinding.Next))
		}
	}

	r.add(checkTimeZone(opt.Local, opt.Zones))
	return r
}

func (o Options) withDefaults() Options {
	if o.Open == nil {
		o.Open = openState
	}
	if o.Zones == nil {
		o.Zones = time.LoadLocation
	}
	if o.Free == nil {
		o.Free = freeSpace
	}
	if o.Local == "" {
		o.Local = localZone()
	}
	return o
}

// openState is the production opener. The store takes the state lock first, so
// a report that gets past it is reading a database nobody else is writing.
func openState(ctx context.Context, dir string) (DB, error) {
	s, err := store.OpenState(ctx, dir, store.Options{})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// dirState is what the state directory turned out to be, which decides how much
// of the rest of the report can be answered at all.
type dirState int

const (
	stateMissing dirState = iota
	stateWide
	stateReady
)

func checkStateDir(dir string) (Finding, dirState) {
	info, err := os.Stat(dir)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Finding{
			Level:  Warn,
			Title:  "state directory",
			Detail: fmt.Sprintf("%s does not exist yet: this directory has no paceq state", dir),
			Next:   []string{"paceq init"},
		}, stateMissing
	case err != nil:
		return Finding{
			Level:  Fail,
			Title:  "state directory",
			Detail: fmt.Sprintf("could not read %s: %v", dir, err),
			Next:   []string{fmt.Sprintf("check that the path exists and is yours: ls -ld %s", dir)},
		}, stateMissing
	case !info.IsDir():
		return Finding{
			Level:  Fail,
			Title:  "state directory",
			Detail: fmt.Sprintf("%s is not a directory", dir),
			Next:   []string{fmt.Sprintf("move it aside and run paceq init: mv %s %s.bak", dir, dir)},
		}, stateMissing
	}

	if f, ok := modeFinding("state directory", dir, info.Mode().Perm(), store.DirMode); !ok {
		return f, stateWide
	}
	return Finding{
		Level:  OK,
		Title:  "state directory",
		Detail: fmt.Sprintf("%s (%#o)", dir, info.Mode().Perm()),
	}, stateReady
}

// checkDatabaseFile reports on the file itself: that it is there, that it is
// private, and how big it has grown. Its contents are a separate question,
// answered only when this one says the file may be opened.
func checkDatabaseFile(path string) (Finding, bool) {
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return Finding{
			Level:  Warn,
			Title:  "database",
			Detail: fmt.Sprintf("%s does not exist yet", path),
			Next:   []string{"paceq init"},
		}, false
	case err != nil:
		return Finding{
			Level:  Fail,
			Title:  "database",
			Detail: fmt.Sprintf("could not read %s: %v", path, err),
			Next:   []string{fmt.Sprintf("check the path: ls -l %s", path)},
		}, false
	}

	if f, ok := modeFinding("database", path, info.Mode().Perm(), store.DatabaseMode); !ok {
		return f, false
	}
	return Finding{
		Level:  OK,
		Title:  "database",
		Detail: fmt.Sprintf("%s (%#o, %s)", path, info.Mode().Perm(), byteText(fileSize(info))),
	}, true
}

// modeFinding is the fail closed rule as a finding. doctor warns where a
// command that writes refuses, because an installation that has been exposed
// for weeks is not fixed by a tool that will not start.
func modeFinding(title, path string, got, want fs.FileMode) (Finding, bool) {
	if got&^want == 0 {
		return Finding{}, true
	}
	return Finding{
		Level: Warn,
		Title: title,
		Detail: fmt.Sprintf("%s has mode %#o, paceq requires %#o: other users can read the state, "+
			"and every command that writes refuses until that is fixed", path, got, want),
		Next: []string{fmt.Sprintf("chmod %#o %s", want, path)},
	}, false
}

// skipped names the checks a report could not reach and why. A silently shorter
// report reads as a healthy one.
func skipped(reason string, next []string) Finding {
	return Finding{
		Level:  Warn,
		Title:  "database contents",
		Detail: "not inspected: " + reason,
		Next:   next,
	}
}

// inspect opens the database and asks it the questions only it can answer. The
// state lock decides whether that is possible at all, so the answer to "who
// holds the lock" comes out of the same attempt.
func inspect(ctx context.Context, dir string, open Opener) []Finding {
	db, err := open(ctx, dir)
	if err != nil {
		var locked *store.LockedError
		if errors.As(err, &locked) {
			return []Finding{
				lockHeld(locked),
				skipped("another process holds the write lock", []string{
					"wait for it to finish and run paceq doctor again",
				}),
			}
		}
		var perm *store.PermissionError
		if errors.As(err, &perm) {
			return []Finding{skipped(perm.Error(), []string{
				fmt.Sprintf("chmod %#o %s", perm.Want, perm.Path),
			})}
		}
		return []Finding{{
			Level:  Fail,
			Title:  "database contents",
			Detail: fmt.Sprintf("could not open the state in %s: %v", dir, err),
			Next: []string{
				fmt.Sprintf("check that the file is a paceq database: sqlite3 %s \"PRAGMA integrity_check\"",
					filepath.Join(dir, store.DatabaseFileName)),
				"restore the last backup of the state directory",
				fmt.Sprintf("or start over: mv %s %s.bak, then paceq init", dir, dir),
			},
		}}
	}
	defer func() { _ = db.Close() }()

	return []Finding{
		{Level: OK, Title: "write lock", Detail: "free"},
		checkJournalMode(ctx, db),
		checkSchemaVersion(ctx, db),
		CheckAutoVacuum(ctx, db),
	}
}

// lockHeld is a warning, never a failure: another paceq running is the normal
// state of a machine that uses paceq, not a broken installation.
func lockHeld(locked *store.LockedError) Finding {
	detail := fmt.Sprintf("held on %s by a process that no session row names", locked.Path)
	next := []string{
		fmt.Sprintf("find the holder: fuser %s", locked.Path),
	}
	if owner := locked.Owner; owner != nil {
		detail = fmt.Sprintf("held by pid %d (paceq %s, started %s)",
			owner.PID, owner.Version, owner.StartedAt.Format(time.RFC3339))
		next = []string{
			"wait for that process to finish, or stop it: " +
				fmt.Sprintf("systemctl stop paceq, or kill %d", owner.PID),
		}
	}
	return Finding{Level: Warn, Title: "write lock", Detail: detail, Next: next}
}

func checkJournalMode(ctx context.Context, db DB) Finding {
	mode, err := db.JournalMode(ctx)
	if err != nil {
		return Finding{
			Level:  Fail,
			Title:  "journal mode",
			Detail: fmt.Sprintf("could not read the setting: %v", err),
			Next:   []string{"run paceq doctor again, and check the disk for I/O errors"},
		}
	}
	if !strings.EqualFold(mode, "wal") {
		return Finding{
			Level: Fail,
			Title: "journal mode",
			Detail: fmt.Sprintf("the database is in %s, paceq needs wal: readers and the writer "+
				"would block each other in any other mode", mode),
			Next: []string{
				fmt.Sprintf("sqlite3 %s \"PRAGMA journal_mode = WAL\"", db.Path()),
				"nothing else may hold the database while the mode is changed",
			},
		}
	}
	return Finding{Level: OK, Title: "journal mode", Detail: mode}
}

func checkSchemaVersion(ctx context.Context, db DB) Finding {
	const title = "schema version"

	found, err := db.SchemaVersion(ctx)
	if err != nil {
		return Finding{
			Level:  Fail,
			Title:  title,
			Detail: fmt.Sprintf("could not read the version: %v", err),
			Next:   []string{"run paceq doctor again, and check the disk for I/O errors"},
		}
	}
	known, err := store.KnownSchemaVersion()
	if err != nil {
		return Finding{
			Level:  Fail,
			Title:  title,
			Detail: fmt.Sprintf("could not read the version this build carries: %v", err),
			Next:   []string{"this is a bug in the build, report it with paceq version"},
		}
	}

	switch {
	case found > known:
		return Finding{
			Level: Fail,
			Title: title,
			Detail: fmt.Sprintf("the database is at schema %d, this build carries %d: it was "+
				"written by a newer paceq, and writing to it would corrupt rows that version defines "+
				"differently", found, known),
			Next: []string{
				"upgrade paceq to the version that wrote this database",
				"or restore the backup taken before that upgrade",
			},
		}
	case found < known:
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("the database is at schema %d, this build carries %d: the missing "+
				"migrations are applied the next time paceq opens this state for writing", found, known),
			Next: []string{
				"back up the state directory first: cp -a the directory somewhere else",
				"then start paceq normally, which applies them",
			},
		}
	default:
		return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%d (this build carries %d)", found, known)}
	}
}

func checkDiskSpace(dir string, free FreeSpace) Finding {
	const title = "disk space"

	bytes, err := free(dir)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: fmt.Sprintf("could not read the free space on %s: %v", dir, err),
			Next:   []string{fmt.Sprintf("check that the path is on a mounted filesystem: df -h %s", dir)},
		}
	}
	if bytes < lowDisk {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("%s free on %s: the database, its write ahead log and the job logs "+
				"share this filesystem", byteText(bytes), dir),
			Next: []string{
				fmt.Sprintf("free space on that filesystem: df -h %s", dir),
				"or keep the state somewhere bigger: paceq --db /other/path/state.db",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%s free on %s", byteText(bytes), dir)}
}

// checkTimeZone proves the zone database is reachable, not merely that the
// process has a local zone. A schedule carries a named zone, and a binary that
// cannot load one runs jobs in the wrong hour without ever failing.
func checkTimeZone(local string, zones ZoneLoader) Finding {
	const title = "time zone"

	if _, err := zones(probeZone); err != nil {
		return Finding{
			Level: Warn,
			Title: title,
			Detail: fmt.Sprintf("local zone %s, but the time zone database is not readable: %v: "+
				"schedules in a named zone cannot be resolved", local, err),
			Next: []string{
				"install the zone database: apt install tzdata, or apk add tzdata",
				"or use a paceq built with -tags timetzdata, which carries its own copy",
			},
		}
	}
	return Finding{Level: OK, Title: title, Detail: fmt.Sprintf("%s (zone database found)", local)}
}

// localZone is the zone name schedules without an explicit zone will run in.
// The runtime calls it "Local" when it loaded the zone from a file rather than
// from a name, which is the usual case on Linux, so the name is recovered from
// the two places that hold it. A report that says "Local" answers nothing.
func localZone() string {
	if name := time.Local.String(); name != "Local" {
		return name
	}
	if tz := os.Getenv("TZ"); tz != "" {
		return strings.TrimPrefix(tz, ":")
	}
	if target, err := os.Readlink(localTimeFile); err == nil {
		if _, name, found := strings.Cut(target, zoneinfoDir); found {
			return name
		}
	}
	return "Local"
}

// nearestExisting walks up until it finds a directory that exists, so free
// space can be reported for a state directory paceq init has not created yet.
func nearestExisting(dir string) string {
	for {
		if _, err := os.Stat(dir); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return dir
		}
		dir = parent
	}
}
