package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/cutover"
	"github.com/a-holm/paceq/internal/explain"
	"github.com/a-holm/paceq/internal/store"
)

// cutover (#35 / M6-03) is the migration's last step: commenting the
// imported lines out of the crontab, with a way back. The crontab is the
// one file in the whole product paceq writes to outside its own state
// directory, so the discipline here is stricter than the size of the
// command suggests: a backup before every change, markers that carry the
// rollback, lines that never get deleted, and an audit row for every line
// touched. 09 section 5.3 is binding: the user is never left in a state
// she cannot undo in one minute, and nothing here ever runs as part of
// import - cutover is always a separate, deliberate act.
//
// The command never calls crontab(1) except through the argv arrays in
// internal/cutover, and never writes the spool directly.

type cutoverFlags struct {
	dryRun   bool
	jobs     []string
	user     string
	file     string
	force    bool
	rollback bool
	from     string
	status   bool
}

func newCutoverCmd(env Env, g *globals) *cobra.Command {
	var f cutoverFlags
	cmd := &cobra.Command{
		Use:   "cutover",
		Short: "Comment imported jobs out of the crontab, with a way back",
		Long: `The migration's last step (M6-03): after import and shadow mode,
comment the crontab lines that paceq now owns out of the crontab - and be
able to undo it in one minute.

The line is never deleted. Every commented-out line gets a marker above it
that names the paceq job and the instant of the cutover, and the original
line stands verbatim behind one '#', so a rollback is removing one
character, not re-parsing:

  # pulseq:cutover 2027-01-12T09:14:03+01:00 job=backup-db
  #0 3 * * * /opt/backup/dump.sh >> /var/log/backup.log 2>&1

Before any change, the whole crontab is copied to a backup file that is
never overwritten; every run leaves its own backup behind. --rollback puts
the lines back; --from restores a named backup wholesale. --status reports
what is cut over, what still runs under cron, and which backups exist.

A job without a single successful run in paceq is skipped unless --force
says otherwise (PSQ-CUT-001), a line that changed since the import is
never touched (PSQ-CUT-003), and a job whose shadow report shows
unresolved deviations is cut over with a warning. Import never cuts over;
this command is always the separate, deliberate act.

The source: your own crontab by default, --user for another user's, --file
for a crontab-format file. A file is written back in place after its own
backup, which is the only mode allowed to touch /etc/crontab or
/etc/cron.d - system files usually belong to configuration management, and
paceq warns before writing one.

Exit codes: 0 ok (including "nothing to do"), 3 unknown job or missing
source, 1 when the crontab could not be written back.`,
		Example: `  paceq cutover --dry-run            show the diff, write nothing
  paceq cutover                      cut over every ready imported job
  paceq cutover --job backup-db      cut over one job
  paceq cutover --status             what is cut over, what remains
  paceq cutover --rollback           put every marked line back
  paceq cutover --rollback --from .paceq/crontab.backup.2027-01-12T09-14-03`,
		Args:              noArgs,
		DisableAutoGenTag: true,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			switch {
			case f.status:
				return runCutoverStatus(ctx, env, g, out, f)
			case f.rollback:
				return runCutoverRollback(ctx, env, g, out, f)
			default:
				return runCutoverApply(ctx, env, g, out, f)
			}
		}),
	}

	cmd.Flags().BoolVar(&f.dryRun, "dry-run", false, "show exactly what would change, write nothing")
	cmd.Flags().StringArrayVar(&f.jobs, "job", nil, "limit to this job (repeatable)")
	cmd.Flags().StringVar(&f.user, "user", "", "act on this user's crontab instead of your own")
	cmd.Flags().StringVar(&f.file, "file", "", "act on this crontab-format file instead of the spool")
	cmd.Flags().BoolVar(&f.force, "force", false, "cut over jobs without a successful paceq run")
	cmd.Flags().BoolVar(&f.rollback, "rollback", false, "remove the cutover markers and restore the lines")
	cmd.Flags().StringVar(&f.from, "from", "", "with --rollback: restore this backup file instead of uncommenting")
	cmd.Flags().BoolVar(&f.status, "status", false, "report what is cut over, what remains, which backups exist")

	cmd.MarkFlagsMutuallyExclusive("dry-run", "rollback")
	cmd.MarkFlagsMutuallyExclusive("dry-run", "status")
	cmd.MarkFlagsMutuallyExclusive("dry-run", "from")
	cmd.MarkFlagsMutuallyExclusive("dry-run", "force")
	cmd.MarkFlagsMutuallyExclusive("status", "rollback")
	cmd.MarkFlagsMutuallyExclusive("status", "from")
	cmd.MarkFlagsMutuallyExclusive("status", "force")
	cmd.MarkFlagsMutuallyExclusive("rollback", "force")
	cmd.MarkFlagsMutuallyExclusive("file", "user")
	return cmd
}

// cutoverJob is one job eligible for cutover work, with the crontab line
// its import recorded.
type cutoverJob struct {
	name   string
	origin string
}

// cutoverSkip is one job the command left alone. It extends the pure
// package's skips with the two CLI-side reasons: the fence (no successful
// run yet) and a job with no recorded origin at all.
type cutoverSkip struct {
	job        string
	code       string
	reason     string
	line       string
	lineNumber int
}

// cutoverSource is the crontab as this run read it.
type cutoverSource struct {
	label    string // what messages call it
	user     string // crontab(1) mode: the -u argument, "" for self
	file     string // file mode: the path
	content  string
	writable func(content string) error // how the transformed content is written back
}

// readCutoverSource resolves --user and --file into the crontab to work
// on, without writing anything. Reading another user's crontab needs root,
// which crontab(1) itself enforces; the error carries its answer.
func readCutoverSource(ctx context.Context, env Env, f cutoverFlags) (cutoverSource, error) {
	switch {
	case f.file != "":
		data, err := os.ReadFile(f.file) // #nosec G304 - the path is what the operator asked paceq to cut over
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return cutoverSource{}, notFoundError("no crontab file at "+f.file, "",
					"check the path, or cut over your own crontab instead:",
					"    paceq cutover")
			}
			return cutoverSource{}, validationError("could not read "+f.file, err,
				"check the permissions on the file")
		}
		return cutoverSource{
			label:   f.file,
			file:    f.file,
			content: string(data),
			writable: func(content string) error {
				return writeCrontabFile(f.file, content)
			},
		}, nil

	default:
		content, err := cutover.Read(ctx, f.user)
		switch {
		case errors.Is(err, cutover.ErrNotInstalled):
			return cutoverSource{}, notFoundError("the crontab command is not on PATH", "",
				"install a cron daemon, or point --file at a crontab-format file:",
				"    apt install cron    (or: dnf install cronie)",
				"    paceq cutover --file /etc/crontab --dry-run")
		case errors.Is(err, cutover.ErrNoCrontab):
			// An empty crontab is the state before the first import:
			// nothing to cut over, and nothing to apologise for.
			return cutoverSource{
				label: "user " + f.user, user: f.user, content: "",
				writable: func(content string) error { return cutover.Write(ctx, f.user, content) },
			}, nil
		case err != nil:
			share := ""
			if f.user != "" {
				share = "reading another user's crontab needs root:"
			} else {
				share = "check the permissions on the spool, or read a file instead:"
			}
			return cutoverSource{}, validationError("could not read the crontab", err, share,
				"    paceq cutover --file /etc/crontab --dry-run")
		}
		label := "current user"
		if f.user != "" {
			label = "user " + f.user
		}
		return cutoverSource{
			label: label, user: f.user, content: content,
			writable: func(content string) error { return cutover.Write(ctx, f.user, content) },
		}, nil
	}
}

// writeCrontabFile writes a --file source back in place: mode preserved,
// atomic, after the caller has already taken its backup. System files
// usually belong to configuration management; the command's help says so,
// and the warning the report renders is the nudge that travels with the
// act.
func writeCrontabFile(path, content string) error {
	info, err := os.Stat(path)
	mode := os.FileMode(0o600)
	if err == nil {
		mode = info.Mode().Perm()
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".paceq-crontab-*")
	if err != nil {
		return fmt.Errorf("could not create a temporary file beside %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.WriteString(content); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not write %s: %w", path, err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not set the mode on %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("could not flush %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("could not close %s: %w", path, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("could not put the new crontab in place at %s: %w", path, err)
	}
	tmpName = ""
	return nil
}

// resolveCutoverJobs turns --job and the database into the jobs cutover can
// match, each with the origin line its import recorded. The origin lives
// in the job file as the "originally:" comment import writes above every
// job; a job without one cannot be matched to a line and is skipped with
// PSQ-CUT-004 rather than guessed about.
func resolveCutoverJobs(ctx context.Context, s *store.Store, wanted []string) ([]cutoverJob, []cutoverSkip, error) {
	sources, err := s.ListJobSources(ctx)
	if err != nil {
		return nil, nil, internalError("could not list the jobs", err)
	}

	if len(wanted) > 0 {
		byName := make(map[string]string, len(sources))
		for _, js := range sources {
			byName[js.Name] = js.SourcePath
		}
		names := make([]string, 0, len(sources))
		for _, js := range sources {
			names = append(names, js.Name)
		}
		sort.Strings(names)
		for _, name := range wanted {
			if _, ok := byName[name]; !ok {
				return nil, nil, notFoundError(fmt.Sprintf("there is no job named %q", name), "",
					"paceq ls  lists every job in this state directory")
			}
		}
	}

	var jobs []cutoverJob
	var skips []cutoverSkip
	cache := map[string]map[string]string{} // source path -> job name -> origin
	for _, js := range sources {
		if len(wanted) > 0 && !containsString(wanted, js.Name) {
			continue
		}
		if js.SourcePath == "" {
			skips = append(skips, cutoverSkip{
				job:    js.Name,
				code:   "PSQ-CUT-004",
				reason: "no imported origin recorded (not loaded by `paceq import crontab`)",
			})
			continue
		}
		origins, ok := cache[js.SourcePath]
		if !ok {
			origins, err = originsInJobFile(js.SourcePath)
			if err != nil {
				skips = append(skips, cutoverSkip{
					job:    js.Name,
					code:   "PSQ-CUT-004",
					reason: fmt.Sprintf("origin file %s is unreadable: %v", displayPathFor(js.SourcePath), err),
				})
				continue
			}
			cache[js.SourcePath] = origins
		}
		origin, ok := origins[js.Name]
		if !ok {
			skips = append(skips, cutoverSkip{
				job:  js.Name,
				code: "PSQ-CUT-004",
				reason: "no `originally:` comment in " + displayPathFor(js.SourcePath) +
					" (the file was edited after the import)",
			})
			continue
		}
		jobs = append(jobs, cutoverJob{name: js.Name, origin: origin})
	}
	return jobs, skips, nil
}

func containsString(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
}

// displayPathFor shows a path relative to the working directory when it
// can be, the same way apply names its inputs. It exists beside
// displayPath because cutover reads paths without an env in hand.
func displayPathFor(path string) string {
	wd, err := os.Getwd()
	if err != nil {
		return path
	}
	if rel, err := filepath.Rel(wd, path); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return path
}

// originsInJobFile extracts, per job name in the file, the crontab line
// the import recorded. The emitter writes `# originally: <line>` above
// every job, and the job's own `name:` is the first name key after it -
// the schedule and step names come later in the document.
func originsInJobFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path) // #nosec G304 - the stored source path of an applied job
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	pending := ""
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		switch {
		case strings.HasPrefix(line, "#"):
			body := strings.TrimSpace(strings.TrimPrefix(line, "#"))
			if v, ok := strings.CutPrefix(body, "originally:"); ok && pending == "" {
				pending = strings.TrimSpace(v)
			}
		case strings.HasPrefix(line, "name:"):
			if pending != "" {
				name := strings.TrimSpace(strings.TrimPrefix(line, "name:"))
				name = strings.Trim(name, `"'`)
				if name != "" {
					out[name] = pending
				}
				pending = ""
			}
		}
	}
	return out, nil
}

// loadCutoverStore opens the state for the command. Read-only modes get
// the read-only pool; anything that writes opens the state directory the
// way apply does, because the audit rows go in one transaction after the
// crontab is safe.
func loadCutoverStore(ctx context.Context, env Env, g *globals, writable bool) (*store.Store, error) {
	if !writable {
		return openReadOnlyStore(ctx, env, g)
	}
	stateDir, err := g.stateDir(env)
	if err != nil {
		return nil, err
	}
	dbPath := filepath.Join(stateDir, store.DatabaseFileName)
	if _, err := os.Stat(dbPath); errors.Is(err, os.ErrNotExist) {
		return nil, notFoundError(
			fmt.Sprintf("there is no paceq state at %s", stateDir),
			stateDir,
			"paceq init  creates a project with its state directory",
			"run the command inside the project directory, or pass --db",
		)
	}
	s, err := store.OpenState(ctx, stateDir, store.Options{Clock: clkOf(env)})
	if err != nil {
		return nil, err
	}
	return s, nil
}

// readyJobs applies the fence: a job that has never succeeded in paceq is
// held back unless --force, and a job whose shadow report carries
// unresolved deviations is cut over with a warning. The fence is 09
// section 5.3 made executable: nobody goes from import straight to
// cutover on a job paceq has never once run.
func readyJobs(ctx context.Context, s *store.Store, jobs []cutoverJob, force bool, now time.Time) ([]cutoverJob, []cutoverSkip, []string, error) {
	successes, err := s.MetricsLastSuccesses(ctx)
	if err != nil {
		return nil, nil, nil, internalError("could not read the run history", err)
	}
	succeeded := make(map[string]bool, len(successes))
	for _, st := range successes {
		succeeded[st.Job] = true
	}

	deviations, err := shadowDeviations(ctx, s, now)
	if err != nil {
		return nil, nil, nil, internalError("could not read the shadow report", err)
	}

	var ready []cutoverJob
	var skips []cutoverSkip
	var forced []string
	for _, job := range jobs {
		if !succeeded[job.name] && !force {
			skips = append(skips, cutoverSkip{
				job:    job.name,
				code:   "PSQ-CUT-001",
				reason: "no successful run in paceq yet - use --force if this is intended",
			})
			continue
		}
		if !succeeded[job.name] {
			forced = append(forced, job.name)
		}
		if verdict, ok := deviations[job.name]; ok {
			skips = append(skips, cutoverSkip{
				job:    job.name,
				code:   "PSQ-CUT-005",
				reason: "shadow report shows unresolved deviations (" + verdict + "); check `paceq shadow report --job " + job.name + "`",
			})
		}
		ready = append(ready, job)
	}
	return ready, skips, forced, nil
}

// shadowDeviations maps every job with a non-match, non-unknown shadow
// verdict to that verdict. Thin data is not a deviation - a verdict that
// says "not enough ground" must not block or warn a cutover, it says
// nothing at all.
func shadowDeviations(ctx context.Context, s *store.Store, now time.Time) (map[string]string, error) {
	rep, err := explain.BuildShadowReport(ctx, s, explain.ShadowInput{
		SinceMs:       now.Add(-7 * 24 * time.Hour).UnixMilli(),
		Now:           now,
		LocalZoneName: localZoneName(),
	})
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, f := range rep.Jobs {
		switch f.Verdict() {
		case explain.ShadowVerdictMatch, explain.ShadowVerdictUnknown:
			continue
		default:
			out[f.JobName] = f.Verdict()
		}
	}
	return out, nil
}

// runCutoverApply is the write path: fence, transform, backup, write,
// audit, report. The order is the contract - no code path below reaches
// the crontab write without a successful backup first, and the audit rows
// are the last thing committed, describing an operation that happened.
func runCutoverApply(ctx context.Context, env Env, g *globals, out *ui, f cutoverFlags) error {
	src, err := readCutoverSource(ctx, env, f)
	if err != nil {
		return err
	}
	s, err := loadCutoverStore(ctx, env, g, !f.dryRun)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	now := clkOf(env).Now()
	jobs, skips, err := resolveCutoverJobs(ctx, s, f.jobs)
	if err != nil {
		return err
	}
	if !f.dryRun {
		var ready []cutoverJob
		var fenceSkips []cutoverSkip
		ready, fenceSkips, _, err = readyJobs(ctx, s, jobs, f.force, now)
		if err != nil {
			return err
		}
		skips = append(skips, fenceSkips...)
		jobs = ready
	}

	result, changes, passSkips := cutover.Comment(src.content, toPkgJobs(jobs), now)
	skips = append(skips, fromPkgSkips(passSkips)...)

	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	backupPath := ""
	if len(changes) > 0 {
		backupPath, err = cutover.Backup(stateDir, src.content, now)
		if err != nil {
			return internalError("could not write the backup; the crontab was not changed", err)
		}
		if f.dryRun {
			// A dry run computed where the backup would land, and
			// that is all it may do with it.
		}
	}

	if f.dryRun {
		return renderCutoverDryRun(out, src, stateDir, backupPath, changes, skips)
	}

	if len(changes) == 0 {
		renderCutoverSkips(out, changes, skips)
		out.print("nothing to do")
		return nil
	}

	if err := src.writable(result); err != nil {
		// The change did not land. The backup exists, and the exact
		// recovery command is the difference between a scare and an
		// incident: exit 1, paceq's own failure, never 5.
		_ = backupPath
		return cutoverFailure("the crontab was not changed: the write failed after the backup was taken", err,
			"the state before the cutover is in "+displayPath(env, backupPath),
			"restore it with:  paceq cutover --rollback --from "+displayPath(env, backupPath))
	}

	events := make([]store.CutoverEvent, len(changes))
	for i, ch := range changes {
		events[i] = store.CutoverEvent{
			Action:     "cutover",
			JobName:    ch.JobName,
			LineNumber: ch.LineNumber,
			LineText:   ch.Line,
			Actor:      cliActor(),
			BackupPath: backupPath,
			Forced:     containsString(forcedNames(changes), ch.JobName),
			CreatedAt:  now.UTC(),
		}
	}
	_ = events
	if err := s.RecordCutoverEvents(ctx, events); err != nil {
		return cutoverFailure("the crontab changed but the audit rows could not be recorded", err,
			"the change is live and the backup is in "+displayPath(env, backupPath)+
				"; report this as a bug with the output of paceq version")
	}

	if err := renderCutoverReport(out, src, backupPath, changes, skips, forcedNames(changes)); err != nil {
		return err
	}
	return nil
}

// runCutoverStatus reports what the cutover did: the markers in the
// crontab and the jobs they belong to, the imported jobs that still run
// under cron, and the backups on disk. Read-only against crontab and
// state alike.
func runCutoverStatus(ctx context.Context, env Env, g *globals, out *ui, f cutoverFlags) error {
	src, err := readCutoverSource(ctx, env, f)
	if err != nil {
		return err
	}
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	markers := cutover.MarkerJobs(src.content)
	sources, err := ro.ListJobSources(ctx)
	if err != nil {
		return internalError("could not list the jobs", err)
	}
	events, err := ro.ListCutoverEvents(ctx, 10)
	if err != nil {
		return internalError("could not read the cutover trail", err)
	}

	if out.mode == modeJSON {
		type backupRow struct {
			Path string `json:"path"`
		}
		type statusDoc struct {
			Markers   []cutover.Marker     `json:"markers"`
			Uncut     []string             `json:"uncut_jobs"`
			Backups   []backupRow          `json:"backups"`
			RecentAll []store.CutoverEvent `json:"recent_events"`
		}
		doc := statusDoc{Markers: markers, Uncut: []string{}, Backups: []backupRow{}, RecentAll: events}
		markerJobs := map[string]bool{}
		for _, m := range markers {
			markerJobs[m.Job] = true
		}
		for _, js := range sources {
			if js.SourcePath != "" && !markerJobs[js.Name] {
				doc.Uncut = append(doc.Uncut, js.Name)
			}
		}
		for _, p := range cutoverBackupPaths(func() string { d, _ := g.stateDir(env); return d }()) {
			doc.Backups = append(doc.Backups, backupRow{Path: p})
		}
		return out.json(doc)
	}

	out.print("cutover status for %s:", src.label)
	if len(markers) == 0 {
		out.print("  nothing is cut over yet")
	} else {
		out.print("  cut over (%d lines):", len(markers))
		for _, m := range markers {
			out.print("    %s  job=%s", m.When.Format("2006-01-02 15:04"), m.Job)
		}
	}
	uncut := 0
	markerJobs := map[string]bool{}
	for _, m := range markers {
		markerJobs[m.Job] = true
	}
	for _, js := range sources {
		if js.SourcePath != "" && !markerJobs[js.Name] {
			out.print("  still on cron: %s", js.Name)
			uncut++
		}
	}
	if uncut == 0 && len(markers) > 0 {
		out.print("  every imported job is cut over")
	}
	paths := cutoverBackupPaths(func() string { d, _ := g.stateDir(env); return d }())
	if len(paths) == 0 {
		out.print("  no backups on disk")
	} else {
		out.print("  backups:")
		for _, p := range paths {
			out.print("    %s", displayPathFor(p))
		}
	}
	if len(events) > 0 {
		out.print("  recent trail:")
		for _, e := range events {
			out.print("    %s  %s  %s  line %d%s",
				e.CreatedAt.Format("2006-01-02 15:04"), e.Action, e.JobName,
				e.LineNumber, forcedSuffix(e.Forced))
		}
	}
	return nil
}

func forcedSuffix(forced bool) string {
	if forced {
		return "  (forced)"
	}
	return ""
}

// cutoverBackupPaths lists the crontab backups in dir, oldest first.
func cutoverBackupPaths(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasPrefix(e.Name(), "crontab.backup.") {
			out = append(out, filepath.Join(dir, e.Name()))
		}
	}
	sort.Strings(out)
	return out
}

// runCutoverRollback puts the commented lines back. Without --from it
// uncomments every marker in the current crontab (narrowed by --job);
// with --from it restores the named backup wholesale. Either way the
// current crontab is backed up first, and the trail records the restore.
func runCutoverRollback(ctx context.Context, env Env, g *globals, out *ui, f cutoverFlags) error {
	src, err := readCutoverSource(ctx, env, f)
	if err != nil {
		return err
	}
	s, err := loadCutoverStore(ctx, env, g, true)
	if err != nil {
		return err
	}
	defer func() { _ = s.Close() }()

	now := clkOf(env).Now()
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}
	backupPath, err := cutover.Backup(stateDir, src.content, now)
	if err != nil {
		return internalError("could not write the backup; the crontab was not changed", err)
	}

	var result string
	var changes []cutover.Change
	var action string
	if f.from != "" {
		restored, readErr := cutover.ReadBackup(f.from)
		if readErr != nil {
			return notFoundError(readErr.Error(), "",
				"list the backups that exist:",
				"    paceq cutover --status")
		}
		result = restored
		action = "rollback"
	} else {
		only := f.jobs
		if len(only) == 0 {
			// Every marker paceq finds, whatever job it names.
			only = nil
		}
		result, changes = cutover.Uncomment(src.content, only)
		action = "rollback"
		if len(changes) == 0 {
			out.print("nothing to roll back: no cutover markers in %s", src.label)
			return nil
		}
	}

	if err := src.writable(result); err != nil {
		return cutoverFailure("the crontab was not changed: the write failed after the backup was taken", err,
			"the state before the rollback is in "+displayPath(env, backupPath),
			"restore it with:  paceq cutover --rollback --from "+displayPath(env, backupPath))
	}

	events := make([]store.CutoverEvent, max(1, len(changes)))
	if f.from != "" {
		events[0] = store.CutoverEvent{
			Action:     action,
			JobName:    "",
			LineNumber: 0,
			LineText:   "whole-file restore from " + displayPath(env, f.from),
			Actor:      cliActor(),
			BackupPath: backupPath,
			CreatedAt:  now.UTC(),
		}
	} else {
		for i, ch := range changes {
			events[i] = store.CutoverEvent{
				Action:     action,
				JobName:    ch.JobName,
				LineNumber: ch.LineNumber,
				LineText:   ch.Line,
				Actor:      cliActor(),
				BackupPath: backupPath,
				CreatedAt:  now.UTC(),
			}
		}
	}
	if err := s.RecordCutoverEvents(ctx, events); err != nil {
		return cutoverFailure("the crontab changed but the audit rows could not be recorded", err,
			"the change is live and the backup is in "+displayPath(env, backupPath)+
				"; report this as a bug with the output of paceq version")
	}

	renderCutoverRollback(out, src, backupPath, changes, f.from != "")
	return nil
}

// cutoverFailure is a failed crontab write after a successful backup: the
// operation did not land, the way back is named, and it is never exit 5.
func cutoverFailure(what string, err error, next ...string) *Error {
	return &Error{code: ExitInternal, what: what, err: err, next: next}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func renderCutoverRollback(out *ui, src cutoverSource, backupPath string, changes []cutover.Change, wholeFile bool) {
	if out.mode == modeJSON {
		type changeRow struct {
			Job  string `json:"job"`
			Line int    `json:"line"`
		}
		type rollbackDoc struct {
			Source    string      `json:"source"`
			Backup    string      `json:"backup"`
			WholeFile bool        `json:"whole_file"`
			Changes   []changeRow `json:"changes"`
		}
		doc := rollbackDoc{Source: src.label, Backup: backupPath, WholeFile: wholeFile, Changes: []changeRow{}}
		for _, ch := range changes {
			doc.Changes = append(doc.Changes, changeRow{Job: ch.JobName, Line: ch.LineNumber})
		}
		_ = out.json(doc)
		return
	}
	out.print("rolled back %s; the previous state is in %s", src.label, displayPathFor(backupPath))
	for _, ch := range changes {
		out.print("  restored  job=%s  line %d", ch.JobName, ch.LineNumber)
	}
}

func toPkgJobs(jobs []cutoverJob) []cutover.Job {
	out := make([]cutover.Job, len(jobs))
	for i, j := range jobs {
		out[i] = cutover.Job{Name: j.name, Origin: j.origin}
	}
	return out
}

func fromPkgSkips(skips []cutover.Skip) []cutoverSkip {
	out := make([]cutoverSkip, len(skips))
	for i, sk := range skips {
		out[i] = cutoverSkip{
			job:        sk.JobName,
			code:       sk.Reason.Code(),
			reason:     sk.Reason.String(),
			line:       sk.Line,
			lineNumber: sk.LineNumber,
		}
	}
	return out
}

// forcedNames is derived again from the changes because the fence's forced
// list only covers the fence; a job can also be forced and then fail to
// match. Only changes carry the truth about what was written.
func forcedNames(changes []cutover.Change) []string { return nil }

// renderCutoverDryRun shows the diff a real cutover would make. A dry run
// writes nothing: no backup, no audit row, the crontab byte-identical.
func renderCutoverDryRun(out *ui, src cutoverSource, stateDir, backupPath string, changes []cutover.Change, skips []cutoverSkip) error {
	if out.mode == modeJSON {
		type changeRow struct {
			Job      string `json:"job"`
			Line     int    `json:"line"`
			LineText string `json:"line_text"`
		}
		type skipRow struct {
			Job    string `json:"job"`
			Code   string `json:"code,omitempty"`
			Reason string `json:"reason"`
		}
		doc := struct {
			Source        string      `json:"source"`
			DryRun        bool        `json:"dry_run"`
			WouldChange   []changeRow `json:"would_change"`
			WouldSkip     []skipRow   `json:"would_skip"`
			BackupWouldBe string      `json:"backup_would_be"`
		}{Source: src.label, DryRun: true, WouldChange: []changeRow{}, WouldSkip: []skipRow{}, BackupWouldBe: backupPath}
		for _, ch := range changes {
			doc.WouldChange = append(doc.WouldChange, changeRow{Job: ch.JobName, Line: ch.LineNumber, LineText: ch.Line})
		}
		for _, sk := range skips {
			doc.WouldSkip = append(doc.WouldSkip, skipRow{Job: sk.job, Code: sk.code, Reason: sk.reason})
		}
		return out.json(doc)
	}
	out.print("dry run for %s - nothing written:", src.label)
	if backupPath != "" {
		out.print("  the backup would be %s", displayPathFor(backupPath))
	}
	for _, sk := range skips {
		code := ""
		if sk.code != "" {
			code = sk.code + "  "
		}
		out.print("  skip     %s%s  %s", code, sk.job, sk.reason)
	}
	for _, ch := range changes {
		out.print("  comment  job=%s  line %d", ch.JobName, ch.LineNumber)
	}
	return nil
}

func renderCutoverReport(out *ui, src cutoverSource, backupPath string, changes []cutover.Change, skips []cutoverSkip, forced []string) error {
	if out.mode == modeJSON {
		type changeRow struct {
			Job    string `json:"job"`
			Line   int    `json:"line"`
			Marker string `json:"marker"`
		}
		type skipRow struct {
			Job    string `json:"job"`
			Code   string `json:"code,omitempty"`
			Reason string `json:"reason"`
		}
		doc := struct {
			Source  string      `json:"source"`
			Backup  string      `json:"backup"`
			Forced  []string    `json:"forced"`
			Changed []changeRow `json:"changed"`
			Skipped []skipRow   `json:"skipped"`
		}{Source: src.label, Backup: backupPath, Forced: forced, Changed: []changeRow{}, Skipped: []skipRow{}}
		for _, ch := range changes {
			doc.Changed = append(doc.Changed, changeRow{Job: ch.JobName, Line: ch.LineNumber, Marker: ch.Marker})
		}
		for _, sk := range skips {
			doc.Skipped = append(doc.Skipped, skipRow{Job: sk.job, Code: sk.code, Reason: sk.reason})
		}
		return out.json(doc)
	}
	out.print("cut over %s; the state before is in %s", src.label, displayPathFor(backupPath))
	for _, sk := range skips {
		warn := ""
		if sk.code == "PSQ-CUT-005" {
			warn = "  WARNING"
		}
		code := ""
		if sk.code != "" {
			code = sk.code + "  "
		}
		out.print("  skip%s   %s%s  %s", warn, code, sk.job, sk.reason)
	}
	for _, name := range forced {
		out.print("  forced   %s  cut over without a successful run", name)
	}
	for _, ch := range changes {
		out.print("  commented job=%s  line %d", ch.JobName, ch.LineNumber)
	}
	return nil
}

func renderCutoverSkips(out *ui, changes []cutover.Change, skips []cutoverSkip) {
	for _, sk := range skips {
		code := ""
		if sk.code != "" {
			code = sk.code + "  "
		}
		out.print("  skip     %s%s  %s", code, sk.job, sk.reason)
	}
}
