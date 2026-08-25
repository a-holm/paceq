package crontab

import (
	"strings"
	"testing"
)

// importLines is the helper every table-row test uses: feed crontab lines,
// get back the named doc and the whole result.
func importLines(t *testing.T, opts Options, lines ...string) (Result, Doc) {
	t.Helper()
	res := Import([]byte(strings.Join(lines, "\n")+"\n"), opts)
	if len(res.Docs) == 0 {
		t.Fatalf("no jobs came out of:\n%s", strings.Join(lines, "\n"))
	}
	return res, res.Docs[0]
}

func firstStep(t *testing.T, d Doc) (argv []string, shell bool) {
	t.Helper()
	if len(d.Job.Steps) != 1 {
		t.Fatalf("want exactly one step, got %d", len(d.Job.Steps))
	}
	return d.Job.Steps[0].Run, d.Job.Steps[0].Shell
}

func noteTexts(d Doc, level int) []string {
	var out []string
	for _, n := range d.Notes {
		if n.Level == level {
			out = append(out, n.Text)
		}
	}
	return out
}

// Row 1: a plain five-field schedule becomes schedules with a timezone.
func TestTableRowPlainSchedule(t *testing.T) {
	res, d := importLines(t, Options{},
		"CRON_TZ=Europe/Oslo",
		"0 6 * * * /usr/bin/backup --full")
	scheds := d.Job.Schedules
	if len(scheds) != 1 || scheds[0].Cron != "0 6 * * *" || scheds[0].Timezone != "Europe/Oslo" {
		t.Fatalf("schedule = %+v", scheds)
	}
	if d.TZFrom != "CRON_TZ" {
		t.Fatalf("TZ source %q", d.TZFrom)
	}
	if res.Report.Lines != 2 || res.Report.Jobs != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
}

// Row 1b: without CRON_TZ or --tz no zone is invented; the system default
// applies by omission.
func TestTableNoTimezoneInvented(t *testing.T) {
	_, d := importLines(t, Options{}, "0 6 * * * /usr/bin/backup")
	if len(d.Job.Schedules) != 1 || d.Job.Schedules[0].Timezone != "" {
		t.Fatalf("timezone should stay empty, got %+v", d.Job.Schedules)
	}
}

// Row 1c: --tz fills the gap CRON_TZ leaves.
func TestTableTzFlagFillsUnsetContext(t *testing.T) {
	_, d := importLines(t, Options{Timezone: "Europe/Oslo"}, "0 6 * * * /usr/bin/backup")
	if d.Job.Schedules[0].Timezone != "Europe/Oslo" || d.TZFrom != "--tz" {
		t.Fatalf("zone %+v from %q", d.Job.Schedules[0], d.TZFrom)
	}
}

// Row 1d: a later CRON_TZ overrides the earlier one, top down like cron.
func TestTableCronTzOrderSemantics(t *testing.T) {
	res := Import([]byte(strings.Join([]string{
		"CRON_TZ=Europe/Oslo",
		"0 5 * * * /bin/morning",
		"CRON_TZ=UTC",
		"30 4 * * * /bin/night",
	}, "\n")+"\n"), Options{})
	if len(res.Docs) != 2 {
		t.Fatalf("docs %d", len(res.Docs))
	}
	if got := res.Docs[0].Job.Schedules[0].Timezone; got != "Europe/Oslo" {
		t.Fatalf("first job zone %q", got)
	}
	if got := res.Docs[1].Job.Schedules[0].Timezone; got != "UTC" {
		t.Fatalf("second job zone %q", got)
	}
}

// Row 2: @daily and friends expand to the expressions cron uses.
func TestTableRowSpecialWords(t *testing.T) {
	cases := map[string]string{
		"@hourly":   "0 * * * *",
		"@daily":    "0 0 * * *",
		"@midnight": "0 0 * * *",
		"@weekly":   "0 0 * * 0",
		"@monthly":  "0 0 1 * *",
		"@yearly":   "0 0 1 1 *",
		"@annually": "0 0 1 1 *",
	}
	for word, want := range cases {
		res := Import([]byte(word+" /usr/local/bin/rydd-tmp\n"), Options{})
		if len(res.Docs) != 1 {
			t.Fatalf("%s: docs %d", word, len(res.Docs))
		}
		d := res.Docs[0]
		if len(d.Job.Schedules) != 1 || d.Job.Schedules[0].Cron != want {
			t.Errorf("%s: schedule %+v, want cron %q", word, d.Job.Schedules, want)
		}
		if d.ScheduleTag != word {
			t.Errorf("%s: tag %q", word, d.ScheduleTag)
		}
	}
}

// Row 3: @reboot becomes an unscheduled job under review with a warning.
func TestTableRowReboot(t *testing.T) {
	res, d := importLines(t, Options{}, "@reboot /usr/local/bin/boot-thing --now")
	if len(d.Job.Schedules) != 0 {
		t.Fatalf("@reboot must not produce a schedule: %+v", d.Job.Schedules)
	}
	if !d.NeedsReview {
		t.Fatal("@reboot must be marked for review")
	}
	if res.Report.Reboot != 1 || res.Report.NeedsReview != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	warns := noteTexts(d, NoteWarn)
	if len(warns) == 0 || !strings.Contains(warns[0], "@reboot") {
		t.Fatalf("no reboot warning in %v", warns)
	}
	argv, shell := firstStep(t, d)
	if shell || len(argv) != 2 || argv[0] != "/usr/local/bin/boot-thing" {
		t.Fatalf("reboot argv = %v shell = %v", argv, shell)
	}
}

// Row 4: MAILTO becomes a commented recipe carried on the job.
func TestTableRowMailto(t *testing.T) {
	_, d := importLines(t, Options{},
		"MAILTO=drift@example.com",
		"0 6 * * * /usr/bin/backup")
	if d.Mailto != "drift@example.com" || d.MailtoOff {
		t.Fatalf("mailto = %q off = %v", d.Mailto, d.MailtoOff)
	}
}

func TestTableRowMailtoEmptyDisables(t *testing.T) {
	res := Import([]byte("MAILTO=\n0 6 * * * /usr/bin/a\n"), Options{})
	if !res.Docs[0].MailtoOff {
		t.Fatal("empty MAILTO must read as switched off")
	}
}

// Row 5: PATH lands in the job's env.
func TestTableRowPathEnv(t *testing.T) {
	_, d := importLines(t, Options{},
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"0 6 * * * /usr/bin/backup")
	if got := d.Job.Env["PATH"]; got != "/usr/local/bin:/usr/bin:/bin" {
		t.Fatalf("env PATH = %q", got)
	}
}

// Row 6: SHELL becomes the interpreter behind shell: true steps.
func TestTableRowShellVariable(t *testing.T) {
	_, d := importLines(t, Options{},
		"SHELL=/bin/zsh",
		"0 6 * * * cd /srv && make all")
	argv, shell := firstStep(t, d)
	if !shell || argv[0] != "/bin/zsh" {
		t.Fatalf("shell step = %v (%v)", argv, shell)
	}
}

// Row 7: flock disappears into max_concurrent: 1, lockfile named.
func TestTableRowFlock(t *testing.T) {
	res, d := importLines(t, Options{},
		"*/5 * * * * flock -n /var/lock/sync.lock /usr/local/bin/sync-files")
	if d.Job.MaxConcurrent != 1 {
		t.Fatalf("max_concurrent = %d", d.Job.MaxConcurrent)
	}
	if d.FlockLock != "/var/lock/sync.lock" {
		t.Fatalf("lockfile %q", d.FlockLock)
	}
	if res.Report.Flock != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	argv, shell := firstStep(t, d)
	if shell || len(argv) != 1 || argv[0] != "/usr/local/bin/sync-files" {
		t.Fatalf("inner command = %v (%v)", argv, shell)
	}
}

func TestTableRowFlockWithTimeoutFlag(t *testing.T) {
	_, d := importLines(t, Options{}, "* * * * * flock -w 30 /var/lock/x /bin/sync")
	if d.Job.MaxConcurrent != 1 {
		t.Fatalf("max_concurrent = %d", d.Job.MaxConcurrent)
	}
}

// Row 8: > /dev/null goes away with the exact warning.
func TestTableRowDevNull(t *testing.T) {
	res := Import([]byte("0 2 * * * /opt/app/generer.sh > /dev/null 2>&1\n"), Options{})
	d := res.Docs[0]
	if res.Report.DevNull != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	warns := noteTexts(d, NoteWarn)
	if len(warns) == 0 || !strings.Contains(warns[0], "/dev/null") ||
		!strings.Contains(warns[0], "keeps it now") {
		t.Fatalf("warning text wrong: %v", warns)
	}
	argv, shell := firstStep(t, d)
	if shell || len(argv) != 1 || strings.Contains(argv[0], "dev") {
		t.Fatalf("redirect survived: %v", argv)
	}
}

// Row 9: >> log 2>&1 goes away with a note about paceq logs.
func TestTableRowAppendLog(t *testing.T) {
	res := Import([]byte("17 3 * * 1 /usr/bin/vacuum >> /var/log/vacuum.log 2>&1\n"), Options{})
	if res.Report.AppendLog != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	infos := noteTexts(res.Docs[0], NoteInfo)
	found := false
	for _, s := range infos {
		if strings.Contains(s, "/var/log/vacuum.log") && strings.Contains(s, "paceq logs") {
			found = true
		}
	}
	if !found {
		t.Fatalf("no append-log note in %v", infos)
	}
}

// Row 10: cd X && cmd becomes workdir plus an absolute run.
func TestTableRowCdAndCommand(t *testing.T) {
	_, d := importLines(t, Options{}, "0 9 * * * cd /srv/app && ./run.sh")
	if d.Job.Workdir != "/srv/app" {
		t.Fatalf("workdir %q", d.Job.Workdir)
	}
	argv, shell := firstStep(t, d)
	if shell {
		t.Fatalf("clean relative command must become argv, got %v", argv)
	}
	if argv[0] != "/srv/app/run.sh" {
		t.Fatalf("run = %v", argv)
	}
}

// Row 11: a logger pipe goes away with a notice.
func TestTableRowLoggerPipe(t *testing.T) {
	res := Import([]byte("* * * * * /usr/bin/noisy 2>&1 | logger -t noisy\n"), Options{})
	if res.Report.LoggerPipe != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	argv, shell := firstStep(t, res.Docs[0])
	if shell || strings.Join(argv, " ") != "/usr/bin/noisy" {
		t.Fatalf("pipe survived: %v (%v)", argv, shell)
	}
}

// Row 12: a healthcheck ping stays untouched and gets its notice.
func TestTableRowHealthcheckKept(t *testing.T) {
	res, d := importLines(t, Options{},
		"0 * * * * /usr/bin/curl -fsS https://hc-ping.com/abc-123 > /dev/null")
	argv, shell := firstStep(t, d)
	if shell || len(argv) < 3 || argv[2] != "https://hc-ping.com/abc-123" {
		t.Fatalf("healthcheck changed: %v (%v)", argv, shell)
	}
	if res.Report.Healthcheck != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
}

// Row 13: percent semantics - unescaped percent feeds stdin via heredoc,
// escaped percent stays literal without forcing a shell.
func TestTableRowPercentStdin(t *testing.T) {
	res := Import([]byte("0 4 * * * mysql db % row one; row two;\n"), Options{})
	d := res.Docs[0]
	if res.Report.StdinSplit != 1 {
		t.Fatalf("report = %+v", res.Report)
	}
	argv, shell := firstStep(t, d)
	if !shell {
		t.Fatal("stdin split must keep a shell to attach the data")
	}
	if !strings.Contains(argv[2], "<<'PACEQ_STDIN_EOF'\n row one; row two;\nPACEQ_STDIN_EOF") {
		t.Fatalf("stdin payload wrong: %q", argv[2])
	}
	if !strings.HasPrefix(argv[2], "mysql db ") {
		t.Fatalf("command part wrong: %q", argv[2])
	}
}

func TestTableRowPercentEscapedStaysArgv(t *testing.T) {
	res, d := importLines(t, Options{}, `0 4 * * * /bin/date +\%Y-\%m-\%d >> /var/log/date.log 2>&1`)
	argv, shell := firstStep(t, d)
	if shell {
		t.Fatalf("escaped percents need no shell: %v", argv)
	}
	want := "+%Y-%m-%d"
	if len(argv) != 2 || argv[0] != "/bin/date" || argv[1] != want {
		t.Fatalf("date argv = %v, want [/bin/date %s]", argv, want)
	}
	_ = res
}

// Row 14: an uninterpretable line stays verbatim and still yields a valid job.
func TestTableRowUninterpretableVerbatim(t *testing.T) {
	res, d := importLines(t, Options{}, "some total nonsense line here")
	if !d.NeedsReview || res.Report.Uninterpreted != 1 {
		t.Fatalf("verbatim handling wrong: %+v", res.Report)
	}
	argv, shell := firstStep(t, d)
	if !shell || argv[2] != "some total nonsense line here" {
		t.Fatalf("verbatim run = %v (%v)", argv, shell)
	}
	if d.Todo == "" || !strings.Contains(d.Todo, "kept it exactly") {
		t.Fatalf("todo text %q", d.Todo)
	}
}

// A command with meta characters gets shell: true and a TODO, never a
// mistranslated argv list. This is design decision 4.
func TestTableMetaCharactersForceShell(t *testing.T) {
	_, d := importLines(t, Options{}, "0 4 * * * /opt/rapport/kjor.sh --dato=$(date +\\%Y-\\%m-\\%d)")
	argv, shell := firstStep(t, d)
	if !shell {
		t.Fatalf("$() must force shell, got argv %v", argv)
	}
	if argv[2] != "/opt/rapport/kjor.sh --dato=$(date +%Y-%m-%d)" {
		t.Fatalf("command altered: %q", argv[2])
	}
	if d.Todo == "" {
		t.Fatal("shell decision must come with a TODO")
	}
}

// A clean command becomes argv with no shell flag at all.
func TestTableCleanCommandBecomesArgv(t *testing.T) {
	_, d := importLines(t, Options{}, "*/5 * * * * /usr/local/bin/sync-files --fast")
	argv, shell := firstStep(t, d)
	if shell {
		t.Fatalf("clean command went to a shell: %v", argv)
	}
	if len(argv) != 2 || argv[1] != "--fast" {
		t.Fatalf("argv = %v", argv)
	}
}

// Comments travel onto the job above the original line.
func TestCommentsTravelToTheJob(t *testing.T) {
	res := Import([]byte("# nightly report\n# keeps the warehouse fresh\n0 2 * * * /opt/etl/run.sh\n"), Options{})
	d := res.Docs[0]
	if len(d.LeadComments) != 2 || d.LeadComments[0] != "nightly report" {
		t.Fatalf("lead comments %v", d.LeadComments)
	}
}

// The original line survives byte for byte as the origin comment.
func TestOriginLinePreservedExactly(t *testing.T) {
	raw := "  0  2  *  *  *   /bin/weird   spacing  "
	res := Import([]byte(raw+"\n"), Options{})
	if res.Docs[0].Origin != raw {
		t.Fatalf("origin %q want %q", res.Docs[0].Origin, raw)
	}
}

// An unreadable schedule keeps the line verbatim instead of rescheduling it.
func TestBadScheduleFallsBackToVerbatim(t *testing.T) {
	// "99" is not a minute cron accepts, so paceq refuses to guess.
	res := Import([]byte("99 6 * * * /bin/never\n"), Options{})
	if len(res.Docs) != 1 {
		t.Fatal("the line must still produce exactly one job")
	}
	d := res.Docs[0]
	if len(d.Job.Schedules) != 0 || !d.NeedsReview {
		t.Fatalf("fallback wrong: schedules %+v review %v", d.Job.Schedules, d.NeedsReview)
	}
}

// A bare name with no absolute path and no workdir needs a shell for PATH,
// and the importer says so instead of guessing a directory.
func TestBareProgramNameNeedsShell(t *testing.T) {
	_, d := importLines(t, Options{}, "0 6 * * * backup-tool --all")
	argv, shell := firstStep(t, d)
	if !shell || argv[0] != defaultShell {
		t.Fatalf("bare name argv = %v shell = %v", argv, shell)
	}
}

// Row 8b: an fd redirection aimed at /dev/null disappears whole. A pattern
// that eats only the ">" half of "2> /dev/null" leaves a stray "2" behind,
// and since a lone digit carries no meta character it sails into argv as an
// argument cron never ran -- silently, with no shell: true and no TODO.
func TestTableRowFdRedirectionLeavesNoStrayElement(t *testing.T) {
	cases := []string{
		"0 2 * * * /opt/app/cleanup.sh 2> /dev/null",
		"0 2 * * * /opt/app/cleanup.sh 2>/dev/null",
		"0 2 * * * /opt/app/cleanup.sh >>/dev/null",
		"0 2 * * * /opt/app/cleanup.sh &>/dev/null",
		"0 2 * * * /opt/app/cleanup.sh >/dev/null 2>/dev/null",
	}
	for _, line := range cases {
		res := Import([]byte(line+"\n"), Options{})
		d := res.Docs[0]
		if res.Report.DevNull != 1 {
			t.Errorf("%s: DevNull = %+v", line, res.Report)
		}
		argv, shell := firstStep(t, d)
		if shell || len(argv) != 1 || argv[0] != "/opt/app/cleanup.sh" {
			t.Errorf("%s: residue after strip: argv %v (shell %v)", line, argv, shell)
		}
		for _, a := range argv {
			if strings.ContainsAny(a, "><") || a == "2" {
				t.Errorf("%s: stray element %q in argv %v", line, a, argv)
			}
		}
	}
}

// Row 9b: a bare ">> log" without the trailing 2>&1 goes away whole too, and
// leaves neither the operator nor a fragment of it behind as an argument.
func TestTableRowBareAppendLogLeavesNoStrayElement(t *testing.T) {
	cases := []string{
		"17 3 * * 1 /usr/bin/vacuum >> /var/log/vacuum.log",
		"17 3 * * 1 /usr/bin/vacuum 2>>/var/log/vacuum.log",
	}
	for _, line := range cases {
		res := Import([]byte(line+"\n"), Options{})
		d := res.Docs[0]
		if res.Report.AppendLog != 1 {
			t.Errorf("%s: AppendLog = %+v", line, res.Report)
		}
		argv, shell := firstStep(t, d)
		if shell || len(argv) != 1 || argv[0] != "/usr/bin/vacuum" {
			t.Errorf("%s: residue after strip: argv %v (shell %v)", line, argv, shell)
		}
		for _, a := range argv {
			if strings.ContainsAny(a, "><") {
				t.Errorf("%s: stray element %q in argv %v", line, a, argv)
			}
		}
	}
}

// Row 7b: flock -c runs its word through a shell, so that word is the inner
// command; the flag itself must never be glued onto the front of it.
func TestTableRowFlockCommandFlag(t *testing.T) {
	res, d := importLines(t, Options{},
		"* * * * * flock -n /var/lock/nginx.lock -c '/bin/systemctl reload nginx'")
	if d.Job.MaxConcurrent != 1 || d.FlockLock != "/var/lock/nginx.lock" || res.Report.Flock != 1 {
		t.Fatalf("wrap wrong: mc %d lock %q report %+v", d.Job.MaxConcurrent, d.FlockLock, res.Report)
	}
	argv, shell := firstStep(t, d)
	if shell || len(argv) != 3 || argv[0] != "/bin/systemctl" || argv[1] != "reload" || argv[2] != "nginx" {
		t.Fatalf("inner command mangled: %v (%v)", argv, shell)
	}
}

// Row 7c: cd X && flock ... extracts both halves -- workdir plus
// max_concurrent: 1 -- where checking flock before cd kept the wrapper
// stranded behind a shell TODO.
func TestTableRowCdThenFlock(t *testing.T) {
	res, d := importLines(t, Options{},
		"*/5 * * * * cd /srv/app && flock -n /var/lock/sync.lock ./sync-files")
	if d.Job.Workdir != "/srv/app" || d.Job.MaxConcurrent != 1 || res.Report.Flock != 1 {
		t.Fatalf("cd+fdlock wrong: workdir %q mc %d report %+v", d.Job.Workdir, d.Job.MaxConcurrent, res.Report)
	}
	if d.FlockLock != "/var/lock/sync.lock" {
		t.Fatalf("lockfile %q", d.FlockLock)
	}
	argv, shell := firstStep(t, d)
	if shell || len(argv) != 1 || argv[0] != "/srv/app/sync-files" {
		t.Fatalf("inner command = %v (%v)", argv, shell)
	}
}
