package crontab

import (
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
	"github.com/a-holm/paceq/internal/spec"
)

// Options tunes one import pass. All fields are optional.
type Options struct {
	// NamePrefix is prepended to every derived job name (--name-prefix).
	NamePrefix string
	// Timezone is the assumed zone (--tz). Jobs whose context carried no
	// CRON_TZ get it on their schedules; a CRON_TZ seen earlier in the file
	// wins over it, because cron reads the file top down too.
	Timezone string
	// SixField reads every entry the way /etc/crontab and /etc/cron.d are
	// written: a user column sits between the schedule and the command.
	// Without it the reader guesses per line, and a five-field reading that
	// parses wins, because guessing a user column into a user crontab is
	// worse than leaving one token too many in a shell command.
	SixField bool
}

// Note levels, mirrored by the symbols the CLI renders them with.
const (
	NoteWarn = iota
	NoteInfo
)

// Note is one observation about one job, printed under the job in the YAML
// and summarised in the report.
type Note struct {
	Level int
	Text  string
}

// Doc is one translated job with everything the emitter draws around it:
// the comments that preceded the line, the original line itself, and the
// notes the translation produced.
type Doc struct {
	Name         string
	LeadComments []string
	Origin       string
	Job          *spec.Job
	Mailto       string // active MAILTO when the line was read; unset otherwise
	MailtoOff    bool   // MAILTO was set empty: mailing was switched off
	ScheduleTag  string // "@daily" or similar, shown beside the cron line
	TZFrom       string // where the schedule timezone came from, for a comment
	FlockLock    string // lockfile the flock wrapper guarded, for an inline comment
	NeedsReview  bool
	Todo         string // the TODO text rendered above the job, when there is one
	Notes        []Note
}

// Result is what one import pass produced.
type Result struct {
	Docs   []Doc
	Report Report
}

// specials maps cron's @words onto the expressions cron itself expands them
// to. @reboot is deliberately absent: it has no paceq equivalent and is
// handled on its own.
var specials = map[string]string{
	"hourly":   "0 * * * *",
	"daily":    "0 0 * * *",
	"midnight": "0 0 * * *",
	"weekly":   "0 0 * * 0",
	"monthly":  "0 0 1 * *",
	"yearly":   "0 0 1 1 *",
	"annually": "0 0 1 1 *",
}

// defaultShell is what cron uses when the file sets no SHELL.
const defaultShell = "/bin/sh"

// heredocMark delimits imported standard input inside a shell command. A
// payload containing the mark cannot be delimited safely, so such a line
// falls back to verbatim with a TODO instead of producing something that
// would paste differently than cron ran it.
const heredocMark = "PACEQ_STDIN_EOF"

// Redirection shapes, all matched at one end of the command. Each pattern
// consumes whole redirection operators -- word boundary, optional standalone
// fd number, operator, target, optional trailing N>&M duplicate -- or refuses
// to match. Half an operator must never be eaten: "2> /dev/null" matched down
// to its ">" used to leave a stray "2" that sailed into argv as an argument
// cron never ran, silently and with no TODO. Refusal is the safe outcome;
// leftovers carry a meta character and get the visible shell decision. The
// fd number binds to the operator only when it stands alone, the way a POSIX
// shell reads it: in "-k2>/dev/null" the 2 belongs to the word.
var (
	// >/dev/null in any spelling (&>, >& , N>, >>, N>>), both streams when
	// they both go there, optionally closed by an N>&M duplicate.
	devNullTail = regexp.MustCompile(`(?:(?:^|\s)(?:\d*&?>{1,2}|>&)\s*/dev/null)+(?:\s+\d*>&\d+)?\s*$`)
	// N>>target with any target, optionally closed by an N>&M duplicate.
	appendTail = regexp.MustCompile(`(?:^|\s)\d*>>\s*(\S+)(?:\s+\d*>&\d+)?\s*$`)
	loggerTail = regexp.MustCompile(`(2>&1\s*)?\|\s*logger(\s+-t\s+\S+)?(\s.*)?$`)
	cdHead     = regexp.MustCompile(`^cd\s+(.+?)\s*&&\s*(.*)$`)
)

// flockFlagValues are flags that consume the next token; every other flag
// stands alone.
var flockFlagAlone = map[string]bool{
	"-n": true, "-x": true, "-s": true, "-u": true, "-o": true,
	"-v": true, "-V": true, "-h": true, "--nonblock": true,
	"--exclusive": true, "--shared": true, "--unlock": true,
	"--verbose": true, "--strip-env": true,
}

// Import translates a whole crontab. It never touches disk and never fails:
// every line ends up either translated or kept verbatim under a TODO, and
// the report says how many of each happened.
func Import(src []byte, opts Options) Result {
	w := &walker{
		opts:      opts,
		env:       map[string]string{},
		used:      map[string]int{},
		shellPath: defaultShell,
	}
	lines := parseLinesAs(src, opts.SixField)
	w.report.Lines = len(lines)

	var pending []string
	for _, ln := range lines {
		switch ln.Kind {
		case kindBlank:
			// Blank lines separate jobs visually; comments survive them so
			// the heading above a job travels with the job.
		case kindComment:
			pending = append(pending, strings.TrimSpace(ln.Comment))
			continue
		case kindEnv:
			pending = nil // a comment naming an env line stays with the env
			w.applyEnv(ln.EnvKey, ln.EnvValue)
			continue
		default:
			doc := w.translateLine(ln)
			doc.LeadComments = pending
			pending = nil
			doc.Name = uniqueName(w.opts.NamePrefix, deriveName(doc.baseToken()), w.used)
			w.docs = append(w.docs, doc)
		}
	}
	w.report.Jobs = len(w.docs)
	for i := range w.docs {
		if w.docs[i].NeedsReview {
			w.report.NeedsReview++
		}
	}
	return Result{Docs: w.docs, Report: w.report}
}

// walker carries the state cron reads top down: environment, mail target,
// timezone and shell are set by their lines and affect everything after them
// until changed.
type walker struct {
	opts      Options
	env       map[string]string
	mailto    string
	mailtoOff bool
	tz        string
	tzSet     bool
	shellPath string
	used      map[string]int
	docs      []Doc
	report    Report
}

func (w *walker) applyEnv(key, value string) {
	switch key {
	case "CRON_TZ":
		w.tz = value
		w.tzSet = true
	case "MAILTO":
		w.mailto = value
		w.mailtoOff = value == ""
		w.report.MailtoLines++
	case "SHELL":
		if strings.HasPrefix(value, "/") {
			w.shellPath = value
		}
	default:
		w.env[key] = value
	}
}

// baseToken is the word a name derives from: the program being run.
func (d *Doc) baseToken() string {
	if d.Job == nil || len(d.Job.Steps) == 0 || len(d.Job.Steps[0].Run) == 0 {
		return ""
	}
	run := d.Job.Steps[0].Run
	if d.Job.Steps[0].Shell && len(run) >= 3 {
		fields := strings.Fields(run[2])
		if len(fields) > 0 {
			return filepath.Base(fields[0])
		}
		return ""
	}
	return filepath.Base(run[0])
}

func (w *walker) translateLine(ln line) Doc {
	switch ln.Kind {
	case kindSchedule, kindSystem:
		return w.translateCommand(ln, ln.Command, "")
	case kindSpecial:
		if expr, known := specials[ln.Special]; known && ln.Command != "" {
			return w.translateCommand(ln, ln.Command, expr)
		}
		if ln.Special == "reboot" && ln.Command != "" {
			return w.translateReboot(ln)
		}
		return w.translateVerbatim(ln, ln.Text)
	default:
		return w.translateVerbatim(ln, ln.Text)
	}
}

// translateCommand runs the full pipeline on one command: percent split,
// redirection and wrapper removal, the cd extraction, then the argv versus
// shell decision. specialExpr carries the expansion of an @word, or "" for a
// five-field line.
func (w *walker) translateCommand(ln line, raw, specialExpr string) Doc {
	cmd, stdin := splitPercent(raw)
	if stdin != "" {
		w.report.StdinSplit++
	}

	doc := Doc{Origin: ln.Text, Mailto: w.mailto, MailtoOff: w.mailtoOff}
	workdir := ""

	// Pipes and redirections come off the right end, logger outermost. The
	// /dev/null check runs first so ">>/dev/null" reads as thrown-away
	// output rather than a log file.
	if m := loggerTail.FindString(cmd); m != "" {
		cmd = strings.TrimRight(strings.TrimSuffix(cmd, m), " 	")
		w.report.LoggerPipe++
		doc.Notes = append(doc.Notes, Note{
			NoteInfo,
			"output was piped to logger; paceq records it in `paceq logs <name>` instead",
		})
	}
	if m := devNullTail.FindString(cmd); m != "" {
		cmd = strings.TrimRight(strings.TrimSuffix(cmd, m), " 	")
		w.report.DevNull++
		doc.Notes = append(doc.Notes, Note{
			NoteWarn,
			"output went to /dev/null and the log was thrown away; paceq keeps it now (`paceq logs <name>`)",
		})
	}
	if m := appendTail.FindStringSubmatch(cmd); m != nil {
		cmd = strings.TrimRight(strings.TrimSuffix(cmd, m[0]), " 	")
		w.report.AppendLog++
		doc.Notes = append(doc.Notes, Note{
			NoteInfo,
			"the log went to " + m[1] + "; it lives in `paceq logs <name>` now",
		})
	}

	// cd and flock hide behind each other: "cd X && flock ..." buries the
	// wrapper after the workdir, "flock ... cd X && Y" buries the workdir
	// inside the wrapped command. Two passes of the pair unwind both orders;
	// each pass is a no-op once nothing matches.
	for pass := 0; pass < 2; pass++ {
		if m := cdHead.FindStringSubmatch(cmd); m != nil {
			workdir = unquote(m[1])
			cmd = strings.TrimSpace(m[2])
		}
		if inner, lock, ok := unwrapFlock(cmd); ok {
			cmd = inner
			doc.Job = &spec.Job{MaxConcurrent: 1} // carrier until the real job exists
			doc.FlockLock = lock
			w.report.Flock++
		}
	}
	if strings.Contains(cmd, "hc-ping.com") || strings.Contains(cmd, "healthchecks.io") {
		w.report.Healthcheck++
		doc.Notes = append(doc.Notes, Note{
			NoteInfo,
			"the dead-man URL stays exactly as it is; paceq can watch for silence itself once this job exists",
		})
	}

	// The schedule decides whether translation survives at all: a good
	// command behind an expression paceq cannot read is kept verbatim rather
	// than silently rescheduled.
	cronText := ln.Schedule
	tag := ""
	if specialExpr != "" {
		cronText = specialExpr
		tag = "@" + ln.Special
	}
	schedules := w.buildSchedules(cronText, &doc)
	if schedules == nil {
		return w.translateVerbatim(ln, ln.Text)
	}

	step := spec.Step{Name: "main"}
	final := cmd
	switch {
	case stdin != "":
		payload, ok := heredoc(stdin)
		if !ok {
			return w.translateVerbatim(ln, ln.Text)
		}
		step.Shell = true
		step.Run = []string{w.shellPath, "-c", final + "\n" + payload}
		doc.Todo = todoShell
		w.report.ShellCommands++
	default:
		if argv, needsShell := toArgv(final); !needsShell {
			if resolved, ok := resolveProgram(argv[0], workdir); ok {
				argv[0] = resolved
				step.Run = argv
			} else {
				step.Shell = true
				step.Run = []string{w.shellPath, "-c", final}
				doc.Todo = todoShell
				w.report.ShellCommands++
			}
		} else {
			step.Shell = true
			step.Run = []string{w.shellPath, "-c", final}
			doc.Todo = todoShell
			w.report.ShellCommands++
		}
	}

	job := &spec.Job{
		Timeout: time.Hour, // stated explicitly so the reader sees it and adjusts
		Steps:   []spec.Step{step},
	}
	if workdir != "" {
		job.Workdir = workdir
	}
	if len(w.env) > 0 {
		job.Env = make(map[string]string, len(w.env))
		for k, v := range w.env {
			job.Env[k] = v
		}
	}
	if doc.Job != nil && doc.Job.MaxConcurrent == 1 {
		job.MaxConcurrent = 1
	}
	job.Schedules = schedules
	doc.Job = job
	doc.ScheduleTag = tag
	return doc
}

// buildSchedules validates the expression and attaches the zone in force.
// nil means paceq cannot read the expression and the caller keeps the whole
// line verbatim instead of silently rescheduling it.
func (w *walker) buildSchedules(expr string, doc *Doc) []spec.Schedule {
	if expr == "" {
		return nil
	}
	if _, err := cronx.Parse(expr); err != nil {
		return nil
	}
	s := spec.Schedule{Cron: expr}
	zone, source := "", ""
	if w.tzSet {
		zone, source = w.tz, "CRON_TZ"
	} else if w.opts.Timezone != "" {
		zone, source = w.opts.Timezone, "--tz"
	}
	if zone != "" {
		if _, err := loadZone(zone); err != nil {
			return nil
		}
		s.Timezone = zone
		doc.TZFrom = source
	}
	return []spec.Schedule{s}
}

// translateReboot builds the @reboot case: the command translates normally,
// the job carries no schedule, and both the comment and the report say why.
func (w *walker) translateReboot(ln line) Doc {
	doc := Doc{Origin: ln.Text, Mailto: w.mailto, MailtoOff: w.mailtoOff}
	step := spec.Step{Name: "main"}
	if argv, needsShell := toArgv(ln.Command); !needsShell {
		if resolved, ok := resolveProgram(argv[0], ""); ok {
			argv[0] = resolved
			step.Run = argv
		}
	}
	if step.Run == nil {
		step.Shell = true
		step.Run = []string{w.shellPath, "-c", ln.Command}
		doc.Todo = todoShell
		w.report.ShellCommands++
	}
	doc.Job = &spec.Job{Timeout: time.Hour, Steps: []spec.Step{step}}
	doc.NeedsReview = true
	w.report.Reboot++
	doc.Notes = append(doc.Notes, Note{
		NoteWarn,
		"@reboot ran the command once at boot; paceq starts nothing at boot yet, so this stays unscheduled until you decide",
	})
	return doc
}

// translateVerbatim keeps a line exactly as cron ran it, behind a shell, with
// a TODO. This is the R6 worst case made deliberate: the job works, and what
// it does is visible.
func (w *walker) translateVerbatim(ln line, text string) Doc {
	doc := Doc{Origin: ln.Text, Mailto: w.mailto, MailtoOff: w.mailtoOff}
	step := spec.Step{Name: "main", Shell: true, Run: []string{w.shellPath, "-c", text}}
	doc.Job = &spec.Job{Timeout: time.Hour, Steps: []spec.Step{step}}
	doc.NeedsReview = true
	doc.Todo = todoVerbatim
	w.report.Uninterpreted++
	return doc
}

const (
	todoShell    = "this command needs a shell; shell: true gives it full shell access, so review it"
	todoVerbatim = "paceq could not interpret the original line and kept it exactly as cron ran it"
)

// unwrapFlock recognises `flock [flags] lockfile command...` and returns the
// inner command with the lockfile it guarded. Only flags flock actually takes
// before its first operand are skipped; anything unexpected leaves the
// wrapper in place, which the shell decision handles safely. The -c spelling
// hands ONE word to a shell, so that word is returned whole as the inner
// command -- gluing the flag onto the front produced "-c systemctl ..."
// and a guaranteed command-not-found.
func unwrapFlock(cmd string) (inner string, lockfile string, ok bool) {
	args, good := shlexSplit(cmd)
	if !good || len(args) < 3 || filepath.Base(args[0]) != "flock" {
		return "", "", false
	}
	i := 1
	for i < len(args)-1 {
		a := args[i]
		switch {
		case flockFlagAlone[a]:
			i++
		case a == "-w" || a == "-E" || a == "--timeout" || a == "--conflict-exit-code":
			i += 2
		case strings.HasPrefix(a, "-"):
			i++ // unknown flag: skip it, keep looking for the lockfile
		default:
			rest := args[i+1:]
			if len(rest) >= 2 && (rest[0] == "-c" || rest[0] == "--command") {
				return rest[1], a, true
			}
			return strings.Join(rest, " "), a, true
		}
		if i >= len(args) {
			break
		}
	}
	return "", "", false
}

// resolveProgram makes the exec target satisfy the spec's absolute-path rule
// without changing what executes. An absolute path passes as it is; ./ and
// ../ forms join onto the directory cron would have cd-ed into; anything
// else needs a shell for its PATH lookup, and saying so beats guessing wrong.
func resolveProgram(prog, workdir string) (string, bool) {
	if strings.HasPrefix(prog, "/") {
		return prog, true
	}
	if strings.HasPrefix(prog, "./") || strings.HasPrefix(prog, "../") {
		if workdir == "" {
			return "", false
		}
		return filepath.Join(workdir, prog), true
	}
	return "", false
}

// heredoc renders imported standard input as a quoted heredoc, which a shell
// feeds to the command byte for byte: exactly what cron did after the %.
func heredoc(stdin string) (payload string, ok bool) {
	for _, l := range strings.Split(stdin, "\n") {
		if strings.TrimSpace(l) == heredocMark {
			return "", false
		}
	}
	return "<<'" + heredocMark + "'\n" + stdin + "\n" + heredocMark, true
}

func unquote(s string) string {
	if len(s) >= 2 {
		if (s[0] == '\'' && s[len(s)-1] == '\'') || (s[0] == '"' && s[len(s)-1] == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
