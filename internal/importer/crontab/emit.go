package crontab

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/spec"
)

// Emit writes the whole import as one annotated stream: a header, then one
// YAML document per job separated by --- lines, each with its original
// crontab line as a comment. The stream is what a human edits; every single
// document between the separators parses on its own.
func Emit(docs []Doc, header []string, w io.Writer) error {
	for _, h := range header {
		if _, err := fmt.Fprintf(w, "# %s\n", h); err != nil {
			return err
		}
	}
	for i := range docs {
		if i > 0 || len(header) > 0 {
			if _, err := fmt.Fprintln(w); err != nil {
				return err
			}
		}
		if _, err := fmt.Fprintln(w, "---"); err != nil {
			return err
		}
		if err := emitDoc(&docs[i], w); err != nil {
			return err
		}
	}
	return nil
}

// EmitDoc writes one job document with no separator. It is what -o <dir>
// writes one file of.
func EmitDoc(doc *Doc, w io.Writer) error { return emitDoc(doc, w) }

func emitDoc(d *Doc, w io.Writer) error {
	line := func(format string, args ...any) {
		fmt.Fprintf(w, format+"\n", args...)
	}
	comment := func(text string) {
		fmt.Fprintf(w, "# %s\n", strings.TrimRight(text, " "))
	}

	for _, c := range d.LeadComments {
		comment(c)
	}
	comment("originally: " + d.Origin)
	switch {
	case d.MailtoOff:
		comment("MAILTO was set empty here: cron mailed nothing for this job.")
	case d.Mailto != "":
		comment(fmt.Sprintf("MAILTO=%s is noted; paceq mails nothing yet (arrives in v0.2). Recipe for then:", d.Mailto))
		comment("  on_failure:")
		comment(`    - run: ["/usr/local/bin/mail-notify.sh", "` + d.Mailto + `"]`)
	}
	if d.Todo != "" {
		comment("TODO: review - " + d.Todo + ".")
	}
	if d.Job == nil {
		return nil
	}

	job := d.Job
	line("name: %s", scalar(d.Name))
	if job.Workdir != "" {
		line("workdir: %s", scalar(job.Workdir))
	}
	if len(job.Env) > 0 {
		line("env:")
		keys := make([]string, 0, len(job.Env))
		for k := range job.Env {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			line("  %s: %s", scalar(k), scalar(job.Env[k]))
		}
	}
	if job.MaxConcurrent == 1 {
		lockNote := "replaces the flock wrapper"
		if d.FlockLock != "" {
			lockNote = "replaces flock on " + d.FlockLock
		}
		line("max_concurrent: 1            # %s", lockNote)
	}
	line("timeout: %s                  # explicit so you can see it and adjust it", formatHours(job.Timeout))

	if len(job.Schedules) > 0 {
		line("schedules:")
		for _, s := range job.Schedules {
			extra := ""
			if d.ScheduleTag != "" {
				extra = "        # " + d.ScheduleTag
			}
			line("  - name: cron")
			line("    cron: %s%s", quoted(s.Cron), extra)
			if s.Timezone != "" {
				from := ""
				if d.TZFrom != "" {
					from = "        # from " + d.TZFrom
				}
				line("    timezone: %s%s", scalar(s.Timezone), from)
			}
		}
	}

	line("steps:")
	for _, st := range job.Steps {
		line("  - name: %s", scalar(st.Name))
		if len(st.Run) > 0 {
			parts := make([]string, len(st.Run))
			for i, r := range st.Run {
				parts[i] = quoted(r)
			}
			line("    run: [%s]", strings.Join(parts, ", "))
		}
		if st.Shell {
			line("    shell: true")
		}
	}

	for _, n := range d.Notes {
		mark := "NOTE"
		if n.Level == NoteWarn {
			mark = "WARN"
		}
		comment(mark + ": " + n.Text)
	}
	return nil
}

// SplitDocuments cuts an emitted stream back into its documents: everything
// before the first --- separator is the header preamble, each stretch between
// separators is one document, and the trailing stretch is the last document.
func SplitDocuments(stream string) []string {
	var docs []string
	var current []string
	started := false
	flush := func() {
		text := strings.Join(current, "\n")
		if strings.TrimSpace(text) != "" {
			docs = append(docs, text)
		}
		current = nil
	}
	for _, l := range strings.Split(stream, "\n") {
		if l == "---" {
			if started {
				flush()
			}
			current = nil
			started = true
			continue
		}
		current = append(current, l)
	}
	if !started {
		// No separators: the whole stream is one document, or nothing.
		flush()
		return docs
	}
	flush()
	return docs
}

// scalar renders a string as a plain YAML scalar when that is unambiguous,
// and as a double-quoted one otherwise.
func scalar(s string) string {
	if plainOK(s) {
		return s
	}
	return quote(s)
}

// quoted always renders a double-quoted scalar, JSON compatible and
// therefore valid YAML.
func quoted(s string) string { return quote(s) }

func quote(s string) string {
	var b strings.Builder
	encoder := json.NewEncoder(&b)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(s); err != nil {
		return `"` + strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`) + `"`
	}
	out := b.String()
	// Encode appends a newline; a double-quoted scalar must not carry one.
	return strings.TrimSuffix(out, "\n")
}

// plainOK reports whether s can sit bare in YAML without meaning something
// else: no leading indicator characters, no comment or mapping syntax inside,
// no trailing colon, printable.
func plainOK(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case '-', '?', ':', ',', '[', ']', '{', '}', '#', '&', '*', '!', '|', '>', '\'', '"', '%', '@', '`', ' ', '\t':
		return false
	}
	if strings.ContainsAny(s, "\n\t") {
		return false
	}
	if strings.Contains(s, ": ") || strings.Contains(s, " #") {
		return false
	}
	if strings.HasSuffix(s, ":") {
		return false
	}
	if s[0] == '-' && (len(s) == 1 || s[1] == ' ') {
		return false
	}
	for i := 0; i < len(s); i++ {
		if s[i] < 0x20 {
			return false
		}
	}
	return true
}

// formatHours renders the timeout the way the spec reads it back.
func formatHours(d time.Duration) string {
	s := spec.FormatDuration(d)
	if s == "" {
		return d.String()
	}
	return s
}
