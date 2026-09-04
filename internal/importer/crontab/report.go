package crontab

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/cronx"
)

// Report is the count side of an import pass: what was read, what became a
// job, and every category the translation wants to tell the human about.
// The CLI renders it on stderr so stdout stays pure YAML.
type Report struct {
	Lines         int // physical lines read
	Jobs          int
	NeedsReview   int
	DevNull       int
	Flock         int
	Reboot        int
	Uninterpreted int
	ShellCommands int
	AppendLog     int
	LoggerPipe    int
	Healthcheck   int
	StdinSplit    int
	MailtoLines   int
}

// Symbols carries the marks the report is drawn with; the CLI passes its own
// set so a non UTF-8 terminal degrades cleanly.
type Symbols struct {
	Warn string
	Info string
}

// UnicodeSymbols is the default pair.
var UnicodeSymbols = Symbols{Warn: "⚠", Info: "ⓘ"}

// AsciiSymbols is the fallback.
var AsciiSymbols = Symbols{Warn: "WARN", Info: "NOTE"}

// Plus sums two reports so several sources render as one story.
func (r Report) Plus(o Report) Report {
	r.Lines += o.Lines
	r.Jobs += o.Jobs
	r.NeedsReview += o.NeedsReview
	r.DevNull += o.DevNull
	r.Flock += o.Flock
	r.Reboot += o.Reboot
	r.Uninterpreted += o.Uninterpreted
	r.ShellCommands += o.ShellCommands
	r.AppendLog += o.AppendLog
	r.LoggerPipe += o.LoggerPipe
	r.Healthcheck += o.Healthcheck
	r.StdinSplit += o.StdinSplit
	r.MailtoLines += o.MailtoLines
	return r
}

// Render writes the report in the shape 09 section 5.1 fixes: one summary
// line, then the non-zero categories, then executable next steps.
func (r Report) Render(w io.Writer, sym Symbols, nextSteps []string) {
	fmt.Fprintf(w, "%d lines read -> %d jobs, %d to review\n",
		r.Lines, r.Jobs, r.NeedsReview)

	row := func(n int, one, many string) {
		if n == 0 {
			return
		}
		word := many
		if n == 1 {
			word = one
		}
		fmt.Fprintf(w, "  %s%s %d %s\n", sym.Warn, pad(n), n, word)
	}
	row(r.DevNull,
		"job threw output into /dev/null (the log is kept from now on)",
		"jobs threw output into /dev/null (the log is kept from now on)")
	row(r.Flock,
		"job wrapped its command in flock -> replaced with max_concurrent: 1",
		"jobs wrapped their command in flock -> replaced with max_concurrent: 1")
	row(r.ShellCommands,
		"command needs a shell -> shell: true, see its TODO comment",
		"commands need a shell -> shell: true, see their TODO comments")
	row(r.Reboot,
		"job used @reboot -> paceq has no boot trigger yet",
		"jobs used @reboot -> paceq has no boot trigger yet")
	row(r.Uninterpreted,
		"line could not be interpreted, see its comment in the output",
		"lines could not be interpreted, see their comments in the output")

	info := 0
	for _, n := range []int{r.AppendLog, r.LoggerPipe, r.Healthcheck, r.StdinSplit} {
		info += n
	}
	if info > 0 {
		var parts []string
		if r.AppendLog > 0 {
			parts = append(parts, fmt.Sprintf("%d wrote log files", r.AppendLog))
		}
		if r.LoggerPipe > 0 {
			parts = append(parts, fmt.Sprintf("%d piped to logger", r.LoggerPipe))
		}
		if r.Healthcheck > 0 {
			parts = append(parts, fmt.Sprintf("%d pinged a dead-man URL", r.Healthcheck))
		}
		if r.StdinSplit > 0 {
			parts = append(parts, fmt.Sprintf("%d fed stdin after %%, kept as data", r.StdinSplit))
		}
		fmt.Fprintf(w, "  %s %s\n", sym.Info, strings.Join(parts, ", "))
	}

	if len(nextSteps) > 0 {
		fmt.Fprintln(w)
		for i, step := range nextSteps {
			if i == 0 {
				fmt.Fprintf(w, "Next steps:  %s\n", step)
				continue
			}
			fmt.Fprintf(w, "             %s\n", step)
		}
	}
}

func pad(n int) string {
	if n >= 10 {
		return ""
	}
	return " "
}

// loadZone checks a timezone name through the one authority on zone names, so
// a bad --tz value becomes a verbatim-line decision instead of YAML that would
// refuse later with a worse story.
func loadZone(name string) (*time.Location, error) {
	return cronx.LoadZone(name)
}
