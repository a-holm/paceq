package status

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"
)

// The text form: the same Report the JSON mode writes, drawn as one screen
// for a person at a terminal. Every mark and separator comes from a Style, so
// a terminal without UTF-8 gets plain ASCII instead of boxes and question
// marks, and NO_COLOR strips the colour before anything here sees it.

// Style is the mark set one rendering draws with.
type Style struct {
	OK      string // a healthy job
	Warn    string // attention without a failed run (stuck, SLA)
	Fail    string // a deviation with a failed run
	Pause   string // an operator pause
	Sep     string // the aggregate line's field separator
	Dash    string // an absent value in a table cell
	Branch  string // the twig before a hint line
	Unicode bool
}

// StyleUnicode is the mark set for a UTF-8 locale.
func StyleUnicode() Style {
	return Style{
		OK: "✓", Warn: "⚠", Fail: "✗", Pause: "⏸",
		Sep: "·", Dash: "—", Branch: "└",
		Unicode: true,
	}
}

// StyleASCII is the fallback for every other locale: words instead of marks,
// per 03 section 7.1.
func StyleASCII() Style {
	return Style{
		OK: "ok", Warn: "!!", Fail: "X", Pause: "||",
		Sep: "|", Dash: "-", Branch: "|-",
	}
}

// RenderOptions are the per-run rendering decisions the command layer makes:
// which style, whether colour is wanted, whether --all lifted the screen
// limit, and what quiet means for this report.
type RenderOptions struct {
	Style Style
	Color bool

	// All lifts the DefaultVisibleJobs fold drawn by `paceq status --all`.
	All bool

	// Quiet keeps only what needs attention: the aggregate line, the
	// deviation rows and their hints. A healthy table under -q would be
	// exactly the noise -q exists to suppress.
	Quiet bool
}

// ANSI colours, written directly rather than through a library.
const (
	colorReset  = "\x1b[0m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorRed    = "\x1b[31m"
)

// RenderText draws one overview report. Data goes to out; nothing else ever
// writes there, so `paceq status | jq` stays clean even when it warns.
func RenderText(out io.Writer, rep *Report, opts RenderOptions) {
	renderAggregate(out, rep, opts)
	if len(rep.Jobs) == 0 {
		fmt.Fprintln(out, "no jobs yet: apply a job file to create one")
		return
	}
	renderTable(out, rep, opts)
}

// renderAggregate draws the headline: deviations, jobs, running, queued, and
// what the daemon is doing. Daemon down is marked here because that fact
// changes how much of the picture to trust.
func renderAggregate(out io.Writer, rep *Report, opts RenderOptions) {
	var b strings.Builder
	fmt.Fprintf(&b, "%d deviations %s %d jobs %s %d running %s %d queued",
		rep.Summary.Deviations, opts.Style.Sep, rep.Summary.Jobs,
		opts.Style.Sep, rep.Summary.Running, opts.Style.Sep, rep.Summary.Queued)
	if !rep.Daemon.Up {
		b.WriteString(" " + opts.Style.Sep + " daemon down")
	} else if since := parseStamp(rep.Daemon.Since); !since.IsZero() && rep.GeneratedAt != "" {
		if up := parseStamp(rep.GeneratedAt).Sub(since); up > 0 {
			fmt.Fprintf(&b, " %s daemon up %s", opts.Style.Sep, compactDuration(up))
		}
	} else {
		b.WriteString(" " + opts.Style.Sep + " daemon up")
	}
	fmt.Fprintln(out, b.String())
}

// renderTable draws the header, the visible job lines and their hints, then
// the fold line when the list was cut.
func renderTable(out io.Writer, rep *Report, opts RenderOptions) {
	style := opts.Style

	visible := rep.Jobs
	folded := 0
	if !opts.All && len(rep.Jobs) > DefaultVisibleJobs {
		visible = rep.Jobs[:DefaultVisibleJobs]
		folded = len(rep.Jobs) - DefaultVisibleJobs
	}

	widths := columnWidths(visible, rep.GeneratedAt, style)
	header := fmt.Sprintf("%s  %s  %s  %s  %s",
		pad("JOB", widths.name), pad("", widths.mark),
		pad("LAST", widths.last),
		pad("DURATION", widths.dur), "RESULT")
	fmt.Fprintln(out, header)

	for _, job := range visible {
		if opts.Quiet && !IsDeviation(job.State) {
			continue
		}
		renderJobLine(out, job, rep.GeneratedAt, widths, opts)
		for _, hint := range hintLines(job) {
			fmt.Fprintf(out, "  %s %s\n", style.Branch, hint)
		}
	}
	if folded > 0 {
		padWidth := widths.name + widths.mark + widths.last + widths.dur + widths.result + 10
		fmt.Fprintf(out, "%sand %d more (paceq status --all)\n", pad("", padWidth), folded)
	}
}

// renderJobLine draws one row of the table plus its state colouring.
func renderJobLine(out io.Writer, job Job, generatedAt string, widths widths, opts RenderOptions) {
	style := opts.Style
	mark, colour := markFor(job.State, style)
	marked := pad(mark, widths.mark)
	if opts.Color {
		marked = colour + marked + colorReset
	}

	last := style.Dash
	dur := style.Dash
	if l, d := whenCells(job, parseStamp(generatedAt)); l != "" || d != "" {
		last, dur = l, d
	}

	result := resultText(job, style)
	next := nextText(job, style, parseStamp(generatedAt))

	fmt.Fprintf(out, "%s  %s  %s  %s  %s  %s\n",
		pad(job.Name, widths.name),
		marked,
		pad(last, widths.last),
		pad(dur, widths.dur),
		pad(result, widths.result)+"  ",
		next)
}

// hintLines returns the follow-up lines under a deviation row: what happened,
// then the runnable command. Only deviations ever produce one.
func hintLines(job Job) []string {
	if job.Hint == "" {
		return nil
	}
	switch job.State {
	case StateFailed:
		return []string{fmt.Sprintf("last run did not succeed - run `%s`", job.Hint)}
	case StateStuck:
		return []string{"a run was lost mid-flight - run `" + job.Hint + "`"}
	case StateSLABreached:
		return []string{"no successful run within its freshness SLA - run `" + job.Hint + "`"}
	}
	return nil
}

type widths struct {
	name, last, dur, result, mark int
}

// columnWidths measures the widest cell of each column over the VISIBLE rows,
// so a long hidden name cannot stretch the fold line's padding.
func columnWidths(jobs []Job, generatedAt string, style Style) widths {
	w := widths{name: len("JOB"), last: len("LAST"), dur: len("DURATION"), result: len("RESULT"), mark: 0}
	now := parseStamp(generatedAt)
	for _, job := range jobs {
		w.name = max(w.name, len([]rune(job.Name)))
		mark, _ := markFor(job.State, style)
		w.mark = max(w.mark, len([]rune(mark)))
		last, dur := whenCells(job, now)
		w.last = max(w.last, len([]rune(last)))
		w.dur = max(w.dur, len([]rune(dur)))
		w.result = max(w.result, len([]rune(resultText(job, style))))
	}
	return w
}

// whenCells are the LAST and DURATION cells of one row.
func whenCells(job Job, now time.Time) (last, dur string) {
	if job.LastRun == nil {
		return "", ""
	}
	last = relWhen(now, parseStamp(job.LastRun.FinishedAt))
	dur = compactDuration(time.Duration(job.LastRun.DurationMS) * time.Millisecond)
	return last, dur
}

// markFor picks the row mark and its colour for a state.
func markFor(state string, style Style) (mark, colour string) {
	switch state {
	case StateFailed, StateSLABreached:
		return style.Fail, colorRed
	case StateStuck:
		return style.Warn, colorYellow
	case StatePaused:
		return style.Pause, ""
	case StateIdle:
		return style.Dash, ""
	default:
		return style.OK, colorGreen
	}
}

// resultText is the RESULT cell: the state word, with a failure's reason code
// when one is known.
func resultText(job Job, style Style) string {
	switch job.State {
	case StateOK:
		return "ok"
	case StateIdle:
		return "no runs yet"
	case StatePaused:
		return "paused"
	case StateFailed:
		reason := ""
		if job.LastRun != nil && job.LastRun.ReasonCode != "" {
			reason = " (" + job.LastRun.ReasonCode + ")"
		}
		return "FAILED" + reason
	case StateStuck:
		return "STUCK"
	case StateSLABreached:
		return "SLA BREACHED"
	}
	return job.State
}

// nextText is the NEXT cell: the pending fire-time, the sensor cadence, or
// the pause word.
func nextText(job Job, style Style, now time.Time) string {
	switch {
	case job.State == StatePaused:
		return "paused"
	case job.NextRunAt != "":
		return relWhen(now, parseStamp(job.NextRunAt))
	case job.SensorIntervalMS > 0:
		return fmt.Sprintf("sensor every %s", compactDuration(time.Duration(job.SensorIntervalMS)*time.Millisecond))
	default:
		return style.Dash
	}
}

// relWhen says roughly when something happened or will happen, on the clock
// the command was handed: clock time inside today and yesterday and tomorrow,
// day counts beyond that.
func relWhen(now, then time.Time) string {
	if then.IsZero() {
		return ""
	}
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	days := float64(then.Sub(today)) / float64(24*time.Hour)
	clock_ := then.Format("15:04")
	switch whole := int(math.Floor(days)); whole {
	case 0:
		return clock_
	case 1:
		return "tomorrow " + clock_
	case -1:
		return "yesterday " + clock_
	default:
		if whole < 0 {
			return fmt.Sprintf("%dd ago", -whole)
		}
		return fmt.Sprintf("in %dd", whole)
	}
}

// compactDuration renders a duration the way the tables do: two units, no
// fractions, "0s" never "-".
func compactDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	switch {
	case d < time.Minute:
		s := int(d.Seconds())
		if s == 0 && d > 0 {
			return "<1s"
		}
		return fmt.Sprintf("%ds", s)
	case d < time.Hour:
		m := int(d.Minutes())
		s := int(d.Seconds()) % 60
		if s > 0 && d < 10*time.Minute {
			return fmt.Sprintf("%dm %ds", m, s)
		}
		return fmt.Sprintf("%dm", m)
	case d < 24*time.Hour:
		h := int(d.Hours())
		m := int(d.Minutes()) % 60
		if m > 0 {
			return fmt.Sprintf("%dh %dm", h, m)
		}
		return fmt.Sprintf("%dh", h)
	default:
		days := int(d.Hours() / 24)
		h := int(d.Hours()) % 24
		if h > 0 {
			return fmt.Sprintf("%dd %dh", days, h)
		}
		return fmt.Sprintf("%dd", days)
	}
}

// parseStamp reads back the RFC3339 stamps this package writes. An empty or
// unreadable stamp is the zero time, never an error: a table cell renders a
// dash either way.
func parseStamp(s string) time.Time {
	if s == "" {
		return time.Time{}
	}
	t, err := time.Parse("2006-01-02T15:04:05Z", s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// pad appends spaces until the text occupies width columns on screen.
func pad(text string, width int) string {
	if n := len([]rune(text)); n < width {
		return text + strings.Repeat(" ", width-n)
	}
	return text
}
