package explain

import (
	"fmt"
	"io"
	"strings"
	"time"
)

// The text form: the same Report the JSON mode writes, drawn as prose for a
// person at a terminal. Every symbol comes from a Style, so a terminal
// without UTF-8 gets plain ASCII instead of boxes and question marks.

// Style is the mark set one rendering draws with.
type Style struct {
	Tick    string // check for a good outcome
	Warn    string // attention without failure
	Fail    string // failure
	Arrow   string // "what to do next"
	Times   string // multiplication sign for coalesced repeats
	Unicode bool
}

// StyleUnicode is the mark set for a UTF-8 locale.
func StyleUnicode() Style {
	return Style{Tick: "✓", Warn: "⚠", Fail: "✗", Arrow: "→", Times: "×", Unicode: true}
}

// StyleASCII is the fallback for every other locale: words instead of marks,
// per 03 section 7.1.
func StyleASCII() Style {
	return Style{Tick: "OK", Warn: "WARN", Fail: "FAIL", Arrow: "->", Times: "x"}
}

// RenderText draws one report as prose. Data goes to out; nothing else ever
// writes there, so `paceq explain ... | jq` stays clean even when it warns.
func RenderText(out io.Writer, report *Report, style Style) {
	subject := report.Subject
	switch subject.Kind {
	case "job":
		renderJobHeader(out, report)
	case "schedule":
		renderScheduleHeader(out, report)
	case "sensor":
		renderSensorHeader(out, report)
	case "run":
		renderRunReport(out, report, style)
		return
	}
	renderSummary(out, report, style)
	renderTimeline(out, report.Entries, style, 2)
	if len(report.Entries) == 0 {
		renderEmptyHistory(out, report)
	}
}

func renderJobHeader(out io.Writer, r *Report) {
	name := r.Subject.Job
	fmt.Fprintf(out, "job %s\n", name)
}

func renderScheduleHeader(out io.Writer, r *Report) {
	fmt.Fprintf(out, "schedule %s.%s (of job %s)\n", r.Subject.Job, r.Subject.Schedule, r.Subject.Job)
}

func renderSensorHeader(out io.Writer, r *Report) {
	fmt.Fprintf(out, "sensor %s (of job %s)\n", r.Subject.Sensor, r.Subject.Job)
}

// renderSummary draws the headline: last success, last outcome, next due,
// pause state, concurrency. An all-clear answer lives here first.
func renderSummary(out io.Writer, r *Report, style Style) {
	s := r.Summary
	now := time.UnixMilli(r.GeneratedAt).UTC()

	var b strings.Builder
	b.WriteString("summary:")
	if s.LastSuccessAt != nil {
		at := time.UnixMilli(*s.LastSuccessAt).UTC()
		fmt.Fprintf(&b, " last succeeded %s (%s ago),", at.Format("2006-01-02 15:04"), humanDuration(now.Sub(at).Milliseconds()))
	} else {
		b.WriteString(" never succeeded,")
	}
	if s.LastOutcome != "" {
		fmt.Fprintf(&b, " last outcome %s,", s.LastOutcome)
	}
	if s.NextTickAt != nil {
		next := time.UnixMilli(*s.NextTickAt).UTC()
		fmt.Fprintf(&b, " next %s (%s from now),", next.Format("2006-01-02 15:04"), humanDuration(*s.NextTickAt-r.GeneratedAt))
	}
	line := strings.TrimSuffix(b.String(), ",")
	line += fmt.Sprintf(" paused=%t active runs %d/%d freshness=%s",
		s.Paused, s.ActiveRuns, s.MaxConcurrent, s.FreshnessState)
	fmt.Fprintln(out, line)
	if s.Shadow || s.InstanceShadow {
		fmt.Fprintln(out, "SHADOW MODE: nothing executes - the ticks above are recorded decisions, not runs")
	}
	fmt.Fprintln(out, "")
}

// renderTimeline draws the decision list. Indentation carries nesting:
// timeline entries at two spaces, their children deeper.
func renderTimeline(out io.Writer, entries []Entry, style Style, indent int) {
	pad := strings.Repeat(" ", indent)
	for i := range entries {
		e := &entries[i]
		mark := style.Tick
		switch e.Outcome {
		case "skipped", "deduped", "missed", "cancelled", "pending", "queued":
			mark = style.Warn
		case "error", "failed", "rejected":
			mark = style.Fail
		case "running":
			mark = style.Warn
		}
		when := time.UnixMilli(e.At).UTC().Format("2006-01-02 15:04:05Z")

		head := fmt.Sprintf("%s%s  %-7s %-12s", pad, when, e.Outcome, e.Kind)
		if e.Ref != "" {
			ref := e.Ref
			if e.Kind == "tick" || e.Kind == "run" || e.Kind == "trigger" {
				ref = shortenID(e.Ref)
			}
			head += " " + ref
		}
		if e.DurationMS != nil && *e.DurationMS > 0 {
			head += "  " + humanDuration(*e.DurationMS)
		}
		if e.RepeatCount > 1 {
			head += fmt.Sprintf("  %s%d coalesced", style.Times, e.RepeatCount-1)
		}
		fmt.Fprintf(out, "%s  %s\n", head, mark)

		if e.ReasonCode != "" {
			fmt.Fprintf(out, "%s  %s %s\n", pad, pad, e.ReasonCode)
			if e.ReasonText != "" {
				fmt.Fprintf(out, "%s  %s %s\n", pad, pad, e.ReasonText)
			}
		} else if e.ReasonText != "" {
			fmt.Fprintf(out, "%s  %s %s\n", pad, pad, e.ReasonText)
		}
		for _, hint := range e.Hints {
			fmt.Fprintf(out, "%s  %s %s %s\n", pad, pad, style.Arrow, hint)
		}
		if key := headlineData(e); key != "" {
			fmt.Fprintf(out, "%s  %s %s\n", pad, pad, key)
		}
		if len(e.Children) > 0 {
			renderTimeline(out, e.Children, style, indent+4)
		}
	}
}

// renderEmptyHistory answers a fresh install: no decisions yet is a fact
// about the future, not an empty table.
func renderEmptyHistory(out io.Writer, r *Report) {
	if r.Summary.NextTickAt != nil {
		next := time.UnixMilli(*r.Summary.NextTickAt).UTC()
		fmt.Fprintf(out, "\nno decisions recorded in this window: nothing was due before %s\n",
			next.Format("2006-01-02 15:04"))
		return
	}
	fmt.Fprintf(out, "\nno decisions recorded in this window\n")
}

// renderRunReport draws a run whole: header facts, then the step ladder, then
// the ways forward. The error tail comes from the database, so the report is
// just as fast after the log directory is gone.
func renderRunReport(out io.Writer, r *Report, style Style) {
	if len(r.Entries) == 0 {
		fmt.Fprintf(out, "no run matches %s\n", r.Subject.RunID)
		return
	}
	run := r.Entries[0]
	stateMark := style.Warn
	switch run.Outcome {
	case "succeeded":
		stateMark = style.Tick
	case "failed":
		stateMark = style.Fail
	}
	fmt.Fprintf(out, "run %s - job %s - %s %s\n", run.Ref, r.Subject.Job, run.Outcome, stateMark)

	data := run.ReasonData
	if v, ok := asInt(data["duration_ms"]); ok {
		fmt.Fprintf(out, "  duration     %s\n", humanDuration(v))
	}
	if v, ok := data["scheduled_for"].(string); ok {
		fmt.Fprintf(out, "  scheduled for %s\n", v)
	}
	if v, ok := data["run_key"].(string); ok {
		fmt.Fprintf(out, "  run_key      %s\n", v)
	}

	var steps []Entry
	for _, child := range run.Children {
		switch child.Kind {
		case "trigger":
			fmt.Fprintf(out, "  started by   trigger %s (%s)", shortenID(child.Ref), child.Actor)
			if v, ok := child.ReasonData["run_key"].(string); ok && v != "" {
				fmt.Fprintf(out, ", run_key %s", v)
			}
			fmt.Fprintln(out)
			for _, tick := range child.Children {
				fmt.Fprintf(out, "  fired by     %s tick %s at %s\n",
					tick.Actor, shortenID(tick.Ref), time.UnixMilli(tick.At).UTC().Format(time.RFC3339))
			}
		case "event":
			when := time.UnixMilli(child.At).UTC().Format("2006-01-02 15:04:05Z")
			fmt.Fprintf(out, "  event        %s %s %s\n", when, eventLabel(child.Ref), actorSuffix(child.Actor))
		case "step":
			steps = append(steps, child)
		}
	}

	for _, step := range steps {
		fmt.Fprintln(out, "")
		stepMark := style.Warn
		switch step.Outcome {
		case "succeeded":
			stepMark = style.Tick
		case "failed":
			stepMark = style.Fail
		}
		line := fmt.Sprintf("  step %s: %s %s", step.Ref, step.Outcome, stepMark)
		if d := step.DurationMS; d != nil && *d > 0 {
			line += " after " + humanDuration(*d)
		}
		fmt.Fprintln(out, line)
		if step.ReasonData != nil {
			d := step.ReasonData
			attempt, _ := asInt(d["attempt"])
			maxAttempts, _ := asInt(d["max_attempts"])
			fmt.Fprintf(out, "    attempt %d of %d", attempt, maxAttempts)
			if exit, ok := asInt(d["exit_code"]); ok {
				fmt.Fprintf(out, ", exit %d", exit)
			}
			if sig, ok := d["signal"].(string); ok {
				fmt.Fprintf(out, ", signal %s", sig)
			}
			fmt.Fprintln(out)
			if next, ok := d["next_attempt_at"].(string); ok {
				fmt.Fprintf(out, "    next attempt scheduled at %s\n", next)
			}
		}
		if step.ReasonCode != "" {
			fmt.Fprintf(out, "    %s\n", step.ReasonCode)
			if step.ReasonText != "" {
				fmt.Fprintf(out, "    %s\n", step.ReasonText)
			}
		}
		for _, hint := range step.Hints {
			fmt.Fprintf(out, "    %s %s\n", style.Arrow, hint)
		}
		if tail, ok := step.ReasonData["error_tail"].(string); ok && tail != "" {
			fmt.Fprintln(out, "    last lines of output (from the database):")
			for _, line := range strings.Split(strings.TrimRight(tail, "\n"), "\n") {
				fmt.Fprintf(out, "      %s\n", line)
			}
		}
	}

	fmt.Fprintln(out, "")
	fmt.Fprintf(out, "  %s logs:   paceq logs %s\n", style.Arrow, run.Ref)
	if run.Outcome == "failed" || run.Outcome == "cancelled" {
		fmt.Fprintf(out, "  %s retry:  paceq runs retry %s\n", style.Arrow, run.Ref)
	}
}

// eventLabel splits "run.deferred" into the word that matters.
func eventLabel(ref string) string {
	_, rest, found := strings.Cut(ref, "/")
	if !found {
		return ref
	}
	return rest
}

// actorSuffix names who acted on an event, empty for the system nobody asks about.
func actorSuffix(actor string) string {
	if actor == "" || actor == "system" {
		return ""
	}
	return "(" + actor + ")"
}

// headlineData picks the one data fact worth its own line under a timeline
// entry: where a sensor's cursor moved, or how many triggers were deduped.
func headlineData(e *Entry) string {
	if e.ReasonData == nil {
		return ""
	}
	if before, ok := e.ReasonData["cursor_before"]; ok {
		after := ""
		if v, ok := e.ReasonData["cursor_after"].(string); ok {
			after = v
		}
		b := ""
		if v, ok := before.(string); ok {
			b = v
		}
		return fmt.Sprintf("cursor %q -> %q", b, after)
	}
	if n, ok := asInt(e.ReasonData["missed_ticks"]); ok && e.Kind == "outage" {
		return fmt.Sprintf("%d schedules were due in this window", n)
	}
	if n, ok := asInt(e.ReasonData["deduped_count"]); ok && n > 0 {
		return fmt.Sprintf("%d triggers deduped onto earlier runs", n)
	}
	return ""
}

// asInt coerces a number out of a decoded detail object. Values read back
// from the database arrive through JSON as float64; values built in process
// are Go ints. Both must read.
func asInt(v any) (int64, bool) {
	switch n := v.(type) {
	case float64:
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}

// shortenID keeps ids readable and deterministic: the first ten characters of
// a ULID are its timestamp, which a frozen fixture clock makes stable, so a
// golden may pin them while the random tail stays out of the picture.
func shortenID(ref string) string {
	if len(ref) > 10 {
		return ref[:10]
	}
	return ref
}

// humanDuration renders a span the way people say it: 20h 3m, not 72180s.
func humanDuration(ms int64) string {
	if ms < 0 {
		ms = 0
	}
	d := time.Duration(ms) * time.Millisecond
	switch {
	case d < time.Second:
		return fmt.Sprintf("%dms", ms)
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	case d < time.Hour:
		return fmt.Sprintf("%dm%02ds", int(d.Minutes()), int(d.Seconds())%60)
	default:
		h := int(d.Hours())
		if h < 48 {
			return fmt.Sprintf("%dh%02dm", h, int(d.Minutes())%60)
		}
		return fmt.Sprintf("%dd%dh", h/24, h%24)
	}
}
