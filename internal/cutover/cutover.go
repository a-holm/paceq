// Package cutover turns a crontab over to paceq and back again.
//
// The transformation is deliberately two string operations. Comment writes a
// marker line above every imported line and prefixes the line itself with
// exactly one '#'. Uncomment finds our markers, removes them, and removes
// exactly one '#' from the line beneath - never re-parsing, never
// re-formatting, so a rollback is bit-identical with the state before the
// cutover. The package holds no I/O: Read, Write and Backup against
// crontab(1) live beside these functions, and the CLI decides what runs.
//
// The marker format is the whole contract, and it is machine-readable in
// both directions:
//
//	# pulseq:cutover 2027-01-12T09:14:03+01:00 job=backup-db
//	#0 3 * * * /opt/backup/dump.sh >> /var/log/backup.log 2>&1
//
// The marker line carries everything rollback needs - the timestamp of the
// cutover and the paceq job name - and the original line stands verbatim
// behind the '#', so restoring is removing one character. paceq never
// deletes a crontab line; commenting out keeps the line visible in
// `crontab -e`, which is where the user goes looking.
//
// Idempotence lives in the functions, not the caller:
//
//	Comment(Comment(c)) == Comment(c)
//	Uncomment(Uncomment(c)) == Uncomment(c)
//	Uncomment(Comment(c)) == c
package cutover

import (
	"strings"
	"time"
)

// MarkerPrefix marks a cutover marker line. The prefix is reserved: a line
// starting with it belongs to this package's round trip, in both
// directions.
const MarkerPrefix = "# pulseq:cutover "

// Job is one imported job and the crontab line it was translated from. The
// origin is the raw line exactly as the import recorded it, one line, with
// no line terminator.
type Job struct {
	Name   string
	Origin string
}

// Change is one line a pass would rewrite or did rewrite. LineNumber is the
// line's one-based position in the content the pass was given, markers not
// counted, which is the numbering `crontab -l` prints before the edit.
type Change struct {
	JobName    string
	LineNumber int
	// When is the cutover timestamp the marker carried; zero for a
	// Change that has no marker behind it.
	When time.Time
	// Line is the original line, verbatim: for a Comment the line that
	// got the '#', for an Uncomment the restored line.
	Line string
	// Marker is the marker line a Comment writes above Line. An
	// Uncomment reports the marker it removed.
	Marker string
}

// SkipReason says why a job was left alone.
type SkipReason int

const (
	// SkipAlreadyCut: a marker for this job is already in the crontab.
	// Re-running cutover finds these, changes nothing, and the command
	// reports "nothing to do" with exit 0.
	SkipAlreadyCut SkipReason = iota
	// SkipChanged: a line resembling the origin differs from it - the
	// crontab moved on since the import (PSQ-CUT-003).
	SkipChanged
	// SkipMissing: no line in the crontab resembles the origin. It may
	// have been removed by hand, or the job never came from this file.
	SkipMissing
)

// Skip is one job a pass left alone, and the evidence for why.
type Skip struct {
	JobName string
	Reason  SkipReason
	// Line and LineNumber carry the deciding line for SkipChanged;
	// LineNumber is 0 when no line was involved.
	Line       string
	LineNumber int
}

// Code is the stable error code the report renders beside the reason, the
// way 03's message anatomy asks for: a code a script can grep, not a
// sentence a human has to re-read. SkipAlreadyCut carries no code: being
// cut over already is the ordinary state a repeated cutover meets, not a
// fault. The other two codes live beside their CLI-side siblings
// PSQ-CUT-001 (no successful run yet) and PSQ-CUT-004 (no recorded
// origin).
func (r SkipReason) Code() string {
	switch r {
	case SkipChanged:
		return "PSQ-CUT-003"
	case SkipMissing:
		return "PSQ-CUT-002"
	default:
		return ""
	}
}

// String is the human text under the code. It states the fact, never the
// feeling; the command adds the next step.
func (r SkipReason) String() string {
	switch r {
	case SkipChanged:
		return "crontab line changed since the import"
	case SkipMissing:
		return "line not found in the crontab"
	default:
		return "already cut over"
	}
}

// MarkerLine renders the marker for one job at one instant. The timestamp
// is RFC 3339 in the clock's own zone, which is what a human compares
// against `date` output.
func MarkerLine(now time.Time, job string) string {
	return MarkerPrefix + now.Format(time.RFC3339) + " job=" + job
}

// ParseMarker splits a marker line into its timestamp and job name. ok is
// false for every line that is not a marker, whatever else it resembles.
func ParseMarker(line string) (when time.Time, job string, ok bool) {
	rest, found := strings.CutPrefix(line, MarkerPrefix)
	if !found {
		return time.Time{}, "", false
	}
	stamp, jobPart, _ := strings.Cut(rest, " job=")
	parsed, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		return time.Time{}, "", false
	}
	return parsed, jobPart, true
}

// Comment returns content with every job's line commented out, the changes
// it made, and the jobs it had to leave alone. Jobs already cut over are
// skipped, which is what makes a repeated cutover a no-op instead of a
// double comment. A line that matches no job's origin is copied through
// byte for byte, comments, blank lines, environment assignments and lines
// paceq does not own included.
//
// Matching is on the origin exactly as import recorded it: a byte-identical
// line always matches, and so does a line whose only difference is spacing,
// because cron reads those identically and the line under the marker is
// preserved verbatim either way. Anything else - a command with a new flag,
// a different minute - is not the line that was imported, is never touched,
// and comes back as a Skip the caller reports.
//
// Line endings are normalised to "\n" on the way in, the same reading the
// import uses; a crontab with CRLF endings is already unreadable to cron
// and is repaired rather than preserved in its breakage.
func Comment(content string, jobs []Job, now time.Time) (string, []Change, []Skip) {
	lines := splitLines(content)

	// Pass one: which jobs are already cut over. Their presence is what
	// turns a repeated cutover into "nothing to do".
	already := map[string]bool{}
	for _, line := range lines {
		if _, job, ok := ParseMarker(line); ok {
			already[job] = true
		}
	}

	// Pass two: rewrite. A job consumes the first line that matches it,
	// so two identical lines cut over two jobs and never mark one line
	// twice.
	var b strings.Builder
	var changes []Change
	matched := map[string]bool{}
	for i, line := range lines {
		name := ""
		for _, job := range jobs {
			if matched[job.Name] {
				continue
			}
			if lineEqualsOrigin(line, job.Origin) {
				name = job.Name
				break
			}
		}
		if name == "" {
			b.WriteString(line)
			b.WriteString("\n")
			continue
		}
		marker := MarkerLine(now, name)
		b.WriteString(marker)
		b.WriteString("\n")
		b.WriteString("#")
		b.WriteString(line)
		b.WriteString("\n")
		matched[name] = true
		changes = append(changes, Change{
			JobName:    name,
			LineNumber: i + 1,
			When:       now,
			Line:       line,
			Marker:     marker,
		})
	}

	// Pass three: report the jobs that stayed.
	var skips []Skip
	for _, job := range jobs {
		if matched[job.Name] {
			continue
		}
		if already[job.Name] {
			skips = append(skips, Skip{JobName: job.Name, Reason: SkipAlreadyCut})
			continue
		}
		if line, number, ok := findChanged(lines, job.Origin); ok {
			skips = append(skips, Skip{
				JobName: job.Name, Reason: SkipChanged,
				Line: line, LineNumber: number,
			})
			continue
		}
		skips = append(skips, Skip{JobName: job.Name, Reason: SkipMissing})
	}
	return b.String(), changes, skips
}

// Uncomment removes our markers and, for each, exactly one '#' from the
// line beneath. It is the exact inverse of Comment on our own output: every
// other byte travels through untouched. only narrows the pass to the named
// jobs; empty means every marker paceq finds.
//
// A marker whose line beneath has lost its '#' - an edit between the two
// passes - loses only the marker; the line itself is never guessed at. A
// marker for a filtered-out job survives with its line, so a partial
// rollback composes with the markers a later pass still needs.
func Uncomment(content string, only []string) (string, []Change) {
	filter := map[string]bool{}
	for _, name := range only {
		filter[name] = true
	}

	lines := splitLines(content)
	var b strings.Builder
	var changes []Change
	for i := 0; i < len(lines); i++ {
		when, job, ok := ParseMarker(lines[i])
		if !ok || (len(filter) > 0 && !filter[job]) {
			b.WriteString(lines[i])
			b.WriteString("\n")
			continue
		}
		change := Change{
			JobName:    job,
			LineNumber: i + 1,
			When:       when,
			Line:       "",
			Marker:     lines[i],
		}
		if i+1 < len(lines) && strings.HasPrefix(lines[i+1], "#") {
			change.Line = strings.TrimPrefix(lines[i+1], "#")
			i++
		} else {
			// Orphan marker: nothing beneath to restore.
			change.Line = ""
		}
		b.WriteString(change.Line)
		b.WriteString("\n")
		changes = append(changes, change)
	}
	return b.String(), changes
}

// HasMarkers reports whether content carries at least one cutover marker.
// It is how --status and --rollback tell "rolled back already" from "never
// cut over".
func HasMarkers(content string) bool {
	for _, line := range splitLines(content) {
		if _, _, ok := ParseMarker(line); ok {
			return true
		}
	}
	return false
}

// MarkerJobs lists the job names of every marker in content, in order of
// appearance, without deduplication: two markers for one job mean two
// cutovers in the file's history.
func MarkerJobs(content string) []Marker {
	var out []Marker
	for i, line := range splitLines(content) {
		when, job, ok := ParseMarker(line)
		if !ok {
			continue
		}
		out = append(out, Marker{Job: job, When: when, LineNumber: i + 1})
	}
	return out
}

// Marker is one cutover marker found in a crontab.
type Marker struct {
	Job        string
	When       time.Time
	LineNumber int
}

// lineEqualsOrigin reports whether a crontab line is the job's origin: byte
// equal, or equal once whitespace runs collapse, because cron gives those
// the same meaning and the line is preserved verbatim under the marker.
func lineEqualsOrigin(line, origin string) bool {
	if line == origin {
		return true
	}
	if line == "" || origin == "" {
		return false
	}
	return normalise(line) == normalise(origin)
}

// normalise collapses whitespace the way cron's field scanner reads it.
func normalise(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// findChanged looks for the line a changed job became: an entry line that
// resembles the origin but is no longer it. Resemblance is narrow, because
// a false accusation here tells the user their file is wrong when it is
// merely unfamiliar: either the schedule fields match and the commands
// drifted, or the commands match and the schedule moved. Anything looser is
// reported as absent (PSQ-CUT-002), which is the honest answer when no
// line resembles the origin at all.
func findChanged(lines []string, origin string) (string, int, bool) {
	originHead, originCmd := splitEntry(origin)
	if originHead == "" && originCmd == "" {
		return "", 0, false
	}
	for i, line := range lines {
		head, cmd := splitEntry(line)
		if head == "" && cmd == "" {
			continue // blank, comment or env: not an entry
		}
		if normalise(line) == normalise(origin) {
			continue // equal is a match, not a change
		}
		sameSchedule := head != "" && head == originHead
		sameCommand := cmd != "" && cmd == originCmd
		prefixDrift := cmd != "" && originCmd != "" &&
			(strings.HasPrefix(cmd, originCmd) || strings.HasPrefix(originCmd, cmd))
		if (sameSchedule && prefixDrift) || (sameCommand && head != originHead) {
			return line, i + 1, true
		}
	}
	return "", 0, false
}

// splitEntry splits a cron entry into its five schedule fields and its
// command, both normalised, or two empty strings when the line is not an
// entry: blank, a comment, an environment assignment, or too short to be
// schedule and command.
func splitEntry(line string) (head, command string) {
	fields := strings.Fields(line)
	if len(fields) < 6 {
		return "", ""
	}
	if strings.HasPrefix(line, "#") {
		return "", ""
	}
	for i := 0; i < 5; i++ {
		if !isCronToken(fields[i]) {
			return "", ""
		}
	}
	return strings.Join(fields[:5], " "), strings.Join(fields[5:], " ")
}

// isCronToken reports whether a word can sit in a cron schedule field:
// digits, names, and the range, step and list punctuation between them.
func isCronToken(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z',
			c == '-', c == '*', c == '/', c == ',', c == '?':
		default:
			return false
		}
	}
	return true
}

// splitLines cuts content into lines the way cron and the import read it:
// CRLF becomes LF, the trailing terminator does not produce a trailing
// empty line, and every other byte stays where it is. Reassembly joins
// with "\n", which is why the round trip is exact for anything cron can
// read.
func splitLines(content string) []string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	content = strings.TrimSuffix(content, "\n")
	if content == "" {
		return nil
	}
	return strings.Split(content, "\n")
}
