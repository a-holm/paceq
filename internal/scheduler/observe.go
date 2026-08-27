package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// Observation capture (#32): what did cron actually start, seen from outside
// paceq? The shadow report pairs these observations against Pulseq's own
// would-run ticks, so their value lives or dies with honest provenance: every
// stored observation names its source, and a source nobody could read degrades
// the report to the analytic diff instead of inventing rows.
//
// Two mechanical sources exist, both optional:
//
//   - journald: one journalctl call per sweep, any line mentioning a cron
//     start; unit names differ across distributions (cron, crond), so the
//     parser matches the shape of a start rather than a unit filter;
//   - file: a syslog-style log (/var/log/syslog, /var/log/cron, ...),
//     including the Debian, Ubuntu and RHEL layouts.
//
// Parsing is pure and fuzzer-tested; nothing here executes anything - reading
// a log has no more side effects than reading a tick.

// ObservedStart is one cron start as a log recorded it.
type ObservedStart struct {
	At      time.Time
	User    string
	Command string
	Raw     string
}

// ObserveSpec is the validated form of serve's --observe flag.
type ObserveSpec struct {
	Kind string // "none", "journald" or "file"
	Path string // file kind only
}

// Valid reports whether the spec means anything.
func (o ObserveSpec) Valid() bool {
	switch o.Kind {
	case "", "none":
		return true
	case "journald":
		return o.Path == ""
	case "file":
		return o.Path != ""
	default:
		return false
	}
}

// StoreName is the word persisted into meta and printed in reports.
func (o ObserveSpec) StoreName() string {
	if o.Kind == "" {
		return "none"
	}
	return o.Kind
}

// ParseObserveSpec turns --observe none | journald | file=/path/log into its
// validated form. Everything else is a usage error: a silent fallback to
// "none" would quietly hollow out the report's comparison side.
func ParseObserveSpec(flag string) (ObserveSpec, error) {
	flag = strings.TrimSpace(flag)
	spec := ObserveSpec{}
	switch {
	case flag == "", flag == "none":
		spec.Kind = "none"
	case flag == "journald":
		spec.Kind = "journald"
	case strings.HasPrefix(flag, "file="):
		spec.Kind = "file"
		path := strings.TrimPrefix(flag, "file=")
		if path == "" {
			return spec, errors.New("--observe file= needs a path")
		}
		spec.Path = path
	default:
		return spec, fmt.Errorf("--observe must be none, journald or file=<path>, got %q", flag)
	}
	if !spec.Valid() {
		return spec, fmt.Errorf("--observe %q does not define a usable source", flag)
	}
	return spec, nil
}

// ObservationStore is what the sweeper writes through.
type ObservationStore interface {
	JobCommandsForMatching(ctx context.Context) ([]store.JobCommandRef, error)
	InsertShadowObservation(ctx context.Context, o store.ShadowObservation) (bool, error)
}

// ObservationSource is one log world. Tests stub it; production has
// JournaldSource and FileSource below.
type ObservationSource interface {
	// Name matches the stored source marker (journald/file).
	Name() string
	Fetch(ctx context.Context, since time.Time) ([]ObservedStart, error)
}

// ObserveInterval is how often a running shadow instance re-reads its log
// source. One minute matches the finest cron granularity; missing nothing
// matters more than saving a read.
const ObserveInterval = time.Minute

// ObserveOverlap refetches this far before the stored watermark, so a slow
// writer appending between sweeps cannot be lost between windows. The UNIQUE
// key on (source, observed_at, raw) makes the double-read free.
const ObserveOverlap = 5 * time.Minute

// FirstFetchWindow is how far back the very first sweep looks after a fresh
// boot. Further back waits for `paceq import` of history nobody can prove.
const FirstFetchWindow = time.Hour

// Sweep fetches new observations, matches them against imported jobs and
// stores what it found. It returns how many rows were newly inserted. Every
// failure is returned; the caller logs and continues, because one unreadable
// log line must never kill the daemon.
func Sweep(ctx context.Context, st ObservationStore, src ObservationSource,
	watermark time.Time, clkNow time.Time,
) (int, error) {
	since := watermark.Add(-ObserveOverlap)
	if watermark.IsZero() {
		since = clkNow.Add(-FirstFetchWindow)
	}
	starts, err := src.Fetch(ctx, since)
	if err != nil {
		return 0, err
	}
	refs, err := st.JobCommandsForMatching(ctx)
	if err != nil {
		return 0, err
	}
	inserted := 0
	for _, s := range starts {
		o := store.ShadowObservation{
			ObservedAt: s.At.UTC(),
			Source:     src.Name(),
			Raw:        s.Raw,
			Command:    s.Command,
			CronUser:   s.User,
		}
		if job := MatchedJob(s.Command, refs); job != "" {
			o.JobName, o.HasJob = job, true
		}
		ok, err := st.InsertShadowObservation(ctx, o)
		if err != nil {
			return inserted, err
		}
		if ok {
			inserted++
		}
	}
	return inserted, nil
}

// startLinePattern markers. A logged cron start looks like
// `(user) CMD (command)` - Ubuntu and Debian spell CRON, RHEL spells CROND,
// and some setups run cron under a binary whose comm name varies. The shape
// after the colon is what counts.
var cmdPrefixes = [...]string{"CMD(", "CMD ("}

// extractCommand finds the command inside a log suffix like
// `(johan) CMD (/usr/bin/backup.sh >> /var/log/b)`. It returns ok=false on
// every line that is not a start record: outputs (CMDOUT), begin/end notices
// and session noise all fail the exact prefix test.
func extractCommand(afterColon string) (user, command string, ok bool) {
	idx := strings.Index(afterColon, "(")
	if idx < 0 {
		return "", "", false
	}
	rest := afterColon[idx:]
	end := strings.Index(rest, ") ")
	if end < 0 {
		return "", "", false
	}
	user = strings.TrimSuffix(strings.TrimPrefix(rest[:end], "("), ")")
	if user == "" || strings.ContainsAny(user, " \t") {
		return "", "", false
	}
	body := rest[end+2:]
	for _, p := range cmdPrefixes {
		if !strings.HasPrefix(body, p) {
			continue
		}
		cmd := strings.TrimPrefix(body, p)
		cmd = strings.TrimSuffix(cmd, ")")
		if cmd == "" {
			return "", "", false
		}
		return user, cmd, true
	}
	return "", "", false
}

// ParseJournalLine parses one `journalctl -o short-iso` row:
//
//	2027-01-04T06:00:01.123456+0100 host CRON[5432]: (johan) CMD (backup.sh)
//
// The timestamp carries its zone, so no inference happens here.
func ParseJournalLine(line string) (ObservedStart, bool) {
	colon := strings.Index(line, ": ")
	if colon < 4 {
		return ObservedStart{}, false
	}
	head := line[:colon]
	stamp, comm := splitHead(head)
	if comm == "" {
		return ObservedStart{}, false
	}
	if !isCronComm(comm) {
		return ObservedStart{}, false
	}
	at, err := parseJournalStamp(stamp)
	if err != nil {
		return ObservedStart{}, false
	}
	user, cmd, ok := extractCommand(line[colon+2:])
	if !ok {
		return ObservedStart{}, false
	}
	return ObservedStart{At: at, User: user, Command: cmd, Raw: line}, true
}

// splitHead takes `2027-01-04T06:00:01+0100 host CRON[5432]` apart into its
// stamp and its process tag; the tag must be the last whitespace word.
func splitHead(head string) (stamp, comm string) {
	fields := strings.Fields(head)
	if len(fields) < 2 {
		return "", ""
	}
	return fields[0], fields[len(fields)-1]
}

// isCronComm reports whether a logged process tag is one cron writes starts
// under. Anything else - CMDOUT records included - fails and is skipped.
func isCronComm(tag string) bool {
	open := strings.IndexByte(tag, '[')
	if open > 0 {
		tag = tag[:open]
	}
	switch strings.ToLower(tag) {
	case "cron", "crond", "cronie":
		return true
	default:
		return false
	}
}

// parseJournalStamp accepts `2006-01-02T15:04:05+0100`,
// `...05.123456+0100` and `...05Z`.
func parseJournalStamp(stamp string) (time.Time, error) {
	base := "2006-01-02T15:04:05"
	if strings.HasSuffix(stamp, "Z") {
		t, err := time.Parse(base+"Z", stamp)
		if err != nil {
			return time.Time{}, err
		}
		return t.UTC(), nil
	}
	dot := strings.IndexByte(stamp, '.')
	if dot > 0 {
		rest := stamp[dot+1:]
		digits := 0
		for digits < len(rest) && rest[digits] >= '0' && rest[digits] <= '9' {
			digits++
		}
		if digits == 0 || digits+1 >= len(rest) {
			return time.Time{}, fmt.Errorf("unreadable fraction in %q", stamp)
		}
		core, zone := stamp[:dot], rest[digits:]
		nines := ""
		for i := 0; i < digits; i++ {
			nines += "9"
		}
		return time.Parse(base+"."+nines+"-0700", core+"."+strings.Repeat("0", digits)+zone)
	}
	return time.Parse(base+"-0700", stamp)
}

// monthIndex maps English abbreviations used by classic syslog to months.
var monthIndex = map[string]int{
	"Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
	"Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}

// ParseSyslogLine parses one classic syslog row, the layouts
//
//	Jan  4 06:00:01 host cron[543]: (johan) CMD (backup.sh)
//	Jan  4 06:00:01 host CROND[543]: (root) CMD (/bin/foo bar baz)
//
// Classic syslog has no year and no zone: the year is inferred so the result
// lies within two days of now (the natural wrap across New Year), and the
// time is taken in the machine's local zone - which is exactly the zone cron
// scheduled in on the machine that wrote the log.
func ParseSyslogLine(line string, now time.Time) (ObservedStart, bool) {
	colon := strings.LastIndex(line, ": ")
	if colon < 0 {
		return ObservedStart{}, false
	}
	comm := line[:colon]
	commTag := strings.ToLower(lastWordComm(comm))
	if commTag != "cron" && commTag != "crond" && commTag != "cronie" {
		return ObservedStart{}, false
	}
	// `Jan  4 06:00:01 host cron[543]` - Fields survives the Debian
	// double-space before single-digit days, which SplitN on spaces would
	// turn into an empty day.
	head := strings.Fields(comm)
	if len(head) < 3 {
		return ObservedStart{}, false
	}
	mon, ok := monthIndex[head[0]]
	if !ok {
		return ObservedStart{}, false
	}
	day, hms := head[1], head[2]
	user, cmd, ok := extractCommand(line[colon+2:])
	if !ok {
		return ObservedStart{}, false
	}
	at, ok := inferYearLocal(mon, day, hms, now)
	if !ok {
		return ObservedStart{}, false
	}
	return ObservedStart{At: at, User: user, Command: cmd, Raw: line}, true
}

// lastWordComm takes `Jan  4 06:00:01 host cron[543]` down to `cron`, the
// program name of the last whitespace word with an optional pid suffix off.
func lastWordComm(s string) string {
	i := strings.LastIndexByte(s, ' ')
	if i >= 0 {
		s = s[i+1:]
	}
	if j := strings.IndexByte(s, '['); j > 0 {
		s = s[:j]
	}
	return s
}

// inferYearLocal builds a local timestamp from month/day/hh:mm:ss. Classic
// syslog carries no year: the candidates that can be meant are this year or
// the neighbours across New Year, and the most recent instant no further than
// six hours into the future wins.
func inferYearLocal(mon int, day, hms string, now time.Time) (time.Time, bool) {
	clockParts := strings.Split(hms, ":")
	if len(clockParts) != 3 {
		return time.Time{}, false
	}
	dayNum := trimInt(day)
	if dayNum <= 0 || dayNum > 31 {
		return time.Time{}, false
	}
	var best time.Time
	found := false
	for _, cand := range [...]int{now.Year(), now.Year() - 1, now.Year() + 1} {
		t, err := time.ParseInLocation("2006-01-02 15:04:05",
			fmt.Sprintf("%04d-%02d-%02d %s", cand, mon, dayNum, hms), now.Location())
		if err != nil {
			continue
		}
		if t.After(now.Add(6*time.Hour)) || (found && !t.After(best)) {
			continue
		}
		best, found = t, true
	}
	return best, found
}

func trimInt(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return n
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// ---- concrete sources ------------------------------------------------------

// JournaldSource reads observed starts from the systemd journal.
type JournaldSource struct{}

// Name implements ObservationSource.
func (JournaldSource) Name() string { return store.ObsSourceJournald }

// Fetch runs journalctl once over the requested window. Any failure to find
// or invoke the binary is reported, never swallowed: the report says which
// source actually fed it.
func (JournaldSource) Fetch(ctx context.Context, since time.Time) ([]ObservedStart, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	args := []string{"--quiet", "--output", "short-iso", "--since=" + since.Format(time.RFC3339)}
	out, err := exec.CommandContext(ctx, "journalctl", args...).Output() // #nosec G204 - the binary is the fixed name "journalctl"; every argument is a constant flag or an RFC3339 stamp, and resolving it through PATH is what lets tests plant a stub
	if err != nil {
		return nil, fmt.Errorf("read journald: %w", err)
	}
	return SplitParseJournal(string(out)), nil
}

// SplitParseJournal parses a whole journalctl export.
func SplitParseJournal(text string) []ObservedStart {
	var out []ObservedStart
	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "" {
			continue
		}
		if s, ok := ParseJournalLine(line); ok {
			out = append(out, s)
		}
	}
	return out
}

// FileSource reads a syslog-format log file wholesale each sweep.
type FileSource struct {
	Path string

	// Now supplies the reference instant the yearless syslog stamps are
	// inferred against. Zero uses the system clock.
	Now func() time.Time
}

// Name implements ObservationSource.
func (f FileSource) Name() string { return store.ObsSourceFile }

// Fetch re-parses the file and filters on the window. Logs rotate under us -
// a vanished file between sweeps is reported once and swallowed no further.
func (f FileSource) Fetch(_ context.Context, since time.Time) ([]ObservedStart, error) {
	b, err := os.ReadFile(f.Path) // #nosec G304 - the path comes from the operator's own --observe flag, never from job input
	if err != nil {
		return nil, fmt.Errorf("read the observed log %s: %w", f.Path, err)
	}
	now := clock.System().Now()
	if f.Now != nil {
		now = f.Now()
	}
	var out []ObservedStart
	for _, line := range strings.Split(string(b), "\n") {
		if line == "" {
			continue
		}
		s, ok := ParseSyslogLine(line, now)
		if !ok || s.At.Before(since) {
			continue
		}
		out = append(out, s)
	}
	return out, nil
}

// MatchedJob maps a logged command to an imported job name. Matching is best
// effort and conservative: exact whole-command equality first, then identical
// first token (the executable), because flags and cron's `%` stdin splitting
// reorder tails between YAML and log. Empty return means unmatched.
func MatchedJob(command string, refs []store.JobCommandRef) string {
	command = strings.TrimSpace(command)
	if command == "" || len(refs) == 0 {
		return ""
	}
	head := firstToken(command)
	for _, r := range refs {
		if r.Command == command {
			return r.JobName
		}
	}
	for _, r := range refs {
		if firstToken(r.Command) != "" && firstToken(r.Command) == head {
			return r.JobName
		}
	}
	return ""
}

func firstToken(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexAny(s, " \t"); i >= 0 {
		s = s[:i]
	}
	return s
}
