// Package crontab translates a crontab into paceq job documents.
//
// The translation is one direction only: cron lines come in, job specs go
// out. The package never writes to a crontab, never runs crontab(1) itself
// and never touches /etc/cron.d. Reading a source (the spool directory or
// `crontab -l`) belongs to the caller; this package works on bytes.
//
// The product rules it implements are 09 section 5.1: readable YAML rather
// than machine gruel, uninterpretable lines kept verbatim but still valid,
// flock replaced by max_concurrent: 1, and percent handling that matches
// cron exactly.
package crontab

import (
	"regexp"
	"strings"
)

// lineKind classifies one physical line of a crontab.
type lineKind int

const (
	kindBlank    lineKind = iota // empty or whitespace only
	kindComment                  // starts with #
	kindEnv                      // NAME=value
	kindSchedule                 // five schedule fields and a command
	kindSystem                   // six fields: a user column in front
	kindSpecial                  // @daily and friends
	kindUnknown                  // anything else; kept verbatim by translate
)

// line is one classified physical line. Text keeps the raw text with the
// trailing newline and carriage return removed, so an original line can be
// reproduced byte for byte in a comment above its job.
type line struct {
	Kind     lineKind
	Number   int    // one based, as cron and editors count lines
	Text     string // raw line without the terminator
	Comment  string // kindComment: everything after the leading #
	EnvKey   string // kindEnv
	EnvValue string // kindEnv, value part after the first =
	Special  string // kindSpecial: the word after @ ("daily", "reboot", ...)
	Command  string // what runs, for schedule, system and special lines
	User     string // kindSystem: the user column
	Schedule string // the five schedule fields, joined the way cron reads them
}

var (
	envLinePattern = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)[ \t]*=(.*)$`)
	userPattern    = regexp.MustCompile(`^[a-z_][a-z0-9_-]*$`)
	cronTokPattern = regexp.MustCompile(`^[-0-9*,/a-zA-Z]+$`)
	specialPattern = regexp.MustCompile(`^@([A-Za-z]+)(?:[ \t]+(.*))?$`)
)

// parseLines splits a crontab into classified lines, guessing per line
// whether an entry carries a user column.
func parseLines(src []byte) []line {
	return parseLinesAs(src, false)
}

// parseLinesAs is parseLines with the system-crontab reading forced on, for
// files that are known six field (/etc/crontab, /etc/cron.d). It never
// fails: a line that fits no shape comes back as kindUnknown and the
// translator decides what to do with it. That is what keeps the fuzz target
// honest, since every byte sequence produces a result.
func parseLinesAs(src []byte, sixField bool) []line {
	text := strings.ReplaceAll(string(src), "\r\n", "\n")
	text = strings.TrimSuffix(text, "\n")
	raw := strings.Split(text, "\n")

	out := make([]line, 0, len(raw))
	for i, text := range raw {
		out = append(out, classifyLineAs(i+1, text, sixField))
	}
	return out
}

// classifyLine decides what one physical line is in the per-line guessing
// mode.
func classifyLine(number int, text string) line {
	return classifyLineAs(number, text, false)
}

// classifyLineAs decides what one physical line is. The order matters:
// comment, then environment assignment, then special, then the field
// counting that tells a user crontab entry from a system one. In six-field
// mode the user column is mandatory on every entry, which is how
// /etc/crontab and /etc/cron.d are written.
func classifyLineAs(number int, text string, sixField bool) line {
	trimmed := strings.TrimSpace(text)
	ln := line{Kind: kindBlank, Number: number, Text: strings.TrimRight(text, "\r")}
	switch {
	case trimmed == "":
		return ln
	case strings.HasPrefix(trimmed, "#"):
		ln.Kind = kindComment
		ln.Comment = strings.TrimPrefix(strings.TrimPrefix(trimmed, "#"), " ")
		return ln
	}
	if m := envLinePattern.FindStringSubmatch(trimmed); m != nil {
		ln.Kind = kindEnv
		ln.EnvKey = m[1]
		ln.EnvValue = strings.TrimSpace(m[2])
		return ln
	}
	if m := specialPattern.FindStringSubmatch(trimmed); m != nil {
		ln.Kind = kindSpecial
		ln.Special = strings.ToLower(m[1])
		if len(m) > 2 {
			ln.Command = strings.TrimSpace(m[2])
		}
		if sixField && len(scanFields(ln.Command)) > 1 {
			// A system file writes "@daily root command"; the user column
			// travels on the line just as it does for five-field entries.
			words := scanFields(ln.Command)
			if userPattern.MatchString(words[0].text) {
				ln.User = words[0].text
				ln.Command = restAfter(ln.Command, words[0])
			}
		}
		return ln
	}

	fields := scanFields(trimmed)
	if sixField {
		// /etc/crontab and /etc/cron.d write `m h dom mon dow user command`:
		// the user column sits between the schedule and the command, on every
		// entry. Specials carry it too: "@daily root /usr/bin/x" runs as root.
		if len(fields) > 6 && allCronTokens(fields[:5]) && userPattern.MatchString(fields[5].text) {
			ln.Kind = kindSystem
			ln.User = fields[5].text
			ln.Schedule = joinFields(fields[:5])
			ln.Command = restAfter(trimmed, fields[5])
			return ln
		}
	}
	// A plain entry: five schedule fields and a command. Without the system
	// hint the reader never guesses a user column into a line, because
	// stealing the first command word breaks far more real crontabs than it
	// fixes; /etc-style files come in through the explicit hint instead.
	if len(fields) > 5 && noLetters(fields[0].text) && allCronTokens(fields[:5]) {
		ln.Kind = kindSchedule
		ln.Schedule = joinFields(fields[:5])
		ln.Command = restAfter(trimmed, fields[4])
		return ln
	}

	ln.Kind = kindUnknown
	ln.Command = trimmed
	return ln
}

// fieldToken is one whitespace delimited word and where it ends in the raw
// line. The offsets are why scanning exists: the command after the fifth
// field keeps whatever spacing it had, because the original text travels
// into a comment above the job and must survive byte for byte.
type fieldToken struct {
	text  string
	start int
	end   int // exclusive index into the line
}

func scanFields(s string) []fieldToken {
	var out []fieldToken
	i := 0
	for i < len(s) {
		for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
			i++
		}
		if i >= len(s) {
			break
		}
		start := i
		for i < len(s) && s[i] != ' ' && s[i] != '\t' {
			i++
		}
		out = append(out, fieldToken{text: s[start:i], start: start, end: i})
	}
	return out
}

func joinFields(tokens []fieldToken) string {
	parts := make([]string, len(tokens))
	for i, t := range tokens {
		parts[i] = t.text
	}
	return strings.Join(parts, " ")
}

// restAfter returns everything from just past the given token, with the
// leading whitespace dropped once. The rest keeps its inner spacing.
func restAfter(s string, last fieldToken) string {
	return strings.TrimSpace(s[last.end:])
}

func allCronTokens(tokens []fieldToken) bool {
	for _, t := range tokens {
		if !cronTokPattern.MatchString(t.text) {
			return false
		}
	}
	return true
}

// noLetters reports whether a token is free of letters. Minute and hour
// fields never carry month or day names, so a lettered first token means a
// system crontab's user column is sitting there instead.
func noLetters(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' {
			return false
		}
	}
	return true
}
