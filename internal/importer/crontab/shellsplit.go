package crontab

import (
	"regexp"
	"strings"
)

// shellMeta matches every character that makes a command unsafe to split
// into argv. The boundary is deliberately conservative, per 09 section 5.1:
// a mistranslated command is a silent production failure, while shell: true
// is visible in the diff and in the report. A percent sign is not in the
// class on purpose; splitPercent has already handled it by the time this
// pattern runs.
var shellMeta = regexp.MustCompile("[$`|&;<>()*?\\[\\]{}~!#\\\\]")

// toArgv splits a command into argv when that is provably safe, and says so
// when it is not. needsShell true means the caller must keep the command
// whole behind shell: true; the command text is then handed to a shell
// unchanged.
func toArgv(cmd string) (argv []string, needsShell bool) {
	if shellMeta.MatchString(cmd) {
		return nil, true
	}
	parts, ok := shlexSplit(cmd)
	if !ok || len(parts) == 0 {
		return nil, true
	}
	return parts, false
}

// shlexSplit splits a command the way a POSIX shell would, for the small
// subset of shell syntax a crontab command can use when it carries no meta
// characters at all: single quotes (nothing inside is special), double quotes
// (backslash escapes " \ and newline) and backslash outside quotes escaping
// the next character. Whitespace separates arguments.
//
// The function never fails hard. It reports ok=false when quotes are left
// open or a trailing lone backslash appears, because guessing an argument
// boundary wrong is exactly how commands get mistranslated.
func shlexSplit(cmd string) ([]string, bool) {
	var (
		args   []string
		arg    strings.Builder
		inArg  bool
		inSng  bool
		inDbl  bool
		escape bool
	)
	for i := 0; i < len(cmd); i++ {
		c := cmd[i]
		switch {
		case escape:
			arg.WriteByte(c)
			inArg = true
			escape = false
		case c == '\\' && inDbl:
			// Inside double quotes a backslash escapes only these; any
			// other one stays as written, like the shell does.
			if i+1 < len(cmd) && (cmd[i+1] == '"' || cmd[i+1] == '\\' || cmd[i+1] == '\n') {
				escape = true
			} else {
				arg.WriteByte(c)
				inArg = true
			}
		case c == '\\' && !inSng:
			if i+1 >= len(cmd) {
				return nil, false // a trailing lone backslash ends nothing
			}
			escape = true
		case c == '\'' && !inDbl:
			inSng = !inSng
			inArg = true
		case c == '"' && !inSng:
			inDbl = !inDbl
			inArg = true
		case inSng || inDbl:
			arg.WriteByte(c)
		case c == ' ' || c == '\t' || c == '\n':
			if inArg {
				args = append(args, arg.String())
				arg.Reset()
				inArg = false
			}
		default:
			arg.WriteByte(c)
			inArg = true
		}
	}
	if inSng || inDbl || escape {
		return nil, false
	}
	if inArg {
		args = append(args, arg.String())
	}
	return args, true
}

// splitPercent applies cron's percent rule to a raw crontab command: the
// first unescaped % ends the command, everything after it goes to the
// command's standard input with each further unescaped % turned into a
// newline. \% is a literal percent.
func splitPercent(cmd string) (command string, stdin string) {
	var b strings.Builder
	for i := 0; i < len(cmd); i++ {
		if cmd[i] == '\\' && i+1 < len(cmd) && cmd[i+1] == '%' {
			b.WriteByte('%')
			i++
			continue
		}
		if cmd[i] == '%' {
			rest := strings.ReplaceAll(cmd[i+1:], "%", "\n")
			return b.String(), strings.ReplaceAll(rest, "\\\n", "%")
		}
		b.WriteByte(cmd[i])
	}
	return b.String(), ""
}
