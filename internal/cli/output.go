package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/a-holm/paceq/internal/doctor"
)

// outputMode is how a command renders its result.
type outputMode int

const (
	// modeText is for a human at a terminal: symbols, colour, aligned columns.
	modeText outputMode = iota
	// modeJSON is for everything else. It is the mode a pipe gets, so a script
	// never has to parse text meant for people (03 section 7.1).
	modeJSON
)

// symbolSet is the marks a report is drawn with. Two sets exist because a
// terminal that cannot render UTF-8 shows a box or a question mark, which reads
// as damage rather than as a check that passed.
type symbolSet struct {
	ok    string
	warn  string
	fail  string
	arrow string
}

var (
	unicodeSymbols = symbolSet{ok: "✓", warn: "⚠", fail: "✗", arrow: "→"}
	asciiSymbols   = symbolSet{ok: "OK", warn: "WARN", fail: "FAIL", arrow: "->"}
)

func symbols(unicode bool) symbolSet {
	if unicode {
		return unicodeSymbols
	}
	return asciiSymbols
}

// ANSI colours, written directly rather than through a library: three colours
// and a reset is the whole need, and a terminal styling dependency would cost a
// slot in a budget of eight.
const (
	colorReset  = "\x1b[0m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorRed    = "\x1b[31m"
)

// ui is where a command writes. It holds the decisions taken once per run:
// which mode, whether colour is wanted, which symbols the terminal can show.
type ui struct {
	out     io.Writer
	err     io.Writer
	mode    outputMode
	color   bool
	symbols symbolSet
	quiet   bool
	verbose int
}

// json writes one document to stdout, and nothing else ever goes there in this
// mode: `paceq doctor -o json | jq` has to work with -v on.
func (u *ui) json(document any) error {
	encoder := json.NewEncoder(u.out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(document); err != nil {
		return internalError("could not write the JSON output", err)
	}
	return nil
}

// print writes a line of the human report. Quiet drops it: -q is for the
// scripts and login shells that only want to hear about problems.
func (u *ui) print(format string, args ...any) {
	if u.quiet {
		return
	}
	fmt.Fprintf(u.out, format+"\n", args...)
}

// note writes progress to stderr, at a verbosity the user asked for. Notes are
// never data: they exist so a run that goes wrong can be followed, and stdout
// stays parseable while they are on.
func (u *ui) note(level int, format string, args ...any) {
	if u.verbose < level {
		return
	}
	fmt.Fprintf(u.err, "paceq: "+format+"\n", args...)
}

// mark is the symbol for a finding, padded to width and then coloured. The
// order matters: padding a coloured string would count the escape sequences,
// which take no space on screen, and the columns would drift.
func (u *ui) mark(level doctor.Level, width int) string {
	symbol, colour := u.symbols.ok, colorGreen
	switch level {
	case doctor.Warn:
		symbol, colour = u.symbols.warn, colorYellow
	case doctor.Fail:
		symbol, colour = u.symbols.fail, colorRed
	}
	text := pad(symbol, width)
	if u.color {
		return colour + text + colorReset
	}
	return text
}

// markWidth is how far the symbol column is padded, measured on the widest
// symbol in the set.
func (u *ui) markWidth() int {
	width := 0
	for _, symbol := range []string{u.symbols.ok, u.symbols.warn, u.symbols.fail} {
		if n := len([]rune(symbol)); n > width {
			width = n
		}
	}
	return width
}

// pad appends spaces until the text occupies width columns on screen.
func pad(text string, width int) string {
	if n := len([]rune(text)); n < width {
		return text + strings.Repeat(" ", width-n)
	}
	return text
}

// isTerminal reports whether a writer is a terminal. Everything else, a pipe, a
// file, /dev/null, is not, which is the whole rule behind text versus JSON.
func isTerminal(w io.Writer) bool {
	file, ok := w.(interface{ Fd() uintptr })
	if !ok {
		return false
	}
	return isTTY(file.Fd())
}

// unicodeOutput reads the locale the way every terminal program does. No
// UTF-8 in the locale means the symbols are drawn in ASCII.
func unicodeOutput(env Env) bool {
	for _, name := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		value := env.Getenv(name)
		if value == "" {
			continue
		}
		normalised := strings.ToLower(strings.ReplaceAll(value, "-", ""))
		return strings.Contains(normalised, "utf8")
	}
	return false
}
