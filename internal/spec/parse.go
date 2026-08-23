package spec

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"

	"github.com/a-holm/paceq/internal/diag"
)

// MaxFlowMarkers bounds the [ and { characters in a file, counted on the raw
// bytes before the YAML parser runs.
//
// The reason is the parser's cost, not the schema's shape. Parsing a flow
// collection is quadratic in how deeply it nests, so a one line file of two
// hundred thousand opening brackets keeps a core busy for minutes. The exact
// depth limit below is checked on the syntax tree, which is the right place for
// it, but that check only runs once there is a tree. This one is what makes
// sure there is one.
//
// The count is taken without knowing which brackets sit inside a quoted string,
// so a file could in principle be refused for brackets that are only text. It
// would need two thousand of them, in a file that MaxNodes refuses anyway.
const MaxFlowMarkers = 2000

// Parse reads one job file. The bytes are the file; path is only ever used to
// name it in a diagnostic, so a caller with the content in hand can parse
// without touching a disk.
//
// A job comes back only when nothing refused it. Warnings come back with the
// job, because a job with a warning is a job paceq will run.
func Parse(path string, src []byte) (*Job, diag.List) {
	if len(src) > MaxFileBytes {
		return nil, diag.List{diag.New(CodeFileTooLarge, diag.SeverityError, path, diag.Position{},
			fmt.Sprintf("the job file is %d bytes, and paceq reads at most %d", len(src), MaxFileBytes),
			"Split the job into several files, or move the data it embeds out of the definition.\n"+
				"A job definition describes what to run. Anything that big is an input, not a definition.")}
	}
	if !utf8.Valid(src) {
		return nil, diag.List{diag.New(CodeSyntax, diag.SeverityError, path, diag.Position{},
			"the job file is not valid UTF-8",
			"Save the file as UTF-8. YAML has no other encoding, and paceq will not guess at one.\n\n"+
				"    iconv -f latin1 -t utf8 "+path+" > "+path+".utf8")}
	}
	if markers := countFlowMarkers(src); markers > MaxFlowMarkers {
		return nil, diag.List{diag.New(CodeTooLarge, diag.SeverityError, path, diag.Position{},
			fmt.Sprintf("the job file has %d flow markers, and paceq reads at most %d", markers, MaxFlowMarkers),
			"Count the [ and { characters in the file. A job definition needs a handful.\n"+
				"Write the lists as block sequences instead:\n\n"+
				"    run:\n"+
				"      - /usr/bin/env\n"+
				"      - --chdir=/srv")}
	}

	// Line terminators are normalised before the parser sees them, so the
	// positions it reports count the same lines the renderer will draw.
	src = diag.Normalize(src)

	// Duplicate keys are allowed through the parser so this package can refuse
	// them itself, with the position of the second one and a next step. The
	// parser's own refusal is a syntax error with no code on it.
	file, err := parser.ParseBytes(src, 0, parser.AllowDuplicateMapKey())
	if err != nil {
		return nil, diag.List{syntaxDiagnostic(path, err)}
	}

	body, first := documentBody(path, file)
	if first != nil {
		return nil, diag.List{*first}
	}

	d := &decoder{
		file:    path,
		anchors: map[string]ast.Node{},
		open:    map[string]bool{},
		budget:  MaxExpandedNodes,
	}
	if d.survey(body); d.diags.HasErrors() {
		return nil, d.report()
	}

	// The checks that need the whole job run only on a file that decoded
	// cleanly. Reporting that a step needs a step that does not exist, when
	// half the steps failed to decode, is noise on top of the real problem.
	job := d.job(body)
	if !d.diags.HasErrors() {
		d.crossCheck(job)
	}
	if diags := d.report(); diags.HasErrors() {
		return nil, diags
	}
	return job, d.report()
}

// countFlowMarkers counts the characters that open a flow collection.
func countFlowMarkers(src []byte) int {
	count := 0
	for _, c := range src {
		if c == '[' || c == '{' {
			count++
		}
	}
	return count
}

// syntaxDiagnostic turns the YAML parser's refusal into one of ours. The
// parser's message says what it expected and where, which is the part worth
// keeping; the code, the position and the next step are added here.
func syntaxDiagnostic(path string, err error) diag.Diagnostic {
	message, pos := "the file is not valid YAML: "+err.Error(), diag.Position{}

	var positioned yaml.Error
	if errors.As(err, &positioned) {
		message = "the file is not valid YAML: " + positioned.GetMessage()
		if tk := positioned.GetToken(); tk != nil && tk.Position != nil {
			pos = diag.Position{Line: tk.Position.Line, Col: tk.Position.Column}
		}
	}
	return diag.New(CodeSyntax, diag.SeverityError, path, pos, message,
		"Check the line above for the usual three: a tab used for indentation, a missing space\n"+
			"after a colon, or a list item that is indented less than the key it belongs to.\n"+
			"YAML forbids tabs in indentation entirely.")
}

// documentBody is the one mapping a job file holds. A file with several YAML
// documents in it is refused rather than half read: paceq names jobs by file,
// and two jobs in one file have one name between them.
func documentBody(path string, file *ast.File) (ast.Node, *diag.Diagnostic) {
	var bodies []ast.Node
	for _, doc := range file.Docs {
		if doc.Body != nil {
			bodies = append(bodies, doc.Body)
		}
	}
	switch {
	case len(bodies) == 0:
		d := diag.New(CodeMissingField, diag.SeverityError, path, diag.Position{},
			"the job file is empty",
			"A job needs a name and at least one step:\n\n"+
				"    name: hello\n"+
				"    steps:\n"+
				"      - name: say-hello\n"+
				"        run: [\"/bin/echo\", \"hello\"]")
		return nil, &d
	case len(bodies) > 1:
		d := diag.New(CodeSyntax, diag.SeverityError, path, position(bodies[1]),
			fmt.Sprintf("the job file holds %d YAML documents, and a job file holds one job", len(bodies)),
			"Remove the --- separator, or put the second job in its own file.\n"+
				"paceq names a job by its file, so two jobs in one file have one name between them.")
		return nil, &d
	}
	return bodies[0], nil
}

// decoder walks the syntax tree once and writes both the job and the
// diagnostics. It carries the anchors it has seen and a budget, because
// resolving an alias is the one thing here that can do more work than the file
// it came from is long.
type decoder struct {
	file    string
	diags   diag.List
	anchors map[string]ast.Node
	// open names the anchors currently being resolved, so an anchor that
	// contains an alias to itself is caught rather than followed forever.
	open   map[string]bool
	budget int
	// stopped is set once the file has produced as many problems as paceq
	// reports about one file. Everything that walks the tree checks it, so the
	// walk unwinds rather than carrying on writing messages nobody reads.
	stopped bool
	// steps records where each step's name and needs entries were written, so
	// the checks that need the whole job can still point at a line.
	stepPos []stepPositions
	// sensorPos records where each sensor's name was written, so the whole
	// job check that requires unique sensor names can point at the second one.
	sensorPos []sensorPositions
}

type stepPositions struct {
	name  diag.Position
	needs []diag.Position
}

type sensorPositions struct {
	name    diag.Position
	workdir diag.Position
}

// maxDiagnostics is how many problems paceq reports about one job file.
//
// It is a limit on the output, and it is load bearing for the same reason the
// input limits are. One alias used in the wrong place turns into one message
// per element it expands to, and a hundred and fifty thousand messages is not a
// report, it is another denial of service: nobody reads it, the terminal takes
// minutes to draw it, and the memory it takes to build is orders of magnitude
// more than the file it came from.
//
// A hundred is well past what anybody fixes in one pass, and the last message
// says what was not looked at.
const maxDiagnostics = 100

func (d *decoder) error(code string, pos diag.Position, message, hint string) {
	d.add(diag.New(code, diag.SeverityError, d.file, pos, message, hint))
}

func (d *decoder) warn(code string, pos diag.Position, message, hint string) {
	d.add(diag.New(code, diag.SeverityWarning, d.file, pos, message, hint))
}

// report is the diagnostics in reading order. The message that says paceq
// stopped reading stays at the end whatever the rest carry: it is about where
// the report ends, not about a line in the file.
func (d *decoder) report() diag.List {
	if !d.stopped || len(d.diags) == 0 {
		d.diags.Sort()
		return d.diags
	}
	body := d.diags[:len(d.diags)-1]
	last := d.diags[len(d.diags)-1]
	body.Sort()
	d.diags = append(body, last)
	return d.diags
}

func (d *decoder) add(diagnostic diag.Diagnostic) {
	if d.stopped {
		return
	}
	if len(d.diags) >= maxDiagnostics {
		d.stopped = true
		// No position: this is about where the report ends, not about a line
		// in the file, and pointing at the line the hundred and first problem
		// happened to be on would send the reader to a line that is fine.
		d.diags = append(d.diags, diag.New(CodeTooManyProblems, diag.SeverityError, d.file, diag.Position{},
			fmt.Sprintf("this file has more than %d problems, and paceq stopped reading it here", maxDiagnostics),
			"Work through the ones above first. A file this far off is usually one mistake\n"+
				"near the top, such as a list written where a block belongs, and the rest are\n"+
				"what that one mistake looks like further down."))
		return
	}
	d.diags = append(d.diags, diagnostic)
}

func position(n ast.Node) diag.Position {
	if n == nil {
		return diag.Position{}
	}
	tk := n.GetToken()
	if tk == nil || tk.Position == nil {
		return diag.Position{}
	}
	return diag.Position{Line: tk.Position.Line, Col: tk.Position.Column}
}
