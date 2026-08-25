package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// The $PACEQ_OUTPUT contract (#13): NDJSON, one JSON object per line, two
// line shapes, hard bounds. Reading happens after exit and stops at the
// first bound; everything valid before the cut is kept; an unreadable line
// warns and never fails the step.

func line(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err) // the test values below always marshal
	}
	return string(b)
}

func artifactLine(name, uri string) string {
	return line(map[string]any{
		"artifact": map[string]any{"name": name, "uri": uri},
	})
}

func paramsLine(pairs ...string) string {
	m := map[string]any{}
	for i := 0; i+1 < len(pairs); i += 2 {
		m[pairs[i]] = pairs[i+1]
	}
	return line(map[string]any{"params": m})
}

func TestParseStepOutputReadsBothLineShapes(t *testing.T) {
	out := ParseStepOutput([]byte(
		artifactLine("raw", "/data/raw.parquet")+"\n"+
			paramsLine("rows", "1048576")+"\n"), DefaultOutputLimits)

	if len(out.Warnings) != 0 {
		t.Fatalf("clean input produced warnings: %v", out.Warnings)
	}
	if len(out.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one", out.Artifacts)
	}
	a := out.Artifacts[0]
	if a.Name != "raw" || a.URI != "/data/raw.parquet" {
		t.Errorf("artifact = %+v, want name raw at /data/raw.parquet", a)
	}
	if a.SizeBytes != nil {
		t.Errorf("size_bytes = %d, want absent", *a.SizeBytes)
	}
	if out.Params["rows"] != "1048576" {
		t.Errorf("params = %v, want rows=1048576", out.Params)
	}
}

func TestParseStepOutputKeepsValidLinesAroundAnInvalidOne(t *testing.T) {
	out := ParseStepOutput([]byte(
		artifactLine("a", "/a")+"\n"+
			"{not json}\n"+
			artifactLine("b", "/b")+"\n"), DefaultOutputLimits)

	if got := len(out.Artifacts); got != 2 {
		t.Fatalf("kept %d artifacts, want the 2 valid ones", got)
	}
	if len(out.Warnings) != 1 {
		t.Fatalf("warnings = %v, want exactly one", out.Warnings)
	}
	w := out.Warnings[0]
	if w.Kind != WarnOutputInvalid {
		t.Errorf("warning code = %s, want STEP_OUTPUT_INVALID", w.Kind)
	}
	if w.Detail["count"] != 1 || w.Detail["first_line"] != 2 {
		t.Errorf("warning detail = %v, want count 1 at first_line 2", w.Detail)
	}
}

func TestParseStepOutputTakesTheLastLineForADuplicateName(t *testing.T) {
	out := ParseStepOutput([]byte(
		artifactLine("raw", "/old")+"\n"+
			artifactLine("raw", "/new")+"\n"), DefaultOutputLimits)

	if len(out.Artifacts) != 1 {
		t.Fatalf("artifacts = %+v, want one after the duplicate folded", out.Artifacts)
	}
	if out.Artifacts[0].URI != "/new" {
		t.Errorf("uri = %s, want the last line to win", out.Artifacts[0].URI)
	}
}

func TestParseStepOutputDiscardsAPartialFinalLine(t *testing.T) {
	// A crash mid-write leaves a tail without its newline. It is never a
	// whole line, so it is never parsed and never warned about.
	out := ParseStepOutput([]byte(artifactLine("a", "/a")+"\n"+`{"para`), DefaultOutputLimits)
	if len(out.Artifacts) != 1 || len(out.Warnings) != 0 {
		t.Fatalf("artifacts=%d warnings=%v, want the one valid line and silence",
			len(out.Artifacts), out.Warnings)
	}
}

func TestParseStepOutputLineBoundIsExact(t *testing.T) {
	var ok strings.Builder
	for i := 0; i < DefaultOutputLimits.MaxLines; i++ {
		ok.WriteString(artifactLine(strconv.Itoa(i), "/f"))
		ok.WriteString("\n")
	}
	out := ParseStepOutput([]byte(ok.String()), DefaultOutputLimits)
	if len(out.Warnings) != 0 {
		t.Fatalf("exactly %d lines warned: %v", DefaultOutputLimits.MaxLines, out.Warnings)
	}
	if len(out.Artifacts) != DefaultOutputLimits.MaxLines {
		t.Fatalf("kept %d, want %d", len(out.Artifacts), DefaultOutputLimits.MaxLines)
	}

	over := ok.String() + artifactLine("one-too-many", "/f") + "\n"
	out = ParseStepOutput([]byte(over), DefaultOutputLimits)
	if len(out.Warnings) != 1 || out.Warnings[0].Kind != WarnOutputTruncated {
		t.Fatalf("warnings = %v, want one truncation", out.Warnings)
	}
	if out.Warnings[0].Detail["bound"] != "lines" ||
		out.Warnings[0].Detail["limit"] != DefaultOutputLimits.MaxLines {
		t.Errorf("detail = %v, want bound lines at %d",
			out.Warnings[0].Detail, DefaultOutputLimits.MaxLines)
	}
	if len(out.Artifacts) != DefaultOutputLimits.MaxLines {
		t.Errorf("kept %d, want the %d before the cut", len(out.Artifacts), DefaultOutputLimits.MaxLines)
	}
}

func TestParseStepOutputFileByteBoundIsExact(t *testing.T) {
	const suf = `"}}`
	makeLine := func(i, contentLen int) string {
		pre := `{"artifact":{"name":"n` + strconv.Itoa(i) + `","uri":"/`
		return pre + strings.Repeat("x", contentLen-len(pre)-len(suf)) + suf
	}
	// Fifteen lines at the per-line cap plus one filler line land the file
	// exactly on 1 MiB without tripping either other bound.
	var b strings.Builder
	for i := 0; i < 15; i++ {
		b.WriteString(makeLine(i, DefaultOutputLimits.MaxLineBytes))
		b.WriteString("\n")
	}
	rest := int(DefaultOutputLimits.MaxFileBytes) - b.Len()
	b.WriteString(makeLine(15, rest-1))
	b.WriteString("\n")
	if b.Len() != int(DefaultOutputLimits.MaxFileBytes) {
		t.Fatalf("test construction: file is %d bytes, want %d",
			b.Len(), DefaultOutputLimits.MaxFileBytes)
	}
	out := ParseStepOutput([]byte(b.String()), DefaultOutputLimits)
	if len(out.Warnings) != 0 {
		t.Fatalf("a file exactly on the cap must not warn: %v", out.Warnings)
	}
	if len(out.Artifacts) != 16 {
		t.Fatalf("kept %d refs, want all 16", len(out.Artifacts))
	}

	over := b.String()[:b.Len()-2] + "xy\n"
	out = ParseStepOutput([]byte(over), DefaultOutputLimits)
	if len(out.Warnings) != 1 || out.Warnings[0].Kind != WarnOutputTruncated {
		t.Fatalf("warnings = %v, want one truncation one byte over", out.Warnings)
	}
	if out.Warnings[0].Detail["bound"] != "file_bytes" {
		t.Errorf("detail = %v, want bound file_bytes", out.Warnings[0].Detail)
	}
	if len(out.Artifacts) != 15 {
		t.Errorf("kept %d refs, want the 15 before the line that crossed", len(out.Artifacts))
	}
}

func TestParseStepOutputLineLengthBoundIsExact(t *testing.T) {
	const pre = `{"artifact":{"name":"n","uri":"/`
	const suf = `"}}`
	pad := strings.Repeat("y", DefaultOutputLimits.MaxLineBytes-len(pre)-len(suf))
	content := pre + pad + suf
	if len(content) != DefaultOutputLimits.MaxLineBytes {
		t.Fatalf("test construction: content is %d, want %d", len(content), DefaultOutputLimits.MaxLineBytes)
	}
	out := ParseStepOutput([]byte(content+"\n"), DefaultOutputLimits)
	if len(out.Warnings) != 0 {
		t.Fatalf("a line exactly on the cap must not warn: %v", out.Warnings)
	}

	over := pre + pad + "y" + suf + "\n"
	out = ParseStepOutput([]byte(over), DefaultOutputLimits)
	if len(out.Warnings) != 1 || out.Warnings[0].Kind != WarnOutputTruncated {
		t.Fatalf("warnings = %v, want one truncation", out.Warnings)
	}
	if out.Warnings[0].Detail["bound"] != "line_bytes" {
		t.Errorf("detail = %v, want bound line_bytes", out.Warnings[0].Detail)
	}
}

func TestParseStepOutputSilentOnEmptyMissingAndBlankInput(t *testing.T) {
	out := ParseStepOutput(nil, DefaultOutputLimits)
	if len(out.Artifacts) != 0 || len(out.Warnings) != 0 {
		t.Fatalf("empty input: %+v", out)
	}
	out = ParseStepOutput([]byte("\n\n"), DefaultOutputLimits)
	if len(out.Artifacts) != 0 || len(out.Warnings) != 0 {
		t.Fatalf("blank lines must stay silent: %+v", out)
	}

	path := filepath.Join(t.TempDir(), "never-written.ndjson")
	step, err := ReadStepOutput(path)
	if err != nil {
		t.Fatalf("missing file: %v", err)
	}
	if len(step.Artifacts) != 0 || len(step.Warnings) != 0 {
		t.Fatalf("a file that was never written is not a warning: %+v", step)
	}
}

func TestReadStepOutputWarnsWhenTheFileCannotBeRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "out.ndjson")
	if err := os.WriteFile(path, []byte(artifactLine("a", "/a")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Chmod(dir, 0o700) }()

	step, err := ReadStepOutput(path)
	if err != nil {
		t.Fatalf("an unreadable file is data, not an error: %v", err)
	}
	if len(step.Warnings) != 1 || step.Warnings[0].Kind != WarnOutputInvalid {
		t.Fatalf("warnings = %v, want one invalid-output warning", step.Warnings)
	}
}

func TestParseStepOutputRefusesBrokenReferencesWithoutFailingTheRest(t *testing.T) {
	lines := []string{
		line(map[string]any{"artifact": map[string]any{"name": "rel", "uri": "relative/path"}}),
		line(map[string]any{"artifact": map[string]any{"name": "noscheme", "uri": "localhost:x"}}),
		line(map[string]any{"artifact": map[string]any{"name": "neg", "uri": "/x", "size_bytes": -1}}),
		line(map[string]any{"artifact": map[string]any{"name": "float", "uri": "/x", "size_bytes": 1.5}}),
		line(map[string]any{"artifact": map[string]any{"name": "", "uri": "/x"}}),
		line(map[string]any{"artifact": map[string]any{"name": "extra", "uri": "/x", "wat": 1}}),
		line(map[string]any{"artifact": map[string]any{"name": "both", "uri": "/x"}, "params": map[string]string{}}),
		line(map[string]any{}),
		line(map[string]any{"other": 1}),
		"[1,2]",
		`"bare string"`,
		artifactLine("good", "/good"),
	}
	out := ParseStepOutput([]byte(strings.Join(lines, "\n")+"\n"), DefaultOutputLimits)

	if len(out.Artifacts) != 1 || out.Artifacts[0].Name != "good" {
		t.Fatalf("artifacts = %+v, want only the good one", out.Artifacts)
	}
	if len(out.Warnings) != 1 || out.Warnings[0].Kind != WarnOutputInvalid {
		t.Fatalf("warnings = %+v, want one aggregated invalid-output warning", out.Warnings)
	}
	detail := out.Warnings[0].Detail
	if detail["count"] != len(lines)-1 {
		t.Errorf("count = %v, want %d", detail["count"], len(lines)-1)
	}
	if detail["first_line"] != 1 {
		t.Errorf("first_line = %v, want 1", detail["first_line"])
	}
}

func TestParseStepOutputAcceptsSchemaURIsAndAbsolutePathsOnly(t *testing.T) {
	good := []string{
		"/abs/plain",
		"file:///srv/data/x.csv",
		"s3://bucket/key",
		"postgres://host/db",
		"a+b.c-d://thing",
	}
	var b strings.Builder
	for i, u := range good {
		b.WriteString(artifactLine(strconv.Itoa(i), u))
		b.WriteString("\n")
	}
	out := ParseStepOutput([]byte(b.String()), DefaultOutputLimits)
	if len(out.Warnings) != 0 {
		t.Fatalf("all of %q are legal references, got %v", good, out.Warnings)
	}
	if len(out.Artifacts) != len(good) {
		t.Fatalf("kept %d, want %d", len(out.Artifacts), len(good))
	}
}

func TestParseStepOutputMergesParamsAcrossLinesLaterWins(t *testing.T) {
	out := ParseStepOutput([]byte(
		paramsLine("a", "1", "b", "keep")+"\n"+
			paramsLine("b", "replace")+"\n"), DefaultOutputLimits)
	if out.Params["a"] != "1" || out.Params["b"] != "replace" {
		t.Errorf("params = %v, want a=1 with the later b winning", out.Params)
	}
}

func TestParseStepOutputChecksumAndMediaTypeRideAlongUntouched(t *testing.T) {
	l := line(map[string]any{
		"artifact": map[string]any{
			"name":       "raw",
			"uri":        "file:///x",
			"size_bytes": 12,
			"checksum":   "sha256:abc",
			"media_type": "text/csv",
		},
	})
	out := ParseStepOutput([]byte(l+"\n"), DefaultOutputLimits)
	a := out.Artifacts[0]
	if a.Checksum != "sha256:abc" {
		t.Errorf("checksum = %q, want stored exactly as emitted", a.Checksum)
	}
	if a.MediaType != "text/csv" {
		t.Errorf("media_type = %q", a.MediaType)
	}
	if a.SizeBytes == nil || *a.SizeBytes != 12 {
		t.Errorf("size_bytes = %v, want 12", a.SizeBytes)
	}
}
