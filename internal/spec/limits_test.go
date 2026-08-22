package spec_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// The limits, one test each. Every one of them asserts the code as well as the
// refusal: a file refused with the wrong code sends the operator to the wrong
// explanation, which is worse than no explanation.

// TestAFileOverTheLimitIsRefusedBeforeItIsDecoded is the two megabyte case. The
// file is valid YAML and would parse: what is being asserted is that nothing
// tries.
func TestAFileOverTheLimitIsRefusedBeforeItIsDecoded(t *testing.T) {
	// A comment line is the cheapest way to make a file large and still leave
	// a job in it that would otherwise parse.
	padding := strings.Repeat("# padding\n", (2<<20)/len("# padding\n"))
	src := []byte("name: report\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n" + padding)
	if len(src) <= spec.MaxFileBytes {
		t.Fatalf("the fixture is %d bytes, which is under the limit of %d", len(src), spec.MaxFileBytes)
	}

	job, diags := spec.Parse("big.yaml", src)

	if job != nil {
		t.Error("a file over the size limit produced a job")
	}
	requireCode(t, diags, spec.CodeFileTooLarge)
	if len(diags) != 1 {
		t.Errorf("got %d diagnostics, want the one refusal: %v", len(diags), codesOf(diags))
	}
}

// TestReadFileNeverHoldsMoreThanTheLimit is the same rule at the file reading
// edge. A caller that hands Parse two megabytes has already allocated them, so
// the bounded read is what makes the limit real on disk.
func TestReadFileNeverHoldsMoreThanTheLimit(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.yaml")
	if err := os.WriteFile(path, []byte(strings.Repeat("# padding\n", (4<<20)/len("# padding\n"))), 0o600); err != nil {
		t.Fatalf("write the fixture: %v", err)
	}

	source, diags := spec.ReadFile(path)

	if len(diags) > 0 {
		t.Fatalf("reading a large file failed: %v", codesOf(diags))
	}
	if len(source.Bytes) != spec.MaxFileBytes+1 {
		t.Errorf("read %d bytes of a 4 MiB file, want %d", len(source.Bytes), spec.MaxFileBytes+1)
	}

	_, _, diags = spec.LoadFile(path)
	requireCode(t, diags, spec.CodeFileTooLarge)
}

func TestNestingPastTheDepthLimitIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\ndeep:\n")
	for i := range spec.MaxDepth + 4 {
		b.WriteString(strings.Repeat("  ", i+1))
		b.WriteString("level:\n")
	}
	b.WriteString(strings.Repeat("  ", spec.MaxDepth+5))
	b.WriteString("bottom\n")

	job, diags := spec.Parse("deep.yaml", []byte(b.String()))

	if job != nil {
		t.Error("a file past the depth limit produced a job")
	}
	requireCode(t, diags, spec.CodeTooDeep)
}

func TestMoreAliasesThanTheLimitIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nshared: &shared value\nused:\n")
	for range spec.MaxAliases + 1 {
		b.WriteString("  - *shared\n")
	}
	b.WriteString("steps:\n  - name: only\n    run: [\"/bin/true\"]\n")

	job, diags := spec.Parse("aliases.yaml", []byte(b.String()))

	if job != nil {
		t.Error("a file past the alias limit produced a job")
	}
	requireCode(t, diags, spec.CodeTooManyAliases)
}

func TestMoreStepsThanTheLimitIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n")
	for i := range spec.MaxSteps + 1 {
		fmt.Fprintf(&b, "  - name: step-%d\n    run: [\"/bin/true\"]\n", i)
	}

	job, diags := spec.Parse("steps.yaml", []byte(b.String()))

	if job != nil {
		t.Error("a file past the step limit produced a job")
	}
	requireCode(t, diags, spec.CodeTooManySteps)
}

func TestExactlyTheStepLimitIsAccepted(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n")
	for i := range spec.MaxSteps {
		fmt.Fprintf(&b, "  - name: step-%d\n    run: [\"/bin/true\"]\n", i)
	}

	job, diags := spec.Parse("steps.yaml", []byte(b.String()))

	if diags.HasErrors() {
		t.Fatalf("a job with exactly %d steps was refused:\n%s", spec.MaxSteps, render(t, diags))
	}
	if len(job.Steps) != spec.MaxSteps {
		t.Errorf("got %d steps, want %d", len(job.Steps), spec.MaxSteps)
	}
}

func TestMoreNodesThanTheLimitIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n  - name: only\n    run:\n")
	for range spec.MaxNodes {
		b.WriteString("      - /bin/true\n")
	}

	job, diags := spec.Parse("wide.yaml", []byte(b.String()))

	if job != nil {
		t.Error("a file past the node limit produced a job")
	}
	requireCode(t, diags, spec.CodeTooLarge)
}

func TestMoreFlowMarkersThanTheLimitIsRefused(t *testing.T) {
	src := "name: report\ndeep: " + strings.Repeat("[", spec.MaxFlowMarkers+1) + strings.Repeat("]", spec.MaxFlowMarkers+1) + "\n"

	job, diags := spec.Parse("flow.yaml", []byte(src))

	if job != nil {
		t.Error("a file past the flow marker limit produced a job")
	}
	requireCode(t, diags, spec.CodeTooLarge)
}

// TestTheAliasBombIsRefusedBeforeItIsExpanded is 08 T13, and the acceptance
// criterion that puts a number on it. The file below is the billion laughs
// shape: twelve levels of ten, which expands to ten to the twelfth entries and
// is four hundred bytes on disk.
//
// The assertion that matters is not only that it is refused. It is that the
// refusal is the alias limit and nothing else. Everything else wrong with this
// file, and there is plenty, is found by the decoder, so a run that reports an
// unknown field has decoded a file it should have refused on sight.
//
// The time budget is generous on purpose. What is being measured is the
// difference between a linear walk of a small syntax tree and an exponential
// expansion of it, which is twelve orders of magnitude. A test that only passed
// on a fast machine would be measuring the machine.
func TestTheAliasBombIsRefusedBeforeItIsExpanded(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\n")
	b.WriteString(`l0: &l0 ["lol","lol","lol","lol","lol","lol","lol","lol","lol","lol"]` + "\n")
	for level := 1; level <= 12; level++ {
		fmt.Fprintf(&b, "l%d: &l%d [", level, level)
		for i := range 10 {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "*l%d", level-1)
		}
		b.WriteString("]\n")
	}
	b.WriteString("steps:\n  - name: only\n    run: *l12\n")
	bomb := []byte(b.String())

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)

	start := time.Now()
	job, diags := spec.Parse("bomb.yaml", bomb)
	elapsed := time.Since(start)

	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc

	if job != nil {
		t.Error("the bomb produced a job")
	}
	requireCode(t, diags, spec.CodeTooManyAliases)
	if len(diags) != 1 {
		t.Errorf("the bomb was decoded as well as refused, and reported %v", codesOf(diags))
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("refusing the bomb took %v, want under 100ms", elapsed)
	}
	// The file is under half a kilobyte. Anything near a megabyte means the
	// aliases were being expanded before the limit was reached.
	const allocationCap = 8 << 20
	if allocated > allocationCap {
		t.Errorf("refusing the bomb allocated %d bytes, want under %d", allocated, allocationCap)
	}
	t.Logf("%d bytes refused in %v having allocated %d", len(bomb), elapsed, allocated)
}

// TestAWideAliasThatStaysUnderTheLimitsIsStillCheap covers the bomb that keeps
// its alias count legal: five levels of twenty four, which is ninety seven
// aliases and would expand to eight million entries. The schema refuses the
// shape it produces, and the point of the test is the cost of finding that out.
func TestAWideAliasThatStaysUnderTheLimitsIsStillCheap(t *testing.T) {
	const width = 24
	var b strings.Builder
	b.WriteString("name: report\n")
	b.WriteString("l0: &l0 [" + strings.TrimSuffix(strings.Repeat(`"x",`, width), ",") + "]\n")
	for level := 1; level <= 4; level++ {
		fmt.Fprintf(&b, "l%d: &l%d [", level, level)
		for i := range width {
			if i > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "*l%d", level-1)
		}
		b.WriteString("]\n")
	}
	b.WriteString("steps:\n  - name: only\n    run: *l4\n")

	start := time.Now()
	job, diags := spec.Parse("wide.yaml", []byte(b.String()))
	elapsed := time.Since(start)

	if job != nil {
		t.Error("a file that expands to millions of entries produced a job")
	}
	if !diags.HasErrors() {
		t.Fatal("the nested aliases were accepted")
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("refusing it took %v, want under 100ms", elapsed)
	}
	t.Logf("refused in %v with %v", elapsed, codesOf(diags))
}

// TestOneFileReportsABoundedNumberOfProblems is a limit on the output rather
// than on the input, and it is load bearing for the same reason.
//
// An anchor used many times turns one mistake into one message per use, and one
// mistake per field into the product of the two. Without a cap that is orders
// of magnitude more memory than the file it came from, minutes for a terminal
// to draw, and nothing anybody reads. The fuzzer found this before a user did.
func TestOneFileReportsABoundedNumberOfProblems(t *testing.T) {
	// One anchored step full of fields nobody has heard of, used as every step
	// in the job.
	var fields strings.Builder
	for i := range 100 {
		if i > 0 {
			fields.WriteString(",")
		}
		fmt.Fprintf(&fields, "u%d: 1", i)
	}
	var b strings.Builder
	b.WriteString("name: report\n")
	b.WriteString("step: &s {" + fields.String() + "}\n")
	b.WriteString("steps: [" + strings.TrimSuffix(strings.Repeat("*s,", 90), ",") + "]\n")

	start := time.Now()
	_, diags := spec.Parse("many.yaml", []byte(b.String()))
	elapsed := time.Since(start)

	if len(diags) > 101 {
		t.Errorf("got %d diagnostics for one file, want the cap plus the message that says so", len(diags))
	}
	requireCode(t, diags, spec.CodeTooManyProblems)
	if last := diags[len(diags)-1]; last.Code != spec.CodeTooManyProblems {
		t.Errorf("the last diagnostic is %s, want the one that says paceq stopped reading", last.Code)
	}
	if elapsed > 100*time.Millisecond {
		t.Errorf("reporting on it took %v, want under 100ms", elapsed)
	}
}

// TestAFileWithManyMistakesReportsThemAllUpToTheCap keeps the cap from becoming
// a reason to report less than a person can work with in one pass.
func TestAFileWithManyMistakesReportsThemAllUpToTheCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n")
	for i := range 20 {
		fmt.Fprintf(&b, "unknown%d: x\n", i)
	}

	_, diags := spec.Parse("many.yaml", []byte(b.String()))

	if len(diags) != 20 {
		t.Errorf("got %d diagnostics for 20 unknown fields, want all 20: %v", len(diags), codesOf(diags))
	}
	if _, capped := find(diags, spec.CodeTooManyProblems); capped {
		t.Error("twenty problems tripped the cap")
	}
}

// TestAnchorsThatStayInsideTheLimitsStillWork keeps the limits from being a ban
// on the feature. Anchors are the reason a job file can share one env block.
func TestAnchorsThatStayInsideTheLimitsStillWork(t *testing.T) {
	path := "testdata/ok/anchors.yaml"

	job, diags := spec.Parse(path, read(t, path))

	if diags.HasErrors() {
		t.Fatalf("a file with anchors was refused:\n%s", render(t, diags))
	}
	if job.Steps[0].Timeout != job.Steps[1].Timeout {
		t.Errorf("the shared timeout resolved to %v and %v", job.Steps[0].Timeout, job.Steps[1].Timeout)
	}
	if job.Steps[0].Timeout != 30*time.Second {
		t.Errorf("the anchored timeout is %v, want 30s", job.Steps[0].Timeout)
	}
}

// TestParsingAHundredFilesStaysUnderTheBudget is the performance floor from the
// test plan: apply and shell completion both walk a whole jobs directory, and
// both have a budget measured in hundreds of milliseconds (03 section 9.1).
func TestParsingAHundredFilesStaysUnderTheBudget(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector distorts the timings this test calibrates")
	}

	src := read(t, "testdata/ok/full.yaml")

	start := time.Now()
	for i := range 100 {
		job, diags := spec.Parse(fmt.Sprintf("job-%d.yaml", i), src)
		if diags.HasErrors() {
			t.Fatalf("round %d failed", i)
		}
		spec.Compile(job)
	}
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Errorf("parsing and compiling 100 job files took %v, want under 200ms", elapsed)
	}
	t.Logf("100 files parsed, canonicalised and hashed in %v", elapsed)
}

// TestATabIndentIsRefusedWithAPosition. YAML forbids tabs in indentation, and
// it is the mistake an editor makes for you.
func TestATabIndentIsRefusedWithAPosition(t *testing.T) {
	_, diags := spec.Parse("tab.yaml", []byte("name: report\nsteps:\n\t- name: only\n"))

	d := requireCode(t, diags, spec.CodeSyntax)
	if d.Line == 0 {
		t.Errorf("the refusal has no position: %s", d.Message)
	}
	if !strings.Contains(d.Hint, "tab") {
		t.Errorf("the next step does not mention tabs: %s", d.Hint)
	}
}

// TestSeveralDocumentsInOneFileAreRefused. paceq names a job by its file.
func TestSeveralDocumentsInOneFileAreRefused(t *testing.T) {
	_, diags := spec.Parse("two.yaml", []byte(
		"name: first\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n---\nname: second\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))

	requireCode(t, diags, spec.CodeSyntax)
}

// TestAFileThatIsNotUTF8IsRefused keeps the encoder total: every string that
// reaches the canonical form came out of a file that was valid UTF-8.
func TestAFileThatIsNotUTF8IsRefused(t *testing.T) {
	src := append([]byte("name: report\ndescription: "), 0xff, 0xfe, '\n')

	_, diags := spec.Parse("latin.yaml", src)

	d := requireCode(t, diags, spec.CodeSyntax)
	if !strings.Contains(d.Message, "UTF-8") {
		t.Errorf("the message does not name the encoding: %s", d.Message)
	}
}

// requireCode is the assertion every limit test ends in: the refusal exists and
// carries the code the operator will look up.
func requireCode(t *testing.T, diags diag.List, code string) diag.Diagnostic {
	t.Helper()

	d, ok := find(diags, code)
	if !ok {
		t.Fatalf("no %s among %v", code, codesOf(diags))
	}
	return d
}
