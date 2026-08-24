package spec_test

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/spec"
)

// The DAG is tested as a pure function because that is where the decision
// lives: needs on a step is an edge towards the step it names, and a graph is
// a valid scheduling order exactly when that edge set has no cycle. TopoOrder
// returns the deterministic order and, when the graph has a cycle, the names
// of one. Building the steps from a map keeps iteration order an accident the
// function must not depend on.

// buildSteps maps needs by their owner's name.
func buildSteps(needsByStep map[string][]string) []spec.Step {
	steps := make([]spec.Step, 0, len(needsByStep))
	for name, needs := range needsByStep {
		steps = append(steps, spec.Step{Name: name, Needs: needs})
	}
	return steps
}

func assertOrder(t *testing.T, got []string, want []string) {
	t.Helper()
	if !sameOrder(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func sameOrder(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range want {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestLinearChain is the shortest non-trivial graph: each step waits on the
// one before it, so the order is forced.
func TestLinearChain(t *testing.T) {
	order, cycle := spec.TopoOrder(buildSteps(map[string][]string{
		"a": {},
		"b": {"a"},
		"c": {"b"},
	}))
	if cycle != "" {
		t.Fatalf("a linear chain reported a cycle %q", cycle)
	}
	assertOrder(t, order, []string{"a", "b", "c"})
}

// TestDiamondTiesBreakLexicographically pins the determinism rule. b and c
// both wait on a, so when a is emitted both are ready at once. Whether the
// iteration handed the steps in one order or the other, the tie must break
// the same way, by name.
func TestDiamondTiesBreakLexicographically(t *testing.T) {
	one := buildOrderDiamond(t, map[string][]string{
		"a": {}, "b": {"a"}, "c": {"a"}, "d": {"b", "c"},
	})
	two := buildOrderDiamond(t, map[string][]string{
		"a": {}, "c": {"a"}, "b": {"a"}, "d": {"c", "b"},
	})
	assertOrder(t, one, []string{"a", "b", "c", "d"})
	assertOrder(t, two, []string{"a", "b", "c", "d"})
}

func buildOrderDiamond(t *testing.T, m map[string][]string) []string {
	t.Helper()
	order, cycle := spec.TopoOrder(buildSteps(m))
	if cycle != "" {
		t.Fatalf("a diamond reported a cycle %q", cycle)
	}
	return order
}

// TestMultipleRootsEmitInNameOrder pins the tie-break for several roots too:
// independent steps are ready at once and sort by name.
func TestMultipleRoots(t *testing.T) {
	order, cycle := spec.TopoOrder(buildSteps(map[string][]string{
		"z": {"m"},
		"a": {},
		"m": {},
		"b": {"a"},
	}))
	if cycle != "" {
		t.Fatalf("a graph of roots reported a cycle %q", cycle)
	}
	// a and m are both roots, but once a is emitted b is ready too, and the
	// tie-break is the smallest name among every ready step. b sorts before
	// m, and z waits on m, so the deterministic order falls out.
	assertOrder(t, order, []string{"a", "b", "m", "z"})
}

// TestSelfNeedNamesItself is the flag-position: a step may not need itself.
func TestSelfNeedIsACycle(t *testing.T) {
	_, cycle := spec.TopoOrder(buildSteps(map[string][]string{"a": {"a"}}))
	if cycle == "" {
		t.Fatal("a step that needs itself reported no cycle")
	}
	if !strings.Contains(cycle, "a") {
		t.Errorf("the cycle %q does not name the step in it", cycle)
	}
}

// TestTwoStepCycleReportsAPath: the returned path is a real cycle, which
// means it names the steps it goes around.
func TestTwoStepCycleReportsMembers(t *testing.T) {
	_, cycle := spec.TopoOrder(buildSteps(map[string][]string{
		"a": {"b"}, "b": {"a"},
	}))
	if cycle == "" {
		t.Fatal("a two step cycle reported no cycle")
	}
	if !strings.Contains(cycle, "a") || !strings.Contains(cycle, "b") {
		t.Errorf("cycle %q does not name a and b", cycle)
	}
}

// TestLongCycleWithTailsInAndOut: the cycle has steps feeding into it and
// steps fed from it; neither may hide the cycle.
func TestLongCycleWithTailsInAndOut(t *testing.T) {
	_, cycle := spec.TopoOrder(buildSteps(map[string][]string{
		"root": {},
		"a":    {"root"},
		"b":    {"a"},
		"c":    {"b", "e"},
		"d":    {"c"},
		"e":    {"d"},
		"leaf": {"c"},
	}))
	if cycle == "" {
		t.Fatal("a cycle with tails in and out was not reported")
	}
}

// TestDisjointCycles: two cycles share nothing; finding one must not hide
// the other.
func TestDisjointCycles(t *testing.T) {
	first, c1 := spec.TopoOrder(buildSteps(map[string][]string{
		"a": {"b"}, "b": {"a"},
	}))
	second, c2 := spec.TopoOrder(buildSteps(map[string][]string{
		"x": {"y"}, "y": {"x"},
	}))
	if c1 == "" || c2 == "" {
		t.Errorf("one of two disjoint cycles was missed: %q vs %q", c1, c2)
	}
	if c1 == c2 {
		t.Errorf("both cycles reported the same path %q", c1)
	}
	if len(first) != 0 || len(second) != 0 {
		t.Errorf("a cyclic graph still returned an order: %v and %v", first, second)
	}
}

// TestParseRejectsACycleInASpec pins the end to end rule: a job whose steps
// loop must be refused by Parse, carrying the cycle code, so that neither
// validate nor apply can ever let the graph reach a run.
func TestParseRejectsACycleInASpec(t *testing.T) {
	_, diags := spec.Parse("cycle.yaml", []byte(`
name: report
steps:
  - name: a
    run: ["/bin/a"]
    needs: [c]
  - name: b
    run: ["/bin/b"]
    needs: [a]
  - name: c
    run: ["/bin/c"]
    needs: [b]
`))
	d := findDiag(t, diags, spec.CodeCycle)
	if !strings.Contains(d.Message, "->") {
		t.Errorf("the cycle message does not print the path: %s", d.Message)
	}
	if d.Line == 0 {
		t.Errorf("the cycle refusal has no position")
	}
}

// TestParseRejectsASelfReference: a step that needs its own name is the
// shortest possible cycle and must be refused the same way.
func TestParseRejectsASelfReference(t *testing.T) {
	_, diags := spec.Parse("self.yaml", []byte(`
name: report
steps:
  - name: extract
    run: ["/bin/extract"]
  - name: transform
    run: ["/bin/transform"]
    needs: [transform]
`))
	d := findDiag(t, diags, spec.CodeCycle)
	if !strings.Contains(d.Message, "transform") {
		t.Errorf("the message does not name the step: %s", d.Message)
	}
}

// TestParseAcceptsAnAcyclicSet pins the other direction: a legitimate diamond
// parses clean, so the cycle check is not refusing every needs graph.
func TestParseAcceptsAnAcyclicSet(t *testing.T) {
	job, diags := spec.Parse("dag.yaml", []byte(`
name: report
steps:
  - name: extract
    run: ["/bin/extract"]
  - name: transform
    run: ["/bin/transform"]
    needs: [extract]
  - name: summarize
    run: ["/bin/summarize"]
    needs: [extract]
  - name: load
    run: ["/bin/load"]
    needs: [transform, summarize]
`))
	if diags.HasErrors() {
		t.Fatalf("a clean diamond was refused:\n%s", renderDiagnostics(t, diags))
	}
	if len(job.Steps) != 4 {
		t.Errorf("got %d steps, want 4", len(job.Steps))
	}
}

// TestMaxParallelBounds pins the acceptance criterion on the field that
// becomes the per-run semaphore: 1 is the floor, MaxParallelHi is the ceiling,
// and anything outside is refused at the door with its own code.
func TestMaxParallelBounds(t *testing.T) {
	cases := []struct {
		name string
		line string
		code string
	}{
		{"below the floor", "max_parallel: 0", spec.CodeBadMaxParallel},
		{"above the ceiling", "max_parallel: 65", spec.CodeBadMaxParallel},
		{"not a number", "max_parallel: lots", spec.CodeBadMaxParallel},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, diags := spec.Parse("mp.yaml", []byte("name: report\n"+tc.line+"\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))
			findDiag(t, diags, tc.code)
		})
	}
}

// TestMaxParallelDefaultIsMaterialised: a job that says nothing and a job that
// writes the default are the same job, with the same hash.
func TestMaxParallelDefaultIsMaterialised(t *testing.T) {
	oj, od := spec.Parse("a.yaml", []byte("name: report\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))
	xj, xd := spec.Parse("b.yaml", []byte("name: report\nmax_parallel: 4\nsteps:\n  - name: only\n    run: [\"/bin/true\"]\n"))
	if od.HasErrors() || xd.HasErrors() {
		t.Fatalf("either form was refused")
	}
	if oj.MaxParallel != spec.DefaultMaxParallel {
		t.Errorf("omitted parses to %d, want default %d", oj.MaxParallel, spec.DefaultMaxParallel)
	}
	oa := spec.Canonical(oj)
	xa := spec.Canonical(xj)
	if !bytes.Equal(oa, xa) {
		t.Errorf("omitting and spelling out the default hash differently:\n%s\n%s", oa, xa)
	}
}

// TestDeterminismOverMapIteration runs the sort over shuffled step orders and
// demands the same result every time, which is the acceptance criterion that
// gives a stable spec_hash and a stable topo_order.
func TestDeterminismOverMapIteration(t *testing.T) {
	byName := map[string][]string{
		"e": {"a", "d"},
		"a": {},
		"d": {"b", "c"},
		"b": {"a"},
		"c": {"a"},
	}
	want := []string{"a", "b", "c", "d", "e"}
	for i := range 50 {
		order, cycle := spec.TopoOrder(buildSteps(byName))
		if cycle != "" {
			t.Fatalf("iteration %d found a cycle %q", i, cycle)
		}
		if !sameOrder(order, want) {
			t.Errorf("iteration %d ordered %v, want %v", i, order, want)
		}
	}
}

func findDiag(t *testing.T, diags diag.List, code string) diag.Diagnostic {
	t.Helper()
	for _, d := range diags {
		if d.Code == code {
			return d
		}
	}
	t.Fatalf("no %s among %v", code, codesOnly(diags))
	return diag.Diagnostic{}
}

func codesOnly(diags diag.List) []string {
	out := make([]string, 0, len(diags))
	for _, d := range diags {
		out = append(out, d.Code)
	}
	return out
}

// renderDiagnostics renders a diagnostic list for a failure message. It is
// read by the tests above when a parse that should pass refuses on error.
func renderDiagnostics(t *testing.T, diags diag.List) string {
	t.Helper()
	var b bytes.Buffer
	_ = diag.ASCII.RenderAll(&b, diags, nil)
	return b.String()
}

// TestFanOutOverTheCeilingIsRefused builds a step that names one too many
// needs, each of which names nothing, so the only refusal is the fan-out one.
func TestFanOutOverTheCeilingIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n")
	// MaxFanOut roots, each a real step, plus one extra so the fan step can
	// cross the ceiling without naming a duplicate (duplicates are deduped).
	for i := range spec.MaxFanOut + 1 {
		fmt.Fprintf(&b, "  - name: root%d\n    run: [\"/bin/true\"]\n", i)
	}
	// The fan step names one more than the ceiling.
	fanNeeds := make([]string, 0, spec.MaxFanOut+1)
	for i := range spec.MaxFanOut + 1 {
		fanNeeds = append(fanNeeds, fmt.Sprintf("root%d", i))
	}
	fmt.Fprintf(&b, "  - name: fan\n    run: [\"/bin/true\"]\n    needs: [%s]\n", strings.Join(fanNeeds, ","))

	_, diags := spec.Parse("fan.yaml", []byte(b.String()))
	if !hasCode(diags, spec.CodeFanOutLimit) {
		t.Errorf("no fan-out refusal among %v", codesOnly(diags))
	}
}

// TestDepthOverTheCeilingIsRefused builds a chain one deeper than the
// ceiling and demands the depth refusal.
func TestDepthOverTheCeilingIsRefused(t *testing.T) {
	var b strings.Builder
	b.WriteString("name: report\nsteps:\n")
	for i := range spec.MaxDAGDepth + 2 {
		fmt.Fprintf(&b, "  - name: s%d\n    run: [\"/bin/true\"]\n", i)
		if i > 0 {
			// needs points at the step before it, making a chain.
			fmt.Fprintf(&b, "    needs: [s%d]\n", i-1)
		}
	}
	_, diags := spec.Parse("deep.yaml", []byte(b.String()))
	if !hasCode(diags, spec.CodeDAGDepthLimit) {
		t.Errorf("no depth refusal among %v", codesOnly(diags))
	}
}

func hasCode(diags diag.List, code string) bool {
	for _, d := range diags {
		if d.Code == code {
			return true
		}
	}
	return false
}

// TestEngineReadsAFrozenCycleWithoutRevalidating is the runtime boundary:
// the engine reads the spec back from the frozen JSON it was applied with,
// and FromIR does NOT run the graph validator. Validation happens at the
// door, when the file is applied or validated; a run that already exists was
// built from a graph that passed, so the engine trusts the bytes. This test
// pins that FromIR carries no cycle check, which is what keeps the engine
// from importing the jobspec validation at all.
func TestEngineReadsAFrozenCycleWithoutRevalidating(t *testing.T) {
	// A canonical document whose graph is cyclic. A user cannot get this
	// into the store, but the store is not the engine: the engine must still
	// be able to read whatever its own job_versions hold.
	doc := `{"schema":"paceq.job.v1","name":"report","steps":[` +
		`{"name":"a","run":["/bin/a"],"needs":["b"]},` +
		`{"name":"b","run":["/bin/b"],"needs":["a"]}]}`
	job, err := spec.FromIR([]byte(doc))
	if err != nil {
		t.Fatalf("the engine refused a frozen cycle it must be able to read: %v", err)
	}
	if len(job.Steps) != 2 {
		t.Errorf("read %d steps, want 2", len(job.Steps))
	}
}
