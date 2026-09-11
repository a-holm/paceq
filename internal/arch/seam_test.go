package arch_test

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"
)

// Issue #274 generalises internal/arch/activation_test.go. That guard watches
// one seam, because #182 was an inert sensor runtime and nothing between the
// evaluator's tests and the store's tests looked at the join. paceq has taken
// the same wound at least six times: limits: (#209), max_parallel (#229),
// --jobs-dir (#231), the sensor failure backoff (#251), Config.KillGrace and
// Config.RuntimeDir. Each landed in a commit that shipped a complete,
// self-tested layer and left the join for the next milestone. Both halves were
// green, because each suite certified half a feature.
//
// Every other guard here sits at the edge of a layer: SQL stays in store, time
// stays in clock, no -short in the gate. An edge guard cannot see a value that
// enters at the top and reaches nothing at the bottom. This one watches the
// path instead of either end of it.
//
// The two definitions are the whole difficulty, and both are drawn narrow on
// purpose. A guard that fires on internal plumbing gets a blanket exception
// added and is dead from that day.
//
// Boundary one, what is operator-facing. An operator names two things and
// nothing else: a command-line flag and a key in config.yaml. So the subject
// set grows from those two sites, and exactly one link further:
//
//	(A) a struct with a field bound to a pflag FlagSet, which is serveFlags
//	    and its siblings. Binding a flag puts the field in scope with no list
//	    to maintain, which is what makes the next --jobs-dir hard to miss.
//	(B) a struct with a yaml tag, which is a config.yaml section.
//	(C) a struct that a field of an (A) or (B) struct is copied straight into,
//	    by a composite literal key or an assignment. This is the hop that
//	    reaches daemon.Config, where --jobs-dir actually died.
//
// (C) does not iterate, and that is the boundary. It is why obs.DiskGuardConfig
// stays out: daemon.Config is carried into it, but daemon.Config is itself only
// in the set by (C), so the walk stops. Were (C) transitive the subject set
// would grow until it covered every struct the daemon touches, which is the
// failure this issue names.
//
// Two things sit outside the boundary and stay outside. A flag declared with no
// backing field (errorcmd's --list, read back through GetBool) has no field to
// check. A struct documented as operator-facing but wired to no decoder is not
// reachable from an input site, so this guard is silent about it.
//
// Boundary two, what is a production reader. A field is read if a selector
// expression in a production package resolves, through go/types, to that exact
// field, is not the left side of a plain assignment, and sits in a live
// function. Three parts of that carry the weight.
//
// Types, not names. Name matching is not an approximation here, it is a
// different answer: engine.Engine declares RenewInterval, ClockSkewAllowance,
// RequeueBackoff and MaxCrashCount of its own and reads all of them, so a guard
// matching .FieldName sees the engine's reads and calls the daemon's dead
// copies of those four names alive.
//
// Production, by construction. The load sets Tests:false, so test files are not
// in the corpus and no filename filter can be got wrong. It is also much
// cheaper than loading the test variants, which type-check every package twice
// over.
//
// Live, not merely present. Config.KillGrace is read exactly once, by the
// unexported accessor killGrace(). Under a plain depth-one check, adding a
// private getter would be a standing way to silence this guard, and this repo
// writes one such accessor per field. A function is live when it is exported,
// or is init or main, or is reached from a live function; file scope is live.
//
// Where it is weak, plainly: the guard proves a value leaves the struct into
// live production code. It cannot prove anything acts on it. Config.RuntimeDir
// was copied and logged for a milestone, and a log call is a read by this
// predicate. Catching that needs a person or a behavioural test.
//
// Boundary three, what is a constructor. An exported function under internal/,
// named New or New<Something>, returning a type declared in its own package.
// worker.New is the historical case and the shape is this narrow on purpose.
// Widening to every exported function under internal/ turns up subjects by the
// dozen, nearly all of them test support, assertion helpers and catalogue
// accessors, and a guard whose allowlist is longer than its rule is a guard
// nobody reads.

// seamExceptions are the subjects that reach no live production reader and are
// allowed to, each with its reason. An entry is not an excuse: for a dead
// operator input it is the filed finding, and it names the issue that decides
// whether the input is wired or removed.
//
// The keys are checked against the subjects the walk actually finds, so an
// entry cannot outlive its subject: deleting Config.JobsDir turns this table
// red until the line goes with it.
var seamExceptions = map[string]string{
	"daemon.Config.JobsDir": "the --jobs-dir flag reaches the config and stops there; the " +
		"scheduler has never read it. #231 decides wire or remove.",
	"daemon.Config.RuntimeDir": "accepted and carried, never acted on; the runtime directory " +
		"contract was left to the systemd work and never collected. No decision is queued.",
	"daemon.Config.RenewInterval": "documented as the lease renewal override and never forwarded " +
		"to the engine, which resolves its own. No decision is queued.",
	"daemon.Config.ClockSkewAllowance": "documented as the reaper's skew grace and never " +
		"forwarded to the engine, which resolves its own. No decision is queued.",
	"daemon.Config.RequeueBackoff": "documented as the requeue delay and never forwarded to the " +
		"engine, which resolves its own. No decision is queued.",
	"daemon.Config.MaxCrashCount": "documented as the poison quarantine line and never forwarded " +
		"to the engine, which resolves its own. No decision is queued.",
	"cli.runFlags.wait": "--wait is bound and never read, which its own help text admits: a " +
		"foreground run always waits and the flag exists to say so out loud. An inert flag is " +
		"still an operator promise nothing keeps. No decision is queued.",
	"cli.cutoverSource.user": "a dead copy rather than a dead input: readCutoverSource fills it " +
		"from --user, and every reader goes to cutoverFlags.user instead. No decision is queued.",
	"cli.cutoverSource.file": "a dead copy rather than a dead input: readCutoverSource fills it " +
		"from --file, and every reader goes to cutoverFlags.file instead. No decision is queued.",
	"worker.New": "the parallel executor is built and exercised by its own tests only; the claim " +
		"gate shipped in #5 and the pool was never joined to it. #229 decides wire or remove, " +
		"and names this constructor.",
	"clock.NewDetector": "the wall-clock jump detector has tests and no caller, the same shape as " +
		"worker.New. No decision is queued.",
}

// --- loading ---

// seamModule is the loaded module. The load is the expensive part of this file
// and it is identical for all three checks, so they share one.
var seamModule struct {
	once sync.Once
	pkgs []*packages.Package
	fset *token.FileSet
	err  error
}

func loadSeamModule(t *testing.T) ([]*packages.Package, *token.FileSet) {
	t.Helper()

	root := repoRoot(t)
	seamModule.once.Do(func() {
		fset := token.NewFileSet()
		pkgs, err := packages.Load(&packages.Config{
			// Types for the module's own packages, from source. Their
			// dependencies come from export data rather than being
			// type-checked, which is what keeps the pass cheap.
			Mode: packages.NeedName | packages.NeedFiles | packages.NeedSyntax |
				packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Fset:  fset,
			Dir:   root,
			Tests: false,
		}, "./...")
		seamModule.pkgs, seamModule.fset, seamModule.err = pkgs, fset, err
	})
	if seamModule.err != nil {
		t.Fatalf("load the module: %v", seamModule.err)
	}
	if len(seamModule.pkgs) == 0 {
		t.Fatal("./... matched no packages, so this guard proved nothing")
	}
	var broken []string
	for _, p := range seamModule.pkgs {
		for _, e := range p.Errors {
			broken = append(broken, p.PkgPath+": "+e.Error())
		}
	}
	if len(broken) > 0 {
		t.Fatalf("the module does not type-check, so this guard proved nothing:\n  %s",
			strings.Join(broken, "\n  "))
	}
	return seamModule.pkgs, seamModule.fset
}

// --- the walk ---

// site is what one function body, or one file scope, refers to.
type site struct {
	fields []*types.Var
	funcs  []*types.Func
}

// seamWorld is everything the three checks read off one pass over the module.
type seamWorld struct {
	root string
	fset *token.FileSet

	// subjects maps an operator-facing struct to the name a person calls it.
	subjects map[*types.Struct]string
	// flagNames maps a bound field to the flag an operator types for it.
	flagNames map[*types.Var]string
	// readFields holds every field a live production selector reads.
	readFields map[*types.Var]bool
	// usedFuncs holds every function live production code names.
	usedFuncs map[*types.Func]bool
	// ctors are the exported constructors under internal/.
	ctors map[*types.Func]string

	// The recognition counters. A zero in any of them means the walk stopped
	// finding its subject, and the green that follows is worthless.
	flagBindings, yamlStructs, carried int
}

var seamOnce struct {
	sync.Once
	world *seamWorld
}

func seamScan(t *testing.T) *seamWorld {
	t.Helper()

	pkgs, fset := loadSeamModule(t)
	root := repoRoot(t)
	seamOnce.Do(func() { seamOnce.world = buildSeamWorld(pkgs, fset, root) })
	return seamOnce.world
}

func buildSeamWorld(pkgs []*packages.Package, fset *token.FileSet, root string) *seamWorld {
	w := &seamWorld{
		root:       root,
		fset:       fset,
		subjects:   map[*types.Struct]string{},
		flagNames:  map[*types.Var]string{},
		readFields: map[*types.Var]bool{},
		usedFuncs:  map[*types.Func]bool{},
		ctors:      map[*types.Func]string{},
	}

	names := structNames(pkgs)

	// bound are the selectors a pflag binding takes the address of. pflag
	// writes through that pointer, so the binding is not a read of the field:
	// counting it as one would let a flag be declared, bound and never looked
	// at again while this guard stayed green, which is #231 exactly.
	bound := map[*ast.SelectorExpr]bool{}

	// (A) and (B): the two sites an operator names a field at.
	seeds := map[*types.Struct]bool{}
	admit := func(st *types.Struct, label string) {
		if st == nil {
			return
		}
		if label == "" {
			label = names[st]
		}
		if label == "" {
			label = st.String()
		}
		if _, known := w.subjects[st]; !known {
			w.subjects[st] = label
		}
	}

	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				field, owner, target, flag, ok := flagBinding(p, call)
				if !ok {
					return true
				}
				w.flagBindings++
				bound[target] = true
				w.flagNames[field] = flag
				seeds[owner] = true
				admit(owner, "")
				return true
			})
		}
	}

	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				spec, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				def := p.TypesInfo.Defs[spec.Name]
				if def == nil {
					return true
				}
				st, ok := def.Type().Underlying().(*types.Struct)
				if !ok || seeds[st] || !hasYAMLTag(st) {
					return true
				}
				// A section written inline, as notifierDoc's notify_defaults
				// is, has no type name of its own and would otherwise never
				// be looked at.
				admitYAMLSections(st, short(p.PkgPath)+"."+spec.Name.Name, seeds, admit, &w.yamlStructs)
				return true
			})
		}
	}

	// (C): one hop. A struct a seed's field is copied straight into.
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				switch node := n.(type) {
				case *ast.CompositeLit:
					st := structOf(p.TypesInfo.TypeOf(node))
					if st == nil || seeds[st] {
						return true
					}
					for _, elt := range node.Elts {
						kv, ok := elt.(*ast.KeyValueExpr)
						if !ok || !carriesSeedField(p, seeds, kv.Value) {
							continue
						}
						w.carried++
						admit(st, "")
					}
				case *ast.AssignStmt:
					for i, lhs := range node.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || i >= len(node.Rhs) {
							continue
						}
						st := structOf(p.TypesInfo.TypeOf(sel.X))
						if st == nil || seeds[st] || !carriesSeedField(p, seeds, node.Rhs[i]) {
							continue
						}
						w.carried++
						admit(st, "")
					}
				}
				return true
			})
		}
	}

	// Reads and references, attributed to the function that holds them, then
	// narrowed to the live ones.
	fileScope := &site{}
	sites := map[*types.Func]*site{}
	callees := map[*types.Func][]*types.Func{}
	live := map[*types.Func]bool{}

	for _, p := range pkgs {
		for _, f := range p.Syntax {
			written := writtenSelectors(f)
			for _, decl := range f.Decls {
				fn, _ := decl.(*ast.FuncDecl)
				if fn == nil {
					collect(p, decl, written, bound, fileScope, nil, callees)
					continue
				}
				obj, _ := p.TypesInfo.Defs[fn.Name].(*types.Func)
				if obj == nil {
					continue
				}
				s := sites[obj]
				if s == nil {
					s = &site{}
					sites[obj] = s
				}
				if obj.Exported() || fn.Name.Name == "init" || fn.Name.Name == "main" {
					live[obj] = true
				}
				collect(p, fn, written, bound, s, obj, callees)
			}
		}
	}

	// File scope always runs, so what it names is live.
	for _, fn := range fileScope.funcs {
		live[fn] = true
	}
	for changed := true; changed; {
		changed = false
		for fn, called := range callees {
			if !live[fn] {
				continue
			}
			for _, c := range called {
				if !live[c] {
					live[c] = true
					changed = true
				}
			}
		}
	}

	record := func(s *site) {
		for _, v := range s.fields {
			w.readFields[v] = true
		}
		for _, fn := range s.funcs {
			w.usedFuncs[fn] = true
		}
	}
	record(fileScope)
	for fn, s := range sites {
		if live[fn] {
			record(s)
		}
	}

	// Constructors.
	for _, p := range pkgs {
		if !strings.HasPrefix(p.PkgPath, internalPrefix) {
			continue
		}
		scope := p.Types.Scope()
		for _, name := range scope.Names() {
			fn, ok := scope.Lookup(name).(*types.Func)
			if !ok || !fn.Exported() || !strings.HasPrefix(name, "New") {
				continue
			}
			sig, ok := fn.Type().(*types.Signature)
			if !ok || sig.Recv() != nil || !returnsOwnType(sig, p.Types) {
				continue
			}
			w.ctors[fn] = strings.TrimPrefix(p.PkgPath, internalPrefix) + "." + name
		}
	}

	return w
}

// collect walks one declaration and records what it reads and what it names.
func collect(p *packages.Package, node ast.Node, written, bound map[*ast.SelectorExpr]bool, s *site, owner *types.Func, callees map[*types.Func][]*types.Func) {
	note := func(fn *types.Func) {
		s.funcs = append(s.funcs, fn)
		if owner != nil {
			callees[owner] = append(callees[owner], fn)
		}
	}
	ast.Inspect(node, func(n ast.Node) bool {
		switch e := n.(type) {
		case *ast.SelectorExpr:
			if written[e] || bound[e] {
				// Filling a field is not reading it, but the expression the
				// value comes from still is.
				return true
			}
			if sel := p.TypesInfo.Selections[e]; sel != nil {
				if v, ok := sel.Obj().(*types.Var); ok && v.IsField() {
					s.fields = append(s.fields, v)
					return true
				}
			}
			if fn, ok := p.TypesInfo.Uses[e.Sel].(*types.Func); ok {
				note(fn)
			}
		case *ast.Ident:
			if fn, ok := p.TypesInfo.Uses[e].(*types.Func); ok {
				note(fn)
			}
		}
		return true
	})
}

// writtenSelectors are the selectors on the left of a plain assignment in one
// file. A compound assignment (x.f += 1) reads the field, so it is not here.
func writtenSelectors(f *ast.File) map[*ast.SelectorExpr]bool {
	written := map[*ast.SelectorExpr]bool{}
	ast.Inspect(f, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || (as.Tok != token.ASSIGN && as.Tok != token.DEFINE) {
			return true
		}
		for _, lhs := range as.Lhs {
			if sel, ok := lhs.(*ast.SelectorExpr); ok {
				written[sel] = true
			}
		}
		return true
	})
	return written
}

// --- the three checks ---

func TestOperatorInputHasAProductionReader(t *testing.T) {
	w := seamScan(t)

	if w.flagBindings == 0 {
		t.Fatal("no field is bound to a command-line flag anywhere in the module: the flag " +
			"surface moved off pflag and this guard watches nothing")
	}
	if w.yamlStructs == 0 {
		t.Fatal("no struct carries a yaml tag: the configuration file surface moved and this " +
			"guard watches nothing")
	}
	if w.carried == 0 {
		t.Fatal("no operator field is copied into another struct: the hop from the flag set to " +
			"the daemon config moved, and this guard now watches the flag structs alone")
	}

	checked := 0
	var dead []string
	for st, label := range w.subjects {
		for i := 0; i < st.NumFields(); i++ {
			field := st.Field(i)
			if field.Embedded() {
				continue
			}
			checked++
			subject := label + "." + field.Name()
			if w.readFields[field] {
				continue
			}
			if _, excused := seamExceptions[subject]; excused {
				continue
			}
			dead = append(dead, fmt.Sprintf(
				"%s: %s%s is set by an operator and no live production code reads it",
				w.where(field.Pos()), subject, flagSuffix(w.flagNames[field])))
		}
	}
	sort.Strings(dead)
	for _, line := range dead {
		t.Error(line)
	}
	if checked == 0 {
		t.Fatal("the operator-facing structs have no fields between them, so nothing was checked")
	}
	t.Logf("operator-facing structs %d, fields checked %d, excused %d, flag bindings %d, yaml structs %d, carried %d\n  %s",
		len(w.subjects), checked, len(seamExceptions), w.flagBindings, w.yamlStructs, w.carried,
		strings.Join(sortedValues(w.subjects), "\n  "))
}

func TestExportedConstructorsHaveAProductionCaller(t *testing.T) {
	w := seamScan(t)

	if len(w.ctors) == 0 {
		t.Fatal("no exported New constructor exists under internal/: the shape moved and this " +
			"guard proves nothing")
	}
	var dead []string
	for fn, label := range w.ctors {
		if w.usedFuncs[fn] {
			continue
		}
		if _, excused := seamExceptions[label]; excused {
			continue
		}
		dead = append(dead, fmt.Sprintf("%s: %s is exported under internal/ and no production "+
			"code calls it, so its own tests are the only thing holding it up",
			w.where(fn.Pos()), label))
	}
	sort.Strings(dead)
	for _, line := range dead {
		t.Error(line)
	}
	t.Logf("exported constructors under internal/: %d", len(w.ctors))
}

func TestSeamExceptionsNameLiveSubjects(t *testing.T) {
	w := seamScan(t)

	known := map[string]bool{}
	for st, label := range w.subjects {
		for i := 0; i < st.NumFields(); i++ {
			known[label+"."+st.Field(i).Name()] = true
		}
	}
	for _, label := range w.ctors {
		known[label] = true
	}

	for _, subject := range sortedKeysOf(seamExceptions) {
		if strings.TrimSpace(seamExceptions[subject]) == "" {
			t.Errorf("the exception for %s carries no reason", subject)
		}
		if !known[subject] {
			t.Errorf("the exception list names %s, which the walk no longer finds: either the "+
				"subject is gone and this line goes with it, or the walk stopped recognising it",
				subject)
		}
	}
}

// --- helpers ---

// flagBinding reports the field a pflag binding writes into, the struct that
// field belongs to, and the flag an operator types. The receiver type is what
// is checked, not the method name, so a helper wrapping FlagSet cannot hide a
// binding.
func flagBinding(p *packages.Package, call *ast.CallExpr) (field *types.Var, owner *types.Struct, target *ast.SelectorExpr, flag string, ok bool) {
	sel, isSel := call.Fun.(*ast.SelectorExpr)
	if !isSel || !strings.Contains(sel.Sel.Name, "Var") || len(call.Args) < 2 {
		return nil, nil, nil, "", false
	}
	selection := p.TypesInfo.Selections[sel]
	if selection == nil {
		return nil, nil, nil, "", false
	}
	recv, isNamed := unpoint(selection.Recv()).(*types.Named)
	if !isNamed || recv.Obj().Pkg() == nil ||
		recv.Obj().Pkg().Path() != "github.com/spf13/pflag" || recv.Obj().Name() != "FlagSet" {
		return nil, nil, nil, "", false
	}
	unary, isUnary := call.Args[0].(*ast.UnaryExpr)
	if !isUnary || unary.Op != token.AND {
		return nil, nil, nil, "", false
	}
	target, isSel = unary.X.(*ast.SelectorExpr)
	if !isSel {
		return nil, nil, nil, "", false
	}
	targetSel := p.TypesInfo.Selections[target]
	if targetSel == nil {
		return nil, nil, nil, "", false
	}
	v, isVar := targetSel.Obj().(*types.Var)
	if !isVar || !v.IsField() {
		return nil, nil, nil, "", false
	}
	st := structOf(targetSel.Recv())
	if st == nil {
		return nil, nil, nil, "", false
	}
	if lit, isLit := call.Args[1].(*ast.BasicLit); isLit {
		if unquoted, err := strconv.Unquote(lit.Value); err == nil {
			flag = unquoted
		}
	}
	return v, st, target, flag, true
}

// carriesSeedField reports whether the expression is, directly, a read of a
// field of an operator-facing struct. Directly is the point: it is what holds
// the hop to one link.
func carriesSeedField(p *packages.Package, seeds map[*types.Struct]bool, expr ast.Expr) bool {
	for {
		switch e := expr.(type) {
		case *ast.ParenExpr:
			expr = e.X
		case *ast.CallExpr:
			// A conversion carries the value across; a call does not.
			if len(e.Args) != 1 || !p.TypesInfo.Types[e.Fun].IsType() {
				return false
			}
			expr = e.Args[0]
		case *ast.SelectorExpr:
			sel := p.TypesInfo.Selections[e]
			if sel == nil {
				return false
			}
			if v, ok := sel.Obj().(*types.Var); !ok || !v.IsField() {
				return false
			}
			st := structOf(sel.Recv())
			return st != nil && seeds[st]
		default:
			return false
		}
	}
}

// structNames maps every declared struct type to package.TypeName, so a failure
// names the type a person would grep for rather than its written-out form.
func structNames(pkgs []*packages.Package) map[*types.Struct]string {
	names := map[*types.Struct]string{}
	for _, p := range pkgs {
		for _, f := range p.Syntax {
			ast.Inspect(f, func(n ast.Node) bool {
				spec, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				def := p.TypesInfo.Defs[spec.Name]
				if def == nil {
					return true
				}
				if st, ok := def.Type().Underlying().(*types.Struct); ok {
					if _, seen := names[st]; !seen {
						names[st] = short(p.PkgPath) + "." + spec.Name.Name
					}
				}
				return true
			})
		}
	}
	return names
}

// admitYAMLSections puts a config.yaml struct in the subject set along with
// every section written inline inside it, which have no type name to be found
// under.
func admitYAMLSections(st *types.Struct, label string, seeds map[*types.Struct]bool, admit func(*types.Struct, string), count *int) {
	if st == nil || seeds[st] || !hasYAMLTag(st) {
		return
	}
	*count++
	seeds[st] = true
	admit(st, label)
	for i := 0; i < st.NumFields(); i++ {
		if inner, ok := st.Field(i).Type().Underlying().(*types.Struct); ok {
			admitYAMLSections(inner, label+"."+st.Field(i).Name(), seeds, admit, count)
		}
	}
}

func hasYAMLTag(st *types.Struct) bool {
	for i := 0; i < st.NumFields(); i++ {
		if strings.Contains(st.Tag(i), `yaml:"`) {
			return true
		}
	}
	return false
}

func returnsOwnType(sig *types.Signature, own *types.Package) bool {
	for i := 0; i < sig.Results().Len(); i++ {
		if named, ok := unpoint(sig.Results().At(i).Type()).(*types.Named); ok && named.Obj().Pkg() == own {
			return true
		}
	}
	return false
}

// structOf is the struct a value of this type is, through a pointer and through
// a named type. Nil for anything that is not a struct.
func structOf(t types.Type) *types.Struct {
	if t == nil {
		return nil
	}
	st, _ := unpoint(t).Underlying().(*types.Struct)
	return st
}

func unpoint(t types.Type) types.Type {
	if ptr, ok := t.Underlying().(*types.Pointer); ok {
		return ptr.Elem()
	}
	return t
}

func short(pkgPath string) string {
	if strings.HasPrefix(pkgPath, internalPrefix) {
		return strings.TrimPrefix(pkgPath, internalPrefix)
	}
	return strings.TrimPrefix(pkgPath, modulePath+"/")
}

// where is the file and line of a declaration, relative to the module root.
func (w *seamWorld) where(pos token.Pos) string {
	at := w.fset.Position(pos)
	if rel, err := filepath.Rel(w.root, at.Filename); err == nil {
		at.Filename = rel
	}
	return at.Filename + ":" + strconv.Itoa(at.Line)
}

func flagSuffix(flag string) string {
	if flag == "" {
		return ""
	}
	return " (--" + flag + ")"
}

func sortedValues(m map[*types.Struct]string) []string {
	out := make([]string, 0, len(m))
	for _, v := range m {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func sortedKeysOf(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
