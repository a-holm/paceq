package chaos

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// The daemon under test is the real binary, the kills are real SIGKILLs, and
// every assertion is a set of legal outcomes plus invariants. The pieces:
//
//	newChaosWorkspace  one project directory with an applied .paceq state
//	chaosShapes        the DAG shape catalogue (data free effect counters)
//	seedRuns           N manual runs materialised before any daemon lives
//	startDaemon        one serve subprocess, stderr kept in a file per gen
//	killThresholds     the seeded schedule, counted in completed runs
//	chaosRun.Drive     seeds, drives the kill/restart loop, runs the battery
//	check*             one invariant each, returning human findings
//
// Recovery pacing is product reality: a run parked by a SIGKILL waits out
// its dead executor's lease (sixty seconds) plus the reaper's skew and its
// own requeue backoff. The budgets below are sized for that, not against it.

const (
	pollEvery       = 200 * time.Millisecond
	daemonReadyWait = 20 * time.Second
	daemonExitWait  = 45 * time.Second
	orphanWait      = 30 * time.Second

	envSeed      = "PACEQ_CHAOS_SEED"
	envArtifacts = "PACEQ_CHAOS_ARTIFACTS"

	runEnvMarker = "PACEQ_RUN_ID="
)

// chaosWorkspace is one project directory for one sweep. The layout is what
// `paceq` itself resolves: the state directory is <Dir>/.paceq.
type chaosWorkspace struct {
	Dir        string // project directory; serve runs with this as cwd
	StateDir   string // <Dir>/.paceq: lock file, database, logs
	DBPath     string // <StateDir>/state.db
	EffectFile string // every append step's effect log, one line per execution
}

func newChaosWorkspace(t *testing.T) *chaosWorkspace {
	t.Helper()

	dir := t.TempDir()
	ws := &chaosWorkspace{
		Dir:        dir,
		StateDir:   filepath.Join(dir, ".paceq"),
		DBPath:     filepath.Join(dir, ".paceq", store.DatabaseFileName),
		EffectFile: filepath.Join(dir, "effects.txt"),
	}
	if err := os.MkdirAll(ws.StateDir, 0o700); err != nil {
		t.Fatalf("create the state directory: %v", err)
	}
	s := openStore(t, ws)
	if err := s.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	closeStore(t, s)
	// A plain store.Open creates the database without the mode rule
	// OpenState enforces; tighten it so the daemon accepts what we seeded.
	if err := os.Chmod(ws.DBPath, 0o600); err != nil {
		t.Fatalf("tighten the database mode: %v", err)
	}
	return ws
}

func openStore(t *testing.T, ws *chaosWorkspace) *store.Store {
	t.Helper()

	s, err := store.Open(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open store at %s: %v", ws.DBPath, err)
	}
	return s
}

func closeStore(t *testing.T, s *store.Store) {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Errorf("close store: %v", err)
	}
}

// moduleRoot walks up from the working directory until it finds go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("find the working directory: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("no go.mod above the harness")
		}
		dir = parent
	}
}

// durableTempDir makes a directory that lives for the whole test binary run.
// A subtest tempdir would take a once-built fixture away from later rows.
func durableTempDir(t *testing.T, pattern string) string {
	t.Helper()

	dir, err := os.MkdirTemp("", pattern)
	if err != nil {
		t.Fatalf("tempdir: %v", err)
	}
	return dir
}

var (
	paceqOnce  sync.Once
	paceqPath  string
	fakeOnce   sync.Once
	fakePath   string
	appendOnce sync.Once
	appendPath string
)

// paceqBinary builds the real daemon once.
func paceqBinary(t *testing.T) string {
	t.Helper()

	paceqOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-chaos-bin"), "paceq")
		build := exec.Command("go", "build", "-buildvcs=false", "-o", path, "./cmd/paceq")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build paceq: %v\n%s", err, out)
		}
		paceqPath = path
	})
	return paceqPath
}

// fakeCommand builds internal/runner's fakecmd once. Its sleep gives a kill
// something to interrupt, exit fails a step on demand, grandchild leaves a
// deeper orphan tree behind.
func fakeCommand(t *testing.T) string {
	t.Helper()

	fakeOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-chaos-fakecmd"), "fakecmd")
		build := exec.Command("go", "build", "-buildvcs=false", "-o", path, "./internal/runner/testdata/fakecmd")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build fakecmd: %v\n%s", err, out)
		}
		fakePath = path
	})
	return fakePath
}

// appendCommand builds the root testdata/fakecmd once: the effect counter,
// a separate fixture from the runner's process-behaviour one. Its append
// mode is what every app step runs; its lines of idempotency key, attempt
// and stamp are AC-7's attributable effect count.
func appendCommand(t *testing.T) string {
	t.Helper()

	appendOnce.Do(func() {
		path := filepath.Join(durableTempDir(t, "paceq-chaos-append"), "append-fixture")
		build := exec.Command("go", "build", "-buildvcs=false", "-o", path, "./testdata/fakecmd")
		build.Dir = moduleRoot(t)
		if out, err := build.CombinedOutput(); err != nil {
			t.Fatalf("build the append fixture: %v\n%s", err, out)
		}
		appendPath = path
	})
	return appendPath
}

// shape is one DAG skeleton. The spec JSON and the frozen NewStep edges say
// the same thing, because the executor reads the frozen edges and nothing
// else. Every step command is data free: effects land in one shared file as
// lines of idempotency key, attempt, stamp (AC-7's attributable counter).
type shape struct {
	name  string
	spec  func(job string) string
	steps []store.NewStep
}

// chaosShapes returns the shape catalogue. Policy choices live here and only
// here: which shapes join the mix and how long their sleeps run. No shape
// uses first-drip: an un-killed first attempt would drip until its cap, and
// a chaos suite must not hang because one kill failed to arrive.
func chaosShapes(t *testing.T, ws *chaosWorkspace) []shape {
	fix := fakeCommand(t)
	appFix := appendCommand(t)
	eff := ws.EffectFile

	jobJSON := func(job string, maxParallel int, steps []string) string {
		return fmt.Sprintf(`{"schema":"paceq.job.v1","name":%q,"max_concurrent":64,`+
			`"max_parallel":%d,"steps":[%s]}`, job, maxParallel, strings.Join(steps, ","))
	}
	stepJSON := func(name, needs string, cmd []string) string {
		parts := make([]string, 0, len(cmd))
		for _, c := range cmd {
			parts = append(parts, strconv.Quote(c))
		}
		need := ""
		if needs != "" {
			need = `,"needs":[` + strconv.Quote(needs) + `]`
		}
		return fmt.Sprintf(`{"name":%q,"run":[%s],"shell":false%s}`,
			name, strings.Join(parts, ","), need)
	}
	app := func(name, needs string) string {
		return stepJSON(name, needs, []string{appFix, "append", eff})
	}
	sleep := func(name, needs, d string) string {
		return stepJSON(name, needs, []string{fix, "sleep", d})
	}

	return []shape{
		{
			name: "solo",
			spec: func(job string) string {
				return jobJSON(job, 1, []string{app("only", "")})
			},
			steps: []store.NewStep{{Name: "only"}},
		},
		{
			name: "chain3",
			spec: func(job string) string {
				return jobJSON(job, 2, []string{
					app("a", ""),
					sleep("b", "a", "900ms"),
					app("c", "b"),
				})
			},
			steps: []store.NewStep{
				{Name: "a"},
				{Name: "b", DependsOn: []string{"a"}},
				{Name: "c", DependsOn: []string{"b"}},
			},
		},
		{
			// failfan plants a deterministic failure under a fan-out so
			// convergence has to carry STEP_SKIPPED_UPSTREAM_FAILED rows
			// through restarts (skip propagation under chaos).
			name: "failfan",
			spec: func(job string) string {
				return jobJSON(job, 3, []string{
					app("root", ""),
					stepJSON("mid", "root", []string{fix, "exit", "1"}),
					app("leaf-a", "mid"),
					app("leaf-b", "mid"),
				})
			},
			steps: []store.NewStep{
				{Name: "root"},
				{Name: "mid", DependsOn: []string{"root"}},
				{Name: "leaf-a", DependsOn: []string{"mid"}},
				{Name: "leaf-b", DependsOn: []string{"mid"}},
			},
		},
		{
			// diamond is the classic parallel join; the sleep and the
			// grandchild widen the window in which a kill lands mid
			// fan-out and leave deeper process trees to reap.
			name: "diamond",
			spec: func(job string) string {
				return jobJSON(job, 4, []string{
					app("root", ""),
					sleep("left", "root", "1200ms"),
					stepJSON("right", "root", []string{fix, "grandchild", "1500ms"}),
					app("join", "left,right"),
				})
			},
			steps: []store.NewStep{
				{Name: "root"},
				{Name: "left", DependsOn: []string{"root"}},
				{Name: "right", DependsOn: []string{"root"}},
				{Name: "join", DependsOn: []string{"left", "right"}},
			},
		},
		{
			// wide4 spreads four parallel leaves under a capped step width,
			// so claims queue inside one run too.
			name: "wide4",
			spec: func(job string) string {
				return jobJSON(job, 2, []string{
					app("root", ""),
					sleep("w1", "root", "600ms"),
					app("w2", "root"),
					sleep("w3", "root", "300ms"),
					app("w4", "root"),
					app("collector", "w1,w2,w3,w4"),
				})
			},
			steps: []store.NewStep{
				{Name: "root"},
				{Name: "w1", DependsOn: []string{"root"}},
				{Name: "w2", DependsOn: []string{"root"}},
				{Name: "w3", DependsOn: []string{"root"}},
				{Name: "w4", DependsOn: []string{"root"}},
				{Name: "collector", DependsOn: []string{"w1", "w2", "w3", "w4"}},
			},
		},
	}
}

// seedRuns materialises n manual runs cycling through the shapes, before any
// daemon exists to claim them. Every run belongs to one job name, which is
// also the listing filter during the sweep.
func seedRuns(t *testing.T, ws *chaosWorkspace, shapes []shape, n int, jobPrefix string) []string {
	t.Helper()

	ctx := context.Background()
	s := openStore(t, ws)
	defer closeStore(t, s)

	versions := map[string]string{}
	for _, sh := range shapes {
		version, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
			JobName:  jobNameForShape(jobPrefix, sh.name),
			SpecHash: "sha256:" + sh.name + "-" + strconv.Itoa(os.Getpid()),
			SpecJSON: sh.spec(jobNameForShape(jobPrefix, sh.name)),
		})
		if err != nil {
			t.Fatalf("apply shape %s: %v", sh.name, err)
		}
		versions[sh.name] = version.ID
	}

	ids := make([]string, 0, n)
	for i := 0; i < n; i++ {
		sh := shapes[i%len(shapes)]
		run, err := s.CreateRunWithSteps(ctx, store.NewRun{
			JobName:      jobNameForShape(jobPrefix, sh.name),
			JobVersionID: versions[sh.name],
			Origin:       "manual",
			Actor:        "chaos",
			Steps:        sh.steps,
		})
		if err != nil {
			t.Fatalf("materialise run %d of shape %s: %v", i, sh.name, err)
		}
		ids = append(ids, run.ID)
	}
	return ids
}

func jobNameForShape(prefix, shape string) string {
	return prefix + "-" + shape
}

// daemonProc is one running paceq serve subprocess. Its stderr streams into
// a file named after the generation, so every kill's evidence survives in
// the workspace and lands in the failure archive untouched.
type daemonProc struct {
	cmd    *exec.Cmd
	stderr string // path of this generation's stderr file
	exited chan int
}

func startDaemon(t *testing.T, ws *chaosWorkspace, gen int) *daemonProc {
	t.Helper()

	errFile := filepath.Join(ws.Dir, fmt.Sprintf("daemon-%d.stderr", gen))
	out, err := os.OpenFile(errFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("open %s: %v", errFile, err)
	}

	p := &daemonProc{
		cmd:    exec.Command(paceqBinary(t), "serve"),
		stderr: errFile,
		exited: make(chan int, 1),
	}
	p.cmd.Dir = ws.Dir
	p.cmd.Stderr = out
	if err := p.cmd.Start(); err != nil {
		_ = out.Close()
		t.Fatalf("start paceq serve: %v", err)
	}
	go func() {
		err := p.cmd.Wait()
		code := 0
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				code = ee.ExitCode()
			} else {
				code = -1
			}
		}
		_ = out.Close()
		p.exited <- code
	}()
	t.Cleanup(func() {
		select {
		case <-p.exited:
		default:
			_ = p.cmd.Process.Kill()
		}
	})
	return p
}

// waitReady blocks until the daemon logged its ready line. The scan reads
// the generation's stderr file, never memory, so it works across restarts.
func (p *daemonProc) waitReady(t *testing.T) {
	t.Helper()

	deadline := time.Now().Add(daemonReadyWait)
	for {
		raw, err := os.ReadFile(p.stderr)
		if err == nil && bytes.Contains(raw, []byte(`"msg":"daemon ready"`)) {
			return
		}
		select {
		case code := <-p.exited:
			p.exited <- code // leave the death visible for later readers
			snippet := lastLines(string(raw), 15)
			t.Fatalf("the daemon exited %d before becoming ready:\n%s", code, snippet)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("the daemon never became ready:\n%s", lastLines(string(raw), 25))
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (p *daemonProc) signal(t *testing.T, sig syscall.Signal) {
	t.Helper()
	if err := p.cmd.Process.Signal(sig); err != nil {
		t.Fatalf("send %s: %v", sig, err)
	}
}

func (p *daemonProc) waitExit(t *testing.T, within time.Duration) int {
	t.Helper()

	select {
	case code := <-p.exited:
		return code
	case <-time.After(within):
		t.Fatalf("the daemon did not exit within %s", within)
		return -1
	}
}

func (p *daemonProc) alive() bool {
	select {
	case <-p.exited:
		return false
	default:
		return true
	}
}

func lastLines(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// killThresholds turns a seed into the kill schedule: k distinct points,
// counted in completed runs, sorted ascending. Pure function of seed, n and
// k, so naming the seed replays the schedule (review warning five of #20).
func killThresholds(seed int64, n, k int) []int {
	r := rand.New(rand.NewSource(seed))
	seen := map[int]bool{}
	for len(seen) < k && len(seen) < n-1 {
		seen[1+r.Intn(n-1)] = true
	}
	out := make([]int, 0, len(seen))
	for v := range seen {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// readRunStates lists the sweep's runs straight from the database through a
// read-only open. SQL stays inside internal/store; the filter does the rest.
func readRunStates(t *testing.T, ws *chaosWorkspace, jobPrefix string) map[string]string {
	t.Helper()

	s, err := store.OpenReadOnly(context.Background(), ws.DBPath, store.Options{})
	if err != nil {
		t.Fatalf("open the state read-only: %v", err)
	}
	defer func() { _ = s.Close() }()

	rows, err := s.ListRuns(context.Background(), store.RunFilter{Limit: 500})
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	out := make(map[string]string, len(rows))
	for _, r := range rows {
		if strings.HasPrefix(r.JobName, jobPrefix+"-") || r.JobName == jobPrefix {
			out[r.ID] = r.State
		}
	}
	return out
}

var terminalRunStates = map[string]bool{
	"succeeded": true,
	"failed":    true,
	"cancelled": true,
}

func countTerminal(states map[string]string) int {
	n := 0
	for _, st := range states {
		if terminalRunStates[st] {
			n++
		}
	}
	return n
}

// chaosRun carries one sweep's parameters and accumulates its evidence. The
// zero value plus Seed, Runs, Kills and Budget is enough; Drive does the rest.
type chaosRun struct {
	Seed   int64
	Runs   int
	Kills  int // requested schedule size; KillsDone is what really fired
	Budget time.Duration

	WS         *chaosWorkspace
	IDs        []string
	thresholds []int
	KillsDone  int
	started    time.Time
	histogram  map[string]int
	walBytes   int64
}

func (c *chaosRun) jobPrefix() string {
	// The pid keeps concurrent sweeps on one machine out of each other's
	// listings; the database itself is private to the workspace anyway.
	return fmt.Sprintf("chaos-%d", os.Getpid())
}

// Drive is the whole smoke or nightly body: seed, loop with kills, converge,
// stop clean, then the battery. Any failure first registers the archive via
// t.Cleanup, so AC-11's evidence always exists when the test goes red.
func (c *chaosRun) Drive(t *testing.T) {
	t.Helper()

	c.started = time.Now()
	c.WS = newChaosWorkspace(t)
	t.Cleanup(func() {
		if t.Failed() {
			c.archiveFailure(t)
		}
	})

	shapes := chaosShapes(t, c.WS)
	c.IDs = seedRuns(t, c.WS, shapes, c.Runs, c.jobPrefix())
	c.thresholds = killThresholds(c.Seed, c.Runs, c.Kills)
	t.Logf("chaos seed=%d runs=%d requested kills=%d thresholds=%v budget=%s",
		c.Seed, c.Runs, c.Kills, c.thresholds, c.Budget)

	var problems []string
	gen := 0
	p := startDaemon(t, c.WS, gen)
	p.waitReady(t)
	deadline := time.Now().Add(c.Budget)

	for {
		states := readRunStates(t, c.WS, c.jobPrefix())
		done := countTerminal(states)

		crossed := false
		for len(c.thresholds) > 0 && done >= c.thresholds[0] {
			c.thresholds = c.thresholds[1:]
			crossed = true
		}
		if crossed && p.alive() && done < c.Runs {
			c.KillsDone++
			p.signal(t, syscall.SIGKILL)
			_ = p.waitExit(t, daemonExitWait)
			gen++
			p = startDaemon(t, c.WS, gen)
			p.waitReady(t)
			t.Logf("SIGKILL #%d landed at %d/%d terminal; daemon gen %d up",
				c.KillsDone, done, c.Runs, gen)
		}

		if done == c.Runs {
			break
		}
		if time.Now().After(deadline) {
			problems = append(problems, fmt.Sprintf(
				"the sweep did not converge within %s: %d/%d terminal",
				c.Budget, done, c.Runs))
			break
		}
		if !p.alive() {
			code := <-p.exited
			p.exited <- code
			raw, _ := os.ReadFile(p.stderr)
			problems = append(problems, fmt.Sprintf(
				"the daemon died on its own with exit %d:\n%s", code, lastLines(string(raw), 20)))
			break
		}
		time.Sleep(pollEvery)
	}

	// Converged or not, the daemon owes a clean final stop: SIGTERM drains,
	// hands interrupted work back and exits zero. A daemon that died on its
	// own above has its finding already; signalling a corpse would only
	// hide it, so the demand is made only of a live one.
	if p.alive() {
		p.signal(t, syscall.SIGTERM)
		if code := p.waitExit(t, daemonExitWait); code != 0 {
			raw, _ := os.ReadFile(p.stderr)
			problems = append(problems, fmt.Sprintf(
				"the final graceful stop exited %d, want 0:\n%s", code, lastLines(string(raw), 20)))
		}
	}

	problems = append(problems, c.battery(t)...)

	c.report(t)
	if len(problems) > 0 {
		t.Fatalf("the chaos sweep broke its promises:\n - %s",
			strings.Join(problems, "\n - "))
	}
	if c.KillsDone == 0 {
		t.Fatalf("the sweep converged without a single SIGKILL (thresholds were not reached); "+
			"seed %d produced no chaos at runs=%d", c.Seed, c.Runs)
	}
}

// battery runs every invariant check and returns the findings. It is a
// function returning problems rather than a Fatalf caller so the planted
// proofs can hold the same checks against hand-made databases.
func (c *chaosRun) battery(t *testing.T) []string {
	t.Helper()

	s, err := store.Open(context.Background(), c.WS.DBPath, store.Options{})
	if err != nil {
		return []string{"reopen the state after the sweep: " + err.Error()}
	}
	defer func() { _ = s.Close() }()
	ctx := context.Background()

	var problems []string
	problems = append(problems, checkFsckFindings(ctx, s)...)
	problems = append(problems, checkDoubleCompletedRunKeys(ctx, s)...)
	problems = append(problems, checkTerminalReasons(ctx, s, c.IDs)...)
	problems = append(problems, checkEffectBounds(c.WS.EffectFile, c.KillsDone)...)
	problems = append(problems, checkNoOrphans(c.IDs, orphanWait)...)
	return problems
}

// report logs the facts AC-9 asks the full test to carry: seed, counts,
// duplication and WAL size.
func (c *chaosRun) report(t *testing.T) {
	t.Helper()

	s, err := store.OpenReadOnly(context.Background(), c.WS.DBPath, store.Options{})
	if err != nil {
		t.Errorf("reopen for the report: %v", err)
		return
	}
	defer func() { _ = s.Close() }()

	rows, err := s.ListRuns(context.Background(), store.RunFilter{Limit: 500})
	if err != nil {
		t.Errorf("list runs for the report: %v", err)
		return
	}
	c.histogram = map[string]int{}
	duplication := 0
	effects := readEffectCounts(t, c.WS.EffectFile)
	for _, count := range effects {
		if count-1 > duplication {
			duplication = count - 1
		}
	}
	for _, r := range rows {
		if !strings.HasPrefix(r.JobName, c.jobPrefix()+"-") {
			continue
		}
		c.histogram[r.State]++
	}
	if info, err := os.Stat(c.WS.DBPath + "-wal"); err == nil {
		c.walBytes = info.Size()
	}

	var states []string
	for st, n := range c.histogram {
		states = append(states, fmt.Sprintf("%s=%d", st, n))
	}
	sort.Strings(states)
	t.Logf("chaos report: seed=%d runs=%d sigkills=%d wall=%s final states [%s]"+
		" effect keys=%d max duplication=%d wal=%dB",
		c.Seed, c.Runs, c.KillsDone, time.Since(c.started).Round(time.Second),
		strings.Join(states, " "), len(effects), duplication, c.walBytes)
}

// checkFsckFindings runs the whole fsck engine and phrases every violation
// as one finding string.
func checkFsckFindings(ctx context.Context, s *store.Store) []string {
	violations, err := s.Fsck(ctx)
	if err != nil {
		return []string{"fsck could not run: " + err.Error()}
	}
	out := make([]string, 0, len(violations))
	for _, v := range violations {
		out = append(out, fmt.Sprintf("%s: %s: %s", v.Check, v.Subject, v.Detail))
	}
	return out
}

func checkDoubleCompletedRunKeys(ctx context.Context, s *store.Store) []string {
	keys, err := s.DoubleCompletedRunKeys(ctx)
	if err != nil {
		return []string{"the doubled completed run key check could not run: " + err.Error()}
	}
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		out = append(out, "runkey: completed run key "+k+" sits behind two succeeded runs")
	}
	return out
}

// checkTerminalReasons enforces AC-8 on both levels: every terminal row,
// run or step, must say why it is where it stopped.
func checkTerminalReasons(ctx context.Context, s *store.Store, ids []string) []string {
	var out []string
	for _, id := range ids {
		detail, err := s.GetRun(ctx, id)
		if err != nil {
			out = append(out, fmt.Sprintf("read run %s: %v", id, err))
			continue
		}
		if terminalRunStates[detail.Run.State] && detail.Run.ReasonCode == "" {
			out = append(out, fmt.Sprintf("reason: run %s ended %s with no reason_code",
				id, detail.Run.State))
		}
		if strings.Contains(detail.Run.ReasonCode, "UNKNOWN") {
			out = append(out, fmt.Sprintf("reason: run %s carries UNKNOWN (%s)",
				id, detail.Run.ReasonCode))
		}
		for _, st := range detail.Steps {
			terminalStep := st.State == "succeeded" || st.State == "failed" ||
				st.State == "skipped" || st.State == "cancelled"
			if !terminalStep {
				if st.State != "pending" && st.State != "running" {
					out = append(out, fmt.Sprintf(
						"reason: run %s step %s sits in unknown state %q", id, st.Name, st.State))
				}
				continue
			}
			if st.ReasonCode == "" {
				out = append(out, fmt.Sprintf(
					"reason: run %s step %s ended %s with no reason_code",
					id, st.Name, st.State))
			} else if strings.Contains(st.ReasonCode, "UNKNOWN") {
				out = append(out, fmt.Sprintf(
					"reason: run %s step %s carries UNKNOWN (%s)", id, st.Name, st.ReasonCode))
			}
		}
	}
	return out
}

// readEffectCounts groups the effect file by idempotency key. The format is
// the append fixture's: key TAB attempt TAB nanostamp.
func readEffectCounts(t *testing.T, path string) map[string]int {
	t.Helper()

	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return map[string]int{}
	}
	if err != nil {
		t.Fatalf("read the effect file: %v", err)
	}
	counts := map[string]int{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		key, _, ok := strings.Cut(line, "\t")
		if !ok {
			t.Fatalf("effect line %q does not have three fields", line)
		}
		counts[key]++
	}
	return counts
}

// checkEffectBounds enforces AC-7's bound at sweep scale: a key may show one
// clean execution plus at most one extra line per SIGKILL that could have
// interrupted it.
func checkEffectBounds(path string, kills int) []string {
	raw, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		if kills == 0 {
			return nil
		}
		return []string{fmt.Sprintf("effects: the effect file is missing after %d kills", kills)}
	}
	if err != nil {
		return []string{"effects: read the effect file: " + err.Error()}
	}
	counts := map[string]int{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" {
			continue
		}
		key, _, ok := strings.Cut(line, "\t")
		if !ok {
			return []string{fmt.Sprintf("effects: line %q does not have three fields", line)}
		}
		counts[key]++
	}
	bound := 1 + kills
	var out []string
	for key, n := range counts {
		if n < 1 || n > bound {
			out = append(out, fmt.Sprintf(
				"effects: key %s executed %d times, outside [1,%d]", key, n, bound))
		}
	}
	sort.Strings(out)
	return out
}

// checkNoOrphans scans /proc environments for our run marker and waits a
// bounded time for stragglers to die before reporting what remains. Only
// values naming OUR runs count, so a neighbour test's processes elsewhere on
// the machine can never fail this sweep.
func checkNoOrphans(ids []string, within time.Duration) []string {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	deadline := time.Now().Add(within)
	var found []string
	for {
		found = carriersAmong(want)
		if len(found) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	sort.Strings(found)
	out := make([]string, 0, len(found))
	for _, id := range found {
		out = append(out, "orphan: a live process still carries PACEQ_RUN_ID="+id)
	}
	return out
}

// carriersAmong returns the wanted run ids that some live process names in
// its environment right now.
func carriersAmong(want map[string]bool) []string {
	var found []string
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil
	}
	for _, entry := range entries {
		if _, err := strconv.Atoi(entry.Name()); err != nil {
			continue // not a process directory
		}
		raw, err := os.ReadFile("/proc/" + entry.Name() + "/environ")
		if err != nil {
			continue // gone between listing and reading
		}
		for _, env := range bytes.Split(raw, []byte{0}) {
			if after, ok := bytes.CutPrefix(env, []byte(runEnvMarker)); ok {
				id := string(after)
				if want[id] {
					found = append(found, id)
				}
			}
		}
	}
	return found
}

// archiveFailure copies the sweep's evidence into one artifact directory:
// state.db with its wal and shm siblings, the whole logs tree, every daemon
// generation's stderr and the effect file, beside a manifest. The parent is
// PACEQ_CHAOS_ARTIFACTS when set, otherwise test/chaos/artifacts next to the
// package, so CI uploads one stable glob (AC-11).
func (c *chaosRun) archiveFailure(t *testing.T) {
	t.Helper()

	parent := os.Getenv(envArtifacts)
	if parent == "" {
		parent = filepath.Join(moduleRoot(t), "test", "chaos", "artifacts")
	}
	name := fmt.Sprintf("%s-seed%d-n%d-kills%d",
		time.Now().UTC().Format("20060102T150405"), c.Seed, c.Runs, c.KillsDone)
	dir := filepath.Join(parent, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Errorf("make the artifact directory: %v", err)
		return
	}

	for _, suffix := range []string{"", "-wal", "-shm"} {
		copyInto(t, c.WS.DBPath+suffix, filepath.Join(dir, "state.db"+suffix))
	}
	copyTree(t, filepath.Join(c.WS.StateDir, "logs"), filepath.Join(dir, "logs"))
	entries, err := os.ReadDir(c.WS.Dir)
	if err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "daemon-") && strings.HasSuffix(e.Name(), ".stderr") {
				copyInto(t, filepath.Join(c.WS.Dir, e.Name()), filepath.Join(dir, e.Name()))
			}
		}
	}
	copyInto(t, c.WS.EffectFile, filepath.Join(dir, "effects.txt"))

	manifest := fmt.Sprintf(
		"paceq chaos failure archive\nseed: %d\nruns: %d\nrequested kills: %d\n"+
			"fired kills: %d\neffects bound: 1 + %d\nreplay: PACEQ_CHAOS_SEED=%d go test -count=1 ./test/chaos\n"+
			"(for the tagged sweep: PACEQ_CHAOS_SEED=%d go test -tags chaos -count=1 ./test/chaos)\n",
		c.Seed, c.Runs, c.Kills, c.KillsDone, c.KillsDone, c.Seed, c.Seed)
	if err := os.WriteFile(filepath.Join(dir, "manifest.txt"), []byte(manifest), 0o600); err != nil {
		t.Errorf("write the manifest: %v", err)
	}
	t.Logf("failure artifacts archived under %s", dir)
}

func copyInto(t *testing.T, src, dst string) {
	t.Helper()

	raw, err := os.Open(src)
	if err != nil {
		if os.IsNotExist(err) {
			return // a wal that was checkpointed away is not evidence we owe
		}
		t.Errorf("open %s for archiving: %v", src, err)
		return
	}
	defer func() { _ = raw.Close() }()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Errorf("create %s: %v", dst, err)
		return
	}
	defer func() { _ = out.Close() }()
	if _, err := io.Copy(out, raw); err != nil {
		t.Errorf("copy %s: %v", src, err)
	}
}

func copyTree(t *testing.T, src, dst string) {
	t.Helper()

	if _, err := os.Stat(src); err != nil {
		return // no logs yet is itself information the manifest carries
	}
	if err := os.MkdirAll(dst, 0o700); err != nil {
		t.Errorf("mkdir %s: %v", dst, err)
		return
	}
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Errorf("read %s: %v", src, err)
		return
	}
	for _, e := range entries {
		s := filepath.Join(src, e.Name())
		d := filepath.Join(dst, e.Name())
		if e.IsDir() {
			copyTree(t, s, d)
			continue
		}
		copyInto(t, s, d)
	}
}
