package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/rogpeppe/go-internal/diff"
	"github.com/rogpeppe/go-internal/testscript"
	"github.com/rogpeppe/go-internal/txtar"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// The commands in this file are the harness the golden scripts run on. Two
// jobs only: keep what no golden may pin out of the comparison (ids, work
// paths), and give a script exact, bounded ways to wait for a background
// paceq process. None of it is product code; it all lives behind _test.go.

// fakeClockDefault is the frozen wall clock every script runs on unless one
// command overrides it. Chosen once, written down here, so every golden in
// testdata carries the same stamps.
const fakeClockDefault = "2026-09-24T09:00:00Z"

// ulidPattern matches the identifiers the store mints: 26 Crockford base32
// characters starting with a digit. Ordinary words never fit, and a spec
// hash is 64 characters, not 26.
var ulidPattern = regexp.MustCompile(`[0-9][0-9ABCDEFGHJKMNPQRSTVWXYZ]{25}`)

// maskVolatile replaces everything a golden must not pin down: the absolute
// work directory becomes <WORK>, and every ULID becomes <RUN1>, <RUN2>, ...,
// numbered in first-seen order. The same id always gets the same
// placeholder, so a document still shows which fields repeat.
func maskVolatile(workDir string, in []byte) []byte {
	if workDir != "" {
		in = bytes.ReplaceAll(in, []byte(workDir), []byte("<WORK>"))
	}
	// The local zone name leaks the host's /etc/timezone into suggestions
	// (a shadow-offset hint proposes "set timezone: <here>"). Suggestions
	// must compare equal across machines, so the host zone is masked too.
	if zone := localZoneName(); zone != "" {
		in = bytes.ReplaceAll(in, []byte(zone), []byte("<LOCALZONE>"))
	}
	seen := map[string]string{}
	return ulidPattern.ReplaceAllFunc(in, func(match []byte) []byte {
		id := string(match)
		placeholder, ok := seen[id]
		if !ok {
			placeholder = fmt.Sprintf("<RUN%d>", len(seen)+1)
			seen[id] = placeholder
		}
		return []byte(placeholder)
	})
}

// canonicalJSON renders a document the way the comparison and the diff want
// it: decoded and re-encoded, so object keys come out sorted no matter how
// the producer wrote them, indented so a failing diff reads by eye.
func canonicalJSON(in []byte) ([]byte, error) {
	var document any
	if err := json.Unmarshal(in, &document); err != nil {
		return nil, err
	}
	var out bytes.Buffer
	encoder := json.NewEncoder(&out)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(document); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// jsonEqual compares two JSON documents by value. Object key order carries no
// meaning (the decoder sees a map); array order does, because the run
// listing's newest-first order is part of the contract.
func jsonEqual(a, b []byte) (bool, error) {
	canonicalA, err := canonicalJSON(a)
	if err != nil {
		return false, fmt.Errorf("left side is not JSON: %w", err)
	}
	canonicalB, err := canonicalJSON(b)
	if err != nil {
		return false, fmt.Errorf("right side is not JSON: %w", err)
	}
	return bytes.Equal(canonicalA, canonicalB), nil
}

// setupScriptEnv is the determinism layer every script runs under: a frozen
// clock the harness reads, ASCII symbols, colour off, and a fixed width for
// anything that ever grows a width-aware renderer. A script can override a
// variable for one command by prefixing the line, which is how a later run
// is given a later timestamp.
func setupScriptEnv(e *testscript.Env) error {
	for key, value := range map[string]string{
		"PULSEQ_FAKE_CLOCK": fakeClockDefault,
		"LC_ALL":            "C",
		"NO_COLOR":          "1",
		"COLUMNS":           "100",
	} {
		e.Setenv(key, value)
	}
	return nil
}

// paceqEnv builds the environment for a paceq process this harness spawns
// itself (the tty rows and exact exit code rows). The script's own
// environment is not visible here, so the determinism layer is applied
// again, and the caller's KEY=VALUE overrides go last.
func paceqEnv(overrides []string) ([]string, error) {
	// PACEQ_SOCKET and XDG_RUNTIME_DIR come before the state directory in
	// socket resolution, so a developer's login session would otherwise
	// decide which socket the commands under test dial.
	drop := map[string]bool{
		"PULSEQ_FAKE_CLOCK": true, "LC_ALL": true, "NO_COLOR": true,
		"CLICOLOR_FORCE": true, "COLUMNS": true,
		"PACEQ_SOCKET": true, "XDG_RUNTIME_DIR": true,
	}
	var env []string
	for _, entry := range os.Environ() {
		name := entry[:strings.IndexByte(entry, '=')]
		if drop[name] {
			continue
		}
		env = append(env, entry)
	}
	env = append(env,
		"PULSEQ_FAKE_CLOCK="+fakeClockDefault,
		"LC_ALL=C", "NO_COLOR=1", "COLUMNS=100")
	for _, override := range overrides {
		if !strings.Contains(override, "=") {
			return nil, fmt.Errorf("%q is not a KEY=VALUE override", override)
		}
		env = append(env, override)
	}
	return env, nil
}

// spawnPaceq starts the paceq command the same way a script's exec does: as
// a real process of the binary testscript installed on PATH, with its own
// pipes and its own exit code. There is no second way into the CLI: the
// process runs cli.MainEnv, exactly like the shipped binary's main.
func spawnPaceq(ts *testscript.TestScript, overrides, args []string) *exec.Cmd {
	bin, err := exec.LookPath("paceq")
	if err != nil {
		ts.Fatalf("the paceq command is not on PATH: %v", err)
	}
	// The script's own clock wins over the default: a script that moved it
	// with `env PULSEQ_FAKE_CLOCK=...` moves every paceq it spawns too.
	clock := ts.Getenv("PULSEQ_FAKE_CLOCK")
	if clock == "" {
		clock = fakeClockDefault
	}
	overrides = append([]string{"PULSEQ_FAKE_CLOCK=" + clock}, overrides...)
	env, err := paceqEnv(overrides)
	if err != nil {
		ts.Fatalf("%v", err)
	}
	cmd := exec.Command(bin, args...)
	cmd.Dir = ts.MkAbs(".")
	cmd.Env = env
	return cmd
}

// cmdWantExit runs one paceq command and demands one exact exit code. The
// exit code is the loudest half of the CLI contract, so a matrix row never
// asserts it through a side door: the failure names the code that came back.
// Standard output and standard error land in the two named files, so the
// lines after it can assert the words around the failure.
//
//	wantexit 5 out.json err.txt run broken-job
//
// An argument of the form @FILE is replaced by that file's first line, so a
// script can hand the CLI an id it learned from an earlier command:
//
//	writeid latest.id
//	wantexit 0 show.json - runs show @latest.id
func cmdWantExit(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: wantexit CODE STDOUT-FILE STDERR-FILE ARGS...")
	}
	code, err := strconv.Atoi(args[0])
	if err != nil {
		ts.Fatalf("%q is not an exit code", args[0])
	}
	paceqArgs, err := expandFileArgs(ts, args[3:])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	cmd := spawnPaceq(ts, nil, paceqArgs)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	got := 0
	if runErr != nil {
		exitErr, ok := runErr.(*exec.ExitError)
		if !ok {
			ts.Fatalf("paceq could not be started: %v", runErr)
		}
		got = exitErr.ExitCode()
	}
	if got != code {
		ts.Fatalf("%v exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			args[3:], got, code, stdout.String(), stderr.String())
	}
	if args[1] != "-" {
		ts.Check(os.WriteFile(ts.MkAbs(args[1]), stdout.Bytes(), 0o644))
	}
	if args[2] != "-" {
		ts.Check(os.WriteFile(ts.MkAbs(args[2]), stderr.Bytes(), 0o644))
	}
}

// cmdMaskFile masks ids and work paths in the named files, in place. A
// golden never carries a raw identifier: it carries the placeholder, so the
// comparison is about shape and values, not about this run's randomness.
//
//	exec paceq runs list -o json
//	cp stdout actual.json
//	maskfile actual.json
func cmdMaskFile(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 1 {
		ts.Fatalf("usage: maskfile FILE...")
	}
	for _, name := range args {
		data := ts.ReadFile(name)
		ts.Check(os.WriteFile(ts.MkAbs(name), maskVolatile(workDirOf(ts), []byte(data)), 0o644))
	}
}

// workDirOf is the absolute work directory a script runs in, for masking.
func workDirOf(ts *testscript.TestScript) string {
	return ts.MkAbs(".")
}

// expandFileArgs replaces every @FILE argument with that file's first line,
// trimmed. It is how a script hands an id it read earlier to a later
// command, without the harness ever inventing the value itself.
func expandFileArgs(ts *testscript.TestScript, args []string) ([]string, error) {
	out := make([]string, len(args))
	for i, arg := range args {
		if !strings.HasPrefix(arg, "@") {
			out[i] = arg
			continue
		}
		data := strings.TrimSpace(ts.ReadFile(strings.TrimPrefix(arg, "@")))
		if data == "" || strings.ContainsAny(data, "\n\r") {
			return nil, fmt.Errorf("@%s must hold exactly one line, got %q", arg[1:], data)
		}
		out[i] = data
	}
	return out, nil
}

// cmdWriteID writes the newest run's id, or a prefix of it, so a script can
// address that run the way a user would: by typing (part of) the id.
//
//	writeid latest.id            # the whole id
//	writeid -prefix=8 short.id   # any prefix this long names one run here
//	writeid -prefix=4 both.id    # two runs minted in the same millisecond
func cmdWriteID(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 1 || len(args) > 2 {
		ts.Fatalf("usage: writeid [-prefix=N] FILE")
	}
	prefix := 0
	file := args[len(args)-1]
	for _, flagArg := range args[:len(args)-1] {
		if !strings.HasPrefix(flagArg, "-prefix=") {
			ts.Fatalf("%q is not a -prefix=N option", flagArg)
		}
		n, err := strconv.Atoi(strings.TrimPrefix(flagArg, "-prefix="))
		if err != nil || n <= 0 {
			ts.Fatalf("-prefix needs a positive number, got %q", flagArg)
		}
		prefix = n
	}
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	ro, err := store.OpenReadOnly(ctx, dbPath, store.Options{})
	if err != nil {
		ts.Fatalf("could not read the state to find a run: %v", err)
	}
	defer func() { _ = ro.Close() }()
	rows, err := ro.ListRuns(ctx, store.RunFilter{Limit: 1})
	if err != nil {
		ts.Fatalf("could not list the runs: %v", err)
	}
	if len(rows) == 0 {
		ts.Fatalf("no run exists yet: paceq run <job> makes the first one")
	}
	id := rows[0].ID
	if prefix > 0 {
		if prefix > len(id) {
			ts.Fatalf("the id is %d characters, shorter than the %d asked for", len(id), prefix)
		}
		id = id[:prefix]
	}
	ts.Check(os.WriteFile(ts.MkAbs(file), []byte(id+"\n"), 0o644))
}

// cmdCmpJSON compares two JSON documents by value and reports a readable
// diff when they disagree. The right side is usually a golden embedded in
// the script file itself.
//
//	cmpjson actual.json expected.json
//
// With -update a mismatch rewrites the golden inside the script instead of
// failing it, which is the whole regeneration story: one command refreshes
// every expectation, and git diff shows what moved. The rewrite is queued
// rather than performed here; goldenState says why.
func cmdCmpJSON(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 2 {
		ts.Fatalf("usage: cmpjson ACTUAL EXPECTED")
	}
	actual := []byte(ts.ReadFile(args[0]))
	expected := []byte(ts.ReadFile(args[1]))
	goldens := goldensOf(ts)
	equal, err := jsonEqual(actual, expected)
	if err != nil {
		// An unreadable right side with -update is a golden waiting to
		// be born, so write it instead of failing. A broken LEFT side
		// is always a bug in the script.
		if goldens.update {
			canonicalActual, canonErr := canonicalJSON(actual)
			if canonErr != nil {
				ts.Fatalf("%s is not JSON: %v", args[0], canonErr)
			}
			if err := goldens.queue(ts, args[1], canonicalActual); err != nil {
				ts.Fatalf("could not regenerate %s: %v", args[1], err)
			}
			return
		}
		ts.Check(err)
	}
	if equal {
		return
	}
	canonicalActual, errA := canonicalJSON(actual)
	canonicalExpected, errB := canonicalJSON(expected)
	if errA != nil || errB != nil {
		ts.Fatalf("%s and %s hold different JSON shapes", args[0], args[1])
	}
	if goldens.update {
		if err := goldens.queue(ts, args[1], canonicalActual); err != nil {
			ts.Fatalf("could not regenerate %s: %v", args[1], err)
		}
		return
	}
	// diff.Diff renders a readable unified diff between the canonical
	// forms, so a broken contract names the field that moved.
	patch := diff.Diff(args[0], canonicalActual, args[1], canonicalExpected)
	ts.Fatalf("%s does not match %s:\n%s", args[0], args[1], patch)
}

// goldenStateKey addresses the golden update state a suite's Setup puts in
// the script environment.
type goldenStateKey struct{}

// goldenState carries one running script's golden rewrites. testscript keeps
// the script in an archive it parses at setup and, when cmp has recorded an
// update, serialises that archive over the script file as the run ends.
// Anything written to the same file while the script runs is discarded by
// that write, which is why cmpjson only queues here. The archive is not
// reachable from a custom command: TestScript exposes neither it nor the
// update map. What it does expose is ordering, since ts.Defer runs after the
// serialisation, so the queued goldens are applied to the file testscript
// just wrote and one -update pass carries both kinds.
type goldenState struct {
	dir     string
	update  bool
	t       testscript.T
	pending map[string][]byte
}

// goldensOf returns the golden state of the running script.
func goldensOf(ts *testscript.TestScript) *goldenState {
	state, ok := ts.Value(goldenStateKey{}).(*goldenState)
	if !ok {
		ts.Fatalf("this suite installs no golden state; cmpjson needs the Setup scriptParams builds")
	}
	return state
}

// queue records one embedded file section of the running script for
// rewriting after the run. A golden the script never declared is a mistake
// in the script, and it is reported on the line that made it.
func (g *goldenState) queue(ts *testscript.TestScript, name string, content []byte) error {
	scriptPath := filepath.Join(g.dir, ts.Name()+".txtar")
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		return err
	}
	if _, ok := txtarFile(data, name); !ok {
		return fmt.Errorf("no section named %s in %s", name, scriptPath)
	}
	if g.pending == nil {
		g.pending = map[string][]byte{}
		ts.Defer(func() { g.apply(scriptPath) })
	}
	g.pending[name] = append(bytes.TrimRight(content, "\n"), '\n')
	return nil
}

// apply writes the queued goldens into the script file, re-reading it first
// so testscript's own updates from the same run survive.
func (g *goldenState) apply(scriptPath string) {
	data, err := os.ReadFile(scriptPath)
	if err != nil {
		g.t.Fatal(fmt.Sprintf("could not reread %s to update its JSON goldens: %v", scriptPath, err))
		return
	}
	archive := txtar.Parse(data)
	for i := range archive.Files {
		if content, ok := g.pending[archive.Files[i].Name]; ok {
			archive.Files[i].Data = content
		}
	}
	if err := os.WriteFile(scriptPath, txtar.Format(archive), 0o644); err != nil {
		g.t.Fatal(fmt.Sprintf("could not update the JSON goldens in %s: %v", scriptPath, err))
	}
}

// sigProcess is one backgrounded paceq run this harness controls.
type sigProcess struct {
	cmd    *exec.Cmd
	stdout bytes.Buffer
	stderr bytes.Buffer
	out    string
	err    string
}

// cmdStartRun starts one paceq command in the background and remembers it,
// so sigwait can watch the state database and then interrupt exactly that
// process. Standard output and standard error are held until the run ends,
// then written to the two named files, whatever way it ended.
//
//	startrun out.txt err.txt -- run slow
//	sigwait 8 cancelled 30s
func cmdStartRun(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: startrun STDOUT-FILE STDERR-FILE -- ARGS...")
	}
	separator := -1
	for i, arg := range args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		ts.Fatalf("startrun needs a \"--\" between the files and the paceq command")
	}
	key := "sigproc/" + ts.Name()
	if harnessGet(key) != nil {
		ts.Fatalf("a run is already started in this script; sigwait it first")
	}
	paceqArgs, err := expandFileArgs(ts, args[separator+1:])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	if len(paceqArgs) == 0 {
		ts.Fatalf("startrun needs a paceq command after \"--\"")
	}
	p := &sigProcess{cmd: spawnPaceq(ts, nil, paceqArgs), out: args[0], err: args[1]}
	p.cmd.Stdout = &p.stdout
	p.cmd.Stderr = &p.stderr
	if err := p.cmd.Start(); err != nil {
		ts.Fatalf("could not start %v: %v", paceqArgs, err)
	}
	harnessSet(key, p)
	// A script that fails between startrun and sigwait must not leave a
	// paceq process behind: the safety net kills it, ungraded.
	ts.Defer(func() {
		if live := harnessGet(key); live != nil {
			run := live.(*sigProcess)
			_ = run.cmd.Process.Signal(syscall.SIGKILL)
		}
		finishSigProcess(ts, key, 0, false)
	})
}

// cmdSigWait drives one started run to its cancelled end: it waits until
// the store shows the run actually executing, sends SIGINT exactly as a
// user's Ctrl-C would, demands one exact exit code from the process, and
// then confirms the record reached the named end state. Every step is
// bounded and event driven: the waits poll the store every 20 milliseconds
// and give up with an explanation at the deadline; nothing sleeps a fixed
// amount hoping time has passed.
//
//	sigwait 8 cancelled 30s
func cmdSigWait(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 3 {
		ts.Fatalf("usage: sigwait EXITCODE STATE DEADLINE")
	}
	wantExit, err := strconv.Atoi(args[0])
	if err != nil {
		ts.Fatalf("%q is not an exit code", args[0])
	}
	deadline, err := time.ParseDuration(args[2])
	if err != nil || deadline <= 0 {
		ts.Fatalf("%q is not a positive deadline such as 30s", args[2])
	}
	key := "sigproc/" + ts.Name()
	value := harnessGet(key)
	p, ok := value.(*sigProcess)
	if !ok {
		ts.Fatalf("no started run to wait for; startrun begins one")
	}

	// First the run must be alive: interrupting before anything runs
	// would prove nothing about the durable path.
	waitForRunState(ts, "running", deadline)

	// The interrupt is a request, delivered as the same signal a user's
	// Ctrl-C becomes. A process that already ended makes itself heard
	// below, as the wrong exit code rather than as silence.
	if sigErr := p.cmd.Process.Signal(syscall.SIGINT); sigErr != nil {
		ts.Logf("the signal arrived after the process ended: %v", sigErr)
	}
	finishSigProcess(ts, key, wantExit, true)

	// The record must say what the exit code claimed.
	waitForRunState(ts, args[1], deadline)
}

// finishSigProcess reaps the backgrounded run, grades its exit code when
// asked, and writes what it wrote to the files startrun was given.
func finishSigProcess(ts *testscript.TestScript, key string, wantExit int, grade bool) {
	value := harnessGet(key)
	p, ok := value.(*sigProcess)
	if !ok {
		return
	}
	harnessDelete(key)
	got := 0
	err := p.cmd.Wait()
	if err != nil {
		exitErr, isExit := err.(*exec.ExitError)
		if !isExit {
			ts.Logf("the started paceq could not be reaped: %v", err)
			return
		}
		got = exitErr.ExitCode()
	}
	if grade && got != wantExit {
		ts.Fatalf("the interrupted run exited %d, want %d\nstdout:\n%s\nstderr:\n%s",
			got, wantExit, p.stdout.String(), p.stderr.String())
	}
	ts.Check(os.WriteFile(ts.MkAbs(p.out), p.stdout.Bytes(), 0o644))
	ts.Check(os.WriteFile(ts.MkAbs(p.err), p.stderr.Bytes(), 0o644))
}

// waitForRunState polls the state database until some run reaches the named
// state, or fails naming what it waited for.
func waitForRunState(ts *testscript.TestScript, state string, deadline time.Duration) {
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	ctx := context.Background()
	var ro *store.Store
	defer func() {
		if ro != nil {
			_ = ro.Close()
		}
	}()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	giveUp := time.Now().Add(deadline)
	for {
		if ro == nil {
			if opened, openErr := store.OpenReadOnly(ctx, dbPath, store.Options{}); openErr == nil {
				ro = opened
			}
		}
		if ro != nil {
			rows, listErr := ro.ListRuns(ctx, store.RunFilter{Limit: 10})
			if listErr == nil {
				for _, row := range rows {
					if row.State == state {
						return
					}
				}
			}
		}
		if !time.Now().Before(giveUp) {
			ts.Fatalf("no run reached state %q within %s. Is the paceq run still alive, and did the job's steps start?", state, deadline)
		}
		<-ticker.C
	}
}

// holdState is one background holder of the state lock, for the busy rows.
type holdState struct {
	close    chan struct{}
	done     chan struct{}
	acquired chan struct{}
}

// cmdHoldState takes the state directory lock the way a second paceq
// process would, and keeps it until releasehold or a two minute guard. The
// next paceq command the script runs must then answer with the busy exit.
//
//	holdstate mine
//	wantexit 6 - busy_err.txt status
//	releasehold mine
func cmdHoldState(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: holdstate NAME")
	}
	key := "holdstate/" + ts.Name() + "/" + args[0]
	if harnessGet(key) != nil {
		ts.Fatalf("a holder named %s already exists", args[0])
	}
	// OpenState takes the state DIRECTORY, exactly as paceq run does; the
	// database file name is its business, not ours.
	stateDir := filepath.Join(workDirOf(ts), stateDirName)
	h := &holdState{close: make(chan struct{}), done: make(chan struct{}), acquired: make(chan struct{})}
	go func() {
		defer close(h.done)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		s, err := store.OpenState(ctx, stateDir, store.Options{})
		if err != nil {
			ts.Logf("holdstate %s: could not take the lock: %v", args[0], err)
			close(h.acquired)
			return
		}
		defer func() { _ = s.Close() }()
		close(h.acquired)
		select {
		case <-h.close:
		case <-time.After(2 * time.Minute):
		}
	}()
	harnessSet(key, h)
	ts.Defer(func() { releaseHolder(ts, args[0]) })

	// The row after holdstate must meet a holder that HAS the lock, not
	// one still reaching for it, so this command returns only once the
	// lock is confirmed held (or the attempt has plainly failed).
	select {
	case <-h.acquired:
	case <-time.After(30 * time.Second):
		ts.Fatalf("holdstate %s: the lock was never taken within 30s", args[0])
	}
}

// releaseHolder closes a holder and waits until the lock is let go. It is
// the body of releasehold and the safety net behind holdstate's Defer.
func releaseHolder(ts *testscript.TestScript, name string) {
	key := "holdstate/" + ts.Name() + "/" + name
	value := harnessGet(key)
	h, ok := value.(*holdState)
	if !ok {
		return
	}
	close(h.close)
	<-h.done
	harnessDelete(key)
}

// cmdReleaseHold lets go of a lock taken with holdstate, and waits until it
// is let go, so the next command in the script sees an unlocked directory.
func cmdReleaseHold(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: releasehold NAME")
	}
	if harnessGet("holdstate/"+ts.Name()+"/"+args[0]) == nil {
		ts.Fatalf("no holder named %s exists", args[0])
	}
	releaseHolder(ts, args[0])
}

// cmdWriteGarbage replaces a file with binary nonsense, for the one row that
// needs a state database a store cannot even open.
//
//	writegarbage .paceq/state.db
func cmdWriteGarbage(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: writegarbage FILE")
	}
	garbage := bytes.Repeat([]byte{0xde, 0xad, 0xbe, 0xef}, 128)
	ts.Check(os.WriteFile(ts.MkAbs(args[0]), garbage, 0o600))
}

// cmdPlantRun queues a real run through the store and then appends two run
// events whose chain does not add up. The second event claims the run went
// from queued to failed while the first already said queued to running,
// which is exactly the broken story invariant I15 exists to catch.
//
//	plantrun nightly
//	wantexit 1 - fsck_err.txt fsck
func cmdPlantRun(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: plantrun JOB")
	}
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	s, err := store.Open(ctx, dbPath, store.Options{})
	if err != nil {
		ts.Fatalf("could not open the state to plant a run: %v", err)
	}
	defer func() { _ = s.Close() }()
	queued, err := s.MaterializeManualTrigger(ctx, store.ManualTriggerInput{
		JobName: args[0],
		Actor:   "cli:test",
	})
	if err != nil {
		ts.Fatalf("could not queue the run to plant events on: %v", err)
	}
	breaks := []store.RunEvent{
		{RunID: queued.Run.ID, Kind: "run.state", FromState: "queued", ToState: "running", Actor: "cli:test"},
		{RunID: queued.Run.ID, Kind: "run.state", FromState: "queued", ToState: "failed", Actor: "cli:test"},
	}
	for _, event := range breaks {
		if err := s.AppendRunEvent(ctx, event); err != nil {
			ts.Fatalf("could not plant the broken event: %v", err)
		}
	}
	ts.Logf("planted a broken event chain on run %s", queued.Run.ID)
}

// cmdPlantSensor seeds one sensor row directly into the state database, so a
// golden script can exercise the sensors CLI before the apply path that
// materialises rows in the same base. The row carries the M3 exec contract
// object so sensorSpecFromRow can read its argv.
//
//	plantsensor finder polling-job '["/bin/echo","hi"]' [paused]
func cmdPlantSensor(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: plantsensor NAME JOB EXECJSON [PAUSED]")
	}
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	s, err := store.Open(ctx, dbPath, store.Options{Clock: plantClock(ts)})
	if err != nil {
		ts.Fatalf("could not open the state to plant a sensor: %v", err)
	}
	defer func() { _ = s.Close() }()

	name, job, execJSON := args[0], args[1], args[2]
	paused := len(args) > 3 && args[3] == "paused"
	// The sensors table references jobs; plant the job row first so the FK
	// holds. A re-seed with the same job is a no-op upsert.
	if _, _, err := s.UpsertJobVersion(ctx, store.JobVersionInput{
		JobName:       job,
		SpecHash:      "sha256:planted",
		SpecJSON:      `{"schema":"paceq.job.v1","name":"` + job + `","steps":[{"name":"collect","run":["true"]}]}`,
		MaxConcurrent: 10,
	}); err != nil {
		ts.Fatalf("could not plant job %s: %v", job, err)
	}
	if err := s.UpsertSensor(ctx, store.SensorSeedInput{
		Name: name, JobName: job, ExecJSON: execJSON, Paused: paused,
	}); err != nil {
		ts.Fatalf("could not plant sensor %s: %v", name, err)
	}
	ts.Logf("planted sensor %s on job %s", name, job)
}

// plantClock returns the fake clock a script runs on, so rows a plant helper
// writes carry the same deterministic stamps the paceq process sees. The
// default matches harnessClock; an explicit PULSEQ_FAKE_CLOCK wins.
func plantClock(ts *testscript.TestScript) clock.Clock {
	value := ts.Getenv("PULSEQ_FAKE_CLOCK")
	if value == "" {
		value = fakeClockDefault
	}
	stamp, err := time.Parse(time.RFC3339, value)
	if err != nil {
		ts.Fatalf("PULSEQ_FAKE_CLOCK %q is not RFC3339: %v", value, err)
	}
	return clock.NewFake(stamp)
}

// cmdPlantSensorTick commits one sensor tick on a planted sensor, so a golden
// script can give `sensors show` a history to render.
//
//	plantsensortick finder
func cmdPlantSensorTick(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) != 1 {
		ts.Fatalf("usage: plantsensortick SENSOR")
	}
	ctx := context.Background()
	dbPath := filepath.Join(workDirOf(ts), stateDirName, store.DatabaseFileName)
	s, err := store.Open(ctx, dbPath, store.Options{Clock: plantClock(ts)})
	if err != nil {
		ts.Fatalf("could not open the state to plant a sensor tick: %v", err)
	}
	defer func() { _ = s.Close() }()

	// The commit transaction needs the sensor's current job to materialise the
	// run; read it from the row rather than assuming a name.
	sum, err := s.GetSensor(ctx, args[0])
	if err != nil {
		ts.Fatalf("could not read the job of sensor %s: %v", args[0], err)
	}
	job := sum.JobName

	begin, err := s.BeginSensorTick(ctx, store.BeginSensorTickInput{
		SensorName: args[0], CursorBefore: "a",
	})
	if err != nil {
		ts.Fatalf("could not begin a sensor tick: %v", err)
	}
	if _, err := s.CommitSensorTick(ctx, store.SensorTickCommitInput{
		TickID:        begin.TickID,
		SensorName:    args[0],
		JobName:       job,
		CursorVersion: begin.CursorVersion,
		CursorAfter:   "b",
		DedupEpoch:    0,
		Triggers:      []store.SensorTrigger{{RunKey: "file:1"}},
		Outcome:       store.OutcomeTriggered,
		NextEvalAt:    60000,
		DurationMs:    5,
	}); err != nil {
		ts.Fatalf("could not commit a sensor tick: %v", err)
	}
	ts.Logf("planted one triggered tick on %s", args[0])
}

// harnessMu guards harnessValues. testscript runs every script as its own
// subtest and those subtests run in parallel, so cancel's sigwait can be
// mid-read while busy's holdstate is mid-write; the lock, not the script
// name in the key, is what keeps the map honest. holdstate's driver
// goroutine never touches the map itself, so one plain mutex is enough.
var (
	harnessMu     sync.Mutex
	harnessValues = map[string]any{}
)

func harnessGet(key string) any {
	harnessMu.Lock()
	defer harnessMu.Unlock()
	return harnessValues[key]
}

func harnessSet(key string, value any) {
	harnessMu.Lock()
	defer harnessMu.Unlock()
	harnessValues[key] = value
}

func harnessDelete(key string) {
	harnessMu.Lock()
	defer harnessMu.Unlock()
	delete(harnessValues, key)
}
