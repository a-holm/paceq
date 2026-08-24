// fakecmd is the deterministic command fixture for the runner tests. It plays
// the part of a user job: it can fail with a given exit code, sleep, ignore
// SIGTERM, leave a grandchild behind, pour bytes onto stdout, dump its
// environment, exercise $PACEQ_OUTPUT, raise a signal at itself and report its
// own core rlimit.
//
// It lives under testdata so no build tag, test or gate picks it up as package
// code. Every test that needs it builds it once per test binary run with the
// repository module, so its behaviour is exactly what a compiled job would do
// on the same platform.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func die(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "fakecmd: "+format+"\n", args...)
	os.Exit(3)
}

func main() {
	if len(os.Args) < 2 {
		die("usage: fakecmd MODE [args]")
	}

	mode, args := os.Args[1], os.Args[2:]
	switch mode {
	case "exit":
		code, err := strconv.Atoi(arg(args, 0))
		if err != nil {
			die("exit: bad code %q", arg(args, 0))
		}
		os.Exit(code)

	case "exit-unless-attempt":
		// A job that recovers on its Nth try: it prints its own attempt
		// number, so every attempt's log stream carries a marker line,
		// then exits 75 while PACEQ_ATTEMPT is still below the named
		// one. Exit 75 is EX_TEMPFAIL, the always retryable code. The
		// attempts share no state; the runner's environment is what
		// tells them apart.
		want, err := strconv.Atoi(arg(args, 0))
		if err != nil {
			die("exit-unless-attempt: bad attempt %q", arg(args, 0))
		}
		attempt := 0
		if v := os.Getenv("PACEQ_ATTEMPT"); v != "" {
			if parsed, err := strconv.Atoi(v); err == nil {
				attempt = parsed
			}
		}
		fmt.Printf("attempt %d\n", attempt)
		if attempt < want {
			os.Exit(75)
		}

	case "sleep":
		d, err := time.ParseDuration(arg(args, 0))
		if err != nil {
			die("sleep: %v", err)
		}
		time.Sleep(d)

	case "ignore-term":
		// Any extra argument is a marker carried in argv, so a test can find
		// this exact process in /proc while it is alive.
		signal.Ignore(syscall.SIGTERM)
		d, err := time.ParseDuration(arg(args, 0))
		if err != nil {
			die("ignore-term: %v", err)
		}
		time.Sleep(d)

	case "grandchild", "tree":
		// grandchild: start a grandchild in the same process group that
		// ignores SIGTERM, then exit at once.
		// tree: the same grandchild, but this process stays alive ignoring
		// SIGTERM too, so a timeout must kill both. Any extra argument is a
		// marker carried in argv for /proc scans.
		d, err := time.ParseDuration(arg(args, 0))
		if err != nil {
			die("%s: %v", mode, err)
		}
		self, err := os.Executable()
		if err != nil {
			die("%s: %v", mode, err)
		}
		child := execSelf(self, append([]string{"ignore-term", d.String()}, args[1:]...))
		fmt.Println(filepath.Base(self), "left grandchild", child)
		if mode == "tree" {
			signal.Ignore(syscall.SIGTERM)
			time.Sleep(d)
		} else {
			os.Exit(0)
		}

	case "spew":
		mib, err := strconv.Atoi(arg(args, 0))
		if err != nil {
			die("spew: %v", arg(args, 0))
		}
		chunk := make([]byte, 64*1024)
		for i := 0; i < mib*16; i++ {
			if _, err := os.Stdout.Write(chunk); err != nil {
				die("spew: %v", err)
			}
		}

	case "fds":
		// Report the descriptor table: name and readlink target of every
		// entry under /proc/self/fd. This is how the parent proves what the
		// job could and could not touch.
		out := map[string]string{}
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			die("fds: %v", err)
		}
		for _, entry := range entries {
			target, err := os.Readlink("/proc/self/fd/" + entry.Name())
			if err != nil {
				continue
			}
			out[entry.Name()] = target
		}
		b, err := json.Marshal(out)
		if err != nil {
			die("fds: %v", err)
		}
		fmt.Println(string(b))

	case "env-dump":
		env := map[string]string{}
		for _, kv := range os.Environ() {
			k, v, _ := strings.Cut(kv, "=")
			env[k] = v
		}
		out, err := json.Marshal(env)
		if err != nil {
			die("env-dump: %v", err)
		}
		fmt.Println(string(out))

	case "write-output":
		path := os.Getenv("PACEQ_OUTPUT")
		if path == "" {
			die("write-output: PACEQ_OUTPUT is not set")
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			die("write-output: %v", err)
		}
		defer f.Close()
		for _, line := range []string{
			`{"artifact":{"name":"out.txt","uri":"file:///tmp/out.txt","size_bytes":3}}`,
			`{"params":{"rows":"42"}}`,
		} {
			if _, err := fmt.Fprintln(f, line); err != nil {
				die("write-output: %v", err)
			}
		}

	case "signal-self":
		sig, ok := signalByName(arg(args, 0))
		if !ok {
			die("signal-self: unknown signal %q", arg(args, 0))
		}
		if err := syscall.Kill(os.Getpid(), sig); err != nil {
			die("signal-self: %v", err)
		}
		// Give the pending signal a moment to land; a raised standard signal
		// with default disposition terminates the process here.
		time.Sleep(10 * time.Second)

	case "rlimits":
		var rl syscall.Rlimit
		if err := syscall.Getrlimit(syscall.RLIMIT_CORE, &rl); err != nil {
			die("rlimits: %v", err)
		}
		out, err := json.Marshal(map[string]uint64{
			"core_cur": uint64(rl.Cur),
			"core_max": uint64(rl.Max),
		})
		if err != nil {
			die("rlimits: %v", err)
		}
		fmt.Println(string(out))

	case "sensor-ok":
		// A sensor that found work: one cursor, one trigger. Reads stdin to
		// prove the contract object was delivered, then answers.
		readStdinOrDie()
		fmt.Print(`{"cursor":"c9","triggers":[{"run_key":"k1","params":{"p":1}}]}`)

	case "sensor-skip":
		readStdinOrDie()
		fmt.Print(`{"skip_reason":"no new files"}`)

	case "sensor-skip-and-triggers":
		// The contract's both-set corner: triggers must win.
		fmt.Print(`{"skip_reason":"fallback text","triggers":[{"run_key":"k-trig"}]}`)

	case "sensor-invalid-json":
		fmt.Print("this is not json at all")

	case "sensor-empty":
		// Exit 0 with no stdout: silence means skip with a fixed reason.

	case "sensor-stderr-tag":
		// A valid trigger payload with a known marker on stderr, so the host
		// can prove the 4 KiB tail is carried.
		fmt.Fprint(os.Stderr, "SENSOR_STDERR_MARKER")
		readStdinOrDie()
		fmt.Print(`{"triggers":[{"run_key":"stderr-k"}]}`)

	case "sensor-env-dump":
		// Write the whole environment to the named file so a test can assert
		// the deny by default contract and the PACEQ_ keys precisely. Exit 0.
		env := map[string]string{}
		for _, kv := range os.Environ() {
			k, v, _ := strings.Cut(kv, "=")
			env[k] = v
		}
		out, err := json.Marshal(env)
		if err != nil {
			die("sensor-env-dump: %v", err)
		}
		if err := os.WriteFile(arg(args, 0), out, 0o600); err != nil {
			die("sensor-env-dump: %v", err)
		}

	case "sensor-track":
		// Append a start marker with the sensor's name, sleep, then append an
		// end marker. The host reconstructs how many evaluations of one name
		// (or how many total) overlapped in time, which is what proves the
		// serialization and the global semaphore from observation, not from
		// reading the runtime's internals.
		path, dur := arg(args, 0), arg(args, 1)
		d, err := time.ParseDuration(dur)
		if err != nil {
			die("sensor-track: %v", err)
		}
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			die("sensor-track: %v", err)
		}
		fmt.Fprintf(f, "start %s %d\n", os.Getenv("PACEQ_SENSOR"), time.Now().UnixNano())
		time.Sleep(d)
		fmt.Fprintf(f, "end %s %d\n", os.Getenv("PACEQ_SENSOR"), time.Now().UnixNano())
		_ = f.Close()

	case "require-empty-output":
		// The output contract begins before exec: the file must exist,
		// empty and 0600, the moment the command starts. It also publishes
		// the path it was handed, so a test can pin where paceq put it.
		path := os.Getenv("PACEQ_OUTPUT")
		if path == "" {
			die("require-empty-output: PACEQ_OUTPUT is not set")
		}
		info, err := os.Stat(path)
		if err != nil {
			die("require-empty-output: %v", err)
		}
		if info.Size() != 0 {
			die("require-empty-output: output is %d bytes at start", info.Size())
		}
		if perm := info.Mode().Perm(); perm != 0o600 {
			die("require-empty-output: output mode is %o, want 0600", perm)
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			die("require-empty-output: %v", err)
		}
		fmt.Fprintln(f, `{"artifact":{"name":"output-path","uri":`+strconv.Quote(path)+`}}`)
		f.Close()

	case "write-broken-output":
		path := os.Getenv("PACEQ_OUTPUT")
		if path == "" {
			die("write-broken-output: PACEQ_OUTPUT is not set")
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			die("write-broken-output: %v", err)
		}
		fmt.Fprintln(f, `{this is not json`)
		fmt.Fprintln(f, `{"artifact":{"name":"kept","uri":"/kept"}}`)
		f.Close()

	case "write-then-exit":
		code, err := strconv.Atoi(arg(args, 0))
		if err != nil {
			die("write-then-exit: bad code %q", arg(args, 0))
		}
		path := os.Getenv("PACEQ_OUTPUT")
		if path == "" {
			die("write-then-exit: PACEQ_OUTPUT is not set")
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			die("write-then-exit: %v", err)
		}
		fmt.Fprintln(f, `{"artifact":{"name":"doomed","uri":"/doomed"}}`)
		f.Close()
		os.Exit(code)

	case "publish-input-uri":
		// The two-step handoff: read the merged upstream references out of
		// $PACEQ_INPUTS and republish one uri under a new name. A downstream
		// step that saw nothing publishes the empty marker instead.
		name := arg(args, 0)
		want := arg(args, 1)
		uri := "(none)"
		var inputs struct {
			Artifacts map[string]struct {
				URI string `json:"uri"`
			} `json:"artifacts"`
		}
		if raw := os.Getenv("PACEQ_INPUTS"); raw != "" && raw != "null" {
			if err := json.Unmarshal([]byte(raw), &inputs); err != nil {
				die("publish-input-uri: $PACEQ_INPUTS is not JSON: %v", err)
			}
			if ref, ok := inputs.Artifacts[want]; ok && ref.URI != "" {
				uri = ref.URI
			}
		}
		path := os.Getenv("PACEQ_OUTPUT")
		if path == "" {
			die("publish-input-uri: PACEQ_OUTPUT is not set")
		}
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			die("publish-input-uri: %v", err)
		}
		fmt.Fprintln(f, `{"artifact":{"name":`+strconv.Quote(name)+`,"uri":`+strconv.Quote(uri)+`}}`)
		f.Close()

	case "inputs-must-not-contain":
		// A side-by-side step must be invisible: fail loudly if the named
		// marker appears anywhere in $PACEQ_INPUTS.
		sub := arg(args, 0)
		raw := os.Getenv("PACEQ_INPUTS")
		fileRaw := ""
		if p := os.Getenv("PACEQ_INPUTS_FILE"); p != "" {
			b, err := os.ReadFile(p)
			if err != nil {
				die("inputs-must-not-contain: %v", err)
			}
			fileRaw = string(b)
		}
		if strings.Contains(raw, sub) || strings.Contains(fileRaw, sub) {
			fmt.Printf("inputs leaked %q\n", sub)
			os.Exit(8)
		}

	default:
		die("unknown mode %q", mode)
	}
}

// readStdinOrDie reads all of stdin; a sensor that cannot read its input is a
// broken run, which the host wants to see.
func readStdinOrDie() {
	if _, err := io.ReadAll(os.Stdin); err != nil {
		die("stdin: %v", err)
	}
}

func arg(args []string, i int) string {
	if i >= len(args) {
		return ""
	}
	return args[i]
}

func execSelf(self string, args []string) int {
	attr := &os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	}
	p, err := os.StartProcess(self, append([]string{self}, args...), attr)
	if err != nil {
		die("grandchild: %v", err)
	}
	// Deliberately not waited on here: the grandchild outlives this process by
	// design and is reparented. The runner's process-group handling owns what
	// happens to it next.
	return p.Pid
}

func signalByName(name string) (syscall.Signal, bool) {
	n := strings.ToUpper(strings.TrimPrefix(strings.ToUpper(name), "SIG"))
	table := map[string]syscall.Signal{
		"HUP": syscall.SIGHUP, "INT": syscall.SIGINT, "QUIT": syscall.SIGQUIT,
		"ABRT": syscall.SIGABRT, "KILL": syscall.SIGKILL, "SEGV": syscall.SIGSEGV,
		"TERM": syscall.SIGTERM, "USR1": syscall.SIGUSR1, "USR2": syscall.SIGUSR2,
	}
	s, ok := table[n]
	return s, ok
}
