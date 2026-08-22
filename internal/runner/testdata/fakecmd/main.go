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
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			die("write-output: %v", err)
		}
		defer f.Close()
		for _, line := range []string{
			`{"artifact":{"name":"out.txt","bytes":3}}`,
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

	default:
		die("unknown mode %q", mode)
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
