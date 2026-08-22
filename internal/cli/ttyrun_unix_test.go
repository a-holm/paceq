//go:build unix

package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"unsafe"

	"github.com/rogpeppe/go-internal/testscript"
)

// cmdTtyRun runs one paceq command with its standard output attached to a
// real pseudo terminal, and writes what the terminal saw to a file.
//
//	ttyrun 0 doctor_tty.txt NO_COLOR= -- doctor
//
// The first argument is the exit code the command must end with, the second
// the file the terminal output lands in, and any KEY=VALUE arguments before
// the "--" override the determinism environment for this one command, which
// is how a row turns colour back on at the terminal. Standard error goes to
// a file beside it, named by appending .stderr.
//
// A fake that answers "yes, a terminal" would prove nothing: the mode
// decision asks the kernel about a descriptor, so only a descriptor the
// kernel calls a terminal can drive the text-at-a-terminal rows.
func cmdTtyRun(ts *testscript.TestScript, neg bool, args []string) {
	if neg || len(args) < 3 {
		ts.Fatalf("usage: ttyrun CODE OUTFILE [KEY=VALUE ...] -- ARGS...")
	}
	separator := -1
	for i, arg := range args {
		if arg == "--" {
			separator = i
			break
		}
	}
	if separator < 0 {
		ts.Fatalf("ttyrun needs a \"--\" between the overrides and the paceq command")
	}
	code := args[0]
	outFile := args[1]
	overrides := args[2:separator]
	paceqArgs, err := expandFileArgs(ts, args[separator+1:])
	if err != nil {
		ts.Fatalf("%v", err)
	}
	if len(paceqArgs) == 0 {
		ts.Fatalf("ttyrun needs a paceq command after \"--\"")
	}

	slave, readTerminal, cleanup, err := openTerminal()
	if err != nil {
		// Scripts guard their tty rows with [unix], so this line only
		// fires when a terminal was expected and the machine could not
		// provide one.
		ts.Fatalf("could not open a pseudo terminal: %v", err)
	}
	defer cleanup()

	cmd := spawnPaceq(ts, overrides, paceqArgs)
	cmd.Stdout = slave
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	cmd.Stdin, err = os.Open(os.DevNull)
	if err != nil {
		ts.Fatalf("could not open %s: %v", os.DevNull, err)
	}
	runErr := cmd.Run()
	exitCode := 0
	if runErr != nil {
		exitErr, ok := runErr.(*exec.ExitError)
		if !ok {
			ts.Fatalf("paceq could not be started: %v", runErr)
		}
		exitCode = exitErr.ExitCode()
	}
	_ = slave.Close()
	terminalSaw := readTerminal()
	want, err := parseExitCode(code)
	if err != nil {
		ts.Fatalf("%v", err)
	}
	if exitCode != want {
		ts.Fatalf("%v exited %d, want %d\nterminal:\n%s\nstderr:\n%s",
			paceqArgs, exitCode, want, terminalSaw, stderr.String())
	}
	ts.Check(os.WriteFile(ts.MkAbs(outFile), []byte(terminalSaw), 0o644))
	ts.Check(os.WriteFile(ts.MkAbs(outFile+".stderr"), stderr.Bytes(), 0o644))
}

func parseExitCode(in string) (int, error) {
	n, err := strconv.Atoi(in)
	if err != nil {
		return 0, fmt.Errorf("%q is not an exit code", in)
	}
	return n, nil
}

// openTerminal opens a pseudo terminal pair. The slave side is handed to a
// command as its stdout; everything written there surfaces on the master,
// which is what readTerminal returns once the command is done.
func openTerminal() (*os.File, func() string, func(), error) {
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR, 0)
	if err != nil {
		return nil, nil, nil, err
	}
	cleanup := func() { _ = master.Close() }

	// The slave stays locked until unlockpt, and its number is only
	// readable through the master. This is what grantpt and unlockpt do.
	var unlocked int32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(),
		syscall.TIOCSPTLCK, uintptr(unsafe.Pointer(&unlocked))); errno != 0 {
		cleanup()
		return nil, nil, nil, fmt.Errorf("unlock the pseudo terminal: %v", errno)
	}
	var number uint32
	if _, _, errno := syscall.Syscall(syscall.SYS_IOCTL, master.Fd(),
		syscall.TIOCGPTN, uintptr(unsafe.Pointer(&number))); errno != 0 {
		cleanup()
		return nil, nil, nil, fmt.Errorf("read the pseudo terminal number: %v", errno)
	}

	path := fmt.Sprintf("/dev/pts/%d", number)
	slave, err := os.OpenFile(path, os.O_RDWR|syscall.O_NOCTTY, 0)
	if err != nil {
		cleanup()
		return nil, nil, nil, fmt.Errorf("open %s: %w", path, err)
	}

	// The master is drained as the command writes: a terminal buffers a
	// few kilobytes, and a talkative command would otherwise block.
	done := make(chan string, 1)
	go func() {
		data, _ := io.ReadAll(master)
		done <- string(data)
	}()
	read := func() string { return <-done }
	stop := func() {
		_ = slave.Close()
		_ = master.Close()
	}
	return slave, read, stop, nil
}
