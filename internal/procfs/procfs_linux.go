//go:build linux

package procfs

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// bootIDPath is the kernel's identifier for the current boot. It is
// regenerated on every boot, so a change means the machine restarted, which
// in turn means no process paceq started can have survived.
const bootIDPath = "/proc/sys/kernel/random/boot_id"

// ProcStartTicks returns field 22 of /proc/<pid>/stat: the kernel's start
// time for the process in clock ticks. The bool is false when the process is
// gone or the line could not be parsed, which every caller must read as "this
// is not provably the process you think it is".
func ProcStartTicks(pid int) (int64, bool) {
	raw, err := os.ReadFile(statPath(pid))
	if err != nil {
		return 0, false
	}
	ticks, err := parseStartTicks(raw)
	if err != nil {
		return 0, false
	}
	return ticks, true
}

// BootID reads the machine's boot identifier, trimmed. An empty string means
// the platform has no answer; callers store what they get and compare what
// they stored.
func BootID() string {
	raw, err := os.ReadFile(bootIDPath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func statPath(pid int) string {
	return fmt.Sprintf("/proc/%d/stat", pid)
}

// parseStartTicks pulls field 22 out of one stat file's bytes. Fields 3 and
// onward follow the closing parenthesis of the command name, so start time is
// the 20th field of what remains (fields 1 and 2 were consumed by the pid and
// the name). The index math is pinned by the package test against a stat line
// with spaces and brackets in the command name, which is exactly the input a
// job named "$(printf 'x () []')" produces.
func parseStartTicks(stat []byte) (int64, error) {
	i := bytes.LastIndexByte(stat, ')')
	if i < 0 {
		return 0, errors.New("procfs: unparseable /proc stat: no command name end")
	}
	fields := strings.Fields(string(stat[i+2:]))
	if len(fields) < 20 {
		return 0, errors.New("procfs: too few fields in /proc stat")
	}
	ticks, err := strconv.ParseInt(fields[19], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("procfs: bad start ticks in /proc stat: %w", err)
	}
	return ticks, nil
}
