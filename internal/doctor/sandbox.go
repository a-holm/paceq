package doctor

import (
	"os"
	"strconv"
	"strings"
)

// ProcessStatus is parsed from /proc/self/status for sandbox checks.
type ProcessStatus struct {
	NoNewPrivs int    // 0 or 1 from /proc/self/status
	Seccomp    int    // 0, 1 (strict), 2 (filter), or 3 (disabled)
	CapEff     string // hex capability mask
}

// ReadProcessStatus reads /proc/self/status and extracts the sandbox fields.
func ReadProcessStatus() (ProcessStatus, error) {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return ProcessStatus{}, err
	}
	var ps ProcessStatus
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		key, val, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		val = strings.TrimSpace(val)
		switch strings.TrimSpace(key) {
		case "NoNewPrivs":
			ps.NoNewPrivs, _ = strconv.Atoi(val)
		case "Seccomp":
			ps.Seccomp, _ = strconv.Atoi(val)
		case "CapEff":
			ps.CapEff = val
		}
	}
	return ps, nil
}

// StatusReader is the edge a report reads the process sandbox from. The real
// one reads /proc/self/status; a test plants a status so doctor answers on a
// machine that sandboxes nothing.
type StatusReader func() (ProcessStatus, error)

// CheckSandbox reports the effective systemd sandbox from /proc/self/status.
// It warns when the process is not running under the expected restrictions, so
// an operator that deployed without the hardened unit gets a heads-up from
// doctor before anything goes wrong, together with the command that fixes it.
func CheckSandbox(reader StatusReader) Finding {
	const title = "sandbox"
	if reader == nil {
		reader = ReadProcessStatus
	}
	ps, err := reader()
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: "could not read /proc/self/status: " + err.Error(),
			Next:   []string{"doctor needs /proc to inspect the sandbox: run it on a Linux host, not in a restricted container without /proc"},
		}
	}

	var parts []string
	if ps.NoNewPrivs != 1 {
		parts = append(parts, "NoNewPrivs is off")
	}
	if ps.Seccomp != 2 {
		parts = append(parts, "seccomp is not in filter mode (systemd's SystemCallFilter)")
	}

	if len(parts) > 0 {
		detail := strings.Join(parts, "; ") +
			": not running under the hardened systemd unit. " +
			"Run paceq under the hardened unit to get NoNewPrivs and the seccomp filter back."
		return Finding{
			Level:  Warn,
			Title:  title,
			Detail: detail,
			Next: []string{
				"run paceq under the hardened systemd unit: paceq install-service, then systemctl enable --now paceq",
				"or use the relaxed variant for jobs that need broad filesystem access",
			},
		}
	}

	detail := "NoNewPrivs=1, seccomp=filter"
	if ps.CapEff == "0000000000000000" {
		detail += ", no capabilities"
	}
	return Finding{
		Level:  OK,
		Title:  title,
		Detail: detail,
	}
}

// CheckJobWorkdirWritable warns when a configured job workdir falls outside
// paths writable under the hardned unit. ProtectSystem=strict makes / read-only
// except for StateDirectory, RuntimeDirectory, and explicit ReadWritePaths.
// This check is a heuristic: it warns for common non-writable paths.
func CheckJobWorkdirWritable(workdir string) Finding {
	const title = "job workdir"

	// Under ProtectSystem=strict, these paths are read-only by default.
	readOnlyPrefixes := []string{
		"/srv/", "/opt/", "/usr/", "/etc/",
	}
	for _, prefix := range readOnlyPrefixes {
		if strings.HasPrefix(workdir, prefix) {
			return Finding{
				Level: Warn,
				Title: title,
				Detail: workdir + " is read-only under ProtectSystem=strict: " +
					"add ReadWritePaths=" + workdir + " to the unit, or use the relaxed variant",
				Next: []string{
					"systemctl edit paceq.service and add: [Service]\\nReadWritePaths=" + workdir,
					"or use the relaxed variant: cp /usr/share/paceq/paceq.service.relaxed /etc/systemd/system/paceq.service",
				},
			}
		}
	}
	return Finding{
		Level:  OK,
		Title:  title,
		Detail: workdir + " is writable",
	}
}
