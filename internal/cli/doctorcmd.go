package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"runtime"
	"time"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/obs"
)

func newDoctorCmd(env Env, g *globals) *cobra.Command {
	var forceJSON bool
	cmd := &cobra.Command{
		Use:   "doctor",
		Short: "Check the installation and say what to do about what is wrong",
		Long: `Check the installation: the state directory, its permissions, the database,
its pragma discipline, the write lock, free disk against the disk-guard's
thresholds, the log directory against its byte cap, the write ahead log
against its alarm levels, the clock, the time zone database, the result
spool, leftover job processes, jobs that monitoring cannot alarm on, the
filesystem the state sits on, the running daemon's version, and the
database's critical invariants.

Every finding carries its own next step. doctor never changes anything. It
exits 0 when nothing failed, so it can stand in a login script, and 1 when
something is broken. Warnings do not fail: another paceq holding the state is
normal, and a machine that has never run paceq init is not broken.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			if forceJSON {
				out.mode = modeJSON
			}
			return runDoctor(ctx, env, g, out)
		}),
	}
	cmd.Flags().BoolVar(&forceJSON, "json", false,
		"emit the JSON contract, the same document -o json pins")
	return cmd
}

// findingJSON is one finding as a script reads it. The level is a word rather
// than a number so a jq filter reads like the report does.
type findingJSON struct {
	Level  string   `json:"level"`
	Title  string   `json:"title"`
	Detail string   `json:"detail"`
	Next   []string `json:"next,omitempty"`
}

type doctorReport struct {
	Status   string        `json:"status"`
	Findings []findingJSON `json:"findings"`
}

func runDoctor(ctx context.Context, env Env, g *globals, out *ui) error {
	stateDir, err := g.stateDir(env)
	if err != nil {
		return err
	}

	out.note(1, "checking %s", stateDir)
	report := doctor.Report{Findings: []doctor.Finding{buildFinding()}}
	// The disk-guard's limits come from the same config.yaml the daemon
	// reads (#44), best effort: a config the daemon will refuse loudly is
	// the daemon's business, and a report with defaults is still honest
	// about what it assumed.
	var limits obs.DiskLimits
	if cfg, err := daemon.LoadNotificationConfig(stateDir, ""); err != nil {
		out.note(2, "config.yaml could not be read; the disk checks use the shipped defaults")
	} else if cfg != nil {
		limits = cfg.Limits
	}
	report.Findings = append(report.Findings,
		doctor.Run(ctx, stateDir, doctor.Options{Status: env.Status, Procs: env.Procs, Limits: limits}).Findings...)
	report.Findings = append(report.Findings, checkDaemonVersion(ctx, env, g, buildinfo.Get().Version))
	for _, finding := range report.Findings {
		out.note(2, "%s: %s", finding.Title, finding.Level)
	}

	if err := writeDoctorReport(out, report); err != nil {
		return err
	}
	if report.Worst() != doctor.Fail {
		return nil
	}
	return &Error{
		code: ExitInternal,
		what: "this installation has failures, listed above",
		next: []string{
			"work through the findings marked " + out.symbols.fail + ", each carries its own next step",
			"paceq doctor  again once they are fixed",
		},
	}
}

// buildFinding is the binary reporting on itself, so a report pasted into an
// issue says which paceq produced it.
func buildFinding() doctor.Finding {
	return doctor.Finding{
		Level:  doctor.OK,
		Title:  "paceq",
		Detail: fmt.Sprintf("%s (%s/%s, %s)", buildinfo.Get().Version, runtime.GOOS, runtime.GOARCH, runtime.Version()),
	}
}

func writeDoctorReport(out *ui, report doctor.Report) error {
	if out.mode == modeJSON {
		document := doctorReport{Status: report.Worst().String(), Findings: make([]findingJSON, 0, len(report.Findings))}
		for _, finding := range report.Findings {
			document.Findings = append(document.Findings, findingJSON{
				Level:  finding.Level.String(),
				Title:  finding.Title,
				Detail: finding.Detail,
				Next:   finding.Next,
			})
		}
		return out.json(document)
	}

	width := 0
	for _, finding := range report.Findings {
		if n := len(finding.Title); n > width {
			width = n
		}
	}
	markWidth := out.markWidth()
	indent := pad("", markWidth+1) + pad("", width+2)

	for _, finding := range report.Findings {
		// print is deliberately not used: -q keeps the findings that need
		// attention and drops only the ones that passed.
		if out.quiet && finding.Level == doctor.OK {
			continue
		}
		fmt.Fprintf(out.out, "%s %s  %s\n", out.mark(finding.Level, markWidth), pad(finding.Title, width), finding.Detail)
		for _, step := range finding.Next {
			fmt.Fprintf(out.out, "%s%s %s\n", indent, out.symbols.arrow, step)
		}
	}
	return nil
}

// checkDaemonVersion asks the running daemon for its version and compares.
// A CLI that predates its daemon (or the reverse) answers questions from a
// schema the other side may not share, so a mismatch is worth naming before
// it becomes a confusing answer.
//
// Whether a daemon exists at all is not this check's own decision (#215). An
// absent socket file used to read as an absent daemon, which put "no daemon is
// running" in the same report as the write lock held by that daemon's pid. The
// open session row answers it, the same row the lock finding names, so one
// report makes one statement.
func checkDaemonVersion(ctx context.Context, env Env, g *globals, cliVersion string) doctor.Finding {
	const title = "daemon version"
	socketPath, err := daemonSocket(env, g)
	if err != nil {
		return refusedSocketFinding(title, err)
	}
	if socketPath == "" {
		return unreachableDaemonFinding(ctx, title, env, g)
	}
	version, err := daemonVersion(ctx, socketPath)
	if err != nil {
		var refused *untrustedSocket
		if errors.As(err, &refused) {
			return refusedSocketFinding(title, refused)
		}
		return doctor.Finding{
			Level:  doctor.Warn,
			Title:  title,
			Detail: fmt.Sprintf("the daemon did not answer: %v", err),
			Next: []string{
				"if it is mid-restart, wait a moment and run paceq doctor again",
				"otherwise remove a stale socket: rm " + socketPath,
			},
		}
	}
	if version == cliVersion {
		return doctor.Finding{Level: doctor.OK, Title: title, Detail: "daemon " + version + ", matches this CLI"}
	}
	return doctor.Finding{
		Level: doctor.Warn,
		Title: title,
		Detail: fmt.Sprintf("this CLI is %s, the daemon is %s: answers may come from a schema "+
			"the other side does not share", cliVersion, version),
		Next: []string{"restart the service so both sides are the same build: systemctl restart paceq"},
	}
}

// unreachableDaemonFinding is what the report says when there is no socket to
// ask. A daemon may still be holding this state directory: --socket defaults to
// empty and the shipped unit does not pass it, so the healthy installation and
// the unreachable one look identical from the filesystem. The session row
// tells them apart, and naming the flag is what makes the silence fixable.
func unreachableDaemonFinding(ctx context.Context, title string, env Env, g *globals) doctor.Finding {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		if isNotFound(err) {
			// No state, so nothing can be serving it. Every other finding in
			// this report already covers a state that should be there.
			return doctor.Finding{Level: doctor.OK, Title: title, Detail: "no daemon is running"}
		}
		return doctor.Finding{
			Level:  doctor.Warn,
			Title:  title,
			Detail: fmt.Sprintf("the state could not be read, so this report cannot say whether a daemon holds it: %v", err),
			Next:   []string{"the findings above name what is wrong with the state directory"},
		}
	}
	defer func() { _ = ro.Close() }()

	owner, running, err := ro.DaemonSession(ctx)
	switch {
	case err != nil:
		return doctor.Finding{
			Level:  doctor.Warn,
			Title:  title,
			Detail: fmt.Sprintf("the session row could not be read, so this report cannot say whether a daemon holds it: %v", err),
			Next:   []string{"the findings above name what is wrong with the state directory"},
		}
	case !running:
		return doctor.Finding{Level: doctor.OK, Title: title, Detail: "no daemon is running"}
	}
	return doctor.Finding{
		Level: doctor.Warn,
		Title: title,
		Detail: fmt.Sprintf("a daemon holds this state (pid %d, paceq %s, started %s) but exposes no socket",
			owner.PID, owner.Version, owner.StartedAt.Format(time.RFC3339)),
		Next: []string{
			"start it with --socket so the CLI can reach it: paceq serve --socket " +
				filepath.Join(g.stateDirOrEmpty(env), socketName),
			"until then every command that writes has to wait for that daemon",
		},
	}
}

// refusedSocketFinding reports a socket paceq will not talk to. It fails the
// installation rather than warning about it: another account answering for the
// daemon is the one thing this socket's whole authorisation model rests on not
// happening.
func refusedSocketFinding(title string, err error) doctor.Finding {
	return doctor.Finding{
		Level:  doctor.Fail,
		Title:  title,
		Detail: err.Error(),
		Next: []string{
			"ls -l the path: another account owning it, or a mode wider than 0600, is how a stranger answers for the daemon",
			"remove it if a crash left it behind, then start the daemon again",
		},
	}
}

func daemonVersion(ctx context.Context, socketPath string) (string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://unix/readyz", nil)
	if err != nil {
		return "", err
	}
	resp, err := socketClient(socketPath, 2*time.Second).Do(req)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	var doc struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return "", err
	}
	if doc.Version == "" {
		return "", errors.New("the health answer carried no version")
	}
	return doc.Version, nil
}
