package cli

import (
	"context"
	"fmt"
	"runtime"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/daemon"
	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/obs"
)

func newDoctorCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:   "doctor",
		Short: "Check the installation and say what to do about what is wrong",
		Long: `Check the installation: the state directory, its permissions, the database,
the write lock, free disk against the disk-guard's thresholds, the log
directory against its byte cap, the write ahead log against its alarm
levels, and the time zone database.

doctor never changes anything. It exits 0 when nothing failed, so it can stand
in a login script, and 1 when something is broken. Warnings do not fail: another
paceq holding the state is normal, and a machine that has never run paceq init
is not broken.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runDoctor(ctx, env, g, out)
		}),
	}
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
	report.Findings = append(report.Findings, doctor.Run(ctx, stateDir, doctor.Options{Status: env.Status, Limits: limits}).Findings...)
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
