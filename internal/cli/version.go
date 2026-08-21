package cli

import (
	"context"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// Build information, set with -ldflags -X at release time. The defaults are
// what a go install or a plain go build reports, and they are deliberately
// honest: a binary that claims a version it was not built from is worse than
// one that says it does not know.
var (
	version   = "dev"
	commit    = "unknown"
	buildTime = "unknown"
)

// versionReport is what version prints, in both modes. The JSON field names are
// the interface: an upgrade check and a bug report template read them.
type versionReport struct {
	Version       string `json:"version"`
	Commit        string `json:"commit"`
	Built         string `json:"built"`
	Go            string `json:"go"`
	Platform      string `json:"platform"`
	SchemaVersion int    `json:"schema_version"`
}

func newVersionCmd(env Env, g *globals) *cobra.Command {
	asJSON := false

	cmd := &cobra.Command{
		Use:   "version",
		Short: "Show the version, the build and the schema this binary carries",
		Long: `Show the version, the build and the schema this binary carries.

The schema version is the highest migration in this build, not the one in a
database. It answers on a machine that has no state directory yet.`,
		Args: noArgs,
		RunE: runE(env, g, func(_ context.Context, out *ui) error {
			if asJSON {
				out.mode = modeJSON
			}
			return writeVersion(out)
		}),
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "same as -o json")
	return cmd
}

func writeVersion(out *ui) error {
	known, err := store.KnownSchemaVersion()
	if err != nil {
		return internalError("could not read the schema this build carries", err)
	}
	report := versionReport{
		Version:       version,
		Commit:        commit,
		Built:         buildTime,
		Go:            runtime.Version(),
		Platform:      runtime.GOOS + "/" + runtime.GOARCH,
		SchemaVersion: known,
	}
	if out.mode == modeJSON {
		return out.json(report)
	}

	out.print("%s", report.text())
	return nil
}

// text is the human form, built as one string so --version and the version
// command cannot drift apart: cobra renders the flag from a template, and the
// template is this.
func (r versionReport) text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "paceq %s", r.Version)
	for _, row := range [][2]string{
		{"commit", r.Commit},
		{"built", r.Built},
		{"go", r.Go},
		{"platform", r.Platform},
		{"schema version", strconv.Itoa(r.SchemaVersion)},
	} {
		fmt.Fprintf(&b, "\n  %s %s", pad(row[0], len("schema version")), row[1])
	}
	return b.String()
}

// versionTemplate is what paceq --version prints. A build whose schema cannot
// be read still answers, because the version of a broken build is exactly what
// a bug report needs.
func versionTemplate() string {
	known, err := store.KnownSchemaVersion()
	if err != nil {
		known = 0
	}
	report := versionReport{
		Version:       version,
		Commit:        commit,
		Built:         buildTime,
		Go:            runtime.Version(),
		Platform:      runtime.GOOS + "/" + runtime.GOARCH,
		SchemaVersion: known,
	}
	return report.text() + "\n"
}
