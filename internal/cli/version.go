package cli

import (
	"context"
	"runtime"
	"strconv"

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

	out.print("paceq %s", report.Version)
	for _, row := range [][2]string{
		{"commit", report.Commit},
		{"built", report.Built},
		{"go", report.Go},
		{"platform", report.Platform},
		{"schema version", strconv.Itoa(report.SchemaVersion)},
	} {
		out.print("  %s %s", pad(row[0], len("schema version")), row[1])
	}
	return nil
}
