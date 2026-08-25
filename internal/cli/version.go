package cli

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/buildinfo"
	"github.com/a-holm/paceq/internal/store"
)

// versionReport is what version prints, in both modes. The build facts come
// from internal/buildinfo, the single source the Makefile stamps and the
// release pipeline injects (issue #43); schema_version joins them here because
// it answers on a machine that has no state directory yet. The JSON field
// names are the interface: an upgrade check and a bug report template read
// them.
type versionReport struct {
	buildinfo.Info
	SchemaVersion int `json:"schema_version"`
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
		Info:          buildinfo.Get(),
		SchemaVersion: known,
	}
	if out.mode == modeJSON {
		return out.json(report)
	}

	out.print("%s", report.text())
	return nil
}

// text is the human form, built as one string so the command and the --version
// flag print exactly the same report. The labels stay stable even where the
// JSON names follow the frozen buildinfo contract.
func (r versionReport) text() string {
	var b strings.Builder
	fmt.Fprintf(&b, "paceq %s", r.Version)
	for _, row := range [][2]string{
		{"commit", r.Commit},
		{"built", r.Date},
		{"go", r.GoVersion},
		{"platform", r.OS + "/" + r.Arch},
		{"schema version", strconv.Itoa(r.SchemaVersion)},
	} {
		fmt.Fprintf(&b, "\n  %s %s", pad(row[0], len("schema version")), row[1])
	}
	return b.String()
}
