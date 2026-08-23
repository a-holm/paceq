package cli

import (
	"context"
	"embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

//go:embed deploy/paceq.service deploy/paceq.service.relaxed
var deployFS embed.FS

func newInstallServiceCmd(env Env, g *globals) *cobra.Command {
	var (
		dest   string
		dryRun bool
		force  bool
	)

	cmd := &cobra.Command{
		Use:   "install-service",
		Short: "Write the systemd unit file and its relaxed variant",
		Long: `Write the systemd unit file and its relaxed variant
to /etc, so systemd can supervise paceq.

  paceq install-service [--dest /etc/systemd/system] [--dry-run] [--force]
`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runInstallService(out, dest, dryRun, force)
		}),
	}
	cmd.Flags().StringVar(&dest, "dest", "/etc/systemd/system",
		"directory to write the unit file into")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false,
		"write the unit file to stdout instead of disk")
	cmd.Flags().BoolVar(&force, "force", false,
		"overwrite an existing unit file without asking")
	return cmd
}

func runInstallService(out *ui, dest string, dryRun, force bool) error {
	if dryRun {
		data, err := deployFS.ReadFile("deploy/paceq.service")
		if err != nil {
			return fmt.Errorf("install-service: could not read embedded unit: %w", err)
		}
		fmt.Fprintln(out.out, string(data))
		return nil
	}

	if err := writeIfAbsent(filepath.Join(dest, "paceq.service"), "deploy/paceq.service", force); err != nil {
		return err
	}
	out.note(1, "wrote %s/paceq.service", dest)

	if err := writeIfAbsent(filepath.Join(dest, "paceq.service.relaxed"), "deploy/paceq.service.relaxed", force); err != nil {
		return err
	}
	out.note(1, "wrote %s/paceq.service.relaxed", dest)

	out.note(1, "run: systemctl enable --now paceq")
	return nil
}

func writeIfAbsent(target, embeddedPath string, force bool) error {
	if !force {
		if _, err := os.Stat(target); err == nil {
			return fmt.Errorf("install-service: %s already exists; use --force to overwrite", target)
		}
	}
	data, err := deployFS.ReadFile(embeddedPath)
	if err != nil {
		return fmt.Errorf("install-service: could not read embedded %s: %w", embeddedPath, err)
	}
	return os.WriteFile(target, data, 0o644) // #nosec G306 — systemd unit files must be world-readable
}
