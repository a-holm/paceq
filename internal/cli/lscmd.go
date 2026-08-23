package cli

import (
	"context"

	"github.com/spf13/cobra"

	"github.com/a-holm/paceq/internal/store"
)

// newLsCmd is the fast overview command: one line per job with last and next
// run, fitting in 80 columns.
func newLsCmd(env Env, g *globals) *cobra.Command {
	return &cobra.Command{
		Use:     "ls",
		Aliases: []string{"list-jobs"},
		Short:   "List jobs with their last and next run",
		Long: `One line per job, 80 columns wide: the job name, its last run id with
outcome, and the next schedule fire-time when one exists.

When a job has no runs yet the last column reads '---'; when it has no
schedule the next column reads '---'. The command reads from the database
only, so it works while the daemon is shut down.`,
		Args: noArgs,
		RunE: runE(env, g, func(ctx context.Context, out *ui) error {
			return runLs(ctx, env, g, out)
		}),
	}
}

func runLs(ctx context.Context, env Env, g *globals, out *ui) error {
	ro, err := openReadOnlyStore(ctx, env, g)
	if err != nil {
		return err
	}
	defer func() { _ = ro.Close() }()

	schedules, err := ro.ListAllSchedules(ctx)
	if err != nil {
		return internalError("could not list schedules", err)
	}

	runs, err := ro.ListRuns(ctx, store.RunFilter{Limit: 50})
	if err != nil {
		return internalError("could not list runs", err)
	}

	// Index runs by job name for quick lookup.
	jobRuns := make(map[string]*store.RunSummary)
	for _, r := range runs {
		if _, exists := jobRuns[r.JobName]; !exists {
			cp := r
			jobRuns[r.JobName] = &cp
		}
	}

	// Collect job names from both schedules and runs.
	seen := make(map[string]bool)
	var jobs []string
	for _, s := range schedules {
		if !seen[s.JobName] {
			seen[s.JobName] = true
			jobs = append(jobs, s.JobName)
		}
	}
	for _, r := range runs {
		if !seen[r.JobName] {
			seen[r.JobName] = true
			jobs = append(jobs, r.JobName)
		}
	}

	// Sort jobs alphabetically.
	sortStrings(jobs)

	if len(jobs) == 0 {
		out.print("no jobs yet: apply a job file to create one")
		return nil
	}

	// Compute column widths.
	wJob, wLast, wStatus, wNext := 3, 2, 6, 4
	for _, jn := range jobs {
		wJob = max(wJob, len(jn))
	}
	for _, jn := range jobs {
		if r, ok := jobRuns[jn]; ok {
			short := r.ID
			if len(short) > 12 {
				short = short[:12]
			}
			wLast = max(wLast, len(short))
			wStatus = max(wStatus, len(r.State))
			wNext = max(wNext, len(relAge(clockForEnv(env).Now(), r.CreatedAt)))
		}
	}
	// Cap total width at 80.
	avail := 80 - (wJob + 2 + wLast + 2 + wStatus + 2 + wNext + 2)
	if avail < 0 {
		wJob = max(5, wJob+avail)
	}

	for _, jn := range jobs {
		lastID := "---"
		status := ""
		when := ""
		if r, ok := jobRuns[jn]; ok {
			short := r.ID
			if len(short) > 12 {
				short = short[:12]
			}
			lastID = short
			status = r.State
			when = relAge(clockForEnv(env).Now(), r.CreatedAt)
		}
		next := "---"
		for _, s := range schedules {
			if s.JobName == jn && !s.Paused {
				next = s.NextTickAt.UTC().Format("15:04")
				break
			} else if s.JobName == jn && s.Paused {
				next = "(paused)"
				break
			}
		}
		out.print("%s  %s  %s  %s  %s",
			pad(jn, wJob),
			pad(lastID, wLast),
			pad(status, wStatus),
			pad(when, wNext),
			next,
		)
	}
	return nil
}

// sortStrings sorts a slice in place.
func sortStrings(s []string) {
	for i := 0; i < len(s)-1; i++ {
		for j := i + 1; j < len(s); j++ {
			if s[i] > s[j] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}
