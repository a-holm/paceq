package doctor

import (
	"context"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/store"
)

// backupFresh is how old a verified backup may be before doctor starts
// asking when the nightly job last succeeded. A week of silence means a
// broken timer or a failing disk; either deserves a question, not silence.
const backupStaleAfter = 7 * 24 * time.Hour

// humanDuration renders an age the way a person reads it.
func humanDuration(d time.Duration) string {
	switch {
	case d < time.Minute:
		return d.Round(time.Second).String()
	case d < time.Hour:
		return fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%.1fh", d.Hours())
	default:
		return fmt.Sprintf("%dd", int(d.Hours()/24))
	}
}

// CheckBackup reports the age and verification status of the most recent
// backup (issue #36). The finding also carries the one sentence that prevents
// the most common self-inflicted disaster with SQLite: copying a live
// database file with cp.
func CheckBackup(ctx context.Context, db DB, clk clock.Clock) Finding {
	info, err := db.BackupStatus(ctx)
	if err != nil {
		return Finding{
			Level:  Warn,
			Title:  "backup",
			Detail: fmt.Sprintf("could not read the backup record: %v", err),
			Next:   []string{"check that this database was created by paceq init"},
		}
	}
	now := clk.Now

	if !info.HasBackup {
		// An installation paceq init created has never had a daemon, so
		// silence here is the honest answer rather than a warning: the
		// nightly cycle takes the first backup on its first night.
		return Finding{
			Level: OK,
			Title: "backup",
			Detail: "no backup on record yet - the daemon's nightly cycle " +
				"takes the first verified copy",
			Next: []string{
				"never copy state.db while paceq runs - a half-written copy restores nothing",
			},
		}
	}

	age := info.Age(now())
	ageText := humanDuration(age)
	switch {
	case info.Status == store.BackupStatusFailed:
		reason := info.Error
		if reason == "" {
			reason = "verification did not pass"
		}
		return Finding{
			Level: Fail,
			Title: "backup",
			Detail: fmt.Sprintf("the latest attempt (%s ago) FAILED: %s. "+
				"A backup nobody has restored from is a hypothesis", ageText, reason),
			Next: []string{
				"check the daemon log for the maintenance line of that night",
				"check free disk space in the backup directory",
				"never copy state.db while paceq runs - a half-written copy restores nothing",
			},
		}
	case age > backupStaleAfter:
		return Finding{
			Level: Warn,
			Title: "backup",
			Detail: fmt.Sprintf("the newest verified copy is %s old (status %q)",
				ageText, info.Status),
			Next: []string{
				"check that the daemon's maintenance lease is being held: two daemons fence each other",
				"never copy state.db while paceq runs - a half-written copy restores nothing",
			},
		}
	default:
		detail := fmt.Sprintf("verified copy from %s ago", ageText)
		if !info.LastDeepCheck.IsZero() {
			detail += fmt.Sprintf(", deep integrity check %s ago",
				humanDuration(now().Sub(info.LastDeepCheck)))
		}
		return Finding{Level: OK, Title: "backup", Detail: detail}
	}
}
