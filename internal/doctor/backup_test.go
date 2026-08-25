package doctor_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/a-holm/paceq/internal/clock"
	"github.com/a-holm/paceq/internal/doctor"
	"github.com/a-holm/paceq/internal/store"
)

// fixedClock answers Now with one planted moment, so age arithmetic in the
// findings is exact.
type fixedClock struct{ t time.Time }

func (f fixedClock) Now() time.Time                       { return f.t }
func (f fixedClock) Mark() clock.Mono                     { return clock.Mono{} }
func (f fixedClock) Since(clock.Mono) time.Duration       { return 0 }
func (f fixedClock) NewTimer(d time.Duration) *time.Timer { return time.NewTimer(d) }
func (f fixedClock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}

// TestCheckBackupSpeaksForEveryBackupState walks the levels the backup field
// can take: silence, failure, staleness and health each get their own
// verdict, and every one of them repeats the cp warning, because that is
// the disaster people cause themselves while trying to fix backups.
func TestCheckBackupSpeaksForEveryBackupState(t *testing.T) {
	now := time.Date(2026, 8, 25, 12, 0, 0, 0, time.UTC)

	fresh := func(status string) store.BackupInfo {
		return store.BackupInfo{
			HasBackup:     true,
			Status:        status,
			Path:          "/backups/state-20260825T030001Z.db",
			LastAt:        now.Add(-3 * time.Hour),
			LastDeepCheck: now.Add(-2 * 24 * time.Hour),
		}
	}

	cases := []struct {
		name    string
		info    store.BackupInfo
		level   doctor.Level
		contain []string
	}{
		{
			name:    "never attempted",
			info:    store.BackupInfo{},
			level:   doctor.OK,
			contain: []string{"no backup on record yet", "copy state.db"},
		},
		{
			name:    "failed verification",
			info:    fresh(store.BackupStatusFailed),
			level:   doctor.Fail,
			contain: []string{"FAILED", "hypothesis"},
		},
		{
			name: "stale but verified",
			info: func() store.BackupInfo {
				b := fresh(store.BackupStatusVerified)
				b.LastAt = now.Add(-9 * 24 * time.Hour)
				return b
			}(),
			level:   doctor.Warn,
			contain: []string{"old", "lease"},
		},
		{
			name:  "fresh and verified",
			info:  fresh(store.BackupStatusVerified),
			level: doctor.OK,
			contain: []string{
				"verified copy from",
				"deep integrity check",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db := healthy("state")
			db.backup = tc.info
			finding := doctor.CheckBackup(context.Background(), db, fixedClock{t: now})

			if finding.Level != tc.level {
				t.Fatalf("level = %v, want %v (%s)", finding.Level, tc.level, finding.Detail)
			}
			for _, want := range tc.contain {
				if !strings.Contains(finding.Detail+strings.Join(finding.Next, " "), want) {
					t.Fatalf("finding never says %q:\n%s\n%v", want, finding.Detail, finding.Next)
				}
			}
		})
	}
}
