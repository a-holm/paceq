package janitor

import "context"

// drainWindows removes throttle bookkeeping whose opener row is gone, batch
// after batch until a pass comes back empty. It records no total of its own:
// the count is bookkeeping hygiene, not retention policy.
func drainWindows(ctx context.Context, j *Janitor) error {
	for {
		deleted, err := j.st.PruneOrphanedWindowsBatch(ctx)
		if err != nil {
			return err
		}
		if deleted == 0 {
			return nil
		}
	}
}
