package reconcile

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"

	"github.com/a-holm/paceq/internal/spool"
	"github.com/a-holm/paceq/internal/store"
)

// The spool consumer (issue #39, R2's second half). The exec shim of a dead
// attempt wrote what really happened into <state>/spool/attempts before it
// died; this step reads those files back and commits what they say, so a
// crashed executor no longer turns a finished job into a rerun (crash window
// W8).
//
// The routing is deliberately total — every file leaves this function in a
// settled state or leaves the outcome to a transient error:
//
//   - a result that matches the running attempt and its lease epoch is
//     committed with outcome_source='spool' and removed;
//   - a result whose lease has moved on, or whose attempt nobody knows, is
//     archived under spool/unknown with a warn event on the run;
//   - a file that cannot be trusted at all (wrong version, wrong bytes) is
//     archived the same way;
//   - a result naming an attempt whose executor may still be alive (the
//     lease has not expired) is left exactly where it is: the live executor
//     owns that story;
//   - the hidden temp files of interrupted writes are never read: a write
//     that never reached its rename is no outcome at all.
//
// Like the rest of reconciliation, a pass over a clean directory writes
// nothing, so repeating it is always safe.

// SpoolDirUnder returns the attempts directory inside a state directory.
func SpoolDirUnder(stateDir string) string {
	return filepath.Join(stateDir, "spool", "attempts")
}

// ConsumeSpoolResults runs one pass over dir. Files it cannot settle because
// the database is temporarily unreachable end the pass with the error; the
// next start or periodic sweep tries them again.
func ConsumeSpoolResults(ctx context.Context, st *store.Store, dir string, log *slog.Logger) error {
	if dir == "" || st == nil {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	paths, err := spool.List(dir)
	if err != nil {
		return fmt.Errorf("list the result spool: %w", err)
	}
	for _, path := range paths {
		res, err := spool.ReadResult(path)
		if err != nil {
			if errors.Is(err, spool.ErrFormat) {
				archive(ctx, st, dir, path, res, "unusable result file", log)
				continue
			}
			return fmt.Errorf("read the result spool: %w", err)
		}

		outcome, err := store.SpoolVerdict(res)
		if err != nil {
			archive(ctx, st, dir, path, res, "result with no usable verdict", log)
			continue
		}
		err = st.CommitSpooledOutcome(ctx, store.SpoolOutcome{
			RunID:      res.RunID,
			Step:       res.Step,
			Attempt:    res.Attempt,
			ClaimEpoch: res.ClaimEpoch,
			BootID:     res.BootID,
			Outcome:    outcome,
		})
		switch {
		case err == nil:
			if err := spool.Remove(path); err != nil {
				return err
			}
			log.Info("committed a result from the spool",
				"run", res.RunID, "step", res.Step, "attempt", res.Attempt,
				"outcome", res.Outcome, "killed_by", res.KilledBy)
		case errors.Is(err, store.ErrEpochMismatch):
			archive(ctx, st, dir, path, res, "the attempt was reclaimed by a newer lease", log)
		case errors.Is(err, store.ErrUnknownAttempt):
			archive(ctx, st, dir, path, res, "no running attempt matches the result", log)
		case errors.Is(err, store.ErrLeaseLost):
			// A live executor owns this attempt and will consume its
			// own result; the file stays until then.
			continue
		default:
			return fmt.Errorf("commit the spooled result of %s on run %s: %w", res.Step, res.RunID, err)
		}
	}
	return nil
}

// archive moves one rejected result into the unknown directory and writes
// the warn event that explains it. The move happens first: the file must be
// out of the attempts directory even if the event cannot land (a file naming
// a run that no longer exists has no run to hang the event on), and the log
// line keeps the story for whoever reads the unknown directory.
func archive(ctx context.Context, st *store.Store, dir, path string, res spool.Result, why string, log *slog.Logger) {
	if err := spool.Archive(dir, path); err != nil {
		log.Warn("could not archive a result file", "file", filepath.Base(path), "error", err)
		return
	}
	log.Warn("archived a result file without committing it",
		"file", filepath.Base(path), "run", res.RunID, "step", res.Step, "why", why)
	if res.RunID != "" {
		if err := st.RecordSpoolArchive(ctx, res.RunID, res.Step, filepath.Base(path), why); err != nil {
			log.Warn("could not record the archive event", "file", filepath.Base(path), "error", err)
		}
	}
}

// consumeSpool wraps the pass with the sweep's error policy: evidence
// gathering about the spool directory never refuses the start.
func consumeSpool(ctx context.Context, st *store.Store, opts *Options) {
	if opts.SpoolDir == "" {
		return
	}
	if err := ConsumeSpoolResults(ctx, st, opts.SpoolDir, opts.logger()); err != nil {
		opts.logger().Warn("the result spool pass did not finish", "error", err)
	}
}
