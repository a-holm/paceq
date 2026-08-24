package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/model"
	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/spec"
)

// Run level replay, M4-04. Where retry continues one run in place, replay
// makes a NEW run beside it: a fresh id, replay_of pointing back at the
// source, no run_key of its own, and the same frozen job_version_id the
// source ran. Nothing reads the job's current definition, so an apply that
// lands between the original attempt and the replay cannot change what the
// replay does (08 section 7.4).
//
// Steps can be spared a second execution. --from <step> spares the upstream
// closure of that step in the FROZEN graph of the source run; --failed
// spares every step that succeeded in the source. A spared step is born
// succeeded carrying STEP_SKIPPED_REPLAY_REUSED, and the artifact rows the
// source step produced are copied as references: uri, checksum and size move,
// bytes never do.
//
// The whole materialization is ONE transaction, like every other path that
// creates a run: a crash leaves no half run behind.

// ErrConflictingReuse is returned when a caller asks for both reuse rules at
// once. They are two answers to one question, and answering twice is how a
// command grows a third meaning nobody documented.
var ErrConflictingReuse = errors.New("choose one of --from and --failed")

// ReplayOpts says which steps a replay spares. Neither field means the whole
// graph runs again.
type ReplayOpts struct {
	// From spares this step plus everything it transitively depends on in
	// the frozen graph of the source run. The direction matters: naming w
	// in x -> y -> w spares x and y, because they are what w sits on.
	From *string

	// FailedOnly spares exactly the steps that succeeded in the source run.
	FailedOnly bool

	// Actor is who typed the command, recorded on the queued event. Empty
	// becomes "system".
	Actor string
}

// ReplayResult says what one replay made: the new run, which of its steps
// start as succeeded references, and which have to earn their outputs again.
// Both lists are in spec order, and together they hold every step exactly
// once.
type ReplayResult struct {
	NewRunID string
	Reused   []string
	Rerun    []string
}

// MaterializeReplay makes a new run out of a finished one. The source has to
// be terminal: a queued or running run is somebody's live work, and copying
// states still being written would freeze a moment that never existed. The
// new run is queued, so the ordinary claim predicate picks it up with nothing
// special done anywhere.
func (s *Store) MaterializeReplay(ctx context.Context, srcRunID string, opt ReplayOpts) (ReplayResult, error) {
	if opt.From != nil && opt.FailedOnly {
		return ReplayResult{}, fmt.Errorf("replay %s: %w", srcRunID, ErrConflictingReuse)
	}
	actor := opt.Actor
	if actor == "" {
		actor = "system"
	}

	now := s.clk.Now().UTC()
	newID, err := id.New(now)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("mint a replay run id: %w", err)
	}

	var out ReplayResult
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		src, err := readRunTx(tx, srcRunID)
		if err != nil {
			return err
		}
		cur, err := model.ParseRunState(src.State)
		if err != nil {
			return err
		}
		if !cur.IsTerminal() {
			return fmt.Errorf("replay run %s: %w (it is %s)", srcRunID, ErrRunNotTerminal, src.State)
		}

		// The version is read inside the transaction by the id the source
		// froze, never by looking up what the job points at now.
		var specJSON string
		if err := tx.QueryRow(`SELECT v.spec_json
FROM job_versions v WHERE v.id = ?`, src.JobVersionID).Scan(&specJSON); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("replay run %s: the version it was frozen with (%s): %w",
					srcRunID, src.JobVersionID, ErrNotFound)
			}
			return fmt.Errorf("read the frozen spec of run %s: %w", srcRunID, err)
		}
		job, err := spec.FromIR([]byte(specJSON))
		if err != nil {
			return fmt.Errorf("read the frozen spec of run %s (version %s): %w",
				srcRunID, src.JobVersionID, err)
		}

		reuse, err := resolveReuseTx(tx, src.ID, opt)
		if err != nil {
			return err
		}
		reused := make(map[string]bool, len(reuse))
		for _, name := range reuse {
			reused[name] = true
		}

		// The dedup key is deliberately absent: a replay is a deliberate
		// second execution, and writing the source's key here would either
		// collide with the run_keys gate or silently teach it a lie. The
		// trigger is absent too: the original tick decided once, and this
		// decision belongs to whoever typed the command.
		if _, err := tx.Exec(`INSERT INTO runs
(id, job_name, job_version_id, origin, state, available_at, params_json,
 concurrency_key, max_attempts, replay_of, created_at, updated_at)
VALUES (?, ?, ?, 'replay', 'queued', ?, ?, ?, 1, ?, ?, ?)`,
			newID, src.JobName, src.JobVersionID, now.UnixMilli(), src.ParamsJSON,
			nullIfEmpty(src.ConcurrencyKey), nullIfEmpty(src.ID), now.UnixMilli(), now.UnixMilli()); err != nil {
			return fmt.Errorf("create the replay of run %s: %w", srcRunID, err)
		}

		for i, step := range job.Steps {
			maxAttempts := 1
			if step.Retry != nil && step.Retry.Max >= 0 {
				maxAttempts = step.Retry.Max + 1
			}
			if reused[step.Name] {
				// Born terminal at materialisation, the way claim.go
				// admits a step born running: there is no earlier state
				// of this row to transition from, because this run has
				// never executed anything. Nothing updates an existing
				// row, so there is nothing to fence.
				if _, err := tx.Exec(`INSERT INTO steps
(run_id, name, idx, state, max_attempts, finished_at, reason_code, reason_data)
VALUES (?, ?, ?, 'succeeded', ?, ?, ?, ?)`,
					newID, step.Name, i, maxAttempts, now.UnixMilli(),
					string(reason.STEPSkippedReplayReused),
					fmt.Sprintf(`{"replay_of":"%s","source_run":"%s"}`, srcRunID, src.ID)); err != nil {
					return fmt.Errorf("spare step %s of run %s: %w", step.Name, newID, err)
				}
				out.Reused = append(out.Reused, step.Name)
			} else {
				if _, err := tx.Exec(`INSERT INTO steps (run_id, name, idx, state, max_attempts)
VALUES (?, ?, ?, 'pending', ?)`, newID, step.Name, i, maxAttempts); err != nil {
					return fmt.Errorf("create step %s of run %s: %w", step.Name, newID, err)
				}
				out.Rerun = append(out.Rerun, step.Name)
			}
			for _, upstream := range step.Needs {
				if _, err := tx.Exec(`INSERT INTO step_deps (run_id, step_name, depends_on)
VALUES (?, ?, ?)`, newID, step.Name, upstream); err != nil {
					return fmt.Errorf("freeze the edge %s -> %s of run %s: %w",
						upstream, step.Name, newID, err)
				}
			}
		}

		if err := copyArtifactRefsTx(tx, src.ID, newID, reuse, now.UnixMilli()); err != nil {
			return err
		}

		for _, name := range reuse {
			if err := appendRunEvent(tx, RunEvent{
				RunID:      newID,
				StepName:   name,
				At:         now,
				Kind:       "step.skipped",
				ToState:    "succeeded",
				ReasonCode: string(reason.STEPSkippedReplayReused),
				Actor:      actor,
				DetailJSON: fmt.Sprintf(`{"replay_of":"%s"}`, srcRunID),
			}); err != nil {
				return err
			}
		}

		return appendRunEvent(tx, RunEvent{
			RunID:   newID,
			At:      now,
			Kind:    "run.queued",
			ToState: "queued",
			Actor:   actor,
		})
	})
	if err != nil {
		return ReplayResult{}, err
	}
	out.NewRunID = newID
	return out, nil
}

// resolveReuseTx names the steps a replay spares, in spec order where the
// caller needs it. The --from closure walks the FROZEN step_deps of the
// source run upwards with a recursive CTE, UNION so a diamond appears once;
// it is the mirror image of M4-03's skip propagation, which flowed down the
// very same edges. Never the current spec: these rows are the graph the
// source means.
func resolveReuseTx(tx *sql.Tx, srcRunID string, opt ReplayOpts) ([]string, error) {
	switch {
	case opt.From != nil:
		// The named step has to exist in the source, whatever its
		// position in the graph; a root step simply spares nothing.
		if _, err := readStepTx(tx, srcRunID, *opt.From); err != nil {
			return nil, fmt.Errorf("replay run %s --from %s: %w (%s)",
				srcRunID, *opt.From, ErrStepNotInThisRun, *opt.From)
		}
		// The seed is the named step's own dependencies, not the step:
		// --from w spares what w sits on top of, and w earns its own
		// outputs again.
		rows, err := tx.Query(`WITH RECURSIVE upstream(step_name) AS (
			SELECT depends_on FROM step_deps WHERE run_id = ? AND step_name = ?
			UNION
			SELECT d.depends_on FROM step_deps d
				JOIN upstream u ON d.step_name = u.step_name
			WHERE d.run_id = ?
		)
		SELECT step_name FROM upstream ORDER BY step_name`, srcRunID, *opt.From, srcRunID)
		if err != nil {
			return nil, fmt.Errorf("resolve the upstream of %s in run %s: %w", *opt.From, srcRunID, err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, fmt.Errorf("resolve the upstream of %s in run %s: %w", *opt.From, srcRunID, err)
			}
			out = append(out, name)
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("resolve the upstream of %s in run %s: %w", *opt.From, srcRunID, err)
		}
		if err := rows.Close(); err != nil {
			return nil, err
		}
		return out, nil

	case opt.FailedOnly:
		rows, err := tx.Query(`SELECT name FROM steps
WHERE run_id = ? AND state = 'succeeded' ORDER BY idx`, srcRunID)
		if err != nil {
			return nil, fmt.Errorf("list the succeeded steps of run %s: %w", srcRunID, err)
		}
		defer func() { _ = rows.Close() }()
		var out []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return nil, fmt.Errorf("list the succeeded steps of run %s: %w", srcRunID, err)
			}
			out = append(out, name)
		}
		return out, rows.Err()

	default:
		return nil, nil
	}
}

// copyArtifactRefsTx copies the artifact rows of the spared steps as
// references: uri, checksum, size and metadata move, content never does.
// UNIQUE(run_id, name) holds in the source across all of its steps, so the
// copied set can never collide inside the new run either, whatever number of
// artifacts a step produced.
func copyArtifactRefsTx(tx *sql.Tx, srcRunID, newRunID string, reuse []string, at int64) error {
	const all = `SELECT COALESCE(step_name, ''), name, uri, size_bytes, checksum, meta_json
FROM artifacts WHERE run_id = ? ORDER BY created_at, id`

	query := all
	args := []any{srcRunID}
	if reuse != nil {
		// One bound parameter per spared step: the copied set is exactly
		// what those steps produced, never the run's whole artifact table.
		marks := make([]string, len(reuse))
		for i, name := range reuse {
			marks[i] = "?"
			args = append(args, name)
		}
		query = `SELECT COALESCE(step_name, ''), name, uri, size_bytes, checksum, meta_json
FROM artifacts WHERE run_id = ? AND step_name IN (` + strings.Join(marks, ",") + `)
ORDER BY created_at, id`
	}
	rows, err := tx.Query(query, args...)
	if err != nil {
		return fmt.Errorf("read the artifacts of run %s: %w", srcRunID, err)
	}
	type ref struct {
		stepName, name, uri string
		size                sql.NullInt64
		checksum            sql.NullString
		meta                string
	}
	var refs []ref
	for rows.Next() {
		var r ref
		if err := rows.Scan(&r.stepName, &r.name, &r.uri, &r.size, &r.checksum, &r.meta); err != nil {
			_ = rows.Close()
			return fmt.Errorf("read the artifacts of run %s: %w", srcRunID, err)
		}
		refs = append(refs, r)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return fmt.Errorf("read the artifacts of run %s: %w", srcRunID, err)
	}
	if err := rows.Close(); err != nil {
		return err
	}

	now := time.UnixMilli(at).UTC()
	for _, r := range refs {
		refID, err := id.New(now)
		if err != nil {
			return fmt.Errorf("mint an artifact reference id: %w", err)
		}
		if _, err := tx.Exec(`INSERT INTO artifacts
(id, run_id, step_name, name, uri, size_bytes, checksum, meta_json, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			refID, newRunID, nullIfEmpty(r.stepName), r.name, r.uri, r.size,
			r.checksum, r.meta, at); err != nil {
			return fmt.Errorf("copy the artifact %s of run %s: %w", r.name, srcRunID, err)
		}
	}
	return nil
}
