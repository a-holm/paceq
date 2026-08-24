package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/paceq/internal/id"
)

// Fault injection for the negative proofs. Fsck's checks are only worth what
// their detection is worth, and detection is worth nothing until it has been
// seen catching the exact row each check exists to catch (issue #75, test
// plan item 2: plant every invariant violation and require the right check
// to name it). These methods write those rows.
//
// Nothing in the engine, the CLI or any other caller may use them. They
// exist for the fsck negative tests and the crash harness's red-first
// proofs, and they say so in their name.
//
// Every method returns the subject the matching Violation should carry, so a
// test asserts on the named finding and not on a count. The state changing
// statements themselves live beside the real transitions, in transitions.go,
// because that is where the architecture keeps every run and step write.

// InjectOrphanTick writes a manual tick that claims it triggered, with no
// trigger row behind it. AbandonedChains names it; no invariant check does,
// because the chain sweep is where this broken promise lives.
func (s *Store) InjectOrphanTick(ctx context.Context) (string, error) {
	at := s.clk.Now().UTC().UnixMilli()
	res, err := s.w.ExecContext(ctx, `INSERT INTO ticks
(id, source_kind, source_name, started_at, last_started_at, outcome, trigger_count)
VALUES (?, 'manual', 'inject', ?, ?, 'triggered', 1)`,
		fmt.Sprintf("inj-tick-%d", at), at, at)
	if err != nil {
		return "", fmt.Errorf("inject an orphan tick: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return "", fmt.Errorf("inject an orphan tick: wrote %d rows", n)
	}
	return fmt.Sprintf("tick inj-tick-%d says triggered but has no trigger row", at), nil
}

// InjectTerminalStepPending flips one step of a terminal run back to pending:
// the exact row I2 exists for. The run's aggregate moves with it, so the
// sweep reports I2 and I10 for the same planted row.
func (s *Store) InjectTerminalStepPending(ctx context.Context) (string, error) {
	var runID, step string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT r.id, s.name FROM runs r
JOIN steps s ON s.run_id = r.id
WHERE r.state = 'succeeded' AND s.state = 'succeeded' LIMIT 1`)
		if err := sel.Scan(&runID, &step); err != nil {
			return fmt.Errorf("inject a pending step under a terminal run: %w", err)
		}
		return plantStepPendingTx(tx, runID, step)
	})
	if err != nil {
		return "", err
	}
	return "run " + runID + " step " + step, nil
}

// InjectFailedStepUnderSucceededRun marks one step failed while the run still
// says succeeded, so the stored state and the steps' aggregate disagree: I10.
func (s *Store) InjectFailedStepUnderSucceededRun(ctx context.Context) (string, error) {
	var runID string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT r.id FROM runs r
JOIN steps s ON s.run_id = r.id
WHERE r.state = 'succeeded' AND s.state = 'succeeded' LIMIT 1`)
		if err := sel.Scan(&runID); err != nil {
			return fmt.Errorf("inject a failed step under a succeeded run: %w", err)
		}
		return plantFirstSucceededStepFailedTx(tx, runID)
	})
	if err != nil {
		return "", err
	}
	return "run " + runID, nil
}

// InjectBackwardsTimestamp moves a step's finish before its start: I13.
func (s *Store) InjectBackwardsTimestamp(ctx context.Context) (string, error) {
	var key string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT run_id || '/' || name FROM steps
WHERE started_at IS NOT NULL AND finished_at IS NOT NULL LIMIT 1`)
		if err := sel.Scan(&key); err != nil {
			return fmt.Errorf("inject a backwards timestamp: %w", err)
		}
		return plantStepFinishedBeforeStartedTx(tx)
	})
	if err != nil {
		return "", err
	}
	return "step " + key, nil
}

// InjectUnexplainedDeferral pushes a queued run's availability into the
// future and clears its defer_reason: a run held back that no longer says
// why. The CHECK constraint refuses this shape, so the injection lifts the
// checks for the one statement and puts them straight back: I14.
func (s *Store) InjectUnexplainedDeferral(ctx context.Context) (string, error) {
	var runID string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT id FROM runs WHERE state = 'queued' LIMIT 1`)
		if err := sel.Scan(&runID); err != nil {
			return fmt.Errorf("inject an unexplained deferral: %w", err)
		}
		if _, err := tx.Exec(`PRAGMA ignore_check_constraints = 1`); err != nil {
			return err
		}
		if err := plantUnexplainedDeferralTx(tx, runID); err != nil {
			return err
		}
		_, err := tx.Exec(`PRAGMA ignore_check_constraints = 0`)
		return err
	})
	if err != nil {
		return "", err
	}
	return "run " + runID, nil
}

// InjectBrokenEventChain rewrites the second event's from_state, so the chain
// no longer picks up where the first event ended: I15.
func (s *Store) InjectBrokenEventChain(ctx context.Context) (string, error) {
	var runID string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT run_id FROM (
SELECT run_id, LAG(to_state) OVER w AS prev, from_state
FROM run_events
WINDOW w AS (PARTITION BY run_id, COALESCE(step_name, '') ORDER BY id))
WHERE prev IS NOT NULL LIMIT 1`)
		if err := sel.Scan(&runID); err != nil {
			return fmt.Errorf("inject a broken event chain: %w", err)
		}
		_, err := tx.Exec(`UPDATE run_events SET from_state = 'somewhere-else'
WHERE id = (SELECT MIN(id) FROM (
SELECT id, run_id AS rid, LAG(to_state) OVER w AS prev
FROM run_events
WINDOW w AS (PARTITION BY run_id, COALESCE(step_name, '') ORDER BY id))
WHERE prev IS NOT NULL AND rid = ?)`, runID)
		return err
	})
	if err != nil {
		return "", err
	}
	return "run " + runID, nil
}

// InjectUnexplainedTerminal clears a terminal run's reason code: the
// catalogue rule swept along with the invariants.
func (s *Store) InjectUnexplainedTerminal(ctx context.Context) (string, error) {
	var runID string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		sel := tx.QueryRow(`SELECT id FROM runs
WHERE state IN ('succeeded', 'failed', 'cancelled') LIMIT 1`)
		if err := sel.Scan(&runID); err != nil {
			return fmt.Errorf("inject an unexplained terminal: %w", err)
		}
		return plantUnexplainedTerminalRunTx(tx, runID)
	})
	if err != nil {
		return "", err
	}
	return "run " + runID, nil
}

// InjectDuplicateRunKey plants I3: one job whose run_key names two runs.
// There is no UNIQUE index to dodge on purpose, because this invariant lives
// in application law, not in the schema; that is exactly why the health check
// has to re-read it at startup.
func (s *Store) InjectDuplicateRunKey(ctx context.Context) (string, error) {
	var subject string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var (
			baseID  string
			job     string
			version string
			key     = "planted-duplicate-run-key"
		)
		if err := tx.QueryRow(`SELECT id, job_name, job_version_id FROM runs
ORDER BY created_at LIMIT 1`).Scan(&baseID, &job, &version); err != nil {
			return fmt.Errorf("inject a duplicate run key: %w", err)
		}
		// nolint:fencing: this is fault injection for the fsck negative
		// proof, not engine state: the planted duplicate key must exist
		// unfenced so fsck can name it.
		if _, err := tx.Exec(`UPDATE runs SET run_key = (SELECT ?) WHERE id = ?`, key, baseID); err != nil {
			return fmt.Errorf("inject a duplicate run key: %w", err)
		}
		twin := baseID + "-twin"
		if _, err := tx.Exec(`INSERT INTO runs (id, job_name, job_version_id, origin,
state, available_at, run_key, created_at, updated_at)
VALUES (?, ?, ?, 'manual', 'queued', 0, ?, 1, 1)`,
			twin, job, version, key); err != nil {
			return fmt.Errorf("inject a duplicate run key: %w", err)
		}
		subject = "job " + job + " run_key " + key
		return nil
	})
	if err != nil {
		return "", err
	}
	return subject, nil
}

// InjectDependencyCycle plants I9: two steps of one run pointing at each
// other, so no order of execution can ever be legal. Like the duplicate run
// key, nothing in the schema forbids it; only the health check can see it.
func (s *Store) InjectDependencyCycle(ctx context.Context) (string, error) {
	var subject string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var runID, a, b string
		if err := tx.QueryRow(`SELECT r.id,
MIN(CASE WHEN s.idx <= s2.idx THEN s.name ELSE s2.name END),
MAX(CASE WHEN s.idx <= s2.idx THEN s2.name ELSE s.name END)
FROM runs r
JOIN steps s ON s.run_id = r.id
JOIN steps s2 ON s2.run_id = r.id AND s.name < s2.name
GROUP BY r.id LIMIT 1`).Scan(&runID, &a, &b); err != nil {
			return fmt.Errorf("inject a dependency cycle: %w", err)
		}
		for _, e := range [][2]string{{a, b}, {b, a}} {
			if _, err := tx.Exec(`INSERT OR IGNORE INTO step_deps (run_id, step_name, depends_on)
VALUES (?, ?, ?)`, runID, e[0], e[1]); err != nil {
				return fmt.Errorf("inject a dependency cycle: %w", err)
			}
		}
		subject = "run " + runID
		return nil
	})
	if err != nil {
		return "", err
	}
	return subject, nil
}

// InjectActiveRunOverflow plants I12: two running runs on one job whose
// max_concurrent is 1. The seeded night job has one queued run; the
// injection sets it to running and creates a second running run, so the
// active count exceeds the ceiling.
func (s *Store) InjectActiveRunOverflow(ctx context.Context) (string, error) {
	var subject string
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		var (
			runID   string
			job     string
			version string
		)
		if err := tx.QueryRow(`SELECT id, job_name, job_version_id FROM runs
WHERE job_name = 'nightly' ORDER BY created_at LIMIT 1`).Scan(&runID, &job, &version); err != nil {
			return fmt.Errorf("inject an active overflow: %w", err)
		}
		now := s.clk.Now().UTC().UnixMilli()
		// Set the first run to running with its own concurrency
		// key so the second can take a different one.
		if _, err := tx.Exec(`UPDATE runs SET state = 'running',
concurrency_key = 'nightly-1',
lease_owner = 'inject-1',
lease_epoch = 1,
lease_expires_at = ?,
started_at = ?,
updated_at = ?
WHERE id = ?`, now+60_000, now, now, runID); err != nil {
			return fmt.Errorf("inject a running first run: %w", err)
		}
		twin := runID + "-overflow"
		if _, err := tx.Exec(`INSERT INTO runs (id, job_name, job_version_id, origin,
state, available_at, concurrency_key,
lease_owner, lease_epoch, lease_expires_at,
started_at, created_at, updated_at)
VALUES (?, ?, ?, 'manual', 'running',
?, 'nightly-2',
'inject-2', 1, ?,
?, ?, ?)`,
			twin, job, version,
			0,
			now+60_000,
			now, now, now); err != nil {
			return fmt.Errorf("inject a second running run: %w", err)
		}
		subject = "job " + job
		return nil
	})
	if err != nil {
		return "", err
	}
	return subject, nil
}

// The three helpers below stage and read the rows the replay proofs need.
// Like everything else in this file they are for tests alone, and their names
// say so. Production code writes artifacts only through a future runner
// integration; nothing in the engine or the CLI may call any of these.

// InjectArtifact plants one artifact row on a step of a run, so a test can
// stage what a successful command produced before a replay copies it.
func (s *Store) InjectArtifact(ctx context.Context, runID, stepName, name, uri, checksum string) error {
	at := s.clk.Now().UTC().UnixMilli()
	artID, err := id.New(s.clk.Now().UTC())
	if err != nil {
		return fmt.Errorf("inject an artifact: %w", err)
	}
	res, err := s.w.ExecContext(ctx, `INSERT INTO artifacts
(id, run_id, step_name, name, uri, size_bytes, checksum, created_at)
VALUES (?, ?, ?, ?, ?, 12, ?, ?)`,
		artID, runID, stepName, name, uri, checksum, at)
	if err != nil {
		return fmt.Errorf("inject an artifact: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("inject an artifact: wrote %d rows", n)
	}
	return nil
}

// ArtifactRef is one staged or copied artifact reference, as the replay
// proofs compare them: which step made it, what it is called, where the bytes
// live and how they are fingerprinted. Content never moves; only these facts.
type ArtifactRef struct {
	RunID    string
	StepName string
	Name     string
	URI      string
	Checksum string
}

// ArtifactsOf lists one run's artifact references, oldest first. It reads;
// nothing is written, and no state is touched.
func (s *Store) ArtifactsOf(ctx context.Context, runID string) ([]ArtifactRef, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT run_id, COALESCE(step_name, ''), name, uri, COALESCE(checksum, '')
FROM artifacts WHERE run_id = ? ORDER BY created_at, id`, runID)
	if err != nil {
		return nil, fmt.Errorf("list the artifacts of run %s: %w", runID, err)
	}
	defer func() { _ = rows.Close() }()
	var out []ArtifactRef
	for rows.Next() {
		var a ArtifactRef
		if err := rows.Scan(&a.RunID, &a.StepName, &a.Name, &a.URI, &a.Checksum); err != nil {
			return nil, fmt.Errorf("list the artifacts of run %s: %w", runID, err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// InjectRunKey gives one finished run a dedup key as a sensor trigger would:
// the key on the run row and its registration in the run_keys table. A test
// stages both so a replay can prove it touches neither.
func (s *Store) InjectRunKey(ctx context.Context, runID, sourceID, key string) error {
	now := s.clk.Now().UTC()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		// nolint:fencing: this is staging for the dedup proof, not engine
		// state: the planted key must exist unfenced so the replay can be
		// shown to leave it alone.
		if _, err := tx.Exec(`UPDATE runs SET run_key = ? WHERE id = ?`, key, runID); err != nil {
			return fmt.Errorf("inject a run key on %s: %w", runID, err)
		}
		if _, err := tx.Exec(`INSERT INTO run_keys (source_id, epoch, run_key, first_seen_at, run_id)
VALUES (?, 1, ?, ?, ?)`, sourceID, key, now.UnixMilli(), runID); err != nil {
			return fmt.Errorf("inject a run key on %s: %w", runID, err)
		}
		return nil
	})
}

// RunKeysSnapshot reads the whole run_keys table as comparable lines. Two
// snapshots taken either side of an operation prove byte for byte that the
// table did not move.
func (s *Store) RunKeysSnapshot(ctx context.Context) ([]string, error) {
	rows, err := s.r.QueryContext(ctx, `SELECT source_id || '/' || epoch || '/' || run_key ||
'/' || COALESCE(run_id, '') FROM run_keys ORDER BY source_id, epoch, run_key`)
	if err != nil {
		return nil, fmt.Errorf("snapshot run_keys: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err != nil {
			return nil, fmt.Errorf("snapshot run_keys: %w", err)
		}
		out = append(out, line)
	}
	return out, rows.Err()
}
