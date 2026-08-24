package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// This file is the publication side of #13. An artifact here is a REFERENCE
// and nothing else: a name, a uri as the step emitted it, and the optional
// facts the step claimed beside it. paceq never touches the content, never
// verifies that the uri exists and never computes a checksum of its own.
// There is deliberately no lineage table behind this one: what consumed a
// reference is a question the asset model would answer, and that gate stays
// closed through 1.0.

// Artifact is one published reference. MediaType rides in the row's
// meta_json, which is the schema's slot for exactly this kind of claim.
type Artifact struct {
	StepName  string
	Name      string
	URI       string
	SizeBytes *int64 // nil means the step did not say
	Checksum  string // empty means the step did not say
	MediaType string

	// CreatedAt is set on write and filled on read; it is the verdict's
	// stamp, so a reference ages with the attempt that published it.
	CreatedAt time.Time
}

// publishThreshold is the schema's own answer to a name collision between
// two steps of one run. UNIQUE(run_id, name) keeps one row per name; the
// upsert may overwrite only when the incoming publisher sits later in the
// spec order than the row's current owner. Equal passes for a step
// republishing within its own verdict window; an earlier step can never
// take a name a later one already holds.
const publishThreshold = `
WHERE (
	SELECT COALESCE((SELECT cur.idx FROM steps cur
		WHERE cur.run_id = artifacts.run_id AND cur.name = artifacts.step_name), -1)
) <= (
	SELECT COALESCE((SELECT new.idx FROM steps new
		WHERE new.run_id = excluded.run_id AND new.name = excluded.step_name), -1)
)`

// insertArtifactsTx writes every reference a succeeded step published, in
// the caller's transaction: the verdict and its publications commit together
// or not at all. There is no orphan shape left for a crash to produce.
func insertArtifactsTx(tx *sql.Tx, runID, step string, at time.Time, arts []Artifact) error {
	for i := range arts {
		a := arts[i]
		meta := "{}"
		if a.MediaType != "" {
			if b, err := json.Marshal(map[string]string{"media_type": a.MediaType}); err == nil {
				meta = string(b)
			}
		}
		refID, err := id.New(at)
		if err != nil {
			return fmt.Errorf("identify artifact %s of step %s in run %s: %w", a.Name, step, runID, err)
		}
		var size any
		if a.SizeBytes != nil {
			size = *a.SizeBytes
		}
		var checksum any
		if a.Checksum != "" {
			checksum = a.Checksum
		}
		if _, err := tx.Exec(`INSERT INTO artifacts
			(id, run_id, step_name, name, uri, size_bytes, checksum, meta_json, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(run_id, name) DO UPDATE SET
				step_name = excluded.step_name,
				uri = excluded.uri,
				size_bytes = excluded.size_bytes,
				checksum = excluded.checksum,
				meta_json = excluded.meta_json,
				created_at = excluded.created_at
			`+publishThreshold,
			refID, runID, step, a.Name, a.URI, size, checksum, meta, at.UnixMilli()); err != nil {
			return fmt.Errorf("publish artifact %s of step %s in run %s: %w", a.Name, step, runID, err)
		}
	}
	return nil
}

// RunsArtifacts lists every reference a run published, in spec order and by
// name within a step, which is the order runs show and explain speak. It is
// a plain read: references are history, not state.
func (s *Store) RunsArtifacts(ctx context.Context, runID string) ([]Artifact, error) {
	var out []Artifact
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT a.step_name, a.name, a.uri, a.size_bytes,
			a.checksum, a.meta_json, a.created_at
			FROM artifacts a
			JOIN steps st ON st.run_id = a.run_id AND st.name = a.step_name
			WHERE a.run_id = ?
			ORDER BY st.idx, a.name`, runID)
		if err != nil {
			return fmt.Errorf("list the artifacts of run %s: %w", runID, err)
		}
		out, err = scanArtifacts(rows)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func scanArtifacts(rows *sql.Rows) ([]Artifact, error) {
	defer func() { _ = rows.Close() }()
	var out []Artifact
	for rows.Next() {
		var (
			a         Artifact
			size      sql.NullInt64
			checksum  sql.NullString
			meta      string
			createdAt int64
		)
		if err := rows.Scan(&a.StepName, &a.Name, &a.URI, &size, &checksum, &meta, &createdAt); err != nil {
			return nil, fmt.Errorf("scan an artifact: %w", err)
		}
		if size.Valid {
			v := size.Int64
			a.SizeBytes = &v
		}
		a.Checksum = checksum.String
		var claimed struct {
			MediaType string `json:"media_type"`
		}
		if err := json.Unmarshal([]byte(meta), &claimed); err == nil && claimed.MediaType != "" {
			a.MediaType = claimed.MediaType
		}
		a.CreatedAt = timeOrZero(sql.NullInt64{Int64: createdAt, Valid: true})
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan an artifact: %w", err)
	}
	return out, rows.Close()
}

// attachArtifacts fills each step of a run detail with what it published.
// A step that published nothing reads as no artifacts at all, which is what
// most steps are.
func attachArtifacts(ctx context.Context, r reader, runID string, steps []Step) error {
	rows, err := r.QueryContext(ctx, `SELECT step_name, name, uri, size_bytes, checksum,
		meta_json, created_at FROM artifacts WHERE run_id = ? ORDER BY name`, runID)
	if err != nil {
		return fmt.Errorf("read the artifacts of run %s: %w", runID, err)
	}
	arts, err := scanArtifacts(rows)
	if err != nil {
		return err
	}
	byName := make(map[string]int, len(steps))
	for i := range steps {
		byName[steps[i].Name] = i
		steps[i].Artifacts = nil
	}
	for _, a := range arts {
		if i, ok := byName[a.StepName]; ok {
			steps[i].Artifacts = append(steps[i].Artifacts, a)
		}
	}
	return nil
}
