package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/a-holm/paceq/internal/id"
)

// JobVersionInput is one job spec as the loader read it. It carries no id and
// no version number: which version this spec is, is a fact about the database
// and not about the file.
type JobVersionInput struct {
	// JobName is the stable human identity of the job, and its primary key.
	JobName string

	Description string

	// SourcePath is where the spec was read from, for messages. Empty when the
	// spec came from somewhere that has no path.
	SourcePath string

	// MaxConcurrent is how many runs of this job may be active at once. Zero
	// means one, which is the default a job that says nothing gets.
	MaxConcurrent int

	// SpecHash is the digest of the canonical spec, "sha256:<hex>". It is the
	// whole of idempotent reload: the same digest is the same version.
	SpecHash string

	// SpecJSON is the canonical spec itself, the intermediate representation a
	// run is materialised from.
	SpecJSON string
}

// JobVersion is one immutable snapshot of a job spec.
type JobVersion struct {
	ID         string
	JobName    string
	Version    int
	SpecHash   string
	SpecJSON   string
	SourcePath string
	CreatedAt  time.Time
}

// UpsertJobVersion records a job and the spec it currently holds, and reports
// whether that spec is new. Loading the same file twice is not an error and
// does not invent a version: the second load conflicts on (job_name,
// spec_hash), keeps the version that is already there and points the job at it.
//
// The insert comes first and the conflict decides, rather than a read deciding
// whether to insert. Reading first would be a check with a write after it, and
// the answer can be stale by the time the write lands.
//
// Pausing is left alone. It is an operator decision about a job, not a property
// of the file, and a reload that silently resumed a paused job would be the
// worst kind of surprise.
func (s *Store) UpsertJobVersion(ctx context.Context, in JobVersionInput) (JobVersion, bool, error) {
	// The id is minted before the transaction opens. It costs a read of the
	// system entropy source, and the write model forbids that while the write
	// lock is held.
	now := s.clk.Now().UTC()
	versionID, err := id.New(now)
	if err != nil {
		return JobVersion{}, false, fmt.Errorf("mint a job version id: %w", err)
	}

	var out JobVersion
	created := false
	err = s.withTx(ctx, func(tx *sql.Tx) error {
		var err error
		out, created, err = writeJobVersion(tx, versionID, now.UnixMilli(), in)
		return err
	})
	if err != nil {
		return JobVersion{}, false, err
	}
	return out, created, nil
}

// writeJobVersion records one job and the spec it currently holds, inside a
// transaction the caller owns. ApplyJobs calls it once per job in its single
// batch transaction; UpsertJobVersion wraps it with one of its own.
//
// The insert comes first and RowsAffected decides, rather than a read deciding
// whether to insert. Reading first would be a check with a write after it, and
// the answer can be stale by the time the write lands.
func writeJobVersion(tx *sql.Tx, versionID string, at int64, in JobVersionInput) (JobVersion, bool, error) {
	maxConcurrent := in.MaxConcurrent
	if maxConcurrent == 0 {
		maxConcurrent = 1
	}

	out := JobVersion{JobName: in.JobName, SpecHash: in.SpecHash}
	if _, err := tx.Exec(`INSERT INTO jobs
(name, description, max_concurrent, source_path, created_at, updated_at)
VALUES (?, ?, ?, ?, ?, ?)
ON CONFLICT (name) DO UPDATE SET
	description    = excluded.description,
	max_concurrent = excluded.max_concurrent,
	source_path    = excluded.source_path,
	updated_at     = excluded.updated_at`,
		in.JobName, in.Description, maxConcurrent, nullIfEmpty(in.SourcePath), at, at); err != nil {
		return JobVersion{}, false, fmt.Errorf("record job %s: %w", in.JobName, err)
	}

	// The version number is computed inside the statement, so two loads of
	// two different specs cannot read the same maximum and claim it twice.
	result, err := tx.Exec(`INSERT INTO job_versions
(id, job_name, version, spec_hash, spec_json, source_path, created_at)
VALUES (?, ?, (SELECT coalesce(max(version), 0) + 1 FROM job_versions WHERE job_name = ?), ?, ?, ?, ?)
ON CONFLICT (job_name, spec_hash) DO NOTHING`,
		versionID, in.JobName, in.JobName, in.SpecHash, in.SpecJSON,
		nullIfEmpty(in.SourcePath), at)
	if err != nil {
		return JobVersion{}, false, fmt.Errorf("record a version of job %s: %w", in.JobName, err)
	}
	written, err := result.RowsAffected()
	if err != nil {
		return JobVersion{}, false, fmt.Errorf("record a version of job %s: %w", in.JobName, err)
	}
	created := written > 0

	// Read back whichever row carries this hash now: the one just written,
	// or the one that was already there.
	var source sql.NullString
	var createdAt int64
	if err := tx.QueryRow(`SELECT id, version, spec_json, source_path, created_at
FROM job_versions WHERE job_name = ? AND spec_hash = ?`, in.JobName, in.SpecHash).
		Scan(&out.ID, &out.Version, &out.SpecJSON, &source, &createdAt); err != nil {
		return JobVersion{}, false, fmt.Errorf("read back the version of job %s: %w", in.JobName, err)
	}
	out.SourcePath = source.String
	out.CreatedAt = time.UnixMilli(createdAt).UTC()

	if _, err := tx.Exec("UPDATE jobs SET current_version_id = ?, updated_at = ? WHERE name = ?",
		out.ID, at, in.JobName); err != nil {
		return JobVersion{}, false, fmt.Errorf("point job %s at version %d: %w", in.JobName, out.Version, err)
	}
	return out, created, nil
}

// ListJobVersions reads every version of one job, newest first. A job's
// history is short, so the whole list comes back: a report that says "you are
// on version 4 of 7" needs the same rows a rollback check needs.
func (s *Store) ListJobVersions(ctx context.Context, jobName string) ([]JobVersion, error) {
	var out []JobVersion
	err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `SELECT id, job_name, version, spec_hash, spec_json, source_path, created_at
FROM job_versions WHERE job_name = ? ORDER BY version DESC`, jobName)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()

		for rows.Next() {
			var v JobVersion
			var source sql.NullString
			var createdAt int64
			if err := rows.Scan(&v.ID, &v.JobName, &v.Version, &v.SpecHash, &v.SpecJSON,
				&source, &createdAt); err != nil {
				return err
			}
			v.SourcePath = source.String
			v.CreatedAt = time.UnixMilli(createdAt).UTC()
			out = append(out, v)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, fmt.Errorf("list the versions of job %s: %w", jobName, err)
	}
	return out, nil
}
