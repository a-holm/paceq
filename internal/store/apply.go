package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/a-holm/paceq/internal/id"
)

// JobApplyResult says what apply did with one job spec.
type JobApplyResult struct {
	// JobName is the job the input named.
	JobName string

	// VersionID is the id of the version this spec maps to after the apply:
	// the new row when one was written, the existing row otherwise.
	VersionID string

	// Version is that row's number.
	Version int

	// Created reports whether this apply wrote a new job_versions row. A false
	// here is the idempotent case: the file was already loaded as it stands,
	// and the database came out of the transaction byte for byte the same.
	Created bool

	// Sensors is the sensor sync that ran in the same transaction, so a job's
	// new definition and its sensor rows land and are reported together.
	Sensors SyncResult
}

// applySensorWork is the sensor sync settled on for one job before the write
// transaction opens.
type applySensorWork struct {
	plan     []syncPlanItem
	eventIDs []string
}

// ApplyJobs records a batch of job specs and leaves every job pointing at the
// version its spec hashes to. The whole batch is one transaction on purpose:
// apply either lands complete or not at all, because a half applied catalog is
// the one state an operator cannot reason about. Any failure rolls every job
// in the batch back, including the sensor rows each job's spec declares.
//
// Idempotency lives in the schema, not in a check: job_versions carries
// UNIQUE (job_name, spec_hash), the insert comes first, and a conflict means
// the version was already there. Two applies racing on the same file therefore
// end in one row no matter which commit lands first.
//
// The inputs are finished facts: parsing happened before this was called, and
// nothing inside the transaction reads a file or a clock. Ids are minted
// before the transaction opens, for the same reason UpsertJobVersion mints
// its own there.
func (s *Store) ApplyJobs(ctx context.Context, inputs []JobVersionInput) ([]JobApplyResult, error) {
	now := s.clk.Now().UTC()
	at := now.UnixMilli()

	ids := make([]string, len(inputs))
	type planned struct {
		work applySensorWork
		res  SyncResult
	}
	plans := make([]planned, len(inputs))
	for i := range inputs {
		vid, err := id.New(now)
		if err != nil {
			return nil, fmt.Errorf("mint a job version id for %s: %w", inputs[i].JobName, err)
		}
		ids[i] = vid

		existing, err := currentSensorSpecs(ctx, s.r, inputs[i].JobName)
		if err != nil {
			return nil, err
		}
		plan := buildSensorPlan(inputs[i].JobName, inputs[i].Sensors, existing)
		eventIDs := make([]string, len(plan))
		for j := range plan {
			if eventIDs[j], err = id.New(now); err != nil {
				return nil, fmt.Errorf("mint a sensor event id for %s: %w", inputs[i].JobName, err)
			}
		}
		plans[i] = planned{applySensorWork{plan: plan, eventIDs: eventIDs}, resultOf(plan)}
	}

	results := make([]JobApplyResult, 0, len(inputs))
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// The transaction can run more than once on a snapshot conflict, so
		// the batch starts from zero every time.
		results = results[:0]
		for i, in := range inputs {
			version, created, err := writeJobVersion(tx, ids[i], at, in)
			if err != nil {
				return err
			}
			work := plans[i]
			if len(work.work.plan) > 0 {
				if err := syncSensorsTx(tx, at, in.JobName, work.work.plan, work.work.eventIDs); err != nil {
					return err
				}
			}
			results = append(results, JobApplyResult{
				JobName:   in.JobName,
				VersionID: version.ID,
				Version:   version.Version,
				Created:   created,
				Sensors:   work.res,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// resultOf folds a sensor plan into the SyncResult that reports it.
func resultOf(plan []syncPlanItem) SyncResult {
	var res SyncResult
	var unchanged []string
	for _, item := range plan {
		switch item.kind {
		case lifecycleCreated:
			res.Created = append(res.Created, item.name)
		case lifecycleSpecChanged:
			res.Updated = append(res.Updated, item.name)
		case lifecycleRemoved:
			res.Removed = append(res.Removed, item.name)
		case "":
			unchanged = append(unchanged, item.name)
		}
	}
	res.Unchanged = unchanged
	return res
}
