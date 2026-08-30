package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

	// Schedules is the schedule sync that ran in the same transaction, for
	// the same reason: a job file that declares a cron schedule leaves the
	// apply with a schedule row the scheduler loop can already find.
	Schedules SyncResult
}

// applySensorWork is the sensor sync settled on for one job before the write
// transaction opens.
type applySensorWork struct {
	plan     []syncPlanItem
	eventIDs []string
}

// applyScheduleWork is the schedule sync settled on for one job before the
// write transaction opens.
type applyScheduleWork struct {
	plan   []schedulePlanItem
	rowIDs []string
}

// ApplyJobs records a batch of job specs and leaves every job pointing at the
// version its spec hashes to. The whole batch is one transaction on purpose:
// apply either lands complete or not at all, because a half applied catalog is
// the one state an operator cannot reason about. Any failure rolls every job
// in the batch back, including the sensor and schedule rows each job's spec
// declares.
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
		sensors     applySensorWork
		sensorRes   SyncResult
		schedules   applyScheduleWork
		scheduleRes SyncResult
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
		sensorPlan := buildSensorPlan(inputs[i].JobName, inputs[i].Sensors, existing)
		eventIDs := make([]string, len(sensorPlan))
		for j := range sensorPlan {
			if eventIDs[j], err = id.New(now); err != nil {
				return nil, fmt.Errorf("mint a sensor event id for %s: %w", inputs[i].JobName, err)
			}
		}

		known, err := currentScheduleDefs(ctx, s.r, inputs[i].JobName)
		if err != nil {
			return nil, err
		}
		schedulePlan := buildSchedulePlan(inputs[i].Schedules, known)
		rowIDs := make([]string, len(schedulePlan))
		for j := range schedulePlan {
			if rowIDs[j], err = id.New(now); err != nil {
				return nil, fmt.Errorf("mint a schedule id for %s: %w", inputs[i].JobName, err)
			}
		}

		plans[i] = planned{
			sensors:     applySensorWork{plan: sensorPlan, eventIDs: eventIDs},
			sensorRes:   resultOf(sensorPlan),
			schedules:   applyScheduleWork{plan: schedulePlan, rowIDs: rowIDs},
			scheduleRes: scheduleResultOf(schedulePlan),
		}
	}

	results := make([]JobApplyResult, 0, len(inputs))
	err := s.withTx(ctx, func(tx *sql.Tx) error {
		// The transaction can run more than once on a snapshot conflict, so
		// the batch starts from zero every time.
		results = results[:0]

		versions := make([]JobVersion, len(inputs))
		created := make([]bool, len(inputs))
		for i, in := range inputs {
			var err error
			if versions[i], created[i], err = writeJobVersion(tx, ids[i], at, in); err != nil {
				return err
			}
		}

		// Sensors go in three passes over the batch rather than one pass per
		// job, because sensor names are unique across every job and the passes
		// are what keeps the answer independent of how the operator split
		// their files. Every removal first, so a name this apply gives up is
		// free; then the ownership check against what is left; then the
		// upserts.
		for i, in := range inputs {
			if err := syncSensorsTx(tx, at, in.JobName, plans[i].sensors.plan, plans[i].sensors.eventIDs, sensorRemovals); err != nil {
				return err
			}
		}
		if err := refuseTakenSensorNames(tx, inputs); err != nil {
			return err
		}
		for i, in := range inputs {
			if err := syncSensorsTx(tx, at, in.JobName, plans[i].sensors.plan, plans[i].sensors.eventIDs, sensorUpserts); err != nil {
				return err
			}
		}

		for i, in := range inputs {
			work := plans[i]
			// The schedule rows come after the job version because they
			// reference jobs (name), so the job has to exist first.
			if len(work.schedules.plan) > 0 {
				if err := syncSchedulesTx(tx, at, in.JobName, work.schedules.plan, work.schedules.rowIDs); err != nil {
					return err
				}
			}
			results = append(results, JobApplyResult{
				JobName:   in.JobName,
				VersionID: versions[i].ID,
				Version:   versions[i].Version,
				Created:   created[i],
				Sensors:   work.sensorRes,
				Schedules: work.scheduleRes,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// refuseTakenSensorNames stops a batch that declares a sensor name a job
// outside it owns. A sensor name is the primary key its row lives under across
// every job, and the cursor, the dedup epoch and the breaker counters live on
// that row, so a name moving job would hand the old owner's evaluation
// position to the new one.
//
// It runs inside the write transaction, after the batch's own removals, which
// is what makes one apply and two applies agree: a name this batch gives up is
// already gone from the table, and a name nobody gave up is still owned. Doing
// it under the write lock also means a concurrent apply cannot slip between
// the check and the upserts.
func refuseTakenSensorNames(tx *sql.Tx, inputs []JobVersionInput) error {
	declaredBy := map[string]string{}
	args := make([]any, 0, len(inputs))
	for _, in := range inputs {
		for _, sensor := range in.Sensors {
			if sensor.Name == "" {
				continue
			}
			if _, seen := declaredBy[sensor.Name]; seen {
				continue
			}
			declaredBy[sensor.Name] = in.JobName
			args = append(args, sensor.Name)
		}
	}
	if len(args) == 0 {
		return nil
	}

	rows, err := tx.Query(`SELECT name, job_name FROM sensors WHERE name IN (`+
		strings.TrimSuffix(strings.Repeat("?, ", len(args)), ", ")+`) ORDER BY name`, args...)
	if err != nil {
		return fmt.Errorf("read who owns the declared sensor names: %w", err)
	}
	defer func() { _ = rows.Close() }()

	// The scan runs to the end and reports the first conflict by name, so a
	// batch with several of them always refuses on the same one.
	var taken *SensorNameTakenError
	for rows.Next() {
		var name, owner string
		if err := rows.Scan(&name, &owner); err != nil {
			return fmt.Errorf("read who owns the declared sensor names: %w", err)
		}
		if taker := declaredBy[name]; owner != taker && taken == nil {
			taken = &SensorNameTakenError{Sensor: name, Owner: owner, Taker: taker}
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read who owns the declared sensor names: %w", err)
	}
	if taken != nil {
		return taken
	}
	return nil
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
