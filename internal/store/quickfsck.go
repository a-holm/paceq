package store

import (
	"context"
	"database/sql"
	"fmt"
)

// The quick health check behind startup reconciliation (#62): the small,
// read-only subset of the fsck invariants whose violation means the database
// is in a state the code cannot reason about safely. Full fsck with repair
// stays in M6-06; this file exists so a boot can refuse to make things worse.
//
//   - I3  One run per (job, run_key). Dedup's whole promise is that a key
//         names exactly one run; two rows under one key mean history itself
//         is ambiguous.
//   - I6  One tick per schedule slot. The UNIQUE index enforces this on every
//         write; the query re-checks what a corrupted or hand-edited file
//         could still break.
//   - I9  The dependency graph is acyclic. A cycle has no topological order,
//         so no scheduler can ever pick a legal first step.
//   - I12 No job runs more than max_concurrent allows (#68), and no
//         concurrency key is held by more than one active run (#17). The admission
//         control and the claim both enforce this on the way in; the startup
//         subset catches what a hand edit, a future writer or a bug let
//         through.
//
// Every check here reads only. A checker that writes would belong to
// whatever it is checking.

// QuickFsck returns every critical violation it finds, empty when the state
// is sound enough to serve.
func (s *Store) QuickFsck(ctx context.Context) ([]Violation, error) {
	var out []Violation

	// I3: a run_key that names two runs of one job.
	rows, err := s.r.QueryContext(ctx, `SELECT job_name, run_key, COUNT(*)
FROM runs WHERE run_key IS NOT NULL
GROUP BY job_name, run_key HAVING COUNT(*) > 1`)
	if err != nil {
		return nil, fmt.Errorf("quick fsck I3: %w", err)
	}
	out, err = collectPairs(rows, out, func(a, b string) Violation {
		return Violation{
			Check:   "I3",
			Subject: "job " + a + " run_key " + b,
			Detail:  "the run key names more than one run",
		}
	})
	if err != nil {
		return nil, err
	}

	// I6: a schedule slot with more than one tick on it.
	rows, err = s.r.QueryContext(ctx, `SELECT source_name, scheduled_for, COUNT(*)
FROM ticks WHERE scheduled_for IS NOT NULL
GROUP BY source_kind, source_name, scheduled_for HAVING COUNT(*) > 1`)
	if err != nil {
		return nil, fmt.Errorf("quick fsck I6: %w", err)
	}
	out, err = collectPairs(rows, out, func(a, b string) Violation {
		return Violation{
			Check:   "I6",
			Subject: "tick slot " + a + "@" + b,
			Detail:  "one evaluation slot holds more than one tick",
		}
	})
	if err != nil {
		return nil, err
	}

	// I9: a cycle in the step dependency graph. The edges are tiny, so the
	// fold happens in Go over one read: colour the nodes during a depth
	// first walk, and a grey node seen again is the cycle.
	type edge struct{ from, to string }
	byRun := map[string][]edge{}
	rows, err = s.r.QueryContext(ctx, `SELECT run_id, step_name, depends_on FROM step_deps`)
	if err != nil {
		return nil, fmt.Errorf("quick fsck I9: %w", err)
	}
	for rows.Next() {
		var run, from, to string
		if err := rows.Scan(&run, &from, &to); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("scan a dependency edge: %w", err)
		}
		byRun[run] = append(byRun[run], edge{from: from, to: to})
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, fmt.Errorf("scan a dependency edge: %w", err)
	}
	if err := rows.Close(); err != nil {
		return nil, fmt.Errorf("close the dependency walk: %w", err)
	}

	const (
		white = 0
		grey  = 1
		black = 2
	)
	for run, edges := range byRun {
		adj := map[string][]string{}
		colour := map[string]int{}
		for _, e := range edges {
			adj[e.from] = append(adj[e.from], e.to)
			colour[e.from] = white
			colour[e.to] = white
		}
		var visit func(string) bool
		visit = func(n string) bool {
			switch colour[n] {
			case grey:
				return true // back edge: the cycle exists
			case black:
				return false
			}
			colour[n] = grey
			for _, m := range adj[n] {
				if visit(m) {
					return true
				}
			}
			colour[n] = black
			return false
		}
		for n := range colour {
			if visit(n) {
				out = append(out, Violation{
					Check:   "I9",
					Subject: "run " + run,
					Detail:  "the step dependency graph has a cycle",
				})
				break
			}
		}
	}

	// I12: no job runs more than its ceiling allows (#68).
	active, err := s.ActiveRunViolations(ctx)
	if err != nil {
		return nil, fmt.Errorf("quick fsck I12: %w", err)
	}
	out = append(out, active...)

	// I12 keys: no concurrency key held twice (#17). The index is the
	// everyday enforcement; the startup subset still looks, because a
	// double held key is exactly the state the code cannot reason about.
	keyed, err := s.ActiveConcurrencyKeyViolations(ctx)
	if err != nil {
		return nil, fmt.Errorf("quick fsck I12 keys: %w", err)
	}
	out = append(out, keyed...)

	return out, nil
}

// collectPairs folds scanned (a, b, count) rows into violations. The counts
// only decide that a group exists; the pair names it.
func collectPairs(rows *sql.Rows, into []Violation, mk func(a, b string) Violation) ([]Violation, error) {
	defer rows.Close()
	for rows.Next() {
		a, b := "", ""
		var n int
		if err := rows.Scan(&a, &b, &n); err != nil {
			return into, fmt.Errorf("scan a violation pair: %w", err)
		}
		into = append(into, mk(a, b))
	}
	if err := rows.Err(); err != nil {
		return into, fmt.Errorf("walk the violation pairs: %w", err)
	}
	return into, rows.Close()
}
