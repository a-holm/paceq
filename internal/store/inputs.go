package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// The injection side of #13. $PACEQ_INPUTS is the merge of everything the
// steps UPSTREAM of one step published, where upstream means the transitive
// closure over the frozen step_deps edges, walked upward with the same
// recursive-CTE family M4-03 uses for skip propagation. A step without a
// needs edge to a contributor never sees its output: that invisibility is
// the contract, not an optimisation.
//
// The merge is deterministic. Artifact names are unique per run by schema,
// so the read folds defensively: when two rows would still claim one name,
// the publisher latest in spec order holds it and the loser is named.
// Parameter keys have no such constraint, so collisions are ordinary there,
// resolved by exactly the same rule.

// InputsRef is one reference as a downstream step reads it: whose it was
// and what it claims about itself. Field order here is the wire order of
// the frozen shape.
type InputsRef struct {
	StepName  string `json:"step"`
	URI       string `json:"uri"`
	MediaType string `json:"media_type,omitempty"`
	Checksum  string `json:"checksum,omitempty"`
	SizeBytes *int64 `json:"size_bytes"`
}

// InputCollision records one name two contributors claimed, winner first.
type InputCollision struct {
	Name   string `json:"name"`
	Winner string `json:"winner"`
	Loser  string `json:"loser"`
}

// Inputs is the whole payload for one step's environment.
type Inputs struct {
	Artifacts  map[string]InputsRef
	Params     map[string]any
	Collisions []InputCollision
}

// Marshal renders the payload in the frozen shape:
//
//	{"artifacts":{name:{step,uri,media_type,checksum,size_bytes}},"params":{}}
//
// encoding/json sorts map keys, so the same facts always read back the same
// way, byte for byte.
func (in *Inputs) Marshal() (string, error) {
	if in.Artifacts == nil {
		in.Artifacts = map[string]InputsRef{}
	}
	if in.Params == nil {
		in.Params = map[string]any{}
	}
	b, err := json.Marshal(struct {
		Artifacts map[string]InputsRef `json:"artifacts"`
		Params    map[string]any       `json:"params"`
	}{Artifacts: in.Artifacts, Params: in.Params})
	if err != nil {
		return "", fmt.Errorf("encode the inputs: %w", err)
	}
	return string(b), nil
}

// emittedParams reads the carried-forward params out of one verdict detail.
// A detail without the key, or a shape from another era, carries nothing.
func emittedParams(detail string) map[string]any {
	var parsed struct {
		EmittedParams map[string]any `json:"emitted_params"`
	}
	if err := json.Unmarshal([]byte(detail), &parsed); err != nil {
		return nil
	}
	return parsed.EmittedParams
}

// UpstreamInputs builds the payload for one step of one run. It reads only;
// the frozen graph and the published references are history by the time a
// downstream step starts.
func (s *Store) UpstreamInputs(ctx context.Context, runID, step string) (*Inputs, error) {
	in := &Inputs{Artifacts: map[string]InputsRef{}, Params: map[string]any{}}

	// The closure, with each member's position in spec order. UNION keeps
	// diamonds single; the outer join turns names into rows worth reading.
	type node struct {
		Name string
		Idx  int
	}
	var upstream []node
	if err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		rows, err := r.QueryContext(ctx, `WITH RECURSIVE upstream(step_name) AS (
			SELECT d.depends_on FROM step_deps d
				WHERE d.run_id = ? AND d.step_name = ?
			UNION
			SELECT d.depends_on FROM step_deps d
				JOIN upstream u ON d.step_name = u.step_name
				WHERE d.run_id = ?
		)
		SELECT st.name, st.idx FROM steps st
			JOIN upstream ON upstream.step_name = st.name
			WHERE st.run_id = ?
		ORDER BY st.idx`, runID, step, runID, runID)
		if err != nil {
			return fmt.Errorf("close the upstream of %s in run %s: %w", step, runID, err)
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var n node
			if err := rows.Scan(&n.Name, &n.Idx); err != nil {
				return fmt.Errorf("close the upstream of %s in run %s: %w", step, runID, err)
			}
			upstream = append(upstream, n)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("close the upstream of %s in run %s: %w", step, runID, err)
		}
		return rows.Close()
	}); err != nil {
		return nil, err
	}
	if len(upstream) == 0 {
		return in, nil
	}

	names := make([]string, 0, len(upstream))
	for _, n := range upstream {
		names = append(names, n.Name)
	}
	marks := strings.TrimSuffix(strings.Repeat("?,", len(names)), ",")
	// The reads below all lead with the run and follow with the closure
	// members, in that order.
	args := func() []any {
		out := make([]any, 0, len(names)+1)
		out = append(out, runID)
		for _, n := range names {
			out = append(out, n)
		}
		return out
	}

	if err := s.withRead(ctx, func(ctx context.Context, r reader) error {
		// References, highest index first: the first row seen for a name
		// is the holder, and any later row is a named loss.
		qargs := args()
		rows, err := r.QueryContext(ctx, `SELECT a.step_name, a.name, a.uri, a.size_bytes,
			a.checksum, a.meta_json, a.created_at
			FROM artifacts a
			JOIN steps st ON st.run_id = a.run_id AND st.name = a.step_name
			WHERE a.run_id = ? AND a.step_name IN (`+marks+`)
			ORDER BY st.idx DESC, a.name`, qargs...)
		if err != nil {
			return fmt.Errorf("read the upstream references of %s in run %s: %w", step, runID, err)
		}
		refs, err := scanArtifacts(rows)
		if err != nil {
			return fmt.Errorf("read the upstream references of %s in run %s: %w", step, runID, err)
		}
		for i := range refs {
			a := refs[i]
			if held, clash := in.Artifacts[a.Name]; clash {
				// Names are unique per run by schema, so this arm is a
				// defensive fold, not a live path: the higher publisher
				// keeps the name, the lower one is named.
				in.Collisions = append(in.Collisions, InputCollision{
					Name: a.Name, Winner: held.StepName, Loser: a.StepName,
				})
				continue
			}
			in.Artifacts[a.Name] = InputsRef{
				StepName:  a.StepName,
				URI:       a.URI,
				MediaType: a.MediaType,
				Checksum:  a.Checksum,
				SizeBytes: a.SizeBytes,
			}
		}

		// Params, spec order ascending: the later verdict overwrites the
		// earlier one key by key, and every overwritten key is named.
		qargs = args()
		rows, err = r.QueryContext(ctx, `SELECT st.name, COALESCE(st.reason_data, '')
			FROM steps st
			WHERE st.run_id = ? AND st.name IN (`+marks+`)
			ORDER BY st.idx`, qargs...)
		if err != nil {
			return fmt.Errorf("read the upstream verdicts of %s in run %s: %w", step, runID, err)
		}
		defer func() { _ = rows.Close() }()
		holder := map[string]string{}
		for rows.Next() {
			var (
				name   string
				detail string
			)
			if err := rows.Scan(&name, &detail); err != nil {
				return fmt.Errorf("read the upstream verdicts of %s in run %s: %w", step, runID, err)
			}
			for k, v := range emittedParams(detail) {
				// Any key an earlier contributor held is a claimed name,
				// whatever the values: the later verdict takes it.
				if prev, exists := holder[k]; exists {
					in.Collisions = append(in.Collisions, InputCollision{
						Name: k, Winner: name, Loser: prev,
					})
				}
				holder[k] = name
				in.Params[k] = v
			}
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("read the upstream verdicts of %s in run %s: %w", step, runID, err)
		}
		return rows.Close()
	}); err != nil {
		return nil, err
	}
	return in, nil
}
