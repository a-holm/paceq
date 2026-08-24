package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/runner"
	"github.com/a-holm/paceq/internal/store"
)

// The publication half of #13 lives here: everything around the step's
// output file that is sequencing, not process supervision and not storage.
// The file itself is created by the runner before exec and parsed after
// exit; this code decides where it lives, what survives into the database,
// and which facts ride beside the verdict.

// prepareStepOutput creates the per-run working directory and returns the
// path the step's $PACEQ_OUTPUT will point at. The directory is owned 0700
// by the service user, one level under the state directory per run, so a
// spec can never steer the file outside it: the path is built here, never
// from job input. An engine without a state directory (older callers,
// narrow tests) hands back an empty path and the contract degrades to no
// output file at all.
func prepareStepOutput(stateDir, runID, step string, attempt int) (string, error) {
	if stateDir == "" {
		return "", nil
	}
	if strings.ContainsRune(step, '/') || strings.ContainsRune(step, filepath.Separator) {
		return "", fmt.Errorf("step name %q cannot carry a separator", step)
	}
	dir := filepath.Join(stateDir, "runs", runID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create the run directory: %w", err)
	}
	return filepath.Join(dir, fmt.Sprintf("%s.%d.output.ndjson", step, attempt)), nil
}

// collectPublications turns what the step wrote into what the database
// stores. It runs strictly outside any transaction, after exit. A name two
// upstream attempts already hold resolves here, deterministically: the step
// latest in spec order keeps the name, and either way of losing is named in
// a warning beside the winner's verdict.
func (e *Engine) collectPublications(ctx context.Context, runID, stepName string, curIdx int, parsed runner.StepOutput) ([]store.Artifact, []map[string]any, error) {
	owners, err := e.Store.RunArtifactOwners(ctx, runID)
	if err != nil {
		return nil, nil, err
	}
	var keep []runner.PublishedRef
	var warnings []map[string]any
	for _, ref := range parsed.Artifacts {
		owner, held := owners[ref.Name]
		switch {
		case !held || owner.StepName == stepName:
			keep = append(keep, ref)
		case owner.Idx < curIdx:
			warnings = append(warnings, collisionFact(ref.Name, stepName, owner.StepName))
			keep = append(keep, ref)
		default:
			warnings = append(warnings, collisionFact(ref.Name, owner.StepName, stepName))
		}
	}
	for _, w := range parsed.Warnings {
		warnings = append(warnings, warningFact(w))
	}

	arts := make([]store.Artifact, 0, len(keep))
	for _, ref := range keep {
		arts = append(arts, store.Artifact{
			StepName:  stepName,
			Name:      ref.Name,
			URI:       ref.URI,
			SizeBytes: ref.SizeBytes,
			Checksum:  ref.Checksum,
			MediaType: ref.MediaType,
		})
	}
	return arts, warnings, nil
}

// warningCodes maps the runner's warning kinds onto paceq's reason
// registry. The runner names facts; this package, which owns verdicts,
// decides what they are called. The map is total over the kinds the
// reader can emit today.
var warningCodes = map[string]reason.Code{
	runner.WarnOutputInvalid:   reason.STEPOutputInvalid,
	runner.WarnOutputTruncated: reason.STEPOutputTruncated,
}

// publicationDetail's helper: one runner warning becomes one fact beside
// the verdict. An unmapped kind keeps its raw name so a future kind can
// never vanish silently on its way up.
func warningFact(w runner.OutputWarning) map[string]any {
	fact := map[string]any{}
	code, mapped := warningCodes[w.Kind]
	if mapped {
		fact["code"] = string(code)
	} else {
		fact["code"] = w.Kind
	}
	for k, v := range w.Detail {
		fact[k] = v
	}
	return fact
}

func collisionFact(name, winner, loser string) map[string]any {
	return map[string]any{
		"code":   string(reason.STEPOutputCollision),
		"name":   name,
		"winner": winner,
		"loser":  loser,
	}
}

// publicationDetail merges the carried-forward params and the warning facts
// into a verdict's detail object. The verdict itself never changes shape:
// these keys sit beside it.
func publicationDetail(base string, params map[string]any, warnings []map[string]any) string {
	extra := map[string]any{}
	if len(params) > 0 {
		extra["emitted_params"] = params
	}
	if len(warnings) > 0 {
		extra["output_warnings"] = warnings
	}
	if len(extra) == 0 {
		return base
	}
	return mergeDetail(base, extra)
}
