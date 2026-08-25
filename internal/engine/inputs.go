package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/a-holm/paceq/internal/reason"
	"github.com/a-holm/paceq/internal/store"
)

// The injection half of #13 lives here: building one step's $PACEQ_INPUTS
// out of the frozen graph's upstream transitive closure, and handing it to
// the runner in the shape the environment contract froze. Inline while the
// merged document is small; spilled to a file beside the attempt's other
// files once it crosses the bound, with $PACEQ_INPUTS set to null so a jq
// pipeline keeps reading the same way.

// MaxInlineInputsBytes is the largest merged document that travels inside
// the environment. Past it the payload moves to a file: an environment
// entry is a terrible place for hundreds of kilobytes, and every kernel
// draws its own line at exactly where the trouble starts.
const MaxInlineInputsBytes = 128 << 10

// prepareStepInputs builds the merged upstream document for one step of one
// run and returns the two values the runner spec carries: the inline JSON
// and, when the document spilled, the file that holds it. A root step reads
// the empty document; a side step contributes nothing, because the closure
// is the contract.
//
// Like the output file's preparation, a failure here refuses the step
// before any process exists: there is nothing to give a verdict to.
func (e *Engine) prepareStepInputs(ctx context.Context, stateDir, runID, step string, attempt int) (string, string, error) {
	in, err := e.Store.UpstreamInputs(ctx, runID, step)
	if err != nil {
		return "", "", fmt.Errorf("build the inputs of %s in run %s: %w", step, runID, err)
	}
	if err := e.logInputCollisions(ctx, runID, step, in.Collisions); err != nil {
		return "", "", err
	}
	doc, err := in.Marshal()
	if err != nil {
		return "", "", fmt.Errorf("encode the inputs of %s in run %s: %w", step, runID, err)
	}
	if len(doc) <= MaxInlineInputsBytes {
		return doc, "", nil
	}
	path, err := spillInputs(stateDir, runID, step, attempt, doc)
	if err != nil {
		if stateDir == "" {
			// The same degradation the output contract has: an engine
			// without a state directory cannot spill, so the oversized
			// document travels inline rather than not at all.
			return doc, "", nil
		}
		return "", "", err
	}
	return "null", path, nil
}

// logInputCollisions records the losing claims of the merge as a warning on
// the step (#13). The merged document itself stays silent: the frozen wire
// shape carries values, not history, so the event log is where the history
// lives. A warning that cannot be recorded is a store that cannot be
// written to, and the step refuses rather than run unwitnessed.
func (e *Engine) logInputCollisions(ctx context.Context, runID, step string, collisions []store.InputCollision) error {
	if len(collisions) == 0 {
		return nil
	}
	detail, err := json.Marshal(struct {
		Collisions []store.InputCollision `json:"collisions"`
	}{Collisions: collisions})
	if err != nil {
		return fmt.Errorf("encode the input collisions of %s in run %s: %w", step, runID, err)
	}
	if err := e.Store.AppendRunEvent(ctx, store.RunEvent{
		RunID:      runID,
		StepName:   step,
		Kind:       "step.inputs_collision",
		ReasonCode: string(reason.STEPInputCollision),
		DetailJSON: string(detail),
	}); err != nil {
		return fmt.Errorf("record the input collisions of %s in run %s: %w", step, runID, err)
	}
	return nil
}

// spillInputs writes the merged document beside the attempt's other files,
// created 0600 like everything else under the run directory. The path is
// built here, never from job input.
func spillInputs(stateDir, runID, step string, attempt int, doc string) (string, error) {
	if strings.ContainsRune(step, '/') || strings.ContainsRune(step, filepath.Separator) {
		return "", fmt.Errorf("step name %q cannot carry a separator", step)
	}
	dir := filepath.Join(stateDir, "runs", runID)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create the run directory: %w", err)
	}
	path := filepath.Join(dir, fmt.Sprintf("%s.%d.inputs.json", step, attempt))
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		return "", fmt.Errorf("spill the inputs of %s in run %s: %w", step, runID, err)
	}
	return path, nil
}
