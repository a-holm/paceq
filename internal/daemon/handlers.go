package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// handlePauseSchedule handles POST /v1/schedules/{ref...}/pause.
func handlePauseSchedule(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("ref")
	parts := strings.SplitN(name, "/", 2)
	if len(parts) != 2 {
		writeHealth(w, http.StatusBadRequest, map[string]any{"error": "expected job/schedule.name"})
		return
	}
	row, err := st.PauseSchedule(r.Context(), parts[0], parts[1])
	if err != nil {
		writeHealth(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"status":   "paused",
		"job":      row.JobName,
		"schedule": row.Name,
	})
}

// handleResumeSchedule handles POST /v1/schedules/{ref...}/resume.
func handleResumeSchedule(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("ref")
	parts := strings.SplitN(name, "/", 2)
	if len(parts) != 2 {
		writeHealth(w, http.StatusBadRequest, map[string]any{"error": "expected job/schedule.name"})
		return
	}
	// Pass a zero nextTickAt: the daemon's scheduler loop will recompute the
	// proper cursor on its next wake via the notify bus.
	var zero time.Time
	row, err := st.ResumeSchedule(r.Context(), parts[0], parts[1], zero)
	if err != nil {
		writeHealth(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"status":   "resumed",
		"job":      row.JobName,
		"schedule": row.Name,
	})
}

// handleCancelRun handles POST /v1/runs/{id}/cancel.
func handleCancelRun(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	runID := r.PathValue("id")
	if runID == "" {
		writeHealth(w, http.StatusBadRequest, map[string]any{"error": "missing run id"})
		return
	}
	cr, err := st.RequestCancel(r.Context(), runID, "api", "manual")
	if err != nil {
		writeHealth(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"status":              "cancelled",
		"run_id":              runID,
		"cancel_requested_at": cr.CancelRequestedAt.UTC().Format(time.RFC3339),
	})
}

// handleRetryRun handles POST /v1/runs/{id}/retry. It is the daemon's half
// of the operator reopen (M4-04): the actor is recorded as "api", because
// the person's client spoke HTTP and the history says so honestly. The body
// may carry a step restriction and the force flag; both mean what they mean
// on the direct path.
//
// Refusals come back classified ("not_found", "invalid_state", "busy") so
// the CLI can render them with the exit codes it would have chosen itself.
func handleRetryRun(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	runID := r.PathValue("id")
	if runID == "" {
		writeHealth(w, http.StatusBadRequest, map[string]any{"error": "missing run id"})
		return
	}
	var body struct {
		Step  string `json:"step"`
		Force bool   `json:"force"`
	}
	if r.Body != nil {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
			writeHealth(w, http.StatusBadRequest, map[string]any{
				"code":  "bad_request",
				"error": "the body is not JSON a retry can read: " + err.Error(),
			})
			return
		}
	}
	opts := store.ReopenOpts{Forced: body.Force}
	if body.Step != "" {
		opts.OnlyStep = &body.Step
	}

	res, err := st.ReopenTerminalRunByOperator(r.Context(), runID, "api", opts)
	if err != nil {
		writeReopenRefusal(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"run_id":    runID,
		"new_epoch": res.NewEpoch,
		"reopened":  res.Reopened,
	})
}

// writeReopenRefusal renders one refused reopen with its class. The classes
// are the CLI's vocabulary, not SQLite's: a script branching on them never
// has to parse prose.
func writeReopenRefusal(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		writeHealth(w, http.StatusNotFound, map[string]any{"code": "not_found", "error": err.Error()})
	case errors.Is(err, store.ErrRunNotRetryable),
		errors.Is(err, store.ErrNothingToReopen),
		errors.Is(err, store.ErrRunNotTerminal),
		errors.Is(err, store.ErrStepNotInThisRun):
		writeHealth(w, http.StatusConflict, map[string]any{"code": "invalid_state", "error": err.Error()})
	case errors.Is(err, store.ErrLeaseLost):
		writeHealth(w, http.StatusConflict, map[string]any{"code": "busy", "error": err.Error()})
	default:
		writeHealth(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
	}
}
