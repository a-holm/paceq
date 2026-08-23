package daemon

import (
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
