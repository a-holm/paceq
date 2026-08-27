package daemon

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"github.com/a-holm/paceq/internal/store"
)

// handleRetryNotification answers POST /v1/notifications/{id}/retry: the
// socket half of the CLI's dual-mode write. The direct path and this one call
// the same store method, so both sides of a retry behave identically, down
// to using the process wall clock for the new available_at.
func handleRetryNotification(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	raw := r.PathValue("id")
	id, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || id <= 0 {
		writeHealth(w, http.StatusBadRequest, map[string]any{
			"code":  "bad_request",
			"error": "the id must be a positive integer, got " + raw,
		})
		return
	}
	next, err := st.RetryOutbox(r.Context(), id)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrNotificationNotFound), errors.Is(err, store.ErrNotificationDelivered):
			status := http.StatusNotFound
			if errors.Is(err, store.ErrNotificationDelivered) {
				status = http.StatusConflict
			}
			writeHealth(w, status, map[string]any{"error": fmt.Sprint(err)})
		default:
			writeHealth(w, http.StatusInternalServerError, map[string]any{"error": fmt.Sprint(err)})
		}
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"id":             id,
		"previous_state": next,
		"available_at":   "now",
	})
}
