package daemon

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/a-holm/paceq/internal/store"
)

// The sensor write routes (M3-06, issue #11). The daemon is the single writer
// for a live state directory: the CLI dials these instead of flocking, and
// falls back to flock + direct when the daemon is down. The dispatch (who is
// the evaluator, how the runtime owns run_keys) belongs to the M3 sensor
// issues; these handlers are the socket seam the CLI writes through.

func handlePauseSensor(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("name")
	var body struct {
		Reason string `json:"reason"`
	}
	if !decodeSensorBody(w, r, &body, "a pause") {
		return
	}
	if err := st.PauseSensor(r.Context(), name, body.Reason); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{"status": "paused", "sensor": name})
}

func handleResumeSensor(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("name")
	if err := st.ResumeSensor(r.Context(), name); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{"status": "resumed", "sensor": name})
}

func handleResetSensor(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("name")
	var body struct {
		Cursor        *string `json:"cursor"`
		ForgetRunKeys bool    `json:"forget_run_keys"`
	}
	if !decodeSensorBody(w, r, &body, "a reset") {
		return
	}
	out, err := st.ResetSensor(r.Context(), store.ResetSensorInput{
		Name: name, SetCursor: body.Cursor, ForgetRunKeys: body.ForgetRunKeys,
	})
	if err != nil {
		writeStoreErr(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{
		"status": "reset", "sensor": name,
		"old_epoch": out.OldEpoch, "new_epoch": out.NewEpoch,
	})
}

func handleSetSensorCursor(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("name")
	// A pointer, because a plain string cannot tell a request that named no
	// cursor from one that named the empty string, and both used to be
	// written verbatim over whatever the sensor had reached (#207). Refusing
	// is what makes an old client safe against a new daemon: its request
	// fails, and it falls back to the path that still carries the value
	// rather than quietly wiping the cursor.
	var body struct {
		Cursor *string `json:"cursor"`
	}
	if !decodeSensorBody(w, r, &body, "a cursor move") {
		return
	}
	if body.Cursor == nil || *body.Cursor == "" {
		writeHealth(w, http.StatusBadRequest, map[string]any{
			"code":  "bad_request",
			"error": "missing cursor: a cursor move has to name where the sensor goes",
		})
		return
	}
	if err := st.SetSensorCursor(r.Context(), store.CursorInput{Name: name, Cursor: *body.Cursor}); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{"status": "cursor_set", "sensor": name})
}

// handleSensorTick makes a sensor due (next_eval_at = now) so the daemon's
// evaluator runtime picks it up on its next wake. The runtime wake is the M3
// emitter's seam; this handler only moves the row that makes it due, and the
// CLI's --wait path waits on the runtime's result. A CLI-level direct
// evaluation is the no-daemon fallback in the CLI itself.
func handleSensorTick(w http.ResponseWriter, r *http.Request, st *store.Store) {
	if r.Method != http.MethodPost {
		writeHealth(w, http.StatusMethodNotAllowed, map[string]any{"error": "use POST"})
		return
	}
	name := r.PathValue("name")
	// SEAM (#14, #16): forcing due is the M3 emitter's due-wake; the runtime
	// loop (sensorLoop) reacts on its own cadence.
	if err := st.SetSensorDue(r.Context(), name); err != nil {
		writeStoreErr(w, err)
		return
	}
	writeHealth(w, http.StatusOK, map[string]any{"status": "tick_requested", "sensor": name})
}

// decodeSensorBody reads one sensor request body, answering false once it has
// written the refusal. No body at all is not an error: every sensor route has
// a meaning without arguments, and the routes that need one check for it
// themselves. A body that is not JSON is refused rather than swallowed, so a
// client whose request never arrived intact hears about it instead of watching
// the daemon do the default thing.
func decodeSensorBody(w http.ResponseWriter, r *http.Request, into any, what string) bool {
	if r.Body == nil {
		return true
	}
	if err := json.NewDecoder(r.Body).Decode(into); err != nil && !errors.Is(err, io.EOF) {
		writeHealth(w, http.StatusBadRequest, map[string]any{
			"code":  "bad_request",
			"error": "the body is not JSON " + what + " can read: " + err.Error(),
		})
		return false
	}
	return true
}

// writeStoreErr maps a store error onto an HTTP response. Every store write
// error is reported as-is; the CLI re-encodes the exit code it cares about
// (not-found -> 3) from the message it already had.
func writeStoreErr(w http.ResponseWriter, err error) {
	writeHealth(w, http.StatusInternalServerError, map[string]any{"error": err.Error()})
}
