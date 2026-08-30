package daemon

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/a-holm/paceq/internal/obs"
	"github.com/a-holm/paceq/internal/store"
)

// The daemon's unix socket (06 section 7.1), served when --socket names one:
// the health endpoints and the POST /v1 routes the CLI's write commands dial.
// /livez answers from memory and never touches the database, because a health
// check that can hang on a locked database turns one bad moment into a
// restart loop.
//
// degraded (#44) is the disk-guard's state: only /readyz degrades with the
// disk. /livez must not - a full disk that restarted the daemon would kill
// the very loops that are cleaning up (06 §7.1, 06 §15 risiko 4).
func startHealthEndpoint(cfg Config, st *statuses, log *slog.Logger, store *store.Store, collector *obs.Collector, degraded func() bool) (stop func(context.Context)) {
	if cfg.SocketPath == "" {
		return nil
	}
	path := cfg.SocketPath
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		log.Error("the socket directory could not be created", "dir", filepath.Dir(path), "error", err)
		return nil
	}
	// A socket left behind by a crash would make Listen fail with "address
	// already in use" for a daemon that does not exist. The state lock is
	// what proves nobody else is serving; the file is just its litter.
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		log.Error("a stale socket file could not be removed", "path", path, "error", err)
		return nil
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		log.Error("the health socket could not listen", "path", path, "error", err)
		return nil
	}
	// The state directory is 0700; the socket follows suit rather than the
	// process umask, so nothing about it depends on the environment.
	if err := os.Chmod(path, 0o600); err != nil {
		log.Warn("the health socket mode could not be set", "path", path, "error", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		writeHealth(w, http.StatusOK, map[string]any{
			"status": "ok",
			"loops":  loopLines(st.snapshot()),
		})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		// The disk is the one condition that degrades readiness without
		// touching the database (#44): under the floor, new runs are
		// refused, so a load balancer must stop sending work before the
		// refusals start answering for it.
		if degraded != nil && degraded() {
			writeHealth(w, http.StatusServiceUnavailable, map[string]any{
				"status": "degraded",
				"detail": "low disk: new runs are refused until space is freed",
			})
			return
		}
		writeHealth(w, http.StatusOK, map[string]any{
			"status":  "ready",
			"version": cfg.Version,
		})
	})
	// /metrics (#40): the Prometheus text exposition, rendered fresh per
	// scrape. It degrades instead of hanging - the collector carries its
	// own deadline - so a wedged database shows up as a short document
	// with pulseq_metrics_db_error 1 rather than as a stuck scraper.
	if collector != nil {
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			body := collector.Scrape(r.Context())
			w.Header().Set("Content-Type", obs.ContentType)
			_, _ = w.Write(body)
		})
	}
	// API routes for the CLI's write commands: registered only when
	// the caller passes a store.
	if store != nil {
		mux.HandleFunc("POST /v1/schedules/{ref}/pause", func(w http.ResponseWriter, r *http.Request) {
			handlePauseSchedule(w, r, store)
		})
		mux.HandleFunc("POST /v1/schedules/{ref}/resume", func(w http.ResponseWriter, r *http.Request) {
			handleResumeSchedule(w, r, store)
		})
		mux.HandleFunc("POST /v1/runs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
			handleCancelRun(w, r, store)
		})
		mux.HandleFunc("POST /v1/runs/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
			handleRetryRun(w, r, store)
		})
		mux.HandleFunc("POST /v1/runs/{id}/replay", func(w http.ResponseWriter, r *http.Request) {
			handleReplayRun(w, r, store)
		})
		mux.HandleFunc("POST /v1/notifications/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
			handleRetryNotification(w, r, store)
		})
		// Sensor write routes: the daemon is the single writer for pause,
		// resume, reset, cursor set and tick, mirroring the schedule routes
		// above (M2-08 dual-mode: the CLI dials here first, flock is the
		// no-daemon fallback).
		mux.HandleFunc("POST /v1/sensors/{name}/pause", func(w http.ResponseWriter, r *http.Request) {
			handlePauseSensor(w, r, store)
		})
		mux.HandleFunc("POST /v1/sensors/{name}/resume", func(w http.ResponseWriter, r *http.Request) {
			handleResumeSensor(w, r, store)
		})
		mux.HandleFunc("POST /v1/sensors/{name}/reset", func(w http.ResponseWriter, r *http.Request) {
			handleResetSensor(w, r, store)
		})
		mux.HandleFunc("POST /v1/sensors/{name}/cursor", func(w http.ResponseWriter, r *http.Request) {
			handleSetSensorCursor(w, r, store)
		})
		mux.HandleFunc("POST /v1/sensors/{name}/tick", func(w http.ResponseWriter, r *http.Request) {
			handleSensorTick(w, r, store)
		})
	}

	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	served := make(chan struct{})
	go func() {
		close(served)
		_ = srv.Serve(l) // returns ErrServerClosed on Stop
	}()
	<-served
	log.Info("health endpoints listening", "socket", path)

	return func(ctx context.Context) {
		shutCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		_ = l.Close()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Warn("the socket file could not be removed", "path", path, "error", err)
		}
	}
}

// loopLines is the loop snapshot as JSON sees it: an object keyed by name, so
// a reader never depends on order.
func loopLines(all []LoopStatus) map[string]map[string]any {
	out := make(map[string]map[string]any, len(all))
	for _, l := range all {
		last := ""
		if !l.LastTick.IsZero() {
			last = l.LastTick.UTC().Format(time.RFC3339)
		}
		out[l.Name] = map[string]any{"ticks": l.Ticks, "last_tick": last}
	}
	return out
}

func writeHealth(w http.ResponseWriter, code int, body map[string]any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		return // the client is gone; there is nobody to tell
	}
}
