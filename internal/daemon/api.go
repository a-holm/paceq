package daemon

import (
	"context"
	"log/slog"

	"github.com/a-holm/paceq/internal/api"
	"github.com/a-holm/paceq/internal/store"
)

// The unix surface of the daemon (M2-08): everything under internal/api,
// served over cfg.SocketPath. This is the lifecycle seam only: Serve owns
// the directory mode, the stale-socket removal and the clean-stop unlink;
// the handlers live in internal/api and write through the daemon's own
// store, which holds the state lock. /livez answers from memory alone via
// the statuses registry, because a health check that can hang on a locked
// database turns one bad moment into a restart loop (06 section 7.1).
func startHealthEndpoint(cfg Config, st *statuses, log *slog.Logger, db *store.Store) (stop func(context.Context)) {
	if cfg.SocketPath == "" {
		return nil
	}
	stopAPI, err := api.Serve(context.Background(), api.Config{
		Path: cfg.SocketPath,
		Deps: api.Deps{
			Version: cfg.Version,
			Store:   db,
			Loops: func() map[string]map[string]any {
				return loopLines(st.snapshot())
			},
		},
		Log: log,
	})
	if err != nil {
		log.Error("the api socket could not start", "path", cfg.SocketPath, "error", err)
		return nil
	}
	return stopAPI
}

// loopLines is the loop snapshot as JSON sees it: an object keyed by name, so
// a reader never depends on order.
func loopLines(all []LoopStatus) map[string]map[string]any {
	out := make(map[string]map[string]any, len(all))
	for _, l := range all {
		last := ""
		if !l.LastTick.IsZero() {
			last = l.LastTick.UTC().Format("2006-01-02T15:04:05Z")
		}
		out[l.Name] = map[string]any{"ticks": l.Ticks, "last_tick": last}
	}
	return out
}
