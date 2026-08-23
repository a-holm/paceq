package api

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/a-holm/paceq/internal/store"
)

// SocketMode is the file mode the socket carries. 0660 is a deliberate
// reconciliation of the issue text with the earlier health stub: the group
// bits are for the systemd RuntimeDirectory group story that arrives with
// M2-11, and until then the enclosing directory stays 0700, so the group is
// empty in practice. The mode is chmod-ed after Listen precisely so no
// umask can decide it.
const SocketMode os.FileMode = 0o660

// DirMode is the mode of the directory the socket lives in.
const DirMode os.FileMode = 0o700

// Deps are what the handlers work on. Store may be nil only when nothing
// but the health endpoints will be served; the daemon always hands one.
type Deps struct {
	// Version answers /v1/healthz and drives the version gate.
	Version string

	// Store is the daemon's own store, opened read-write under the state
	// lock. The daemon is the single writer; every handler writes through
	// it and never around it.
	Store *store.Store

	// Loops fills the loop lines of /livez. Nil keeps the body to status.
	Loops func() map[string]map[string]any
}

// Config is everything Serve needs.
type Config struct {
	// Path is the unix socket to listen on.
	Path string

	// Deps are the handlers' collaborators.
	Deps Deps

	// Log receives the lifecycle lines. Nil means slog's default.
	Log *slog.Logger
}

// Serve listens on the unix socket at cfg.Path and serves the routes until
// the returned stop is called or the context dies. It creates the socket's
// directory 0700, removes a socket file left by a crashed daemon (the state
// lock, not this file, proves ownership: callers serve only after holding
// the lock), sets the socket mode 0660 whatever the umask says, and removes
// the file again on a clean stop.
func Serve(ctx context.Context, cfg Config) (func(context.Context), error) {
	if cfg.Path == "" {
		return nil, errors.New("api: no socket path was named")
	}
	log := cfg.Log
	if log == nil {
		log = slog.Default()
	}
	path := cfg.Path

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, DirMode); err != nil {
		return nil, fmt.Errorf("create the socket directory %s: %w", dir, err)
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("remove the stale socket file %s: %w", path, err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("listen on %s: %w", path, err)
	}
	if err := os.Chmod(path, SocketMode); err != nil {
		log.Warn("the socket mode could not be set", "path", path, "error", err)
	}

	server := &http.Server{
		Handler:           gated(cfg.Deps),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	served := make(chan struct{})
	go func() {
		close(served)
		_ = server.Serve(listener) // returns ErrServerClosed on stop
	}()
	<-served
	log.Info("the api is listening", "socket", path)

	stop := func(stopCtx context.Context) {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(stopCtx), 2*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
		_ = listener.Close()
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			log.Warn("the socket file could not be removed", "path", path, "error", err)
		}
	}
	return stop, nil
}

// gated wraps the mux in the version gate for everything under /v1/.
func gated(deps Deps) http.Handler {
	mux := newMux(deps)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/v1/") && !compatibleRequest(r.Header.Get(clientHeader), deps.Version) {
			majorClient, majorServer := majorOf(headerVersion(r.Header.Get(clientHeader))), majorOf(deps.Version)
			writeError(w, WireError{
				Status: http.StatusConflict,
				Code:   codeVersionMismatch,
				Message: fmt.Sprintf(
					"this daemon speaks protocol major %s and the client sends major %s: restart the daemon "+
						"with a matching binary, or run the command again with --socket none to talk to the "+
						"state directory directly",
					majorOr(majorServer), majorOr(majorClient)),
			})
			return
		}
		mux.ServeHTTP(w, r)
	})
}

// newMux builds the ServeMux from the route table, so the table and the
// surface cannot drift apart.
func newMux(deps Deps) *http.ServeMux {
	handlers := map[string]func(http.ResponseWriter, *http.Request){
		"create-run": deps.handleCreateRun,
		"cancel-run": deps.handleCancelRun,
		"apply":      deps.handleApply,
		"healthz":    deps.handleHealthz,
		"livez":      deps.handleLivez,
	}
	mux := http.NewServeMux()
	for _, route := range Routes() {
		if !route.Registered {
			continue
		}
		handler, ok := handlers[route.Name]
		if !ok {
			panic(fmt.Sprintf("api: route %q has no handler", route.Name))
		}
		mux.HandleFunc(route.Method+" "+route.Pattern, handler)
	}
	return mux
}

// clientHeader is the version header every paceq client sends.
const clientHeader = "X-Pulseq-Client"

// headerVersion strips the product prefix from the header value, so both
// "paceq/1.4.0" and a bare "1.4.0" parse.
func headerVersion(header string) string {
	_, rest, found := strings.Cut(header, "/")
	if !found {
		return strings.TrimSpace(header)
	}
	return strings.TrimSpace(rest)
}

// compatibleRequest decides whether a request may pass the gate. A side that
// does not parse (a dev build, curl without the header) is compatible: the
// gate exists to catch real drift between releases, not to lock out
// debugging. Equal majors are compatible by definition.
func compatibleRequest(header, server string) bool {
	client := majorOf(headerVersion(header))
	serverMajor := majorOf(server)
	if !client.known || !serverMajor.known {
		return true
	}
	return client.number == serverMajor.number
}

// major is one parsed semver major digit.
type major struct {
	number int
	known  bool
}

// majorOf reads the major digit out of a version string such as v1.4.2 or
// 1.4. Anything unparsable is simply unknown.
func majorOf(version string) major {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	head, _, _ := strings.Cut(version, ".")
	n, err := strconv.Atoi(head)
	if err != nil || n < 0 {
		return major{}
	}
	return major{number: n, known: true}
}

// majorOr renders an unknown major as the word dev, so the 409 message still
// reads like a sentence.
func majorOr(m major) string {
	if !m.known {
		return "dev"
	}
	return strconv.Itoa(m.number)
}
