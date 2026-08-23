// Package api is the wire half of M2-08: HTTP/1.1 carrying JSON over the
// daemon's unix domain socket. A paceq client dials the socket to queue runs,
// cancel them and apply job files that already sit on this machine's disk;
// everything it needs to render answers comes back as documents shaped like
// the ones the direct path prints.
//
// Two rules hold across the whole package. Definitions enter paceq as files
// or they do not enter: no route accepts specification content, so a socket
// client can never inject a job definition the daemon has not read from disk.
// And there is no TCP anywhere: the listener is AF_UNIX only, guarded by an
// arch test, because authorization in the MVP is file permissions alone.
package api

// Route is one row of the API surface. The table is written once and used
// twice: the mux is built from it, and the route-table test holds the built
// surface against it. An endpoint that is not in this table does not exist,
// and an endpoint marked absent-by-design stays visible as a decision rather
// than as an oversight.
type Route struct {
	// Name keys the route for the mux builder.
	Name string

	// Method and Pattern are Go 1.22 ServeMux patterns.
	Method  string
	Pattern string

	// Purpose is one line of what the route does, for readers of the table.
	Purpose string

	// WritesSpecs would mean the route accepts job specification content
	// over the wire. Every row is false and the test fails the day one is
	// not: files are the only way a definition enters paceq.
	WritesSpecs bool

	// Registered marks routes served today. False rows are decisions to
	// wait, named in BlockedBy.
	Registered bool

	// BlockedBy names the milestone that will register the route.
	BlockedBy string
}

// Routes is the whole surface, registered and deliberately absent alike.
func Routes() []Route {
	return []Route{
		{
			Name:       "create-run",
			Method:     "POST",
			Pattern:    "/v1/runs",
			Purpose:    "queue one manual run of a job the daemon has applied",
			Registered: true,
		},
		{
			Name:       "cancel-run",
			Method:     "POST",
			Pattern:    "/v1/runs/{id}/cancel",
			Purpose:    "record a durable cancellation request for one run",
			Registered: true,
		},
		{
			Name:       "apply",
			Method:     "POST",
			Pattern:    "/v1/apply",
			Purpose:    "load job files from paths on the daemon's own disk; the files travel never, only their paths",
			Registered: true,
		},
		{
			Name:       "healthz",
			Method:     "GET",
			Pattern:    "/v1/healthz",
			Purpose:    "readiness plus the version, the facts a client gates on",
			Registered: true,
		},
		{
			Name:       "livez",
			Method:     "GET",
			Pattern:    "/livez",
			Purpose:    "liveness from memory alone, never touching the database",
			Registered: true,
		},
		{
			Name:       "pause-schedule",
			Method:     "POST",
			Pattern:    "/v1/schedules/{name}/pause",
			Purpose:    "pause a schedule",
			Registered: false,
			BlockedBy:  "M2-05/M2-10 bring the pause semantics and the store setter; schema columns already exist",
		},
		{
			Name:       "resume-schedule",
			Method:     "POST",
			Pattern:    "/v1/schedules/{name}/resume",
			Purpose:    "resume a paused schedule",
			Registered: false,
			BlockedBy:  "M2-05/M2-10 bring the resume semantics and the store setter; schema columns already exist",
		},
	}
}
