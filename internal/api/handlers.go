package api

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/a-holm/paceq/internal/diag"
	"github.com/a-holm/paceq/internal/id"
	"github.com/a-holm/paceq/internal/spec"
	"github.com/a-holm/paceq/internal/store"
)

// maxBodyBytes caps every request body. The endpoints carry names, paths and
// ids, never file content; anything bigger is not a paceq client.
const maxBodyBytes = 1 << 20

// actor is who the history books blame when the request came by socket. It
// is the daemon acting as the single writer on behalf of a remote command.
const actor = "api"

// handleHealthz answers with the facts a client gates on before writing.
func (d Deps) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":  "ready",
		"version": d.Version,
	})
}

// handleLivez answers from memory alone: a health check that could hang on a
// locked database would turn one bad moment into a restart loop.
func (d Deps) handleLivez(w http.ResponseWriter, _ *http.Request) {
	body := map[string]any{"status": "ok"}
	if d.Loops != nil {
		body["loops"] = d.Loops()
	}
	writeJSON(w, http.StatusOK, body)
}

// createRunRequest is the body of POST /v1/runs.
type createRunRequest struct {
	Job    string            `json:"job"`
	Params map[string]string `json:"params"`
}

// handleCreateRun queues one manual run through the same materialisation the
// direct path uses, so the tick, trigger and run rows all exist exactly as
// if the command had run locally. Only the actor differs, and that is the
// proof the write went by socket.
func (d Deps) handleCreateRun(w http.ResponseWriter, r *http.Request) {
	var req createRunRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if req.Job == "" {
		writeError(w, WireError{
			Status:  http.StatusBadRequest,
			Code:    codeInvalidRequest,
			Message: "the request names no job to run",
		})
		return
	}
	params := "{}"
	if len(req.Params) > 0 {
		encoded, err := json.Marshal(req.Params)
		if err != nil {
			writeError(w, d.internal("could not encode the parameters", err))
			return
		}
		params = string(encoded)
	}

	queued, err := d.Store.MaterializeManualTrigger(r.Context(), store.ManualTriggerInput{
		JobName:    req.Job,
		Actor:      actor,
		ParamsJSON: params,
	})
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			writeError(w, WireError{
				Status: http.StatusNotFound,
				Code:   codeJobNotFound,
				Message: fmt.Sprintf(
					"no job is named %q here: paceq apply loads the job files of this project first",
					req.Job),
			})
			return
		}
		writeError(w, d.internal("could not queue the run", err))
		return
	}
	d.writeRun(w, http.StatusCreated, r, queued.Run.ID)
}

// cancelRunRequest is the body of POST /v1/runs/{id}/cancel. The reason is
// optional; the durable request is not.
type cancelRunRequest struct {
	Reason string `json:"reason"`
}

// handleCancelRun records the cancellation request and answers with the run
// as it stands. Whoever holds the lease does the stopping; this endpoint only
// makes the wish durable, exactly like the signal path in the direct mode.
func (d Deps) handleCancelRun(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	var req cancelRunRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if _, err := d.Store.RequestCancel(r.Context(), runID, actor, req.Reason); err != nil {
		if errors.Is(err, store.ErrRunNotFound) || errors.Is(err, id.ErrInvalid) {
			writeError(w, WireError{
				Status:  http.StatusNotFound,
				Code:    codeRunNotFound,
				Message: fmt.Sprintf("no run matches %q", runID),
			})
			return
		}
		writeError(w, d.internal("could not request the cancellation", err))
		return
	}
	d.writeRun(w, http.StatusOK, r, runID)
}

// applyRequest names paths on the daemon's own disk. The files never travel:
// the daemon reads them itself, which keeps "definitions enter as files"
// true on the wire too.
type applyRequest struct {
	Paths []string `json:"paths"`
}

// handleApply loads job files the way the direct apply does: parse each one,
// refuse nothing wholesale, record what parsed in one batch. The response is
// the applied/unchanged/failed report, shaped like the CLI's JSON report.
func (d Deps) handleApply(w http.ResponseWriter, r *http.Request) {
	var req applyRequest
	if !decodeBody(w, r, &req) {
		return
	}
	if len(req.Paths) == 0 {
		writeError(w, WireError{
			Status:  http.StatusBadRequest,
			Code:    codeInvalidRequest,
			Message: "apply names at least one path of job files to load",
		})
		return
	}

	type failedFile struct {
		File    string `json:"file"`
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	var (
		inputs    []store.JobVersionInput
		failed    []failedFile
		metaByJob = map[string]jobFileMeta{}
	)
	for _, path := range req.Paths {
		job, source, diags := spec.LoadFile(path)
		if diags.HasErrors() {
			first := diags[0]
			for _, entry := range diags {
				if entry.Severity == diag.SeverityError {
					first = entry
					break
				}
			}
			failed = append(failed, failedFile{File: path, Code: first.Code, Message: first.Message})
			continue
		}
		canonical := spec.Canonical(job)
		sum := sha256.Sum256(source.Bytes)
		inputs = append(inputs, store.JobVersionInput{
			JobName:       job.Name,
			Description:   job.Description,
			SourcePath:    path,
			MaxConcurrent: job.MaxConcurrent,
			SpecHash:      spec.Hash(canonical),
			SpecJSON:      string(canonical),
		})
		metaByJob[job.Name] = jobFileMeta{
			file:      path,
			specHash:  spec.Hash(canonical),
			sourceSHA: hex.EncodeToString(sum[:]),
		}
	}

	report := map[string]any{
		"applied":   []map[string]any{},
		"unchanged": []map[string]any{},
		"failed":    failed,
	}
	if failed == nil {
		report["failed"] = []map[string]any{}
	}
	if len(inputs) > 0 {
		results, err := d.Store.ApplyJobs(r.Context(), inputs)
		if err != nil {
			writeError(w, d.internal("could not record the job specs", err))
			return
		}
		for _, result := range results {
			meta := metaByJob[result.JobName]
			entry := map[string]any{
				"job":         result.JobName,
				"file":        meta.file,
				"version":     result.Version,
				"spec_hash":   meta.specHash,
				"file_sha256": meta.sourceSHA,
			}
			if result.Created {
				entryKeyAppend(report, "applied", entry)
			} else {
				entryKeyAppend(report, "unchanged", entry)
			}
		}
	}
	writeJSON(w, http.StatusOK, report)
}

// jobFileMeta is what the apply report says about one parsed file.
type jobFileMeta struct {
	file      string
	specHash  string
	sourceSHA string
}

// entryKeyAppend grows one list inside the report map.
func entryKeyAppend(report map[string]any, key string, entry map[string]any) {
	list := report[key].([]map[string]any)
	report[key] = append(list, entry)
}

// decodeBody reads one JSON body under the size cap. An empty body decodes
// as the zero value, because cancel carries an optional reason.
func decodeBody[T any](w http.ResponseWriter, r *http.Request, into *T) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(into)
	if err != nil && !errors.Is(err, io.EOF) {
		writeError(w, WireError{
			Status:  http.StatusBadRequest,
			Code:    codeInvalidRequest,
			Message: fmt.Sprintf("the request body is not JSON a paceq client would send: %v", err),
		})
		return false
	}
	return true
}

// writeRun looks the run up fresh and answers with its record, so the body
// always describes the row that is actually in the database.
func (d Deps) writeRun(w http.ResponseWriter, status int, r *http.Request, runID string) {
	detail, err := d.Store.GetRun(r.Context(), runID)
	if err != nil {
		writeError(w, d.internal("could not read the run back", err))
		return
	}
	writeJSON(w, status, map[string]any{"run": newRunRecord(detail)})
}

// internal builds the 500 refusal. The message names what failed; the
// details ride alongside for the operator reading the daemon's log.
func (d Deps) internal(what string, err error) WireError {
	return WireError{
		Status:  http.StatusInternalServerError,
		Code:    codeInternal,
		Message: fmt.Sprintf("%s: %v", what, err),
	}
}

// runStepRecord and runRecord mirror the JSON documents the direct CLI path
// writes (internal/cli runcmd.go), field for field. A jq written against one
// must read the other; a parity test in internal/cli holds them together.
type runStepRecord struct {
	Name       string `json:"name"`
	State      string `json:"state"`
	ReasonCode string `json:"reason_code,omitempty"`
	ExitCode   *int   `json:"exit_code,omitempty"`
	DurationMS int64  `json:"duration_ms,omitempty"`
	LogPath    string `json:"log_path,omitempty"`
}

type runRecord struct {
	ID         string          `json:"id"`
	Job        string          `json:"job"`
	Origin     string          `json:"origin,omitempty"`
	State      string          `json:"state"`
	ReasonCode string          `json:"reason_code,omitempty"`
	CreatedAt  string          `json:"created_at,omitempty"`
	StartedAt  string          `json:"started_at,omitempty"`
	FinishedAt string          `json:"finished_at,omitempty"`
	DurationMS int64           `json:"duration_ms,omitempty"`
	Steps      []runStepRecord `json:"steps"`
}

// newRunRecord flattens a RunDetail for the wire.
func newRunRecord(detail store.RunDetail) runRecord {
	record := runRecord{
		ID:         detail.ID,
		Job:        detail.JobName,
		Origin:     detail.Origin,
		State:      detail.State,
		ReasonCode: detail.ReasonCode,
		CreatedAt:  stamp(detail.CreatedAt),
		StartedAt:  stamp(detail.StartedAt),
		FinishedAt: stamp(detail.FinishedAt),
		DurationMS: millisBetween(detail.StartedAt, detail.FinishedAt),
		Steps:      make([]runStepRecord, 0, len(detail.Steps)),
	}
	for _, step := range detail.Steps {
		var exit *int
		if step.HasExitCode {
			code := step.ExitCode
			exit = &code
		}
		record.Steps = append(record.Steps, runStepRecord{
			Name:       step.Name,
			State:      step.State,
			ReasonCode: step.ReasonCode,
			ExitCode:   exit,
			DurationMS: step.DurationMS,
			LogPath:    step.LogPath,
		})
	}
	return record
}

// stamp renders a time the way the CLI does: UTC, second precision, missing
// stays missing.
func stamp(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

// millisBetween is a duration as the database stores it.
func millisBetween(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

// writeJSON sends one success document.
func writeJSON(w http.ResponseWriter, status int, document any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(document) // a client gone mid-answer cannot be told
}
