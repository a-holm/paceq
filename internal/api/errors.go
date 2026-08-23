package api

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// Stable short labels the error envelope carries. They are wire vocabulary,
// deliberately not reason catalogue codes: reason codes explain run outcomes
// in the database, these name why one HTTP request was refused.
const (
	codeInvalidRequest  = "invalid_request"
	codeJobNotFound     = "job_not_found"
	codeRunNotFound     = "run_not_found"
	codeInternal        = "internal_error"
	codeVersionMismatch = "version_mismatch"
)

// WireError is one refusal as it travels: an HTTP status, a stable short
// label, and a message that says what to do about it.
type WireError struct {
	Status  int
	Code    string
	Message string
}

func (e *WireError) Error() string {
	return fmt.Sprintf("%s (http %d): %s", e.Code, e.Status, e.Message)
}

// errorEnvelope is the JSON shape every refusal travels in:
//
//	{"error": {"code": "<stable label>", "message": "..."}}
type errorEnvelope struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// writeError sends one refusal. The body is written even though the status
// alone would carry the exit code, because a script debugging over curl with
// --unix-socket deserves the sentence too.
func writeError(w http.ResponseWriter, we WireError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(we.Status)
	var envelope errorEnvelope
	envelope.Error.Code = we.Code
	envelope.Error.Message = we.Message
	_ = json.NewEncoder(w).Encode(envelope) // a client gone mid-error cannot be told
}
