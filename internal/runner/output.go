package runner

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"strings"

	"github.com/a-holm/paceq/internal/reason"
)

// The output contract (#13). A step publishes artifact references and
// carried-forward parameters by writing NDJSON to the file $PACEQ_OUTPUT
// points at. paceq reads that file once, after the process has exited, and
// turns what it can read into rows. Three hard bounds cap the read; crossing
// any of them stops the read and warns without failing the step, because the
// step's own exit code stays the verdict (issue design choice 5).
const (
	// MaxOutputFileBytes caps how much of $PACEQ_OUTPUT is ever read.
	MaxOutputFileBytes int64 = 1 << 20 // 1 MiB

	// MaxOutputLines caps how many lines are read.
	MaxOutputLines = 1000

	// MaxOutputLineBytes caps one line's length, newline excluded.
	MaxOutputLineBytes = 64 << 10 // 64 KiB
)

// OutputLimits are the bounds of one read. Production reads use
// DefaultOutputLimits; tests pass exact values to pin boundary behaviour.
type OutputLimits struct {
	MaxFileBytes int64
	MaxLines     int
	MaxLineBytes int
}

// DefaultOutputLimits are the frozen contract's bounds.
var DefaultOutputLimits = OutputLimits{
	MaxFileBytes: MaxOutputFileBytes,
	MaxLines:     MaxOutputLines,
	MaxLineBytes: MaxOutputLineBytes,
}

// PublishedRef is one reference a step published. It says where something
// lives, never what it contains: paceq stores the uri as emitted, verifies
// nothing about its existence and computes no checksum of its own.
type PublishedRef struct {
	Name      string
	URI       string
	SizeBytes *int64 // nil means the step did not say
	Checksum  string // empty means the step did not say
	MediaType string
}

// OutputWarning is one fact worth reading beside a step's verdict.
type OutputWarning struct {
	Code   reason.Code
	Detail map[string]any
}

// StepOutput is everything one read of $PACEQ_OUTPUT produced.
type StepOutput struct {
	Artifacts []PublishedRef
	Params    map[string]any
	Warnings  []OutputWarning
}

// ReadStepOutput reads and parses the output file of one finished attempt.
//
// A file that was never written is an empty result, not a warning: a spawn
// failure or a crash before the first write leaves nothing to read, and
// there is nothing spurious to report about that. Any other read failure is
// an invalid-output warning, because the facts belong beside the verdict,
// not in the error path.
func ReadStepOutput(path string) (StepOutput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return StepOutput{}, nil
		}
		return StepOutput{
			Warnings: []OutputWarning{{
				Code:   reason.STEPOutputInvalid,
				Detail: map[string]any{"error": err.Error()},
			}},
		}, nil
	}
	return ParseStepOutput(data, DefaultOutputLimits), nil
}

// ParseStepOutput applies the contract to already-read bytes.
//
// Line rules, in the order the reader meets them: a line that would push the
// file past MaxFileBytes never fully fit, so the read stops there with a
// file_bytes truncation; a line longer than MaxLineBytes stops the read with
// a line_bytes truncation; a line beyond MaxLines stops it with a lines
// truncation. A final segment without its trailing newline was never a whole
// line (a writer died mid-write), so it is discarded without comment. Blank
// lines carry no information and stay silent. Anything else that does not
// parse into one of the two line shapes counts toward one aggregated
// invalid-output warning naming how many lines were dropped and where the
// first one sat. Everything valid before a cut is kept.
func ParseStepOutput(data []byte, limits OutputLimits) StepOutput {
	out := StepOutput{Params: map[string]any{}}
	refs := map[string]PublishedRef{}
	var order []string
	dropped, firstDropped := 0, 0

	total := int64(0)
	number := 0
	truncated := false
	bound := ""
	limit := any(0)

	consume := func(line []byte, lineno int) bool {
		if trimmed := bytes.TrimSpace(line); len(trimmed) == 0 {
			return true
		}
		ref, params, ok := parseOutputLine(line)
		if !ok {
			dropped++
			if firstDropped == 0 {
				firstDropped = lineno
			}
			return true
		}
		switch {
		case ref != nil:
			if _, seen := refs[ref.Name]; !seen {
				order = append(order, ref.Name)
			}
			refs[ref.Name] = *ref // a publisher revising itself: the last line wins
		default:
			for k, v := range params {
				out.Params[k] = v
			}
		}
		return true
	}

	for len(data) > 0 && !truncated {
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			// Partial tail: bytes still count against the file bound,
			// but the fragment itself is never parsed.
			if total+int64(len(data)) > limits.MaxFileBytes {
				truncated, bound, limit = true, "file_bytes", limits.MaxFileBytes
			}
			break
		}
		line := data[:i]
		data = data[i+1:]
		if total+int64(len(line))+1 > limits.MaxFileBytes {
			truncated, bound, limit = true, "file_bytes", limits.MaxFileBytes
			break
		}
		total += int64(len(line)) + 1
		if len(line) > limits.MaxLineBytes {
			truncated, bound, limit = true, "line_bytes", limits.MaxLineBytes
			break
		}
		number++
		if number > limits.MaxLines {
			truncated, bound, limit = true, "lines", limits.MaxLines
			break
		}
		consume(line, number)
	}

	for _, name := range order {
		ref := refs[name]
		out.Artifacts = append(out.Artifacts, ref)
	}
	if dropped > 0 {
		out.Warnings = append(out.Warnings, OutputWarning{
			Code:   reason.STEPOutputInvalid,
			Detail: map[string]any{"count": dropped, "first_line": firstDropped},
		})
	}
	if truncated {
		out.Warnings = append(out.Warnings, OutputWarning{
			Code:   reason.STEPOutputTruncated,
			Detail: map[string]any{"bound": bound, "limit": limit},
		})
	}
	return out
}

// parseOutputLine decodes one line into exactly one of the two contract
// shapes. Unknown fields, both shapes on one line, neither, or a non-object
// line are all contract breaks, not guesses.
func parseOutputLine(line []byte) (ref *PublishedRef, params map[string]any, ok bool) {
	var top struct {
		Artifact *json.RawMessage `json:"artifact"`
		Params   *json.RawMessage `json:"params"`
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&top); err != nil {
		return nil, nil, false
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return nil, nil, false
	}
	switch {
	case top.Artifact != nil && top.Params != nil:
		return nil, nil, false
	case top.Artifact != nil:
		r, ok := parseArtifactRef(*top.Artifact)
		if !ok {
			return nil, nil, false
		}
		return r, nil, true
	case top.Params != nil:
		p := map[string]any{}
		if err := strictUnmarshal(*top.Params, &p); err != nil {
			return nil, nil, false
		}
		return nil, p, true
	default:
		return nil, nil, false
	}
}

// parseArtifactRef validates one artifact object. The name must be present
// and free of control characters; the uri must be an absolute path or a
// scheme URI, because a relative reference would quietly mean different
// directories in every downstream step. size_bytes, when given, is a whole
// non-negative number. The checksum and media type ride along untouched:
// they are the step's claims, stored verbatim or not at all.
func parseArtifactRef(raw json.RawMessage) (*PublishedRef, bool) {
	var decoded struct {
		Name      string       `json:"name"`
		URI       string       `json:"uri"`
		SizeBytes *json.Number `json:"size_bytes"`
		Checksum  string       `json:"checksum"`
		MediaType string       `json:"media_type"`
	}
	if err := strictUnmarshal(raw, &decoded); err != nil {
		return nil, false
	}
	if decoded.Name == "" || hasControlChar(decoded.Name) {
		return nil, false
	}
	if !validRefURI(decoded.URI) {
		return nil, false
	}
	ref := PublishedRef{
		Name:      decoded.Name,
		URI:       decoded.URI,
		Checksum:  decoded.Checksum,
		MediaType: decoded.MediaType,
	}
	if decoded.SizeBytes != nil {
		size, err := decoded.SizeBytes.Int64()
		if err != nil || size < 0 {
			return nil, false
		}
		ref.SizeBytes = &size
	}
	if hasControlChar(ref.Checksum) || hasControlChar(ref.MediaType) {
		return nil, false
	}
	return &ref, true
}

// validRefURI accepts an absolute POSIX path or a scheme URI like
// s3://bucket/key. Everything else, relative paths included, is refused:
// the uri is data paceq will hand back verbatim, and a shape nobody can
// interpret downstream is worse than no row at all.
func validRefURI(u string) bool {
	if u == "" || hasControlChar(u) {
		return false
	}
	if strings.HasPrefix(u, "/") {
		return true
	}
	i := strings.Index(u, "://")
	if i <= 0 {
		return false
	}
	scheme := u[:i]
	for j, r := range scheme {
		alpha := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z'
		digit := r >= '0' && r <= '9'
		extra := r == '+' || r == '.' || r == '-'
		if !(alpha || (j > 0 && (digit || extra))) {
			return false
		}
	}
	return true
}

func hasControlChar(s string) bool {
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return true
		}
	}
	return false
}

// strictUnmarshal refuses unknown fields: the contract is frozen at v0.1,
// and a field the writer invented is a typo until the contract says more.
// It equally refuses a second JSON value trailing the first on one line.
func strictUnmarshal(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if err := dec.Decode(new(json.RawMessage)); err != io.EOF {
		return errTrailingData
	}
	return nil
}

var errTrailingData = errors.New("trailing data after the JSON value")
