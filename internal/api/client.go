package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

// ErrNoDaemon means "nobody is listening here". It is the answer a caller
// falls back from, never an error it reports: a daemon that is simply not
// running is not a failure, it is the other half of the dual-mode design.
var ErrNoDaemon = errors.New("no daemon answers on this socket")

// SocketRefused is a socket file the client refuses to talk to: world
// writable, or owned by another user. Unlike a silent socket this is loud,
// because falling back would hide whatever put the file there.
type SocketRefused struct {
	Path string
	Why  string
}

func (e *SocketRefused) Error() string {
	return fmt.Sprintf("refusing the socket at %s: %s", e.Path, e.Why)
}

// Probe reports what answers at path. Nil means a live, acceptable daemon;
// ErrNoDaemon means nobody; SocketRefused means something hostile.
func Probe(path, clientVersion string) error {
	if err := checkSocketFile(path); err != nil {
		return err
	}
	client, err := dialHTTP(path)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNoDaemon, err)
	}
	defer func() {
		client.CloseIdleConnections()
	}()

	req, err := jsonRequest(context.Background(), http.MethodGet, "http://localhost/v1/healthz", nil, clientVersion)
	if err != nil {
		return fmt.Errorf("build the probe: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrNoDaemon, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// A 404 on the probe path means somebody is listening but not
		// serving this protocol: an older daemon from before M2-08, or a
		// stranger's socket that passed the stat checks. Nobody home for
		// our purposes, so the caller falls back rather than refuses.
		if resp.StatusCode == http.StatusNotFound {
			return fmt.Errorf("%w: nothing serves this protocol on %s", ErrNoDaemon, path)
		}
		code, message := readCodeAndMessage(decodeLoose(resp.Body))
		if code == "" {
			code = codeVersionMismatch
		}
		if message == "" {
			message = fmt.Sprintf("the daemon answered http %d to the version probe", resp.StatusCode)
		}
		return &WireError{Status: resp.StatusCode, Code: code, Message: message}
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// Client speaks for one CLI process against one daemon socket. It carries
// the product version in every request header, which is how the server's
// gate can refuse a mismatched pair with a sentence instead of garbage.
type Client struct {
	path    string
	version string
	http    *http.Client
}

// Dial checks the socket file and returns a client when a daemon could be
// listening. The probe is deliberately strict before optimistic: a refused
// socket must stop the caller, while ErrNoDaemon just sends it elsewhere.
func Dial(path, clientVersion string) (*Client, error) {
	if err := checkSocketFile(path); err != nil {
		return nil, err
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", path)
		},
	}
	return &Client{
		path:    path,
		version: clientVersion,
		http:    &http.Client{Transport: transport, Timeout: 30 * time.Second},
	}, nil
}

// Path is the socket this client dials.
func (c *Client) Path() string { return c.path }

// Close releases idle connections.
func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

// CreateRun queues one manual run and answers with its id. Every refusal
// comes back as a *WireError with status, stable code and message intact.
func (c *Client) CreateRun(ctx context.Context, job string, params map[string]string) (string, error) {
	body := struct {
		Job    string            `json:"job"`
		Params map[string]string `json:"params"`
	}{Job: job, Params: params}
	document, err := c.call(ctx, http.MethodPost, "/v1/runs", body)
	if err != nil {
		return "", err
	}
	run, ok := document["run"].(map[string]any)
	if !ok {
		return "", fmt.Errorf("the create-run answer carries no run object: %v", document)
	}
	id, _ := run["id"].(string)
	if id == "" {
		return "", fmt.Errorf("the created run has no id: %v", run)
	}
	return id, nil
}

// CancelRun records a durable cancellation request for one run.
func (c *Client) CancelRun(ctx context.Context, runID, reason string) error {
	body := struct {
		Reason string `json:"reason"`
	}{Reason: reason}
	_, err := c.call(ctx, http.MethodPost, "/v1/runs/"+runID+"/cancel", body)
	return err
}

// ApplyReport is the applied/unchanged/failed report, shaped like the CLI's
// own JSON report so a renderer can consume either without translation.
type ApplyReport struct {
	Applied   []ApplyEntry       `json:"applied"`
	Unchanged []ApplyEntry       `json:"unchanged"`
	Failed    []ApplyWireFailure `json:"failed"`
}

// ApplyEntry is one job file that landed or was already there.
type ApplyEntry struct {
	Job        string `json:"job"`
	File       string `json:"file"`
	Version    int    `json:"version"`
	SpecHash   string `json:"spec_hash,omitempty"`
	FileSHA256 string `json:"file_sha256,omitempty"`
}

// ApplyWireFailure is one file the daemon could not parse.
type ApplyWireFailure struct {
	File    string `json:"file"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type applyWireResponse struct {
	Applied   []ApplyEntry       `json:"applied"`
	Unchanged []ApplyEntry       `json:"unchanged"`
	Failed    []ApplyWireFailure `json:"failed"`
}

// Apply asks the daemon to load job files from paths on its own disk.
func (c *Client) Apply(ctx context.Context, paths []string) (ApplyReport, error) {
	body := struct {
		Paths []string `json:"paths"`
	}{Paths: paths}
	var wire applyWireResponse
	document, err := c.call(ctx, http.MethodPost, "/v1/apply", body)
	if err != nil {
		return ApplyReport{}, err
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return ApplyReport{}, fmt.Errorf("re-encode the apply report: %w", err)
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return ApplyReport{}, fmt.Errorf("the apply report is not shaped like the contract: %w", err)
	}
	// The wire shape and the report are the same struct by design; the
	// conversion keeps it that way (a field drift is a compile error).
	return ApplyReport(wire), nil
}

// call performs one request and decodes one JSON object answer. A non-2xx
// answer becomes a *WireError carrying everything the envelope said.
func (c *Client) call(ctx context.Context, method, target string, body any) (map[string]any, error) {
	req, err := jsonRequest(ctx, method, "http://localhost"+target, body, c.version)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err // timeouts keep their identity; the CLI maps them to exit 7
	}
	defer func() { _ = resp.Body.Close() }()

	var document map[string]any
	decodeErr := json.NewDecoder(resp.Body).Decode(&document)
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		code, message := readCodeAndMessage(document)
		return nil, &WireError{Status: resp.StatusCode, Code: code, Message: message}
	}
	if decodeErr != nil {
		return nil, fmt.Errorf("the answer from %s is not JSON a paceq daemon sends: %w", target, decodeErr)
	}
	return document, nil
}

// jsonRequest builds one request with the content type and version header set.
func jsonRequest(ctx context.Context, method, url string, body any, version string) (*http.Request, error) {
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encode the request body: %w", err)
		}
		reader = strings.NewReader(string(encoded))
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set(clientHeader, "paceq/"+version)
	return req, nil
}

// dialHTTP builds the throwaway client Probe uses for one health check.
func dialHTTP(path string) (*http.Client, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "unix", path)
		},
	}
	return &http.Client{Transport: transport, Timeout: 10 * time.Second}, nil
}

// checkSocketFile refuses anything about the file itself that is wrong:
// missing means quiet absence, hostile means a loud refusal. Ownership is
// checked against the effective uid; root owns everything, so there the
// check cannot lie and skips instead.
func checkSocketFile(path string) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrNoDaemon, path)
		}
		return fmt.Errorf("%w: stat %s: %v", ErrNoDaemon, path, err)
	}
	if info.Mode().Perm()&0o002 != 0 {
		return &SocketRefused{Path: path, Why: "it is writable by everybody, so whoever left it invited the whole machine"}
	}
	if !sameOwner(info) && os.Geteuid() != 0 {
		return &SocketRefused{Path: path, Why: "another user owns it"}
	}
	return nil
}

// decodeLoose folds a response body into a map, or nothing when the body is
// not JSON. A refusal's status line already carries half the story; the
// envelope is read only to add the other half.
func decodeLoose(body io.Reader) map[string]any {
	var document map[string]any
	_ = json.NewDecoder(body).Decode(&document)
	return document
}

// readCodeAndMessage pulls the stable label and the sentence out of an error
// envelope, tolerating a body that never became one.
func readCodeAndMessage(document map[string]any) (code, message string) {
	if document == nil {
		return "", ""
	}
	errBody, _ := document["error"].(map[string]any)
	if errBody == nil {
		return "", ""
	}
	code, _ = errBody["code"].(string)
	message, _ = errBody["message"].(string)
	return code, message
}
