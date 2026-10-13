package xflow

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/xbcio/xflow/types"
)

// DefaultRemoteRequestTimeout bounds one ordinary request the Remote makes
// (registration, submission, control, reads). It is deliberately not an
// http.Client.Timeout: the wait route is a hold, and a client-wide timeout
// would cut every hold short of the server's own.
const DefaultRemoteRequestTimeout = 30 * time.Second

// maxRemoteErrorBody caps how much of a NON-enveloped failure body the error
// message carries. A 502 from a proxy in front of the server is not ours to
// parse, and its body is useful for diagnosis only up to a point; an unbounded
// copy of it would end up in whatever log records the error.
const maxRemoteErrorBody = 256

// Remote is the out-of-process control client for a standalone xflow server:
// it drives the server's public /v1 HTTP API (register, invoke, wait, signal,
// cancel, inspect, list) over an endpoint and a bearer token.
//
// It is the fourth mode next to NewLocal, NewCluster and NewServer: where
// those own the engine, the store and the runners, a Remote owns none of them
// and requires no Redis, MySQL, node registry or handler registration. The
// server is the only holder of state.
//
// # Identity
//
// The tenant is bound by the token server-side (the principal's namespace),
// never by anything this client sends: a Remote carries no namespace knob, so
// two tenants are two tokens. This mirrors the API's IDOR defense — a
// cross-namespace id resolves to not-found, not to data.
//
// # Method correspondence
//
// The method set is the remotely expressible subset of *Engine / *Server, with
// the same signatures where they exist: AddWorkflow, ReplaceWorkflow,
// RemoveWorkflow, Invoke, Wait, Signal, RevokeSignal, Cancel, Pause, Resume,
// Rerun, Inspect, ListExecutions. The first four are deliberate deviations the
// HTTP face forces and are documented per method:
//
//   - ReplaceWorkflow takes the workflow id explicitly, because the server
//     mints ids (a uuid per registration) and the PUT route addresses the
//     resource by id; *Server.ReplaceWorkflow resolves by (name, version)
//     in-process and has no id to hand out.
//   - Invoke cannot honour WithExecutionTTL or WithRuntime: the execute routes
//     have no fields for them. Those options are refused rather than dropped
//     silently.
//   - ListExecutions has no *Engine counterpart; it is the collection read
//     (GET /v1/executions) exposed on the Remote only.
type Remote struct {
	endpoint string
	token    string
	client   *http.Client

	// requestTimeout bounds one ordinary request; zero disables the bound and
	// leaves pacing to the caller's context. The wait loop is exempt either
	// way (see send).
	requestTimeout time.Duration
}

// remoteConfig accumulates RemoteOptions.
type remoteConfig struct {
	token          string
	client         *http.Client
	requestTimeout time.Duration
	caFile         string
	certFile       string
	keyFile        string
}

// RemoteOption configures NewRemote.
type RemoteOption func(*remoteConfig)

// WithRemoteToken sets the bearer token every request carries. The server maps
// it to a (subject, namespace, scopes) principal; a token that maps to nothing
// is answered 401. The token travels only in the Authorization header and is
// never logged or embedded in an error message.
func WithRemoteToken(token string) RemoteOption {
	return func(c *remoteConfig) { c.token = token }
}

// WithRemoteHTTPClient injects the client every request uses, for callers that
// need their own transport (a proxy, a trace-injecting RoundTripper, a client
// certificate beyond WithRemoteTLSCert). It cannot be combined with
// WithRemoteCA or WithRemoteTLSCert: the injected client owns its TLS
// configuration, and accepting both would leave the caller guessing which won.
//
// An injected client's own Timeout applies to the wait route too — a timeout
// shorter than the server's hold turns every Wait into a transport error, so
// leave it unset or above 30s.
func WithRemoteHTTPClient(client *http.Client) RemoteOption {
	return func(c *remoteConfig) { c.client = client }
}

// WithRemoteTimeout bounds one ordinary request (registration, submission,
// control, reads). Zero disables the bound and leaves pacing entirely to each
// call's context. It never applies to Wait: the wait route is a hold, and this
// bound exists to keep one request from hanging, not to cap how long a caller
// is willing to wait for an execution.
func WithRemoteTimeout(d time.Duration) RemoteOption {
	return func(c *remoteConfig) { c.requestTimeout = d }
}

// WithRemoteCA trusts the PEM certificate(s) in caFile when verifying the
// server. An https endpoint whose certificate is not in the system pool needs
// this (or WithRemoteHTTPClient).
func WithRemoteCA(caFile string) RemoteOption {
	return func(c *remoteConfig) { c.caFile = caFile }
}

// WithRemoteTLSCert presents a client certificate, for servers that require
// mTLS. Both files are required together; a private CA is usually paired via
// WithRemoteCA.
func WithRemoteTLSCert(certFile, keyFile string) RemoteOption {
	return func(c *remoteConfig) { c.certFile = certFile; c.keyFile = keyFile }
}

// NewRemote validates the endpoint and builds the client. The endpoint is the
// server's base URL (http or https, optionally with a base path); the /v1
// routes are appended to it.
func NewRemote(endpoint string, opts ...RemoteOption) (*Remote, error) {
	cfg := remoteConfig{requestTimeout: DefaultRemoteRequestTimeout}
	for _, opt := range opts {
		opt(&cfg)
	}
	base, err := parseRemoteEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	if cfg.requestTimeout < 0 {
		return nil, fmt.Errorf("xflow: WithRemoteTimeout must not be negative, got %s", cfg.requestTimeout)
	}
	if cfg.client != nil && (cfg.caFile != "" || cfg.certFile != "" || cfg.keyFile != "") {
		return nil, errors.New("xflow: WithRemoteHTTPClient cannot be combined with WithRemoteCA or WithRemoteTLSCert: the injected client owns its TLS configuration")
	}
	client := cfg.client
	if client == nil {
		tlsCfg, err := buildRemoteTLSConfig(cfg.caFile, cfg.certFile, cfg.keyFile)
		if err != nil {
			return nil, err
		}
		client = &http.Client{}
		if tlsCfg != nil {
			client.Transport = &http.Transport{TLSClientConfig: tlsCfg}
		}
	}
	return &Remote{
		endpoint:       base,
		token:          cfg.token,
		client:         client,
		requestTimeout: cfg.requestTimeout,
	}, nil
}

// Stop releases the client's idle connections. A Remote owns no other resource
// — no engine, no store, no goroutine — so this is not a shutdown, and calls
// after it still work.
func (r *Remote) Stop() {
	if r == nil || r.client == nil {
		return
	}
	r.client.CloseIdleConnections()
}

// Endpoint returns the normalized base URL requests are sent to.
func (r *Remote) Endpoint() string { return r.endpoint }

// parseRemoteEndpoint validates the base URL and normalizes away a trailing
// slash. A path is allowed (the server may sit behind a prefix); a query or
// fragment is not, because routes are appended to the path and a base URL with
// its own query would silently apply it to every request.
func parseRemoteEndpoint(endpoint string) (string, error) {
	if strings.TrimSpace(endpoint) == "" {
		return "", errors.New("xflow: remote endpoint is required")
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("xflow: remote endpoint %q is not a URL: %w", endpoint, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("xflow: remote endpoint %q must use http or https", endpoint)
	}
	if u.Host == "" {
		return "", fmt.Errorf("xflow: remote endpoint %q has no host", endpoint)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("xflow: remote endpoint %q must not carry a query or fragment", endpoint)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

// buildRemoteTLSConfig mirrors the runner's TLS construction shape (CA file ->
// root pool, cert+key pair together) without depending on runner config: the
// Remote is a control-plane client, and the two may legitimately trust
// different things.
func buildRemoteTLSConfig(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		caPEM, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("xflow: read remote CA: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("xflow: remote CA %q contains no valid certificates", caFile)
		}
		tlsCfg.RootCAs = pool
	}
	switch {
	case certFile == "" && keyFile == "":
		// TLS without client auth.
	case certFile != "" && keyFile != "":
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("xflow: load remote client keypair: %w", err)
		}
		tlsCfg.Certificates = []tls.Certificate{cert}
	default:
		return nil, errors.New("xflow: WithRemoteTLSCert requires both a certificate and a key file")
	}
	return tlsCfg, nil
}

// remoteEnvelope is the response shape every user-facing /v1 endpoint returns
// (API-SPECIFICATION §3): a success flag, a stable machine-readable code, a
// human message, and the payload. Data is raw so each method decodes only what
// it asked for.
type remoteEnvelope struct {
	Success bool            `json:"success"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
}

// remoteRequest is one call's shape.
type remoteRequest struct {
	method string
	// path is the /v1 route, appended to the Remote's endpoint.
	path string
	// body is marshalled as JSON when non-nil.
	body any
	// headers are added to the request in addition to Authorization and
	// Content-Type (an empty value omits the header).
	headers map[string]string
	// noTimeout exempts this request from requestTimeout. The wait route sets
	// it: a hold is supposed to take as long as the server holds it.
	noTimeout bool
	// retryTransport permits exactly one retry when the request produced no
	// response at all (connection refused, reset, timeout). A response — any
	// response — is never retried here; whether a status is worth retrying is
	// the caller's decision, made from the returned error.
	retryTransport bool
}

// send performs one request and decodes the envelope.
//
// The request timeout, when configured, bounds the whole call — including the
// retry below — so a caller knows the worst case. A failure that consumed the
// budget (a dial timeout) leaves the retry nothing to run with and it fails
// immediately; the retry is for the fast failures (connection refused or
// reset) where a second attempt plausibly succeeds. A response of any status
// is never retried here: whether a status is worth retrying is the caller's
// decision, made from the returned *RemoteError.
func (r *Remote) send(ctx context.Context, req remoteRequest) (remoteEnvelope, error) {
	if !req.noTimeout && r.requestTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, r.requestTimeout)
		defer cancel()
	}
	env, err := r.do(ctx, req)
	if err == nil {
		return env, nil
	}
	if !req.retryTransport || !isRemoteTransportError(err) {
		return remoteEnvelope{}, err
	}
	return r.do(ctx, req)
}

func (r *Remote) do(ctx context.Context, req remoteRequest) (remoteEnvelope, error) {
	var body io.Reader
	if req.body != nil {
		encoded, err := json.Marshal(req.body)
		if err != nil {
			return remoteEnvelope{}, fmt.Errorf("xflow: encode request body: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, r.endpoint+req.path, body)
	if err != nil {
		return remoteEnvelope{}, fmt.Errorf("xflow: build remote request: %w", err)
	}
	if req.body != nil {
		httpReq.Header.Set("Content-Type", "application/json")
	}
	// The token goes in the header only: never a query parameter (org policy
	// §6 bans credentials in URLs), never a log field.
	if r.token != "" {
		httpReq.Header.Set("Authorization", "Bearer "+r.token)
	}
	for k, v := range req.headers {
		if v != "" {
			httpReq.Header.Set(k, v)
		}
	}

	resp, err := r.client.Do(httpReq)
	if err != nil {
		return remoteEnvelope{}, &remoteTransportError{err: fmt.Errorf("xflow: remote %s %s: %w", req.method, req.path, err)}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return remoteEnvelope{}, &remoteTransportError{err: fmt.Errorf("xflow: read remote response: %w", err)}
	}
	var env remoteEnvelope
	decoded := json.Unmarshal(raw, &env) == nil && env.Code != ""
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return remoteEnvelope{}, remoteErrorFrom(resp, raw, decoded, env)
	}
	if !decoded {
		// A 2xx that is not an envelope means something between the client and
		// the server answered — a proxy's captive page, a misrouted host. The
		// caller must not read a success out of it.
		return remoteEnvelope{}, &RemoteError{
			Status:  resp.StatusCode,
			Message: fmt.Sprintf("unexpected non-envelope response: %s", summarizeRemoteBody(raw)),
		}
	}
	if !env.Success {
		// An envelope with success:false on a 2xx is a contract violation, but
		// the envelope is the server speaking, so its code and message stand.
		return remoteEnvelope{}, &RemoteError{Status: resp.StatusCode, Code: env.Code, Message: env.Message}
	}
	return env, nil
}

// remoteErrorFrom builds the error for a non-2xx answer. An enveloped failure
// carries the server's stable code and message; anything else (a proxy's 502,
// an HTML error page) keeps only the status and a bounded excerpt.
func remoteErrorFrom(resp *http.Response, raw []byte, decoded bool, env remoteEnvelope) *RemoteError {
	if decoded {
		return &RemoteError{Status: resp.StatusCode, Code: env.Code, Message: env.Message}
	}
	return &RemoteError{
		Status:  resp.StatusCode,
		Message: fmt.Sprintf("unexpected response: %s", summarizeRemoteBody(raw)),
	}
}

// summarizeRemoteBody renders a bounded, trimmed excerpt of a body that was
// not an envelope.
func summarizeRemoteBody(raw []byte) string {
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return "(empty body)"
	}
	if len(text) > maxRemoteErrorBody {
		return text[:maxRemoteErrorBody] + "…"
	}
	return text
}

// remoteTransportError marks a failure to get any answer from the server, as
// opposed to an answer the server sent. Only the former is retried (and only
// when the request says so): a *RemoteError is the server having decided, and
// repeating a decided request does not change its mind.
type remoteTransportError struct{ err error }

func (e *remoteTransportError) Error() string { return e.err.Error() }
func (e *remoteTransportError) Unwrap() error { return e.err }

// isRemoteTransportError reports whether err is a failure to get any answer —
// safe to retry only when the operation is idempotent or carries an
// idempotency key.
func isRemoteTransportError(err error) bool {
	var transportErr *remoteTransportError
	return errors.As(err, &transportErr)
}

// RemoteError is a failure the remote server reported (or, for a body that was
// not an envelope, that the client observed on the wire).
//
// Status is the HTTP status; Code is the server's stable snake_case business
// code when the response was enveloped; Message is the server's message (for
// an enveloped failure) or a bounded excerpt of a non-enveloped body.
type RemoteError struct {
	Status  int
	Code    string
	Message string
}

func (e *RemoteError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("xflow: remote %d %s: %s", e.Status, e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("xflow: remote %d: %s", e.Status, e.Message)
	}
	return fmt.Sprintf("xflow: remote %d", e.Status)
}

// remoteWorkflowRegistration is the answer to POST /v1/workflows and PUT
// /v1/workflows/{id}.
type remoteWorkflowRegistration struct {
	WorkflowID types.WorkflowID `json:"workflow_id"`
}

// AddWorkflow registers wf on the remote server and returns the id the server
// assigned. The definition is sent as the builder built it (Definition()), and
// the server owns compilation, conflict detection and id minting.
//
// The server's non-fatal diagnostics (compiler warnings, ParamSpec warnings)
// are not returned: they are also logged server-side, and both in-process
// registration calls that return only an id (Server.AddWorkflow,
// Engine.AddWorkflow) set that precedent.
func (r *Remote) AddWorkflow(ctx context.Context, wf *WorkflowBuilder) (types.WorkflowID, error) {
	def, err := wf.Definition()
	if err != nil {
		return "", err
	}
	env, err := r.send(ctx, remoteRequest{method: http.MethodPost, path: "/v1/workflows", body: def})
	if err != nil {
		return "", err
	}
	var out remoteWorkflowRegistration
	if err := json.Unmarshal(env.Data, &out); err != nil {
		return "", fmt.Errorf("xflow: decode workflow registration: %w", err)
	}
	return out.WorkflowID, nil
}

// ReplaceWorkflow replaces the definition registered under workflowID.
//
// It deviates from *Server.ReplaceWorkflow by taking the id: the HTTP PUT
// addresses the resource by path id, and server-minted ids are uuids (the
// embedded path resolves by name+version instead and never needs one), so
// there is nothing the builder alone could name. A registration under a
// different id that collides with the new definition's (name, version) key
// answers 409 workflow_conflict.
func (r *Remote) ReplaceWorkflow(ctx context.Context, workflowID types.WorkflowID, wf *WorkflowBuilder) (types.WorkflowID, error) {
	if workflowID == "" {
		return "", errors.New("xflow: ReplaceWorkflow requires a workflow id")
	}
	def, err := wf.Definition()
	if err != nil {
		return "", err
	}
	env, err := r.send(ctx, remoteRequest{
		method: http.MethodPut,
		path:   "/v1/workflows/" + url.PathEscape(string(workflowID)),
		body:   def,
	})
	if err != nil {
		return "", err
	}
	var out remoteWorkflowRegistration
	if err := json.Unmarshal(env.Data, &out); err != nil {
		return "", fmt.Errorf("xflow: decode workflow replacement: %w", err)
	}
	return out.WorkflowID, nil
}

// RemoveWorkflow deregisters a workflow and clears its entry activations, so
// its triggers stop. It is idempotent in the HTTP sense only: an unknown id is
// 404 (errors.Is(err, store.ErrNotFound)).
func (r *Remote) RemoveWorkflow(ctx context.Context, workflowID types.WorkflowID) error {
	if workflowID == "" {
		return errors.New("xflow: RemoveWorkflow requires a workflow id")
	}
	_, err := r.send(ctx, remoteRequest{
		method: http.MethodDelete,
		path:   "/v1/workflows/" + url.PathEscape(string(workflowID)),
	})
	return err
}
