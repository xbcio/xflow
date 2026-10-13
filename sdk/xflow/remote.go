package xflow

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/store"
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

// remoteResponse is a decoded success: the status (the envelope's code is
// always "200" on success, so the HTTP status is what carries the finer
// distinction — 201 created, 202 the wait hold expiring) plus the envelope.
type remoteResponse struct {
	status   int
	envelope remoteEnvelope
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
func (r *Remote) send(ctx context.Context, req remoteRequest) (remoteResponse, error) {
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
		return remoteResponse{}, err
	}
	return r.do(ctx, req)
}

func (r *Remote) do(ctx context.Context, req remoteRequest) (remoteResponse, error) {
	var body io.Reader
	if req.body != nil {
		encoded, err := json.Marshal(req.body)
		if err != nil {
			return remoteResponse{}, fmt.Errorf("xflow: encode request body: %w", err)
		}
		body = bytes.NewReader(encoded)
	}
	httpReq, err := http.NewRequestWithContext(ctx, req.method, r.endpoint+req.path, body)
	if err != nil {
		return remoteResponse{}, fmt.Errorf("xflow: build remote request: %w", err)
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
		return remoteResponse{}, &remoteTransportError{err: fmt.Errorf("xflow: remote %s %s: %w", req.method, req.path, err)}
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return remoteResponse{}, &remoteTransportError{err: fmt.Errorf("xflow: read remote response: %w", err)}
	}
	var env remoteEnvelope
	decoded := json.Unmarshal(raw, &env) == nil && env.Code != ""
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return remoteResponse{}, remoteErrorFrom(resp, raw, decoded, env)
	}
	if !decoded {
		// A 2xx that is not an envelope means something between the client and
		// the server answered — a proxy's captive page, a misrouted host. The
		// caller must not read a success out of it.
		return remoteResponse{}, &RemoteError{
			Status:  resp.StatusCode,
			Message: fmt.Sprintf("unexpected non-envelope response: %s", summarizeRemoteBody(raw)),
		}
	}
	if !env.Success {
		// An envelope with success:false on a 2xx is a contract violation, but
		// the envelope is the server speaking, so its code and message stand.
		return remoteResponse{}, &RemoteError{Status: resp.StatusCode, Code: env.Code, Message: env.Message}
	}
	return remoteResponse{status: resp.StatusCode, envelope: env}, nil
}

// remoteErrorFrom builds the error for a non-2xx answer. An enveloped failure
// carries the server's stable code and message; anything else (a proxy's 502,
// an HTML error page) keeps only the status and a bounded excerpt.
func remoteErrorFrom(resp *http.Response, raw []byte, decoded bool, env remoteEnvelope) *RemoteError {
	err := &RemoteError{
		Status:     resp.StatusCode,
		RetryAfter: parseRemoteRetryAfter(resp.Header.Get("Retry-After")),
	}
	if decoded {
		err.Code = env.Code
		err.Message = env.Message
		return err
	}
	err.Message = fmt.Sprintf("unexpected response: %s", summarizeRemoteBody(raw))
	return err
}

// parseRemoteRetryAfter reads a Retry-After as either the delay-seconds form
// (what this server sends) or an HTTP-date. An unparseable value is zero: the
// caller then paces itself instead of reading a garbage duration out of a
// header.
func parseRemoteRetryAfter(v string) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
		if secs < 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if when, err := http.ParseTime(v); err == nil {
		if d := time.Until(when); d > 0 {
			return d
		}
	}
	return 0
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

// remoteSignalRequest is the body of POST /v1/executions/{id}/signals.
type remoteSignalRequest struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data,omitempty"`
}

// Signal delivers a named signal to a suspended node within the execution.
// Signal names and their meaning are the workflow's (see Engine.Signal); the
// server records the authenticated caller as the signal's verified actor.
func (r *Remote) Signal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error {
	if id == "" {
		return errors.New("xflow: Signal requires an execution id")
	}
	if name == "" {
		return errors.New("xflow: Signal requires a signal name")
	}
	_, err := r.send(ctx, remoteRequest{
		method: http.MethodPost,
		path:   "/v1/executions/" + url.PathEscape(string(id)) + "/signals",
		body:   remoteSignalRequest{Name: name, Data: data},
	})
	return err
}

// RevokeSignal revokes a pre-delivered signal that has not yet been consumed.
// A signal that already resumed a node is 409 signal_consumed
// (errors.Is(err, engine.ErrSignalConsumed)).
func (r *Remote) RevokeSignal(ctx context.Context, id types.ExecutionID, name string) error {
	if id == "" {
		return errors.New("xflow: RevokeSignal requires an execution id")
	}
	if name == "" {
		return errors.New("xflow: RevokeSignal requires a signal name")
	}
	_, err := r.send(ctx, remoteRequest{
		method: http.MethodDelete,
		path:   "/v1/executions/" + url.PathEscape(string(id)) + "/signals/" + url.PathEscape(name),
	})
	return err
}

// Cancel cancels a running execution and releases suspended nodes. Like
// Engine.Cancel it is best-effort for work already leased to a runner.
func (r *Remote) Cancel(ctx context.Context, id types.ExecutionID) error {
	return r.executionAction(ctx, id, "cancel")
}

// Pause suspends dispatch of a running execution. The server answers with the
// execution view, which is deliberately not returned here: Engine.Pause
// reports the outcome as an error and callers that want the view call
// Inspect.
func (r *Remote) Pause(ctx context.Context, id types.ExecutionID) error {
	return r.executionAction(ctx, id, "pause")
}

// Resume returns a paused execution to dispatch and queues what the pause
// held.
func (r *Remote) Resume(ctx context.Context, id types.ExecutionID) error {
	return r.executionAction(ctx, id, "resume")
}

// executionAction posts one of the parameterless execution controls and
// discards its body. The failures come back through the mapped sentinels:
// engine.ErrExecutionNotPausable / ErrExecutionNotPaused for the two 409s,
// engine.ErrPauseUnsupported for a deployment without pause.
func (r *Remote) executionAction(ctx context.Context, id types.ExecutionID, verb string) error {
	if id == "" {
		return fmt.Errorf("xflow: %s requires an execution id", verb)
	}
	_, err := r.send(ctx, remoteRequest{
		method: http.MethodPost,
		path:   "/v1/executions/" + url.PathEscape(string(id)) + "/" + verb,
	})
	return err
}

// remoteRerunRequest is the body of POST /v1/executions/{id}/rerun. The
// idempotency key is not a body field: the route reads it from the
// Idempotency-Key header, exactly as the execute route does.
type remoteRerunRequest struct {
	FromNode       string `json:"from_node"`
	IncludeSuccess bool   `json:"include_success,omitempty"`
}

// Rerun derives a new execution from a terminal one, starting at fromNode; see
// Engine.Rerun for the semantics, which are the server's here.
//
// RerunIdempotencyKey, when given, travels as the Idempotency-Key header and
// makes the call safe to retry: a transport failure (no response) is then
// retried once with the same key, and a replayed request returns the rerun the
// first one created with Duplicate set. Without a key nothing is retried —
// deriving a second execution from one lost response is the outcome the key
// exists to prevent.
func (r *Remote) Rerun(ctx context.Context, id types.ExecutionID, fromNode string, opts ...RerunOption) (engine.RerunResult, error) {
	if id == "" {
		return engine.RerunResult{}, errors.New("xflow: Rerun requires an execution id")
	}
	if fromNode == "" {
		return engine.RerunResult{}, errors.New("xflow: rerun requires a from node")
	}
	req := engine.RerunRequest{ExecutionID: id, FromNode: fromNode}
	for _, opt := range opts {
		opt(&req)
	}
	headers := map[string]string{}
	if req.IdempotencyKey != "" {
		headers["Idempotency-Key"] = req.IdempotencyKey
	}
	env, err := r.send(ctx, remoteRequest{
		method:  http.MethodPost,
		path:    "/v1/executions/" + url.PathEscape(string(id)) + "/rerun",
		body:    remoteRerunRequest{FromNode: req.FromNode, IncludeSuccess: req.IncludeSuccess},
		headers: headers,
		// Only a keyed rerun is safe to repeat.
		retryTransport: req.IdempotencyKey != "",
	})
	if err != nil {
		return engine.RerunResult{}, err
	}
	var out remoteSubmissionResult
	if err := json.Unmarshal(env.envelope.Data, &out); err != nil {
		return engine.RerunResult{}, fmt.Errorf("xflow: decode rerun response: %w", err)
	}
	res := engine.RerunResult{ExecutionID: out.ExecutionID, QueuePosition: out.QueuePosition, Duplicate: out.Duplicate}
	if out.Status == types.ExecutionStatusQueued {
		res.Queued = true
	}
	return res, nil
}

// RemoteExecution is one row of the execution collection (GET /v1/executions).
//
// WorkflowName, not a workflow id: the server's executions table stores no
// workflow id column, so a name is the only workflow identity a row carries —
// and a display string, not an identity (two namespaces can share one).
type RemoteExecution struct {
	ExecutionID  types.ExecutionID     `json:"execution_id"`
	Namespace    string                `json:"namespace"`
	WorkflowName string                `json:"workflow_name"`
	Status       types.ExecutionStatus `json:"status"`
	Error        string                `json:"error"`
	CreatedAt    time.Time             `json:"created_at"`
	UpdatedAt    time.Time             `json:"updated_at"`
}

// RemoteExecutionQuery narrows ListExecutions. The zero value pages from the
// start with the server's default page size and applies no filter — the four
// filters the route supports, and nothing more.
//
// Status, when set, must be a lifecycle status (pending, queued, paused,
// running, success, failed, canceling, canceled, timeout); anything else is
// refused by the server with 400 execution_status_invalid rather than served
// as an empty page. The time bounds are exclusive at both ends.
type RemoteExecutionQuery struct {
	Status        types.ExecutionStatus
	CreatedAfter  time.Time
	CreatedBefore time.Time
	Page          int
	PageSize      int
}

// RemoteExecutionPage is one page of the collection plus the total matching
// the filter (not just this page).
type RemoteExecutionPage struct {
	Executions []RemoteExecution
	Total      int
}

// ListExecutions returns one page of this token's namespace's executions.
//
// There is no *Engine counterpart — an embedded engine has no execution
// collection to page — so this is Remote's own surface. The namespace is the
// token's, server-side; there is no parameter for it anywhere, by design.
func (r *Remote) ListExecutions(ctx context.Context, query RemoteExecutionQuery) (RemoteExecutionPage, error) {
	q := url.Values{}
	if query.Status != "" {
		q.Set("status", string(query.Status))
	}
	if !query.CreatedAfter.IsZero() {
		q.Set("created_after", query.CreatedAfter.Format(time.RFC3339Nano))
	}
	if !query.CreatedBefore.IsZero() {
		q.Set("created_before", query.CreatedBefore.Format(time.RFC3339Nano))
	}
	if query.Page > 0 {
		q.Set("page", strconv.Itoa(query.Page))
	}
	if query.PageSize > 0 {
		q.Set("page_size", strconv.Itoa(query.PageSize))
	}
	path := "/v1/executions"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	env, err := r.send(ctx, remoteRequest{method: http.MethodGet, path: path, retryTransport: true})
	if err != nil {
		return RemoteExecutionPage{}, err
	}
	var out struct {
		List  []RemoteExecution `json:"list"`
		Total int               `json:"total"`
	}
	if err := json.Unmarshal(env.envelope.Data, &out); err != nil {
		return RemoteExecutionPage{}, fmt.Errorf("xflow: decode execution list: %w", err)
	}
	return RemoteExecutionPage{Executions: out.List, Total: out.Total}, nil
}

// RemoteError is a failure the remote server reported (or, for a body that was
// not an envelope, that the client observed on the wire).
//
// Status is the HTTP status; Code is the server's stable snake_case business
// code when the response was enveloped; Message is the server's message (for
// an enveloped failure) or a bounded excerpt of a non-enveloped body;
// RetryAfter carries the Retry-After the server sent with a refusal that is
// worth waiting out (a concurrency limit, an idempotency claim still in
// progress), zero when it sent none.
//
// Is maps the server's stable codes onto the SDK's sentinels, so a caller can
// keep writing errors.Is(err, engine.ErrConcurrencyLimit) against a Remote
// exactly as it does against an Engine — the two backends answer one question
// with the same vocabulary.
type RemoteError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *RemoteError) Error() string {
	msg := fmt.Sprintf("xflow: remote %d", e.Status)
	if e.Code != "" {
		msg += " " + e.Code
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.RetryAfter > 0 {
		msg += fmt.Sprintf(" (retry after %s)", e.RetryAfter)
	}
	return msg
}

// remoteCapabilityCodes are the server's answers that say "this deployment will
// not do this", not "it cannot right now". Retrying one asks the same question
// of the same process and gets the same answer, so a caller looping on
// Retryable must treat them as terminal. They are also why the status alone is
// not enough: the server answers 503 for idempotency_unsupported, which is
// otherwise a transient class.
var remoteCapabilityCodes = map[string]bool{
	"idempotency_unsupported":       true, // 503: this server holds no idempotency claims
	"execution_control_unsupported": true, // 501: pause/resume not implemented
	"execution_rerun_unsupported":   true, // 501: rerun not implemented
	"runner_listing_unsupported":    true, // 501: no runner directory
	"runner_control_unsupported":    true, // 501: no runner control
}

// Retryable reports whether the failure is the server's own state rather than
// this request's content: a 5xx, except a 501 or one of the capability answers
// in remoteCapabilityCodes.
//
// 501 is a capability answer by status (and execution_control_unsupported /
// execution_rerun_unsupported arrive that way), while idempotency_unsupported
// arrives as a 503 that a bare status rule would call retryable — a caller
// looping on Retryable would then retry "this server cannot hold idempotency
// keys" forever. A 429 is excluded too, but for the opposite reason: the server
// sent a Retry-After with it because retrying is a decision about WHEN, and the
// caller is the only side that knows whether it can wait.
func (e *RemoteError) Retryable() bool {
	if remoteCapabilityCodes[e.Code] {
		return false
	}
	return e.Status >= http.StatusInternalServerError && e.Status != http.StatusNotImplemented
}

// Is maps the failure onto the SDK's sentinels by the server's stable code
// (and, for authentication, the status — the 401/403 codes differ across the
// API's auth paths, the statuses do not).
//
// Unmapped codes match nothing: the caller still has the *RemoteError itself
// for Status/Code/Message/RetryAfter, and an invented correspondence would be
// worse than none.
func (e *RemoteError) Is(target error) bool {
	switch target {
	case ErrRemoteUnauthenticated:
		return e.Status == http.StatusUnauthorized
	case ErrRemoteForbidden:
		return e.Status == http.StatusForbidden
	}
	switch e.Code {
	case "workflow_not_found", "execution_not_found", "route_not_found":
		return target == store.ErrNotFound
	case "workflow_conflict":
		return target == backend.ErrWorkflowConflict
	case "idempotency_key_conflict":
		return target == engine.ErrIdempotencyConflict
	case "idempotency_key_in_progress":
		return target == engine.ErrIdempotencyInProgress
	case "idempotency_unsupported":
		return target == engine.ErrIdempotencyUnsupported
	case "workflow_concurrency_limit":
		return target == engine.ErrConcurrencyLimit
	case "workflow_concurrency_queue_full":
		return target == engine.ErrConcurrencyQueueFull
	case "signal_consumed":
		return target == engine.ErrSignalConsumed
	case "execution_not_pausable":
		return target == engine.ErrExecutionNotPausable
	case "execution_not_paused":
		return target == engine.ErrExecutionNotPaused
	case "execution_control_unsupported":
		return target == engine.ErrPauseUnsupported
	case "execution_not_rerunnable":
		return target == engine.ErrExecutionNotRerunnable
	case "rerun_node_not_found":
		return target == engine.ErrRerunNodeNotFound
	case "rerun_upstream_incomplete":
		return target == engine.ErrRerunUpstreamIncomplete
	case "rerun_graph_unsupported":
		return target == engine.ErrRerunGraphUnsupported
	case "execution_rerun_unsupported":
		return target == engine.ErrRerunUnsupported
	}
	return false
}

// ErrRemoteUnauthenticated reports a token the server would not accept (401):
// absent, expired, or unknown.
var ErrRemoteUnauthenticated = errors.New("xflow: remote server rejected the token")

// ErrRemoteForbidden reports a token the server accepted but that does not
// carry the scope this operation needs (403).
var ErrRemoteForbidden = errors.New("xflow: remote token lacks the required scope")

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
	if err := json.Unmarshal(env.envelope.Data, &out); err != nil {
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
	if err := json.Unmarshal(env.envelope.Data, &out); err != nil {
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

// remoteSubmissionResult is what the execute and rerun routes report about the
// execution they created (executeWorkflowResponse / rerunExecutionResponse on
// the server).
type remoteSubmissionResult struct {
	ExecutionID   types.ExecutionID     `json:"execution_id"`
	Status        types.ExecutionStatus `json:"status"`
	QueuePosition int                   `json:"queue_position"`
	Duplicate     bool                  `json:"duplicate"`
}

// remoteExecuteRequest is the body of POST /v1/workflows/{id}/execute. An
// empty entry would run the default start node, but Invoke requires an entry
// exactly as Engine.Invoke does, so it is always set here.
type remoteExecuteRequest struct {
	Entry string         `json:"entry,omitempty"`
	Input map[string]any `json:"input,omitempty"`
	Test  bool           `json:"test,omitempty"`
}

// Invoke submits a run of a registered workflow, addressed by id, starting at
// the given entry node. It returns the execution the server created.
//
// The signature is Engine.Invoke's, with two options the execute routes cannot
// express refused rather than dropped: WithExecutionTTL (no TTL field on the
// route) and WithRuntime (no runtime field). Silently ignoring either would
// promise a run whose lifetime or inputs differ from what the caller asked
// for. WithTraceID is refused for the same reason.
//
// # Idempotency is not optional here
//
// Every Invoke carries an Idempotency-Key: WithIdempotencyKey's when the
// caller set one, a fresh 128-bit random key otherwise. On a network, "the
// request went out but the response never came back" is an ordinary outcome,
// and without a key the retry it invites creates a second execution nobody can
// reconcile. With one, the retry is answered with the first execution
// (Duplicate: true in InvokeWithResult).
//
// A transport failure — no response at all — is retried once with the same
// key; a response of any status is not retried, because the server has
// decided and a repeat would only ask the same question again. Callers that
// want a key spanning several of their own retries pass WithIdempotencyKey.
func (r *Remote) Invoke(ctx context.Context, workflowID types.WorkflowID, entry Entry, input map[string]any, opts ...InvokeOption) (types.ExecutionID, error) {
	res, err := r.invoke(ctx, workflowID, entry, input, opts...)
	return res.ExecutionID, err
}

// InvokeResult is what InvokeWithResult reports about a submission: the
// execution id Invoke also returns, plus the fields the server sends and
// Invoke has no place for. Duplicate is true when the Idempotency-Key had
// already started this execution — the id is then the ORIGINAL execution and
// this call created nothing.
type InvokeResult struct {
	ExecutionID   types.ExecutionID
	Status        types.ExecutionStatus
	QueuePosition int
	Duplicate     bool
}

// InvokeWithResult is Invoke, additionally reporting where the submission
// landed. It exists because "queued behind the workflow's own concurrency
// limit" and "running" are indistinguishable from an execution id, and
// Duplicate is the only way a caller can tell its own replay from a first
// submission.
func (r *Remote) InvokeWithResult(ctx context.Context, workflowID types.WorkflowID, entry Entry, input map[string]any, opts ...InvokeOption) (InvokeResult, error) {
	res, err := r.invoke(ctx, workflowID, entry, input, opts...)
	if err != nil {
		return InvokeResult{}, err
	}
	return InvokeResult{
		ExecutionID:   res.ExecutionID,
		Status:        res.Status,
		QueuePosition: res.QueuePosition,
		Duplicate:     res.Duplicate,
	}, nil
}

func (r *Remote) invoke(ctx context.Context, workflowID types.WorkflowID, entry Entry, input map[string]any, opts ...InvokeOption) (remoteSubmissionResult, error) {
	if workflowID == "" {
		return remoteSubmissionResult{}, errors.New("xflow: Invoke requires a workflow id")
	}
	if entry.nodeName == "" {
		return remoteSubmissionResult{}, errors.New("entry node name is required")
	}
	cfg := &invokeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if err := checkRemoteInvokeOptions(cfg); err != nil {
		return remoteSubmissionResult{}, err
	}
	key := cfg.idempotencyKey
	if key == "" {
		var err error
		key, err = remoteIdempotencyKey()
		if err != nil {
			return remoteSubmissionResult{}, err
		}
	}
	env, err := r.send(ctx, remoteRequest{
		method:         http.MethodPost,
		path:           "/v1/workflows/" + url.PathEscape(string(workflowID)) + "/execute",
		body:           remoteExecuteRequest{Entry: entry.nodeName, Input: input, Test: cfg.testRun},
		headers:        map[string]string{"Idempotency-Key": key},
		retryTransport: true,
	})
	if err != nil {
		return remoteSubmissionResult{}, err
	}
	var out remoteSubmissionResult
	if err := json.Unmarshal(env.envelope.Data, &out); err != nil {
		return remoteSubmissionResult{}, fmt.Errorf("xflow: decode execute response: %w", err)
	}
	return out, nil
}

// remoteIdempotencyKey returns a fresh key: 128 bits of crypto/rand as 32 hex
// characters, the shape the design specifies. Collision is not a concern at
// this width, and the key is scoped server-side to one workflow, so a
// collision across workflows would not matter either.
func remoteIdempotencyKey() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("xflow: generate idempotency key: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// checkRemoteInvokeOptions refuses the InvokeOptions the HTTP execute route has
// no field for. Each message names the option, so a caller that expected it to
// work learns which call it cannot make remotely instead of getting a run
// whose TTL or runtime differs from the request.
func checkRemoteInvokeOptions(cfg *invokeConfig) error {
	switch {
	case cfg.execTTL > 0:
		return errors.New("xflow: WithExecutionTTL has no remote equivalent: the execute route carries no execution TTL")
	case cfg.runtime != nil:
		return errors.New("xflow: WithRuntime has no remote equivalent: the execute route carries no runtime payload")
	case cfg.traceID != "" || cfg.spanID != "":
		return errors.New("xflow: WithTraceID has no remote equivalent: trace context travels in HTTP headers, not in the execute body")
	}
	return nil
}

// remoteWaitHold is the hold Wait asks the server for on each request.
//
// The route's own default is 5s, which would turn a long wait into a stream of
// requests; its cap is 10m, which would make ctx cancellation wait out the
// hold before it is noticed. 30s is the design's recommended middle: few
// requests, prompt cancellation at the next boundary.
const remoteWaitHold = 30 * time.Second

// Wait blocks until the execution reaches a terminal state or ctx is canceled.
//
// It is a loop over the server's wait route (GET /v1/executions/{id}/wait):
// 200 carries the terminal detail, 202 means the server's hold expired with
// the execution still running, and the loop asks again. Cancellation is
// promptly visible: the in-flight request is aborted by ctx, and ctx.Err() is
// what the caller gets.
//
// The result is rebuilt through resultFromDetail, the same conversion the
// embedded engine's Wait applies to the same detail — so a remote wait and a
// local wait return the same Result shape for the same execution.
func (r *Remote) Wait(ctx context.Context, id types.ExecutionID) (types.Result, error) {
	if id == "" {
		return types.Result{}, errors.New("xflow: Wait requires an execution id")
	}
	for {
		if err := ctx.Err(); err != nil {
			return types.Result{}, err
		}
		env, err := r.send(ctx, remoteRequest{
			method:         http.MethodGet,
			path:           "/v1/executions/" + url.PathEscape(string(id)) + "/wait?timeout=" + url.QueryEscape(remoteWaitHold.String()),
			noTimeout:      true,
			retryTransport: true,
		})
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return types.Result{}, ctxErr
			}
			return types.Result{}, err
		}
		if env.status == http.StatusAccepted {
			continue
		}
		var detail engine.ExecutionDetail
		if err := json.Unmarshal(env.envelope.Data, &detail); err != nil {
			return types.Result{}, fmt.Errorf("xflow: decode wait response: %w", err)
		}
		if !types.IsTerminalExecutionStatus(detail.Status) {
			// A 200 that is not terminal should not happen (the route writes
			// 200 only for a terminal detail). Treating it as terminal would
			// hand the caller a Result that claims a finished execution, so
			// the loop asks again instead.
			continue
		}
		return resultFromDetail(detail), nil
	}
}

// Inspect returns execution and node status details for audit and UI flows.
//
// The GET route has no node-selection parameter: the server always returns the
// whole graph's view, so when nodeNames are given the filtering happens here,
// on that same snapshot. The result is what Engine.Inspect documents — the
// named nodes only, latest state — fetched with one request either way, with
// one difference: a name with no row in the snapshot is DROPPED here, where
// Engine.Inspect builds a placeholder row for it (pending, no output), because
// the embedded side knows the graph and can tell "not run yet" from "not a
// node", while this side sees only the rows the server sent. A caller that
// needs one row per requested name must fill the gaps itself.
func (r *Remote) Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error) {
	if id == "" {
		return engine.ExecutionDetail{}, errors.New("xflow: Inspect requires an execution id")
	}
	env, err := r.send(ctx, remoteRequest{
		method:         http.MethodGet,
		path:           "/v1/executions/" + url.PathEscape(string(id)),
		retryTransport: true,
	})
	if err != nil {
		return engine.ExecutionDetail{}, err
	}
	var detail engine.ExecutionDetail
	if err := json.Unmarshal(env.envelope.Data, &detail); err != nil {
		return engine.ExecutionDetail{}, fmt.Errorf("xflow: decode execution detail: %w", err)
	}
	if len(nodeNames) == 0 {
		return detail, nil
	}
	wanted := make(map[string]bool, len(nodeNames))
	for _, n := range nodeNames {
		wanted[n] = true
	}
	kept := make([]engine.NodeDetail, 0, len(nodeNames))
	for _, n := range detail.Nodes {
		if wanted[n.Name] {
			kept = append(kept, n)
		}
	}
	detail.Nodes = kept
	return detail, nil
}
