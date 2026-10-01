package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// MaxRunnerResponseBodyBytes caps a runner-protocol HTTP response body the
// Client will read: 8 MiB, the same bound as control.MaxRegisterRunnerBodyBytes
// and the gRPC transport's message limit. The control plane fails a lease that
// would encode past it rather than send it, so a larger body is never a valid
// answer.
const MaxRunnerResponseBodyBytes = MaxRunnerDescriptorEnvelopeBytes + 7<<20

// maxRunnerErrorBodyBytes bounds how much of a non-2xx body is quoted in the
// returned error.
const maxRunnerErrorBodyBytes = 4 << 10

// ErrRunnerResponseTooLarge reports a runner-protocol HTTP response body
// larger than MaxRunnerResponseBodyBytes. The body is discarded unread past
// the limit.
var ErrRunnerResponseTooLarge = errors.New("runner protocol response body exceeds size limit")

// MaxRunnerRequestBodyBytes is the largest runner-protocol JSON request body
// the control plane decodes: the same 8 MiB it applies to every runner route.
// The Client refuses to send a larger body, since the server would only
// answer 413.
const MaxRunnerRequestBodyBytes = MaxRunnerResponseBodyBytes

// ErrRunnerRequestTooLarge reports a runner-protocol request body larger than
// MaxRunnerRequestBodyBytes, whether the Client caught it before sending or
// the server answered 413. Resending the same body cannot succeed.
var ErrRunnerRequestTooLarge = errors.New("runner protocol request body exceeds size limit")

type Client struct {
	baseURL string
	http    *http.Client
	token   string
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    httpClient,
	}
}

// WithToken returns a client that adds Authorization: Bearer <token> to every
// request. Empty token disables the header (same behavior as calling NewClient
// alone). Kept as a copy-returning setter so callers can build per-runner
// clients from a shared base without mutating shared state.
func (c *Client) WithToken(token string) *Client {
	cp := *c
	cp.token = token
	return &cp
}

func (c *Client) Register(ctx context.Context, req RegisterRunnerRequest) (RegisterRunnerResponse, error) {
	var resp RegisterRunnerResponse
	err := c.post(ctx, RegisterRunnerPath, req, &resp)
	return resp, err
}

func (c *Client) Heartbeat(ctx context.Context, req HeartbeatRequest) (HeartbeatResponse, error) {
	var resp HeartbeatResponse
	err := c.post(ctx, HeartbeatPath, req, &resp)
	return resp, err
}

func (c *Client) Poll(ctx context.Context, req PollTaskRequest) (PollTaskResponse, error) {
	var resp PollTaskResponse
	err := c.post(ctx, PollTaskPath, req, &resp)
	return resp, err
}

func (c *Client) ReportResult(ctx context.Context, req ReportResultRequest) (ReportResultResponse, error) {
	var resp ReportResultResponse
	err := c.post(ctx, ReportResultPath, req, &resp)
	return resp, err
}

// ActivationAck reports the outcome of an activate directive back to the
// server, so the activation reconciler learns a directive was not taken and
// can redispatch it instead of leaving the runner stuck until it restarts.
// Same shape as Heartbeat: POST the body, no response payload expected.
func (c *Client) ActivationAck(ctx context.Context, ack ActivationAck) error {
	return c.post(ctx, ActivationAckPath, ack, nil)
}

// ReportMetrics ships one gzip-compressed delimited-protobuf metrics snapshot.
//
// It deliberately does NOT go through post(): that helper JSON-encodes its
// body, and base64'ing protobuf into a JSON string costs a flat 33% for no
// benefit. Identity travels in headers because the body is an opaque protobuf
// stream with nowhere to put a RunnerID field.
//
// The server re-authenticates on every call (HTTP carries no connection
// identity) and derives runner_id from that authenticated identity, so the
// header here is a routing hint the server verifies, not a claim it trusts.
func (c *Client) ReportMetrics(ctx context.Context, runnerID, sessionID string, body []byte) error {
	if runnerID == "" || sessionID == "" {
		return fmt.Errorf("runner protocol %s: runner id and session id are required", ReportMetricsPath)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+ReportMetricsPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", ReportMetricsContentType)
	req.Header.Set("Content-Encoding", "gzip")
	req.Header.Set(RunnerIDHeader, runnerID)
	req.Header.Set(SessionIDHeader, sessionID)
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Bounded read: an error body is diagnostic, not a channel, and this
		// string is logged by the reporter. It must never carry the token, so
		// only the response body is quoted here — never the request headers.
		data, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		return fmt.Errorf("runner protocol %s: status %d: %s",
			ReportMetricsPath, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return nil
}

// RenewLease extends the deadline of a lease this runner already holds, so a
// handler that legitimately runs longer than the engine's default lease TTL is
// not reclaimed and redelivered to a second runner mid-flight.
//
// A refusal (stale token, node no longer running) comes back as
// Renewed=false with a reason, NOT as an error: the caller has to be able to
// tell "you lost the lease, stop working" from "the network is down, retry".
//
// Only the HTTP client implements this. The gRPC client does not, so a
// gRPC-transport runner never renews — the same explicit gap as
// MetricsReportClient and activationAckClient.
func (c *Client) RenewLease(ctx context.Context, req RenewLeaseRequest) (RenewLeaseResponse, error) {
	var resp RenewLeaseResponse
	err := c.post(ctx, RenewLeasePath, req, &resp)
	return resp, err
}

func (c *Client) post(ctx context.Context, path string, body any, out any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}
	if buf.Len() > MaxRunnerRequestBodyBytes {
		return fmt.Errorf("runner protocol %s: %w (%d bytes, limit %d)", path, ErrRunnerRequestTooLarge, buf.Len(), MaxRunnerRequestBodyBytes)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Bounded read: an error body is diagnostic only, so it never needs
		// more than a short prefix.
		data, _ := io.ReadAll(io.LimitReader(resp.Body, maxRunnerErrorBodyBytes))
		if resp.StatusCode == http.StatusRequestEntityTooLarge {
			return fmt.Errorf("runner protocol %s: %w: status %d: %s", path, ErrRunnerRequestTooLarge, resp.StatusCode, strings.TrimSpace(string(data)))
		}
		return fmt.Errorf("runner protocol %s: status %d: %s", path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if out == nil {
		return nil
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, MaxRunnerResponseBodyBytes+1))
	if err != nil {
		return fmt.Errorf("runner protocol %s: read response: %w", path, err)
	}
	if len(data) > MaxRunnerResponseBodyBytes {
		return fmt.Errorf("runner protocol %s: %w (limit %d bytes)", path, ErrRunnerResponseTooLarge, MaxRunnerResponseBodyBytes)
	}
	return json.Unmarshal(data, out)
}
