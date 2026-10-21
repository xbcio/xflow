package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// managementOptions holds the flags shared by commands that call the protected
// management HTTP API. The token is sent only in the Authorization header and
// never appears in output, URLs, or error messages.
type managementOptions struct {
	server string
	token  string
	out    io.Writer
}

// addManagementFlags registers the shared --server/--token pair with the same
// environment fallbacks and wording as the dead-letter command.
func addManagementFlags(cmd *cobra.Command, opts *managementOptions) {
	cmd.Flags().StringVar(&opts.server, "server", envOr("XFLOW_API_ADDR", ""),
		"Management API base URL, e.g. http://127.0.0.1:8080 (env: XFLOW_API_ADDR)")
	cmd.Flags().StringVar(&opts.token, "token", envOr("XFLOW_API_TOKEN", ""),
		"Bearer token for the management API (env: XFLOW_API_TOKEN). Sent only in the Authorization header; never logged")
}

// newManagementClient validates the required connection flags and builds a
// client. Both flags are required: unlike the dead-letter command there is no
// break-glass path for these operations, so a missing value is a usage error
// rather than a fallback.
func newManagementClient(opts *managementOptions) (*managementClient, error) {
	if opts.server == "" {
		return nil, errors.New("--server (or XFLOW_API_ADDR) is required for the management API")
	}
	if opts.token == "" {
		return nil, errors.New("--token (or XFLOW_API_TOKEN) is required for the management API")
	}
	return &managementClient{
		server:     strings.TrimRight(opts.server, "/"),
		token:      opts.token,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

// managementClient is a minimal JSON-over-HTTP client for the user-facing
// management API. It speaks the spec §3.1 envelope and maps HTTP statuses to
// stable, actionable errors without echoing response internals.
type managementClient struct {
	server     string
	token      string
	httpClient *http.Client
}

// managementEnvelope mirrors the spec §3.1 response envelope. Success is a
// pointer so a missing key (a response that was never enveloped) is
// distinguishable from an explicit success:false.
type managementEnvelope struct {
	Success *bool           `json:"success"`
	Code    string          `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data"`
	TraceID string          `json:"trace_id"`
}

// do executes one management API call. A non-2xx response (or a 2xx
// success:false envelope) is surfaced as managementAPIError carrying only the
// status and the stable business code — never the response body, which may
// contain server internals. On success the envelope's data field is decoded
// into out; a nil out means the caller expects no payload.
//
// The body is bounded by maxManagementResponseBytes, the same defense the
// dead-letter client uses, so a misbehaving server cannot exhaust CLI memory.
func (c *managementClient) do(ctx context.Context, method, path string, body any, headers map[string]string, out any) error {
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encode request body for %s %s: %w", method, redactURL(path), err)
		}
	}
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.server+path, reader)
	if err != nil {
		return fmt.Errorf("build request %s %s: %w", method, redactURL(path), err)
	}
	// The token travels only in this header. It is never placed in the URL, the
	// body, or an error message.
	req.Header.Set("Authorization", "Bearer "+c.token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("call %s %s: %w", method, redactURL(path), err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, maxManagementResponseBytes+1))
	if rerr != nil {
		return fmt.Errorf("read response from %s %s: %w", method, redactURL(path), rerr)
	}
	if len(raw) > maxManagementResponseBytes {
		return fmt.Errorf("response from %s %s exceeds %d bytes", method, redactURL(path), maxManagementResponseBytes)
	}

	var env managementEnvelope
	// Parsing is deliberately lenient: a non-JSON failure body still yields a
	// status-based error, and the body itself is never propagated.
	_ = json.Unmarshal(raw, &env)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return managementAPIError{status: resp.StatusCode, code: env.Code, path: redactURL(path)}
	}
	if env.Success == nil {
		return notEnvelopeError{method: method, path: redactURL(path)}
	}
	if !*env.Success {
		return managementAPIError{status: resp.StatusCode, code: env.Code, path: redactURL(path)}
	}
	if out == nil {
		return nil
	}
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return missingDataError{method: method, path: redactURL(path)}
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("decode data from %s %s: %w", method, redactURL(path), err)
	}
	return nil
}

// managementAPIError carries a failed management response. It keeps the HTTP
// status and the stable business code (e.g. runner_not_found,
// idempotency_key_reused) — both documented public identifiers — and never the
// raw body or the Authorization header.
type managementAPIError struct {
	status int
	code   string
	path   string
}

func (e managementAPIError) Error() string {
	var hint string
	switch e.status {
	case http.StatusBadRequest:
		hint = "the request was rejected; check the command arguments"
	case http.StatusUnauthorized:
		hint = "the token was rejected; check --token"
	case http.StatusForbidden:
		hint = "the token lacks a required scope for this operation"
	case http.StatusNotFound:
		hint = "the resource was not found; check the id and that the token's namespace owns it"
	case http.StatusConflict:
		hint = "the request conflicts with the current state; a drain/resume retry must reuse its original --request-id"
	case http.StatusNotImplemented:
		if e.code == "runner_listing_unsupported" {
			hint = "this backend cannot enumerate runners; use 'runner get <id>' for a known runner"
		} else {
			hint = "this deployment does not support this operation"
		}
	default:
		hint = "the management API returned an unexpected status"
	}
	if e.code != "" {
		return fmt.Sprintf("management API %s: status %d (%s): %s", e.path, e.status, e.code, hint)
	}
	return fmt.Sprintf("management API %s: status %d: %s", e.path, e.status, hint)
}

// newManagementRequestID generates the idempotency key for a drain or resume
// when the operator does not supply one. The shape satisfies the server's
// X-Request-Id validation set (alphanumerics plus dot, underscore, hyphen,
// 1..128 characters).
func newManagementRequestID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate request id: %w", err)
	}
	return "cli-" + hex.EncodeToString(raw[:]), nil
}

// validateOutputFormat bounds -o to the two formats the commands implement.
func validateOutputFormat(format string) error {
	switch format {
	case "table", "json":
		return nil
	default:
		return fmt.Errorf("--output must be \"table\" or \"json\", got %q", format)
	}
}

// writeRawJSON writes server-provided JSON unchanged. Printing the raw data
// keeps -o json faithful to the API field set instead of re-encoding a local
// subset.
func writeRawJSON(w io.Writer, raw json.RawMessage) error {
	if len(raw) == 0 {
		raw = json.RawMessage("null")
	}
	_, err := fmt.Fprintf(w, "%s\n", raw)
	return err
}
