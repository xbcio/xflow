package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"

	"github.com/xbcio/xflow/backend/providers/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// deadLetterOptions holds the shared flags for the dead-letter CLI. By default
// the CLI talks to the protected management HTTP API (server addr + auth
// token); --break-glass switches to the Redis-direct maintenance path.
type deadLetterOptions struct {
	// API path (default). server is the management API base URL; token is the
	// bearer credential. The token is sent only in the Authorization header —
	// never logged — and cross-namespace executions surface as 404 (the API's
	// IDOR defense).
	server string
	token  string

	// Break-glass path: connect directly to Redis, bypassing the management
	// API and its authz/metric/audit outlet. Requires --break-glass to be set
	// explicitly; emits a prominent stderr warning. The break-glass identity
	// is "cli:breakglass:<user>" so audit can distinguish it from the normal
	// cli path.
	breakGlass bool
	redisAddr  string

	// SQL store DSN for the reconcile subcommand's durable projection. When
	// empty, reconcile runs the projector against the Redis receipt diff and
	// reports unprojected receipts; durability requires a real MySQL DSN.
	mysqlDSN string

	out io.Writer
}

func newDeadLetterCommand(out io.Writer) *cobra.Command {
	opts := &deadLetterOptions{out: out}
	cmd := &cobra.Command{
		Use:   "dead-letter",
		Short: "Inspect and replay durable outbox dead-letter entries",
		Long: `Inspect and replay durable scheduling outbox entries that exceeded the
delivery attempt limit and were moved to dead-letter storage.

By default this command calls the protected management HTTP API
(/v1/management/dead-letters/*) so authorization, metrics, and the durable
SQL receipt projection are owned by the server. Configure --server and
--token (or XFLOW_API_ADDR / XFLOW_API_TOKEN). The token is sent only in
the Authorization header and is never logged; a cross-namespace execution
surfaces as 404 (the API's IDOR defense).

Replay moves an entry atomically back to the ready set; the control plane's
running OutboxDispatcher redelivers it. Replay is activation-safe: it rejects
entries whose node is terminal or whose activation no longer matches the
node's current activation. Replay is idempotent under --request-id: retrying
with the same request-id after a lost response returns already_replayed
with the original audit_id.

The authoritative receipt is written to Redis by the server; the SQL
projection is the durable secondary reconciled against it.

--break-glass switches to a Redis-direct maintenance path that bypasses the
management API (and its authz/metric/audit outlet). It is intended only for
emergencies when the API is unavailable and requires a distinct operator
credential in production; it emits a prominent stderr warning and records a
"cli:breakglass:<user>" operator identity so the break-glass use is
distinguishable in audit.`,
	}
	cmd.PersistentFlags().StringVar(&opts.server, "server", envOr("XFLOW_API_ADDR", ""),
		"Management API base URL, e.g. http://127.0.0.1:8080 (env: XFLOW_API_ADDR). Default path; required unless --break-glass")
	cmd.PersistentFlags().StringVar(&opts.token, "token", envOr("XFLOW_API_TOKEN", ""),
		"Bearer token for the management API (env: XFLOW_API_TOKEN). Sent only in the Authorization header; never logged")
	cmd.PersistentFlags().BoolVar(&opts.breakGlass, "break-glass", false,
		"Bypass the management API and connect directly to Redis (emergency maintenance only)")
	cmd.PersistentFlags().StringVar(&opts.redisAddr, "redis-addr", envOr("XFLOW_REDIS_ADDR", "localhost:6379"),
		"Redis address (break-glass only; env: XFLOW_REDIS_ADDR)")
	cmd.PersistentFlags().StringVar(&opts.mysqlDSN, "mysql-dsn", envOr("XFLOW_MYSQL_DSN", ""),
		"MySQL DSN for the reconcile subcommand's durable projection (env: XFLOW_MYSQL_DSN)")

	cmd.AddCommand(newDeadLetterListCommand(opts))
	cmd.AddCommand(newDeadLetterReplayCommand(opts))
	cmd.AddCommand(newDeadLetterReconcileCommand(opts))
	return cmd
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// deadLetterClient is the transport-agnostic interface shared by the API
// (default) and break-glass (Redis-direct) paths. Both produce the same
// outcome metric + audit projection server-side (API path) or via the
// manager (break-glass path).
type deadLetterClient interface {
	List(ctx context.Context, execID, namespaceName string, page engine.DeadLetterPage) (engine.DeadLetterList, error)
	Replay(ctx context.Context, principal control.DeadLetterReplayPrincipal, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error)
}

// newDeadLetterClient selects the API client by default and the break-glass
// Redis-direct client when --break-glass is set. It emits a prominent stderr
// warning for break-glass so an operator cannot use it silently.
func newDeadLetterClient(opts *deadLetterOptions) (deadLetterClient, func(), error) {
	if opts.breakGlass {
		fmt.Fprintln(os.Stderr, "WARNING: --break-glass bypasses the management API and its authorization/metrics/audit outlet. Use only when the API is unavailable and record a break-glass operator credential.")
		return openBreakGlassDeadLetterClient(opts.redisAddr)
	}
	if opts.server == "" {
		return nil, nil, errors.New("--server (or XFLOW_API_ADDR) is required; alternatively use --break-glass for the Redis-direct maintenance path")
	}
	if opts.token == "" {
		return nil, nil, errors.New("--token (or XFLOW_API_TOKEN) is required for the management API; alternatively use --break-glass")
	}
	return &apiDeadLetterClient{server: strings.TrimRight(opts.server, "/"), token: opts.token}, nil, nil
}

func newDeadLetterListCommand(opts *deadLetterOptions) *cobra.Command {
	var executionID string
	var limit int
	var cursor string
	var namespaceFlag string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List dead-lettered outbox entries for an execution (read-only, paginated)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if executionID == "" {
				return errors.New("--execution is required")
			}
			if err := namespace.Validate(namespace.Namespace(namespaceFlag)); err != nil {
				return fmt.Errorf("invalid --namespace: %w", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, closeFn, err := newDeadLetterClient(opts)
			if err != nil {
				return err
			}
			if closeFn != nil {
				defer closeFn()
			}
			page := engine.DeadLetterPage{Limit: limit, Cursor: cursor}
			list, err := client.List(ctx, executionID, namespaceFlag, page)
			if err != nil {
				return err
			}
			for _, entry := range list.Entries {
				if err := writeJSONLines(opts.out, entry); err != nil {
					return err
				}
			}
			if list.NextCursor != "" {
				if _, err := fmt.Fprintf(opts.out, `{"next_cursor":%q}`+"\n", list.NextCursor); err != nil {
					return err
				}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().IntVar(&limit, "limit", 100, "Maximum entries to return per page (bounded)")
	cmd.Flags().StringVar(&cursor, "cursor", "", "Opaque cursor from a prior page's next_cursor")
	cmd.Flags().StringVar(&namespaceFlag, "namespace", envOr("XFLOW_TENANT", string(namespace.Default)), "Namespace namespace (env: XFLOW_TENANT)")
	return cmd
}

func newDeadLetterReplayCommand(opts *deadLetterOptions) *cobra.Command {
	var executionID, entryID, reason, requestID string
	var namespaceFlag string
	cmd := &cobra.Command{
		Use:   "replay",
		Short: "Replay a dead-lettered entry back to the ready set",
		Long: `Replay moves one dead-lettered entry atomically and activation-safely back
to the ready set so the control plane redelivers it. This is a privileged write
operation: --reason is required and length-bounded; the operator identity is
server-injected from the authenticated principal, not self-reported.

Pass --request-id to make the replay recoverable: if the response is lost,
retrying with the same --request-id returns already_replayed and the original
audit_id, proving the operation happened exactly once.

By default this command calls the protected management API; --break-glass
switches to the Redis-direct maintenance path.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if executionID == "" {
				return errors.New("--execution is required")
			}
			if entryID == "" {
				return errors.New("--entry is required")
			}
			if reason == "" {
				return errors.New("--reason is required (record why this entry is being replayed)")
			}
			if err := namespace.Validate(namespace.Namespace(namespaceFlag)); err != nil {
				return fmt.Errorf("invalid --namespace: %w", err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			client, closeFn, err := newDeadLetterClient(opts)
			if err != nil {
				return err
			}
			if closeFn != nil {
				defer closeFn()
			}
			principal := newDeadLetterPrincipal(opts, namespaceFlag)
			res, err := client.Replay(ctx, principal, engine.ReplayDeadLetterRequest{
				ExecutionID: types.ExecutionID(executionID),
				EntryID:     entryID,
				RequestID:   requestID,
				Reason:      reason,
			})
			if err != nil {
				return err
			}
			return writeJSONLines(opts.out, replayResultJSON(res))
		},
	}
	cmd.Flags().StringVar(&executionID, "execution", "", "Execution ID (required)")
	cmd.Flags().StringVar(&entryID, "entry", "", "Dead-letter entry ID (required)")
	cmd.Flags().StringVar(&reason, "reason", "", "Reason for replay (required, length-bounded)")
	cmd.Flags().StringVar(&requestID, "request-id", "", "Idempotency key; retry with the same value to recover a lost response")
	cmd.Flags().StringVar(&namespaceFlag, "namespace", envOr("XFLOW_TENANT", string(namespace.Default)), "Namespace namespace (env: XFLOW_TENANT)")
	return cmd
}

// newDeadLetterPrincipal builds the principal the CLI injects. The API path
// does not use the principal's scopes (the server authenticates the bearer
// token and injects the real principal); the break-glass path uses the
// "cli:breakglass:<user>" subject so audit can distinguish break-glass use
// from the normal cli path.
func newDeadLetterPrincipal(opts *deadLetterOptions, namespaceFlag string) control.DeadLetterReplayPrincipal {
	if opts.breakGlass {
		return control.DeadLetterReplayPrincipal{
			Subject:   "cli:breakglass:" + envOr("USER", "unknown"),
			Namespace: namespaceFlag,
			Scopes:    []string{control.ScopeDeadLetterReplay},
		}
	}
	// API path: the server injects the real principal from the bearer token.
	return control.DeadLetterReplayPrincipal{
		Subject:   "",
		Namespace: namespaceFlag,
		Scopes:    []string{control.ScopeDeadLetterReplay},
	}
}

func replayResultJSON(res engine.ReplayDeadLetterResult) map[string]any {
	return map[string]any{
		"outcome":     string(res.Outcome),
		"audit_id":    res.AuditID,
		"execution":   string(res.ExecutionID),
		"node":        res.NodeID,
		"activation":  res.ActivationID,
		"occurred_at": time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// apiDeadLetterClient talks to the protected management HTTP API. The token
// is sent only in the Authorization header; it is never logged. Cross-namespace
// executions surface as 404 (the API's IDOR defense).
type apiDeadLetterClient struct {
	server string
	token  string
}

func (c *apiDeadLetterClient) List(ctx context.Context, execID, namespaceName string, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	u := fmt.Sprintf("%s/v1/management/dead-letters/%s?limit=%d", c.server, execID, page.Limit)
	if page.Cursor != "" {
		u += "&cursor=" + page.Cursor
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return engine.DeadLetterList{}, err
	}
	c.setAuth(req)
	req.Header.Set("X-XFlow-Namespace", namespaceName)
	var resp deadLetterListResponse
	if err := c.do(req, &resp); err != nil {
		return engine.DeadLetterList{}, err
	}
	return engine.DeadLetterList{Entries: resp.Entries, NextCursor: resp.NextCursor}, nil
}

func (c *apiDeadLetterClient) Replay(ctx context.Context, _ control.DeadLetterReplayPrincipal, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	body, _ := json.Marshal(deadLetterReplayRequest{
		EntryID:   req.EntryID,
		RequestID: req.RequestID,
		Reason:    req.Reason,
	})
	u := fmt.Sprintf("%s/v1/management/dead-letters/%s/replay", c.server, req.ExecutionID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return engine.ReplayDeadLetterResult{}, err
	}
	c.setAuth(httpReq)
	httpReq.Header.Set("Content-Type", "application/json")
	var resp deadLetterReplayResponse
	if err := c.do(httpReq, &resp); err != nil {
		return engine.ReplayDeadLetterResult{}, err
	}
	return engine.ReplayDeadLetterResult{
		Outcome:      engine.DeadLetterReplayOutcome(resp.Outcome),
		AuditID:      resp.AuditID,
		ExecutionID:  types.ExecutionID(resp.ExecutionID),
		NodeID:       resp.NodeID,
		ActivationID: resp.ActivationID,
	}, nil
}

// setAuth sets the Authorization header. The token is never logged anywhere;
// the do helper surfaces only the URL path (no query) in error messages.
func (c *apiDeadLetterClient) setAuth(req *http.Request) {
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
}

// do executes an HTTP request and decodes the spec §3.1 envelope's data field
// into out on success. A non-2xx response (or success:false) is surfaced as an
// error carrying only the status code — the body is NOT propagated, matching
// the CLI's long-standing discipline of never echoing response internals.
//
// The management API envelopes its success bodies (Step 1 decision A of the
// api-specification rollout): the cursor-paginated {entries,next_cursor} and
// the replay-result shapes ride inside envelope.data. Before this, do() decoded
// the body directly into out; enveloping would have decoded zero values
// silently (no error) — the exact failure mode the envelope rollout must not
// produce. This helper is the CLI's half of the (A) decision.
//
// envelope.Success is *bool (not bool) on purpose: Go's zero value makes a
// missing success key indistinguishable from success:false, and the two need
// different diagnostics. A missing key means the response was never enveloped
// (notEnvelopeError) — the server did not adopt §3.1; a present success:false
// at 2xx is a §4.1 violation (httpStatusError). Collapsing them would point the
// operator at a phantom failure report for a missing envelope.
//
// Both call sites (List, Replay) pass a non-nil out and expect a payload; there
// is no legitimate payload-less 2xx. A success envelope whose data is absent or
// null is a field-name drift or a server omission — surfaced as missingDataError
// rather than a silent zero-value typed target.
func (c *apiDeadLetterClient) do(req *http.Request, out any) error {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("call %s %s: %w", req.Method, redactURL(req.URL.String()), err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, rerr := io.ReadAll(io.LimitReader(resp.Body, maxManagementResponseBytes+1))
	if rerr != nil {
		return fmt.Errorf("read response from %s %s: %w", req.Method, redactURL(req.URL.String()), rerr)
	}
	if len(raw) > maxManagementResponseBytes {
		return fmt.Errorf("response from %s %s exceeds %d bytes", req.Method, redactURL(req.URL.String()), maxManagementResponseBytes)
	}
	// A non-2xx is a failure envelope (or, on the entry-seed face, a bare body —
	// but this client only talks to the management face, which envelopes). The
	// CLI maps outcome from the status code alone; it never reads the error body
	// (§3.4 discipline: the body may carry server internals).
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return httpStatusError{status: resp.StatusCode, path: redactURL(req.URL.String())}
	}
	if out == nil || len(raw) == 0 {
		return nil
	}
	// Decode the §3.1 envelope, then unmarshal data into the typed target.
	// data is read as json.RawMessage because envelope.Data is `any` — a direct
	// unmarshal yields map[string]any, which cannot be re-decoded into a struct.
	var env struct {
		Success *bool           `json:"success"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
		TraceID string          `json:"trace_id"`
	}
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("decode envelope from %s %s: %w", req.Method, redactURL(req.URL.String()), err)
	}
	if env.Success == nil {
		// The success key is absent: the response was never enveloped. This is
		// NOT a §4.1 success:false@2xx violation — surface it as a distinct,
		// semantically correct error so the operator does not chase a phantom
		// failure report. The body is not included (§3.5 discipline).
		return notEnvelopeError{method: req.Method, path: redactURL(req.URL.String())}
	}
	if !*env.Success {
		// A 2xx with success:false present is a spec §4.1 violation; surface the
		// status code without the body so the operator gets a stable, non-leaking
		// error.
		return httpStatusError{status: resp.StatusCode, path: redactURL(req.URL.String())}
	}
	// A success envelope with no data field (absent or null) is a field-name
	// drift or a server omission. Both call sites expect a payload, so this must
	// not silently yield a zero-value typed target — return missingDataError.
	if len(env.Data) == 0 || string(env.Data) == "null" {
		return missingDataError{method: req.Method, path: redactURL(req.URL.String())}
	}
	if err := json.Unmarshal(env.Data, out); err != nil {
		return fmt.Errorf("decode data from %s %s: %w", req.Method, redactURL(req.URL.String()), err)
	}
	return nil
}

// maxManagementResponseBytes bounds a management API response read. The dead-
// letter list is the largest body; a single page is bounded server-side by the
// list limit, and replay results are small. This is a defense against a
// misbehaving server, not a functional limit.
const maxManagementResponseBytes = 4 << 20

// httpStatusError carries the HTTP status of a non-2xx management API
// response. It is the CLI's signal to map replay outcomes (404 → cross-namespace
// or missing; 403 → unauthorized; 400 → invalid request) without leaking the
// body or the Authorization header.
type httpStatusError struct {
	status int
	path   string
}

func (e httpStatusError) Error() string {
	switch e.status {
	case http.StatusNotFound:
		return "execution or entry not found (status 404)"
	case http.StatusForbidden:
		return "replay unauthorized (status 403)"
	case http.StatusBadRequest:
		return "replay rejected: invalid request (status 400)"
	default:
		return fmt.Sprintf("management API %s: status %d", e.path, e.status)
	}
}

// notEnvelopeError signals that a 2xx management API response was not the spec
// §3.1 envelope — the success field was absent. It is distinct from a
// success:false envelope (httpStatusError): a missing success key means the
// server never enveloped the response, not that it reported failure. The body
// is not carried (§3.5); only the method and redacted path surface so an
// operator can locate the offending route. errors.As distinguishes it from
// httpStatusError so tests and callers do not collapse the two diagnoses.
type notEnvelopeError struct {
	method string
	path   string
}

func (e notEnvelopeError) Error() string {
	return fmt.Sprintf("%s %s: response is not the spec §3.1 envelope (success field missing)", e.method, e.path)
}

// missingDataError signals that a 2xx success envelope carried no data field
// (either absent or null). Every do() call site passes a non-nil out and
// expects a payload, so a success envelope without data is a field-name drift
// or a server omission — never a silent zero-value success. The body is not
// carried (§3.5); only the method and redacted path surface.
type missingDataError struct {
	method string
	path   string
}

func (e missingDataError) Error() string {
	return fmt.Sprintf("%s %s: success envelope missing data field", e.method, e.path)
}

// redactURL returns the URL path without the query. The query may carry the
// cursor or namespace but never the token (the token lives only in the header);
// redacting keeps error messages stable and free of caller-supplied params.
func redactURL(u string) string {
	if i := strings.IndexByte(u, '?'); i >= 0 {
		return u[:i]
	}
	return u
}

// breakGlassDeadLetterClient talks directly to Redis via a distributed backend
// (no consumer/dispatcher), wrapping the DeadLetterManager so the break-glass
// path still routes through the single outlet (validation + metrics + audit).
// It is the emergency fallback for when the management API is unavailable. The
// manager's audit sink is the stderr projection (no durable SQL projection in
// break-glass) and the metrics observer is nil — the server's metric outlet is
// bypassed, which is exactly why break-glass requires a distinct operator
// credential and is recorded as "cli:breakglass:<user>".
type breakGlassDeadLetterClient struct {
	mgr     *control.DeadLetterManager
	closeFn func()
}

func openBreakGlassDeadLetterClient(addr string) (deadLetterClient, func(), error) {
	b, err := distributed.New(addr, nil, distributed.WithConsumer(false))
	if err != nil {
		return nil, nil, fmt.Errorf("connect redis %q: %w", addr, err)
	}
	store, ok := b.State().(engine.DeadLetterStore)
	if !ok {
		closeRedis(b)
		return nil, nil, fmt.Errorf("StateStore %T does not implement DeadLetterStore; the configured backend cannot serve dead-letter operations", b.State())
	}
	audit := control.NewStdoutDeadLetterAuditSink(func(line string) {
		fmt.Fprintln(os.Stderr, line)
	})
	// metrics is intentionally nil: break-glass bypasses the server's metric
	// outlet. The Redis receipt remains authoritative; audit is the stderr
	// projection only. This is why break-glass requires a distinct credential
	// and an explicit operator record in production.
	mgr := control.NewDeadLetterManager(store, nil, audit)
	closeFn := func() { closeRedis(b) }
	return &breakGlassDeadLetterClient{mgr: mgr, closeFn: closeFn}, closeFn, nil
}

func (c *breakGlassDeadLetterClient) List(ctx context.Context, execID, namespaceName string, page engine.DeadLetterPage) (engine.DeadLetterList, error) {
	ctx = namespace.WithNamespace(ctx, namespace.Namespace(namespaceName))
	return c.mgr.List(ctx, types.ExecutionID(execID), page)
}

func (c *breakGlassDeadLetterClient) Replay(ctx context.Context, principal control.DeadLetterReplayPrincipal, req engine.ReplayDeadLetterRequest) (engine.ReplayDeadLetterResult, error) {
	ctx = namespace.WithNamespace(ctx, namespace.Namespace(principal.Namespace))
	return c.mgr.Replay(ctx, principal, req)
}

// closeRedis best-effort closes the backend's Redis client. The Cmdable
// interface does not expose Close, so assert to the concrete *redis.Client.
func closeRedis(b *distributed.Backend) {
	if client, ok := b.RedisClient().(*redis.Client); ok {
		_ = client.Close()
	}
}

func writeJSONLines(w io.Writer, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(data))
	return err
}

// deadLetterListResponse mirrors the management API's list JSON shape.
type deadLetterListResponse struct {
	Entries    []engine.OutboxEntry `json:"entries"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

// deadLetterReplayRequest is the request body for replay.
type deadLetterReplayRequest struct {
	EntryID   string `json:"entry_id"`
	RequestID string `json:"request_id"`
	Reason    string `json:"reason"`
}

// deadLetterReplayResponse is the JSON shape for a replay result.
type deadLetterReplayResponse struct {
	Outcome      string `json:"outcome"`
	AuditID      string `json:"audit_id,omitempty"`
	ExecutionID  string `json:"execution_id,omitempty"`
	NodeID       string `json:"node_id,omitempty"`
	ActivationID string `json:"activation_id,omitempty"`
}
