package apiserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// workflowControlModule mounts the workflow/control HTTP API (submit, invoke,
// inspect, signal, revoke-signal, cancel, wait). Stage 3 migrated these routes
// out of control.Server so the control plane serves only the runner protocol;
// the control API now lives behind the apiserver module boundary. The module
// delegates to control.EngineFacade (the *engine.Engine behind the
// ControlPlane) for every engine call.
type workflowControlModule struct {
	authzHolder
	cp   *control.ControlPlane
	eng  control.EngineFacade
	auth WorkflowAuthenticator
	log  engine.Logger
	// tracer instruments submit/invoke so their span context is persisted on
	// the execution snapshot (via engine.WithTraceCarrier) and later inherited
	// by the asynchronous dispatch span — closing the submit→dispatch trace
	// causality gap. NoopTracer when tracing is disabled.
	tracer tracing.Tracer
}

func newWorkflowControlModule(cp *control.ControlPlane, auth WorkflowAuthenticator, log engine.Logger, tracer tracing.Tracer) *workflowControlModule {
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	return &workflowControlModule{cp: cp, eng: cp.Engine(), auth: auth, log: log, tracer: tracer}
}

func (m *workflowControlModule) Name() string { return "workflow-control" }

func (m *workflowControlModule) RegisterHTTP(mux *http.ServeMux) {
	// B3 authz path: when a PrincipalAuthenticator is configured, the module
	// enforces resource/operation-level authorization with append-only audit
	// before each handler runs. Without it, it falls back to the legacy
	// bearer-only WorkflowAuthenticator (fail-closed if RequireWorkflowAuth).
	if m.principalAuth != nil {
		m.registerAuthzRoutes(mux)
		return
	}
	auth := m.auth
	if auth == nil {
		auth = DisabledWorkflowAuth{}
	}
	wrap := func(op string, h http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if err := auth.AuthenticateRequest(r); err != nil {
				if m.log != nil {
					m.log.Error("workflow_api_auth_denied",
						"op", op, "remote_addr", r.RemoteAddr, "err", err)
				}
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
			h(w, r)
		}
	}
	mux.HandleFunc("/v1/workflows", wrap("submit_workflow", m.handleSubmitWorkflow))
	mux.HandleFunc("/v1/workflows/invoke", wrap("invoke_workflow", m.handleInvoke))
	// /v1/executions (no trailing slash) is the entry-seed endpoint; the
	// /v1/executions/ subtree below is the per-execution GET/signal/cancel/wait
	// surface. ServeMux treats the two patterns as distinct.
	mux.HandleFunc("/v1/executions", wrap("seed_execution", m.handleSeedExecution))
	mux.HandleFunc("/v1/executions/", wrap("execution", m.handleExecution))
}

// registerAuthzRoutes mounts the control routes behind the B3 authz wrapper:
// authenticate → resolve operation/resource → authorize (default-deny) → audit
// admission → handler → audit outcome. Mutations fail-closed if the admission
// audit cannot be persisted.
//
// Task 8 blocker 1: the /v1/executions/ subtree is NOT blanket-wrapped as
// execution.read. The sub-path + verb are resolved to the correct (operation,
// isMutation) BEFORE the authz wrapper runs, so signal/revoke/cancel each get
// their own operation + mutation admission audit (fail-closed) + outcome audit:
//   - GET  /v1/executions/{id}            → execution.read      (non-mutation)
//   - GET  /v1/executions/{id}/wait       → execution.read      (non-mutation)
//   - POST /v1/executions/{id}/signal     → execution.signal    (mutation)
//   - POST /v1/executions/{id}/revoke-signal → execution.revoke (mutation)
//   - POST /v1/executions/{id}/cancel     → execution.cancel    (mutation)
//
// An unknown verb resolves to ok=false → 404 (default-deny, no existence leak).
func (m *workflowControlModule) registerAuthzRoutes(mux *http.ServeMux) {
	authz := m.authzWrap
	mux.HandleFunc("/v1/workflows", authz(OpWorkflowCreate, true, m.handleSubmitWorkflow, newExecutionIDResolver()))
	mux.HandleFunc("/v1/workflows/invoke", authz(OpWorkflowInvoke, true, m.handleInvoke, newExecutionIDResolver()))
	// Entry-seed endpoint (exact path, no trailing slash). Distinct from the
	// /v1/executions/ subtree. The authz wrapper injects the principal's
	// namespace into the request context; handleSeedExecution reads it via
	// namespace.FromContext — never from the client body.
	mux.HandleFunc("/v1/executions", authz(OpExecutionSeed, true, m.handleSeedExecution, newExecutionIDResolver()))
	mux.HandleFunc("/v1/executions/", m.authzWrapResolved(m.handleExecution, resolveExecutionRoute))
}

// newExecutionIDResolver is the resource resolver for the workflow create/invoke
// routes. Unlike the /v1/executions/ subtree — which resolves the targeted
// execution id from the path — create/invoke have no path id, so the resolver
// pre-allocates a fresh execution id (R3.1). That id is stamped onto the
// admission audit row by authzWrap and, via engine.WithExecutionID injected by
// the same wrapper, reused by engine Submit/Invoke, so the audit row and the
// persisted execution share one id (closing the audit↔execution correlation
// gap that left reconcile Probe reading an empty ExecutionID).
//
// resource/workflowID/namespace stay empty: create/invoke authz was decided on
// operation alone before this change, and this resolver only supplies the
// correlation id, not authz inputs.
func newExecutionIDResolver() func(*http.Request) (string, string, string, string) {
	return func(*http.Request) (string, string, string, string) {
		return "", "", string(engine.NewExecutionID()), ""
	}
}

// resolveExecutionRoute parses /v1/executions/<id>[/verb] + method and returns
// the stable operation + mutation flag for the authz wrapper. ok=false for an
// unknown verb or missing id (the wrapper answers 404, default-deny). The
// resource is execution-scoped so the audit row carries the targeted execution
// id; ResourceNamespace is left empty — the authoritative IDOR defense is the
// namespace-scoped store read (the authz wrapper injects the principal's namespace
// into the request context so handleInspect/handleSignal read from the
// principal's namespace namespace; a cross-namespace execID resolves to not-found →
// 404, never leaking existence).
func resolveExecutionRoute(r *http.Request) (resolvedRoute, bool) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/executions/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return resolvedRoute{}, false
	}
	execID := parts[0]
	resource := "execution/" + execID
	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		return resolvedRoute{operation: OpExecutionRead, resource: resource, executionID: execID, isMutation: false}, true
	case len(parts) == 2 && parts[1] == "wait" && r.Method == http.MethodGet:
		return resolvedRoute{operation: OpExecutionRead, resource: resource + "/wait", executionID: execID, isMutation: false}, true
	case len(parts) == 2 && parts[1] == "signal" && r.Method == http.MethodPost:
		return resolvedRoute{operation: OpExecutionSignal, resource: resource + "/signal", executionID: execID, isMutation: true}, true
	case len(parts) == 2 && parts[1] == "revoke-signal" && r.Method == http.MethodPost:
		return resolvedRoute{operation: OpExecutionRevoke, resource: resource + "/revoke-signal", executionID: execID, isMutation: true}, true
	case len(parts) == 2 && parts[1] == "cancel" && r.Method == http.MethodPost:
		return resolvedRoute{operation: OpExecutionCancel, resource: resource + "/cancel", executionID: execID, isMutation: true}, true
	default:
		return resolvedRoute{}, false
	}
}

// auditDeny / auditReconcile / statusRecorder live in authz_wrap.go, shared
// with managementModule via the embedded authzHolder.

type errorResponse struct {
	Error string `json:"error"`
}

type submitWorkflowRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params,omitempty"`
}

type submitWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

type invokeRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Entry    string             `json:"entry"`
	Input    map[string]any     `json:"input,omitempty"`
}

type invokeResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

type signalRequest struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data,omitempty"`
}

type waitTimeoutResponse struct {
	ExecutionID types.ExecutionID     `json:"execution_id"`
	Status      types.ExecutionStatus `json:"status"`
	TimedOut    bool                  `json:"timed_out"`
}

func (m *workflowControlModule) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req submitWorkflowRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Workflow == nil {
		writeError(w, http.StatusBadRequest, "workflow is required")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// xflow.workflow.submit starts the inbound trace for a workflow execution.
	// Its SpanContext is persisted on the execution snapshot (via
	// engine.WithTraceCarrier) so the later, asynchronous dispatch span can
	// inherit it as a real W3C remote parent — closing the submit→dispatch
	// causality gap without faking a parent from trace_id/span_id strings.
	tracer := m.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	ctx, span := tracer.Start(r.Context(), "xflow.workflow.submit")
	defer span.End()
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	// Attach the original workflow definition so the durable SQL execution
	// projection (buildExecutionRecord) can persist workflow_def (NOT NULL).
	// Without this, production mode with a SQL store fails submit with a
	// NOT-NULL violation on xflow_executions.workflow_def.
	ctx = engine.WithWorkflowDef(ctx, req.Workflow)
	id, err := m.eng.Submit(ctx, g, req.Params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, submitWorkflowResponse{ExecutionID: id})
}

func (m *workflowControlModule) handleInvoke(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req invokeRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Workflow == nil {
		writeError(w, http.StatusBadRequest, "workflow is required")
		return
	}
	if req.Entry == "" {
		writeError(w, http.StatusBadRequest, "entry is required")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// xflow.workflow.invoke mirrors submit for the explicit-entry path. Its
	// SpanContext is persisted for asynchronous dispatch causality.
	tracer := m.tracer
	if tracer == nil {
		tracer = tracing.NoopTracer{}
	}
	ctx, span := tracer.Start(r.Context(), "xflow.workflow.invoke", "entry", req.Entry)
	defer span.End()
	ctx = engine.WithTraceCarrier(ctx, tracing.InjectCarrier(ctx))
	// Attach the original workflow definition so the durable SQL execution
	// projection can persist workflow_def (see handleSubmitWorkflow).
	ctx = engine.WithWorkflowDef(ctx, req.Workflow)
	id, err := m.eng.Invoke(ctx, g, req.Entry, req.Input)
	if err != nil {
		// An unknown entry node is a client error (400), not a 404: the
		// missing resource is the entry in the submitted graph, not an
		// execution in the store.
		if errors.Is(err, engine.ErrEntryNotFound) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, invokeResponse{ExecutionID: id})
}

// handleSeedExecution serves POST /v1/executions: it atomically seeds an
// execution from an entry-unit (single node or group node) result. The
// authoritative namespace is injected into the request context by the authz
// wrapper (from the authenticated principal) and resolved server-side in the
// control Core — it is NEVER read from the request body, so a forged or
// cross-namespace admission key fails closed. The ResultHash is recomputed
// server-side from the request's outcome + exits; a client-supplied hash is not
// trusted.
func (m *workflowControlModule) handleSeedExecution(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.SeedExecutionRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.AdmissionKey == "" || req.WorkflowID == "" || req.EntryUnitID == "" || req.Outcome == "" {
		writeError(w, http.StatusBadRequest, "admission_key, workflow_id, entry_unit_id and outcome are required")
		return
	}

	exits := make([]engine.BoundaryExit, 0, len(req.Exits))
	for _, ex := range req.Exits {
		exits = append(exits, engine.BoundaryExit{
			NodeName: ex.NodeName,
			Port:     ex.Port,
			Data:     ex.Data,
		})
	}
	outcome := engine.GroupOutcome(req.Outcome)

	engReq := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    engine.AdmissionKey(req.AdmissionKey),
		WorkflowID:      types.WorkflowID(req.WorkflowID),
		WorkflowVersion: req.WorkflowVersion,
		EntryUnitID:     req.EntryUnitID,
		Outcome:         outcome,
		Exits:           exits,
		Error:           req.Error,
		Generation:      req.Generation,
		// ResultHash is computed server-side; the client cannot supply it.
		ResultHash: engine.ComputeResultHash(outcome, exits),
	}

	// Route through the ControlPlane's Core so the server-side namespace
	// resolution and generation fence (spec §11.6) always apply. Namespace is
	// taken from the request context (injected by the authz wrapper), not the
	// body. Fall back to the raw engine only when no ControlPlane is wired
	// (unit tests with a fake facade); that path has no generation fence.
	var resp engine.SeedExecutionFromEntryResponse
	var err error
	if m.cp != nil {
		resp, err = m.cp.SeedExecutionFromEntry(r.Context(), engReq)
	} else {
		resp, err = m.eng.SeedExecutionFromEntry(r.Context(), engReq)
	}
	if err != nil {
		// A stale activation generation for a not-yet-accepted admission key is a
		// fencing rejection, not an internal fault — map it to 409 so the runner
		// can distinguish "you lost the activation" from a transient server error.
		if errors.Is(err, control.ErrStaleGeneration) {
			writeError(w, http.StatusConflict, "stale_generation")
			return
		}
		if m.log != nil {
			m.log.Error("seed_execution_failed", "err", err)
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	if resp.State == engine.AdmissionStateConflict {
		writeJSON(w, http.StatusConflict, protocol.SeedExecutionResponse{
			State:       "conflict",
			ExecutionID: string(resp.ExecutionID),
		})
		return
	}
	writeJSON(w, http.StatusOK, protocol.SeedExecutionResponse{
		State:       string(resp.State),
		ExecutionID: string(resp.ExecutionID),
		Duplicate:   resp.Duplicate,
	})
}

func (m *workflowControlModule) handleExecution(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/executions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	id := types.ExecutionID(parts[0])
	if len(parts) == 1 && r.Method == http.MethodGet {
		m.handleInspect(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "signal" {
		m.handleSignal(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		m.handleCancel(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "revoke-signal" {
		m.handleRevokeSignal(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "wait" && r.Method == http.MethodGet {
		m.handleWait(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "route not found")
}

func (m *workflowControlModule) handleInspect(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	detail, err := m.eng.Inspect(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (m *workflowControlModule) handleSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req signalRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := m.eng.DeliverSignal(r.Context(), id, req.Name, req.Data); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func (m *workflowControlModule) handleCancel(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if err := m.eng.Cancel(r.Context(), id); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func (m *workflowControlModule) handleRevokeSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	// Body is optional; a JSON {"name":...} overrides the query parameter.
	name := r.URL.Query().Get("name")
	if r.ContentLength != 0 {
		var req signalRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		if req.Name != "" {
			name = req.Name
		}
	} else {
		_ = r.Body.Close()
	}
	if name == "" {
		writeError(w, http.StatusBadRequest, "name is required")
		return
	}
	if err := m.eng.RevokeSignal(r.Context(), id, name); err != nil {
		if errors.Is(err, engine.ErrSignalConsumed) {
			writeError(w, http.StatusConflict, "signal already consumed or not found")
			return
		}
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"revoked": true})
}

// handleWait long-polls an execution until it reaches a terminal state or the
// timeout elapses. It uses http.ResponseController to extend the connection's
// write deadline beyond the server's default WriteTimeout so a long poll does
// not get cut off mid-flight. The poll timeout is capped at 10 minutes.
func (m *workflowControlModule) handleWait(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	timeout := parseWaitTimeout(r)
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Now().Add(timeout))

	ctx := r.Context()
	deadline := time.Now().Add(timeout)
	const pollInterval = 200 * time.Millisecond

	for {
		detail, err := m.eng.Inspect(ctx, id)
		if err != nil {
			writeEngineError(w, err)
			return
		}
		if types.IsTerminalExecutionStatus(detail.Status) {
			writeJSON(w, http.StatusOK, detail)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			writeJSON(w, http.StatusAccepted, waitTimeoutResponse{
				ExecutionID: id,
				Status:      detail.Status,
				TimedOut:    true,
			})
			return
		}
		wait := remaining
		if wait > pollInterval {
			wait = pollInterval
		}
		select {
		case <-ctx.Done():
			writeJSON(w, http.StatusAccepted, waitTimeoutResponse{
				ExecutionID: id,
				Status:      detail.Status,
				TimedOut:    true,
			})
			return
		case <-time.After(wait):
		}
	}
}

// parseWaitTimeout resolves the ?timeout= query parameter for handleWait. The
// default is 5s when absent or unparseable; the value is capped at 10m.
func parseWaitTimeout(r *http.Request) time.Duration {
	const (
		defaultWait = 5 * time.Second
		maxWait     = 10 * time.Minute
	)
	q := r.URL.Query().Get("timeout")
	if q == "" {
		return defaultWait
	}
	d, err := time.ParseDuration(q)
	if err != nil || d <= 0 {
		return defaultWait
	}
	if d > maxWait {
		return maxWait
	}
	return d
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer func() { _ = r.Body.Close() }()
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON")
		return false
	}
	return true
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// writeEngineError maps typed engine/store errors to HTTP responses. Every
// unclassified failure is collapsed to a generic 500 message — the underlying
// error (Redis text, internal paths, backend details) must never reach a client.
func writeEngineError(w http.ResponseWriter, err error) {
	if errors.Is(err, engine.ErrExecutionInactive) ||
		errors.Is(err, engine.ErrExecutionNotFound) ||
		errors.Is(err, store.ErrNotFound) {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, "internal server error")
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
