package apiserver

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// workflowControlModule mounts the workflow/control HTTP API (submit, invoke,
// inspect, signal, revoke-signal, cancel, wait). Stage 3 migrated these routes
// out of control.Server so the control plane serves only the runner protocol;
// the control API now lives behind the apiserver module boundary. The module
// delegates to control.EngineFacade (the *engine.Engine behind the
// ControlPlane) for every engine call.
type workflowControlModule struct {
	cp   *control.ControlPlane
	eng  control.EngineFacade
	auth WorkflowAuthenticator
	log  engine.Logger
}

func newWorkflowControlModule(cp *control.ControlPlane, auth WorkflowAuthenticator, log engine.Logger) *workflowControlModule {
	return &workflowControlModule{cp: cp, eng: cp.Engine(), auth: auth, log: log}
}

func (m *workflowControlModule) Name() string { return "workflow-control" }

func (m *workflowControlModule) RegisterHTTP(mux *http.ServeMux) {
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
	mux.HandleFunc("/v1/executions/", wrap("execution", m.handleExecution))
}

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
	id, err := m.eng.Submit(r.Context(), g, req.Params)
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
	id, err := m.eng.Invoke(r.Context(), g, req.Entry, req.Input)
	if err != nil {
		// An unknown entry node is a client error (400), not a 404: the
		// missing resource is the entry in the submitted graph, not an
		// execution in the store. The underlying error text is safe to echo
		// because it contains only the caller-supplied entry name.
		if strings.Contains(err.Error(), "not found") {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}
	writeJSON(w, http.StatusOK, invokeResponse{ExecutionID: id})
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

// writeEngineError maps an engine error to an HTTP response. "not found"
// messages map to 404 so clients can distinguish absent executions; every
// other failure is collapsed to a generic 500 message — the underlying error
// (Redis text, internal paths, backend details) must never reach a client.
func writeEngineError(w http.ResponseWriter, err error) {
	if strings.Contains(strings.ToLower(err.Error()), "not found") {
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
