package control

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

const SubmitWorkflowPath = "/v1/workflows"

type EngineFacade interface {
	Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error)
	Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error)
	DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
	Cancel(ctx context.Context, id types.ExecutionID) error
	CommitTaskResult(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) error
}

type Server struct {
	engine   EngineFacade
	runners  *RunnerPool
	pollWait time.Duration
}

type signalRequest struct {
	Name string         `json:"name"`
	Data map[string]any `json:"data,omitempty"`
}

type submitWorkflowRequest struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params,omitempty"`
}

type submitWorkflowResponse struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func NewServer(engine EngineFacade, runners *RunnerPool) *Server {
	if runners == nil {
		runners = NewRunnerPool()
	}
	return &Server{
		engine:   engine,
		runners:  runners,
		pollWait: time.Second,
	}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	protocol.RegisterRunnerRoutes(mux, s)
	mux.HandleFunc(SubmitWorkflowPath, s.handleSubmitWorkflow)
	mux.HandleFunc("/v1/executions/", s.handleExecution)
	return mux
}

func (s *Server) handleSubmitWorkflow(w http.ResponseWriter, r *http.Request) {
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
	if s.engine == nil {
		writeError(w, http.StatusInternalServerError, "engine not configured")
		return
	}
	g, err := graph.Compile(req.Workflow)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	id, err := s.engine.Submit(r.Context(), g, req.Params)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, submitWorkflowResponse{ExecutionID: id})
}

func (s *Server) HandleRegisterRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.RegisterRunnerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RunnerID == "" || req.Concurrency <= 0 {
		writeError(w, http.StatusBadRequest, "runner_id and concurrency are required")
		return
	}
	s.runners.Register(req.RunnerID, req.Concurrency, req.Capabilities)
	writeJSON(w, http.StatusOK, protocol.RegisterRunnerResponse{RunnerID: req.RunnerID})
}

func (s *Server) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.HeartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RunnerID == "" {
		writeError(w, http.StatusBadRequest, "runner_id is required")
		return
	}
	at := time.Unix(req.Timestamp, 0)
	if req.Timestamp == 0 {
		at = time.Now()
	}
	if !s.runners.Heartbeat(req.RunnerID, req.Capacity, req.InFlight, at) {
		writeError(w, http.StatusNotFound, "runner not found")
		return
	}
	writeJSON(w, http.StatusOK, protocol.HeartbeatResponse{ServerTime: time.Now().Unix()})
}

func (s *Server) HandlePollTask(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.PollTaskRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RunnerID == "" {
		writeError(w, http.StatusBadRequest, "runner_id is required")
		return
	}
	lease, ok := s.runners.Poll(req.RunnerID, req.Capacity, req.Capabilities)
	if !ok {
		writeJSON(w, http.StatusOK, protocol.PollTaskResponse{Wait: s.pollWait})
		return
	}
	writeJSON(w, http.StatusOK, protocol.PollTaskResponse{Lease: &lease})
}

func (s *Server) HandleReportResult(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.ReportResultRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.RunnerID == "" || req.Lease == nil {
		writeError(w, http.StatusBadRequest, "runner_id and lease are required")
		return
	}
	if s.engine == nil {
		writeError(w, http.StatusInternalServerError, "engine not configured")
		return
	}
	if err := s.engine.CommitTaskResult(r.Context(), req.Lease, req.Result); err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			writeJSON(w, http.StatusConflict, protocol.ReportResultResponse{Accepted: false, Error: err.Error()})
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, protocol.ReportResultResponse{Accepted: true})
}

func (s *Server) handleExecution(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/v1/executions/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	id := types.ExecutionID(parts[0])
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handleInspect(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "signal" {
		s.handleSignal(w, r, id)
		return
	}
	if len(parts) == 2 && parts[1] == "cancel" {
		s.handleCancel(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "route not found")
}

func (s *Server) handleInspect(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if s.engine == nil {
		writeError(w, http.StatusInternalServerError, "engine not configured")
		return
	}
	detail, err := s.engine.Inspect(r.Context(), id)
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
}

func (s *Server) handleSignal(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
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
	if s.engine == nil {
		writeError(w, http.StatusInternalServerError, "engine not configured")
		return
	}
	if err := s.engine.DeliverSignal(r.Context(), id, req.Name, req.Data); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func (s *Server) handleCancel(w http.ResponseWriter, r *http.Request, id types.ExecutionID) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	if s.engine == nil {
		writeError(w, http.StatusInternalServerError, "engine not configured")
		return
	}
	if err := s.engine.Cancel(r.Context(), id); err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"accepted": true})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	defer r.Body.Close()
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

func writeEngineError(w http.ResponseWriter, err error) {
	if strings.Contains(strings.ToLower(err.Error()), "not found") {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeError(w, http.StatusInternalServerError, err.Error())
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
