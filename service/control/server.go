package control

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/observability/tracing"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// SubmitWorkflowPath is the HTTP path of the workflow submit endpoint.
// Stage 3 moved the workflow/control routes out of Server.Handler (which now
// serves only the runner protocol) and into the apiserver workflow-control
// module. The constant is retained so external callers (sdk, integration and
// perf tests) that build URLs against this path keep compiling; the route
// itself is hosted by service/apiserver.
const SubmitWorkflowPath = "/v1/workflows"

// EngineFacade is the subset of *engine.Engine the runner-protocol Server and
// the apiserver control modules need. *engine.Engine implements every method
// below (Submit/Invoke/Inspect/DeliverSignal/RevokeSignal/Cancel plus the
// embedded execution.Engine lease/routing surface), so it satisfies this
// interface without any adapter.
type EngineFacade interface {
	execution.Engine
	Submit(ctx context.Context, g *graph.Graph, params map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error)
	// Invoke starts a new execution from an explicit entry node. Implemented
	// by *engine.Engine (engine.go).
	Invoke(ctx context.Context, g *graph.Graph, entryNode string, input map[string]any, runtime ...*types.Runtime) (types.ExecutionID, error)
	Inspect(ctx context.Context, id types.ExecutionID, nodeNames ...string) (engine.ExecutionDetail, error)
	DeliverSignal(ctx context.Context, id types.ExecutionID, name string, data map[string]any) error
	// RevokeSignal atomically revokes a delivered-but-unconsumed signal.
	// Implemented by *engine.Engine (engine.go).
	RevokeSignal(ctx context.Context, id types.ExecutionID, signalName string) error
	Cancel(ctx context.Context, id types.ExecutionID) error
	BuildTaskLease(ctx context.Context, task *engine.Task) (*engine.TaskLease, error)
	CommitTaskResultWithOutcome(ctx context.Context, lease *engine.TaskLease, result engine.TaskResult) (engine.CommitOutcome, error)
}

type Server struct {
	core *Core
}

type errorResponse struct {
	Error string `json:"error"`
}

// ServerOption configures a control-plane Server.
type ServerOption func(*Server)

// WithAuthenticator installs a runner-protocol authenticator. Default is the
// permissive DisabledAuthenticator so today's zero-config behavior stays
// unchanged.
func WithAuthenticator(a Authenticator) ServerOption {
	return func(s *Server) {
		if a != nil {
			s.core.auth = a
		}
	}
}

// WithControlLogger sets the logger used for auth decisions and other
// runner-protocol diagnostics. Optional.
func WithControlLogger(l engine.Logger) ServerOption {
	return func(s *Server) { s.core.logger = l }
}

// WithAuthObserver installs a non-blocking observer for runner auth decisions.
func WithAuthObserver(observer AuthObserver) ServerOption {
	return func(s *Server) { s.core.authObserver = observer }
}

// WithTracer installs a distributed tracing implementation on the control
// plane's runner-protocol server. The tracer instruments task dispatch and
// commit spans and injects W3C trace carriers into TaskLease so runners can
// create properly-parented execution spans. Default is NoopTracer.
func WithTracer(t tracing.Tracer) ServerOption {
	return func(s *Server) {
		if t != nil {
			s.core.tracer = t
		}
	}
}

// WithHTTPPollWait sets the long-poll wait duration returned to runners when
// no task is available. Default is one second.
func WithHTTPPollWait(d time.Duration) ServerOption {
	return func(s *Server) {
		if d > 0 {
			s.core.pollWait = d
		}
	}
}

func NewServer(engine EngineFacade, runners RunnerDirectory, opts ...ServerOption) *Server {
	if runners == nil {
		log.Printf("control: runners directory is nil; using in-memory runner directory; not safe for multi-replica deployments")
		runners = NewMemoryRunnerDirectory()
	}
	srv := &Server{
		core: &Core{
			engine:   engine,
			runners:  runners,
			pollWait: time.Second,
			tracer:   tracing.NoopTracer{},
		},
	}
	for _, o := range opts {
		o(srv)
	}
	return srv
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	protocol.RegisterRunnerRoutes(mux, s)
	return mux
}

func (s *Server) HandleRegisterRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.RegisterRunnerRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.register(r.Context(), req, httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleHeartbeat(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.HeartbeatRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.heartbeat(r.Context(), req, httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandlePollTask(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.PollTaskRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.pollTask(r.Context(), req, httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleReportResult(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.ReportResultRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.reportResult(r.Context(), req, httpTransportInfo(r))
	if err != nil {
		if errors.Is(err, engine.ErrInvalidLeaseToken) {
			writeJSON(w, http.StatusConflict, resp)
			return
		}
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) HandleRenewLease(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodPost) {
		return
	}
	var req protocol.RenewLeaseRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	overrideTokenFromHeader(r, &req.AuthToken)
	resp, err := s.core.renewLease(r.Context(), req, httpTransportInfo(r))
	if err != nil {
		writeRunnerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// overrideTokenFromHeader gives Authorization: Bearer priority over the body
// AuthToken field. Header transport is preferred per the spec.
func overrideTokenFromHeader(r *http.Request, dst *string) {
	if h := r.Header.Get("Authorization"); strings.HasPrefix(h, "Bearer ") {
		*dst = strings.TrimSpace(strings.TrimPrefix(h, "Bearer "))
	}
}

// httpTransportInfo extracts TLS peer identity from the request when the
// connection is a verified client mTLS session. Returns an empty struct on
// plaintext HTTP so the authenticator's mTLS branch will reject.
func httpTransportInfo(r *http.Request) TransportInfo {
	info := TransportInfo{}
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return info
	}
	cert := r.TLS.PeerCertificates[0]
	info.TLSPeerCN = cert.Subject.String()
	info.TLSPeerSAN = append(info.TLSPeerSAN, cert.DNSNames...)
	return info
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

// writeRunnerError maps transport-agnostic Core sentinel errors to HTTP status
// codes. Known sentinels carry an actionable message; the catch-all default
// returns a generic 500 so internal error details are not leaked.
func writeRunnerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrRunnerIDRequired), errors.Is(err, ErrRunnerSessionRequired), errors.Is(err, ErrConcurrencyRequired), errors.Is(err, ErrInvalidNamespace), errors.Is(err, ErrLeaseRequired):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrRunnerSessionStale):
		writeError(w, http.StatusConflict, err.Error())
	case errors.Is(err, ErrRunnerNotFound):
		writeError(w, http.StatusNotFound, "runner not found")
	case errors.Is(err, ErrUnauthenticated):
		writeError(w, http.StatusUnauthorized, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, ErrInternalServer.Error())
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, errorResponse{Error: message})
}
