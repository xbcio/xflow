package apiserver

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/observability/metrics"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// managementModule mounts the ops management HTTP API: leader status,
// single-runner lookup, single-execution inspect, dead-letter list/replay,
// and the process liveness/readiness probes. It is opt-in (registered only
// via WithManagement) because it exposes runner directory, execution state,
// and dead-letter operations that must sit behind authz.
//
// Per R1, the underlying runner directory and store interfaces expose no list
// API, so this module intentionally provides no listing endpoints for
// runners/executions — only single-resource lookups, leader status, dead-letter
// operations, and health probes.
type managementModule struct {
	authzHolder
	cp  *control.ControlPlane
	eng control.EngineFacade
	// metrics wires the OutboxObserver (replay outcome counter + pending/dead
	// gauge) into the shared DeadLetterManager so the API and CLI paths
	// produce identical telemetry. Nil leaves metrics off (dev).
	metrics *metrics.Metrics
	// dlMgr is the shared, lazily-constructed dead-letter manager. It is
	// built once on first use with the metrics observer + durable audit
	// projector and reused for every subsequent list/replay so the API never
	// builds a fresh manager per request with nil metrics.
	dlMgr     *control.DeadLetterManager
	dlMgrOnce sync.Once
}

func newManagementModule(cp *control.ControlPlane) *managementModule {
	return &managementModule{cp: cp, eng: cp.Engine()}
}

func (m *managementModule) Name() string { return "management" }

func (m *managementModule) RegisterHTTP(mux *http.ServeMux) {
	// Task 8 blocker 2: when a PrincipalAuthenticator is configured, the
	// leader/runner/executions routes go through the B3 authz wrapper so each
	// gets its own stable operation + scope + admission/outcome audit, not a
	// blanket management.read. leader → management.leader.read, runner →
	// management.runner.read, execution inspect → management.read (single-
	// resource lookup; no list API). dead-letters self-wrap below. healthz/
	// readyz stay open for probes (minimal public info only).
	//
	// When PrincipalAuth is nil (dev / behind an external gateway) the routes
	// are served directly and the namespace defaults to namespace.Default.
	if m.principalAuth != nil {
		mux.HandleFunc("GET "+PathManagementLeader, m.authzWrap(OpManagementLeaderRead, false, m.handleLeader, func(*http.Request) (string, string, string, string) {
			return "management/leader", "", "", ""
		}))
		mux.HandleFunc("GET "+PathManagementRunnerByID, m.authzWrap(OpManagementRunnerRead, false, m.handleRunner, func(r *http.Request) (string, string, string, string) {
			id := r.PathValue("id")
			return "management/runner/" + id, "", "", ""
		}))
		// Namespace boundary (Task 7.3): the execution-inspect route injects the
		// verified principal's Namespace into the request context. Inspect reads
		// from the principal's namespace namespace; a cross-namespace execID resolves
		// to not-found → 404, which is the IDOR defense and does not leak
		// existence.
		mux.HandleFunc("GET "+PathManagementExecByID, m.authzWrap(OpManagementRead, false, m.handleExecution, func(r *http.Request) (string, string, string, string) {
			id := r.PathValue("id")
			return "management/execution/" + id, "", id, ""
		}))
	} else {
		mux.HandleFunc("GET "+PathManagementLeader, m.handleLeader)
		mux.HandleFunc("GET "+PathManagementRunnerByID, m.handleRunner)
		mux.HandleFunc("GET "+PathManagementExecByID, m.handleExecution)
	}
	mux.HandleFunc("/v1/management/dead-letters/", m.handleDeadLetters)
	mux.HandleFunc("GET "+PathHealthz, m.handleHealthz)
	mux.HandleFunc("GET "+PathReadyz, m.handleReadyz)
}

type leaderResponse struct {
	IsLeader bool `json:"is_leader"`
}

type readyResponse struct {
	Ready  bool `json:"ready"`
	Leader bool `json:"leader"`
}

// handleLeader reports whether this replica currently holds leadership. GET
// only; any other method yields 405.
func (m *managementModule) handleLeader(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeData(w, r, http.StatusOK, leaderResponse{IsLeader: m.cp.IsLeader()})
}

// handleRunner looks up a single runner snapshot by id. The runner directory
// has no list API, so listing is intentionally unsupported. The id is the {id}
// path value the mux matched.
func (m *managementModule) handleRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	dir := m.cp.RunnerDirectory()
	if dir == nil {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	snap, ok := dir.Runner(r.Context(), id)
	if !ok {
		writeFail(w, r, http.StatusNotFound, "runner_not_found", "runner not found")
		return
	}
	writeData(w, r, http.StatusOK, snap)
}

// handleExecution inspects a single execution by id. It delegates to
// inspectExecution — the single shared inspect implementation also used by the
// executions-family route (spec §7.2) — so GET /v1/management/executions/{id}
// (OpManagementRead) and GET /v1/executions/{id} (OpExecutionRead) return
// byte-identical bodies for the same execution. The two Ops stay distinct so
// an ops token and a business token may be granted separately; only the
// implementation is merged. The id is the {id} path value the mux matched.
func (m *managementModule) handleExecution(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := r.PathValue("id")
	if id == "" {
		writeFail(w, r, http.StatusNotFound, "execution_not_found", "execution not found")
		return
	}
	inspectExecution(w, r, m.eng, types.ExecutionID(id))
}

// handleHealthz is a liveness probe. It only confirms the process is serving
// HTTP and emits no sensitive information.
func (m *managementModule) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReadyz is a readiness probe. A process is ready as long as it is
// serving; leader status is reported alongside so callers can route writes to
// the leader if they choose. Non-leader replicas remain ready for the
// read-only management surface.
func (m *managementModule) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, readyResponse{Ready: true, Leader: m.cp.IsLeader()})
}

// deadLetterListResponse is the JSON shape for a dead-letter list page.
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

// handleDeadLetters routes dead-letter requests through the B3 authz wrapper.
//   - GET  /v1/management/dead-letters/{execID}              → list (cursor page)
//   - POST /v1/management/dead-letters/{execID}/replay       → replay
//
// When no PrincipalAuthenticator is configured the routes return 404 so a
// dev/preview server without authz never exposes the privileged replay path.
func (m *managementModule) handleDeadLetters(w http.ResponseWriter, r *http.Request) {
	if m.principalAuth == nil {
		writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/management/dead-letters/")
	rest = strings.Trim(rest, "/")
	if rest == "" {
		writeFail(w, r, http.StatusNotFound, "route_not_found", "execution id required")
		return
	}
	parts := strings.Split(rest, "/")
	execID := parts[0]
	resource := "dead-letters/" + execID

	switch {
	case len(parts) == 1 && r.Method == http.MethodGet:
		m.authzWrap(OpDeadLetterList, false, m.handleDeadLetterList, func(*http.Request) (string, string, string, string) {
			return resource, "", execID, ""
		})(w, r)
	case len(parts) == 2 && parts[1] == "replay" && r.Method == http.MethodPost:
		m.authzWrap(OpDeadLetterReplay, true, m.handleDeadLetterReplay, func(*http.Request) (string, string, string, string) {
			return resource, "", execID, ""
		})(w, r)
	default:
		writeFail(w, r, http.StatusNotFound, "route_not_found", "route not found")
	}
}

func (m *managementModule) handleDeadLetterList(w http.ResponseWriter, r *http.Request) {
	execID := m.deadLetterExecID(r)
	mgr, err := m.deadLetterManager()
	if err != nil {
		writeFail(w, r, http.StatusServiceUnavailable, "dead_letter_unavailable", "dead-letter backend unavailable")
		return
	}
	// IDOR defense (Task 7.3): confirm the execution belongs to the caller's
	// namespace before listing its dead-letters. The authz wrapper injected the
	// principal's Namespace into r.Context(), so Inspect is namespace-scoped: a
	// cross-namespace execID resolves to not-found → 404, never leaking that the
	// execution exists in another namespace. This matches the executions/ endpoint
	// behavior and the design §5.1 requirement (404, not 403).
	if _, err := m.eng.Inspect(r.Context(), types.ExecutionID(execID)); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	q := r.URL.Query()
	limit := 0
	if v := q.Get("limit"); v != "" {
		// Best-effort parse; an invalid limit falls back to the store default.
		var n int
		_, _ = fmt.Sscanf(v, "%d", &n)
		if n > 0 {
			limit = n
		}
	}
	list, err := mgr.List(r.Context(), types.ExecutionID(execID), engine.DeadLetterPage{
		Cursor: q.Get("cursor"),
		Limit:  limit,
	})
	if err != nil {
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	// §3.3 cursor exception: the cursor-paginated payload {entries,next_cursor}
	// stays as-is, but per §3.1 the user-face success body is enveloped — the
	// payload rides inside data, the cursor pagination shape is preserved. The
	// CLI's apiDeadLetterClient decodes the envelope and extracts data.
	writeData(w, r, http.StatusOK, deadLetterListResponse{
		Entries:    list.Entries,
		NextCursor: list.NextCursor,
	})
}

func (m *managementModule) handleDeadLetterReplay(w http.ResponseWriter, r *http.Request) {
	execID := m.deadLetterExecID(r)
	mgr, err := m.deadLetterManager()
	if err != nil {
		writeFail(w, r, http.StatusServiceUnavailable, "dead_letter_unavailable", "dead-letter backend unavailable")
		return
	}
	var req deadLetterReplayRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.EntryID == "" || req.Reason == "" {
		writeFail(w, r, http.StatusBadRequest, "bad_request", "entry_id and reason are required")
		return
	}
	// IDOR defense (Task 7.3): confirm the execution belongs to the caller's
	// namespace before replaying one of its dead-letters. Namespace-scoped via the
	// authz-injected context; cross-namespace execID → 404 (no existence leak).
	if _, err := m.eng.Inspect(r.Context(), types.ExecutionID(execID)); err != nil {
		writeExecEngineFail(w, r, err)
		return
	}
	// Principal is server-injected by the authz wrapper; the operator is taken
	// from it, never self-reported. The manager re-checks the scope.
	p, ok := principalFromRequest(r)
	if !ok {
		writeFail(w, r, http.StatusUnauthorized, "unauthorized", "unauthorized")
		return
	}
	res, derr := mgr.Replay(r.Context(), control.DeadLetterReplayPrincipal{
		Subject:   p.Subject,
		Namespace: p.Namespace,
		Scopes:    p.Scopes,
	}, engine.ReplayDeadLetterRequest{
		ExecutionID: types.ExecutionID(execID),
		EntryID:     req.EntryID,
		RequestID:   req.RequestID,
		Reason:      req.Reason,
	})
	if derr != nil {
		if res.Outcome == engine.ReplayInvalidRequest {
			// Generic: the underlying error text may echo caller-supplied
			// entry-id / reason fragments; keep the message generic and rely
			// on the stable code. §3.5 forbids surfacing internals.
			writeFail(w, r, http.StatusBadRequest, "bad_request", "invalid replay request")
			return
		}
		writeFail(w, r, http.StatusInternalServerError, "internal_error", "internal server error")
		return
	}
	if res.Outcome == engine.ReplayNotFound {
		// §4.1 binds success to 2xx strictly, so a 404 must carry a failure
		// envelope -- data:null, no outcome field. Nothing reads the outcome on
		// this path: the CLI's do() returns on any non-2xx before decoding, and
		// both HTTP tests discard the body. The outcome survives in the stable
		// code so a client can still branch on it.
		writeFail(w, r, http.StatusNotFound, "dead_letter_not_found", "dead-letter entry not found")
		return
	}
	writeData(w, r, http.StatusOK, deadLetterReplayResponse{
		Outcome:      string(res.Outcome),
		AuditID:      res.AuditID,
		ExecutionID:  string(res.ExecutionID),
		NodeID:       res.NodeID,
		ActivationID: res.ActivationID,
	})
}

// deadLetterExecID extracts the execution id the authz wrapper resolved for
// this request (stored on the resource string). Falls back to parsing the
// path again if the context value is absent.
func (m *managementModule) deadLetterExecID(r *http.Request) string {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/management/dead-letters/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 {
		return ""
	}
	return parts[0]
}

// deadLetterManager lazily builds a shared DeadLetterManager from the control
// plane's backend state when it implements engine.DeadLetterStore. The manager
// is constructed exactly once via sync.Once (cached on the module) with:
//   - a non-nil metrics observer (when cfg.Metrics is set) so the replay
//     outcome counter and pending/dead-lettered gauge are emitted at the
//     single outlet the CLI also uses;
//   - a durable receipt projector audit sink (when the configured AuditSink
//     is backed by a SQL store) so replay receipts are durably projected to
//     SQL idempotently for reconcile; otherwise the stdout/stderr sink is the
//     dev-only projection (Redis receipt remains authoritative).
//
// Returns an error when the configured backend cannot serve dead-letter
// operations (e.g. the in-memory backend used in dev); the HTTP route surfaces
// 503 in that case.
func (m *managementModule) deadLetterManager() (*control.DeadLetterManager, error) {
	m.dlMgrOnce.Do(func() {
		if m.cp.Backend() == nil {
			return
		}
		state := m.cp.Backend().State()
		store, ok := state.(engine.DeadLetterStore)
		if !ok {
			return
		}
		var observer engine.OutboxObserver
		if m.metrics != nil {
			observer = metrics.NewOutboxMetrics(m.metrics)
		}
		audit := m.deadLetterAuditSink(observer)
		m.dlMgr = control.NewDeadLetterManager(store, observer, audit)
	})
	if m.dlMgr != nil {
		return m.dlMgr, nil
	}
	if m.cp.Backend() == nil {
		return nil, errors.New("management: no backend")
	}
	state := m.cp.Backend().State()
	if _, ok := state.(engine.DeadLetterStore); !ok {
		return nil, errors.New("management: backend state does not implement DeadLetterStore")
	}
	return nil, errors.New("management: dead-letter manager initialization failed")
}

// deadLetterAuditSink selects the durable receipt-projector sink when the
// configured audit sink is SQL-backed, falling back to the stderr projection
// otherwise. The Redis receipt is always authoritative; the sink is the
// secondary durable projection reconciled against it.
func (m *managementModule) deadLetterAuditSink(observer engine.OutboxObserver) engine.DeadLetterAuditSink {
	if sql, ok := m.audit.(*SQLAuditSink); ok {
		if ra := sql.ReceiptAppender(); ra != nil {
			return control.NewProjectorAuditSink(control.NewReceiptProjector(ra), observer)
		}
	}
	return control.NewStdoutDeadLetterAuditSink(func(line string) {
		// G0/dev projection: replay receipts are emitted to stderr. The
		// authoritative receipt is the Redis record written atomically by the
		// DeadLetterStore. A durable SQL projection of replay receipts is the
		// reconcile target; until a SQL sink is configured, stderr is the
		// audit trail.
		fmt.Fprintln(os.Stderr, line)
	})
}
