package apiserver

import (
	"net/http"
	"strings"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// managementModule mounts the ops read-only management HTTP API: leader
// status, single-runner lookup, single-execution inspect, and the process
// liveness/readiness probes. It is opt-in (registered only via WithManagement)
// because it exposes runner directory and execution state that should sit
// behind an authz middleware.
//
// Per R1, the underlying runner directory and store interfaces expose no list
// API, so this module intentionally provides no listing endpoints — only
// single-resource lookups, leader status, and health probes.
type managementModule struct {
	cp  *control.ControlPlane
	eng control.EngineFacade
}

func newManagementModule(cp *control.ControlPlane) *managementModule {
	return &managementModule{cp: cp, eng: cp.Engine()}
}

func (m *managementModule) Name() string { return "management" }

func (m *managementModule) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/v1/management/leader", m.handleLeader)
	mux.HandleFunc("/v1/management/runners/", m.handleRunner)       // /v1/management/runners/{id}
	mux.HandleFunc("/v1/management/executions/", m.handleExecution) // /v1/management/executions/{id}
	mux.HandleFunc("/healthz", m.handleHealthz)
	mux.HandleFunc("/readyz", m.handleReadyz)
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
	writeJSON(w, http.StatusOK, leaderResponse{IsLeader: m.cp.IsLeader()})
}

// handleRunner looks up a single runner snapshot by id. The runner directory
// has no list API, so listing is intentionally unsupported.
func (m *managementModule) handleRunner(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/management/runners/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeError(w, http.StatusNotFound, "runner not found")
		return
	}
	dir := m.cp.RunnerDirectory()
	if dir == nil {
		writeError(w, http.StatusNotFound, "runner not found")
		return
	}
	snap, ok := dir.Runner(r.Context(), id)
	if !ok {
		writeError(w, http.StatusNotFound, "runner not found")
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

// handleExecution inspects a single execution by id. It reuses the engine
// facade so the response shape matches the workflow-control inspect endpoint.
func (m *managementModule) handleExecution(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/v1/management/executions/")
	id = strings.Trim(id, "/")
	if id == "" {
		writeError(w, http.StatusNotFound, "execution not found")
		return
	}
	detail, err := m.eng.Inspect(r.Context(), types.ExecutionID(id))
	if err != nil {
		writeEngineError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, detail)
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
