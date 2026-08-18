package apiserver

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/service/control"
)

func newManagementMux(t *testing.T, opts ...Option) http.Handler {
	t.Helper()
	srv, err := New(Config{Concurrency: 1}, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv.Handler()
}

func doGet(t *testing.T, mux http.Handler, path string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

func TestManagementHealthz(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/healthz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out["status"] != "ok" {
		t.Fatalf("status = %q, want ok", out["status"])
	}
}

func TestManagementReadyz(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out readyResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if !out.Ready {
		t.Fatal("ready = false, want true")
	}
	if !out.Leader {
		t.Fatal("leader = false, want true for memory backend")
	}
}

func TestManagementLeader(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/v1/management/leader")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// /v1/management/leader is a user-face endpoint and is enveloped (spec §3.1);
	// only /healthz and /readyz are excluded (spec §7). data carries the body.
	var env struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Data    json.RawMessage `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		t.Fatal(err)
	}
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = {success:%v code:%q}, want success=true code=200", env.Success, env.Code)
	}
	var leader leaderResponse
	if err := json.Unmarshal(env.Data, &leader); err != nil {
		t.Fatalf("decode data: %v", err)
	}
	if !leader.IsLeader {
		t.Fatal("is_leader = false, want true for memory backend")
	}
}

func TestManagementLeaderMethodNotAllowed(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	req := httptest.NewRequest(http.MethodPost, "/v1/management/leader", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestManagementRunnerUnknownReturns404(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/v1/management/runners/does-not-exist")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestManagementRunnerMissingIDReturns404(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/v1/management/runners/")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestManagementExecutionUnknownReturns404(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/v1/management/executions/does-not-exist")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestManagementExecutionMissingIDReturns404(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	resp := doGet(t, mux, "/v1/management/executions/")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestManagementNotRegisteredByDefault verifies the opt-in contract (R5): with
// no WithManagement option, the management routes are absent and the server
// returns a 404 from the underlying control plane handler.
func TestManagementNotRegisteredByDefault(t *testing.T) {
	mux := newManagementMux(t)
	resp := doGet(t, mux, "/v1/management/leader")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (management should not be registered by default)", resp.StatusCode)
	}
}

// TestManagementHealthzRegisteredByDefault confirms /healthz and /readyz are
// only mounted when the management module is enabled — consistent with the
// opt-in design.
func TestManagementHealthzNotRegisteredByDefault(t *testing.T) {
	mux := newManagementMux(t)
	resp := doGet(t, mux, "/healthz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (healthz should not be registered without WithManagement)", resp.StatusCode)
	}
}

// compile-time assertion that managementModule satisfies HTTPModule.
var _ HTTPModule = (*managementModule)(nil)

// Ensure control.ControlPlane exposes the RunnerDirectory accessor used by the
// management module at compile time.
var _ = (*control.ControlPlane)(nil).RunnerDirectory
