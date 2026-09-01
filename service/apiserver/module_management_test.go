package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"

	backendlocal "github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/memstore"
)

func newManagementServer(t *testing.T, cfg Config, opts ...Option) *APIServer {
	t.Helper()
	srv, err := New(cfg, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return srv
}

func newManagementMux(t *testing.T, opts ...Option) http.Handler {
	t.Helper()
	return newManagementServer(t, Config{Concurrency: 1}, opts...).Handler()
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
	out := decodeReadyz(t, resp)
	if !out.Ready {
		t.Fatal("ready = false, want true")
	}
	if !out.Leader {
		t.Fatal("leader = false, want true for memory backend")
	}
}

func TestManagementReadyzDependencyErrorReturns503(t *testing.T) {
	depErr := errors.New("dependency unavailable")
	srv := newManagementServer(t, Config{
		Concurrency: 1,
		ReadinessChecker: readinessFunc(func(context.Context) error {
			return depErr
		}),
	}, WithManagement())
	resp := doGet(t, srv.Handler(), "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	out := decodeReadyz(t, resp)
	if out.Ready {
		t.Fatal("ready = true, want false when dependency check fails")
	}
}

func TestManagementReadyzUsesStoreReadinessChecker(t *testing.T) {
	dependencyErr := errors.New("store unavailable")
	tests := []struct {
		name       string
		checkErr   error
		wantStatus int
		wantReady  bool
	}{
		{name: "healthy", wantStatus: http.StatusOK, wantReady: true},
		{name: "unavailable", checkErr: dependencyErr, wantStatus: http.StatusServiceUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := newReadinessStore(tt.checkErr)
			srv := newManagementServer(t, Config{Store: store, Concurrency: 1}, WithManagement())
			t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

			resp := doGet(t, srv.Handler(), "/readyz")
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
			out := decodeReadyz(t, resp)
			if out.Ready != tt.wantReady {
				t.Fatalf("ready = %v, want %v", out.Ready, tt.wantReady)
			}
			if got := store.checks.Load(); got != 1 {
				t.Fatalf("store readiness checks = %d, want 1", got)
			}
		})
	}
}

func TestManagementReadyzDeduplicatesExplicitStoreChecker(t *testing.T) {
	store := newReadinessStore(nil)
	srv := newManagementServer(t, Config{
		Store:            store,
		Concurrency:      1,
		ReadinessChecker: store,
	}, WithManagement())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })

	resp := doGet(t, srv.Handler(), "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if got := store.checks.Load(); got != 1 {
		t.Fatalf("store readiness checks = %d, want 1", got)
	}
}

func TestManagementReadyzRedisDependencyFailureReturns503(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis: %v", err)
	}
	srv := newManagementServer(t, Config{RedisAddr: mr.Addr(), Concurrency: 1}, WithManagement())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	h := srv.Handler()

	resp := doGet(t, h, "/readyz")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthy redis status = %d, want 200", resp.StatusCode)
	}
	healthy := decodeReadyz(t, resp)
	if !healthy.Ready {
		t.Fatal("healthy redis ready = false, want true")
	}
	_ = resp.Body.Close()
	mr.Close()

	resp = doGet(t, h, "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("closed redis status = %d, want 503", resp.StatusCode)
	}
	out := decodeReadyz(t, resp)
	if out.Ready {
		t.Fatal("ready = true, want false when redis ping fails")
	}
}

func TestManagementReadyzShutdownReturns503(t *testing.T) {
	srv := newManagementServer(t, Config{Concurrency: 1}, WithManagement())
	h := srv.Handler()
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	resp := doGet(t, h, "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	out := decodeReadyz(t, resp)
	if out.Ready {
		t.Fatal("ready = true, want false after shutdown")
	}
}

func TestManagementReadyzNonLeaderStillReady(t *testing.T) {
	cp, err := control.NewControlPlane(control.Config{Backend: newNonLeaderBackend()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	srv := newManagementServer(t, Config{Concurrency: 1}, WithControlPlane(cp), WithManagement())
	t.Cleanup(func() { _ = srv.Shutdown(context.Background()) })
	resp := doGet(t, srv.Handler(), "/readyz")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	out := decodeReadyz(t, resp)
	if !out.Ready {
		t.Fatal("ready = false, want true for healthy non-leader")
	}
	if out.Leader {
		t.Fatal("leader = true, want false for non-leader backend")
	}
}

func TestManagementReadyzMethodNotAllowed(t *testing.T) {
	mux := newManagementMux(t, WithManagement())
	req := httptest.NewRequest(http.MethodPost, "/readyz", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func decodeReadyz(t *testing.T, resp *http.Response) readyResponse {
	t.Helper()
	var out readyResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
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

type nonLeaderBackend struct {
	*backendlocal.Backend
	notify chan bool
}

func newNonLeaderBackend() *nonLeaderBackend {
	notify := make(chan bool, 1)
	notify <- false
	return &nonLeaderBackend{
		Backend: backendlocal.New(backendlocal.WithConcurrency(1)),
		notify:  notify,
	}
}

func (b *nonLeaderBackend) Campaign(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (b *nonLeaderBackend) IsLeader() bool { return false }

func (b *nonLeaderBackend) Resign(context.Context) error { return nil }

func (b *nonLeaderBackend) Notify() <-chan bool { return b.notify }

type readinessStore struct {
	*memstore.Store
	err    error
	checks atomic.Int64
}

func newReadinessStore(err error) *readinessStore {
	return &readinessStore{Store: memstore.New(), err: err}
}

func (s *readinessStore) CheckReadiness(context.Context) error {
	s.checks.Add(1)
	return s.err
}
