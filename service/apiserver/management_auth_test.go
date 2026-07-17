package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func newMgmtTestServer(auth WorkflowAuthenticator) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/management/leader", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, leaderResponse{IsLeader: true})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, readyResponse{Ready: true, Leader: true})
	})
	h := ManagementAuthMiddleware(auth)(mux)
	return h
}

func TestManagementAuthMiddlewareLeavesHealthzOpen(t *testing.T) {
	auth := NewBearerTokenAuth("secret")
	srv := newMgmtTestServer(auth)

	// /healthz must be reachable without a token.
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/healthz status = %d, want 200 (probes must be open)", rec.Code)
	}

	// /readyz must be reachable without a token.
	req = httptest.NewRequest(http.MethodGet, "/readyz", nil)
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/readyz status = %d, want 200 (probes must be open)", rec.Code)
	}
}

func TestManagementAuthMiddlewareGatesManagementSurface(t *testing.T) {
	auth := NewBearerTokenAuth("secret")
	srv := newMgmtTestServer(auth)

	// No token → 401 on /v1/management/leader.
	req := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/management/leader without token = %d, want 401", rec.Code)
	}

	// Wrong token → 401.
	req = httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("/v1/management/leader wrong token = %d, want 401", rec.Code)
	}

	// Valid token → 200.
	req = httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/management/leader valid token = %d, want 200", rec.Code)
	}
}

func TestManagementAuthMiddlewareNilAuthAllowsAll(t *testing.T) {
	// Nil auth = dev / behind-external-gateway mode: management surface open.
	srv := newMgmtTestServer(nil)
	req := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("nil auth /v1/management/leader = %d, want 200 (dev mode)", rec.Code)
	}
}

func TestManagementAuthMiddlewareDoesNotGateWorkflowRoutes(t *testing.T) {
	// Workflow routes (/v1/workflows, /v1/executions/*) are not under
	// /v1/management/ and must NOT be gated by this middleware. The workflow
	// module applies its own WorkflowAuthenticator. Verify pass-through.
	auth := NewBearerTokenAuth("secret")
	srv := newMgmtTestServer(auth)

	// Register a fake workflow route to confirm it is not 401'd.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/workflows", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	srv = ManagementAuthMiddleware(auth)(mux)

	req := httptest.NewRequest(http.MethodGet, "/v1/workflows", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("/v1/workflows under mgmt middleware = %d, want 200 (must not gate workflow routes)", rec.Code)
	}
}
