package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestDisabledWorkflowAuthAllowsAll(t *testing.T) {
	auth := DisabledWorkflowAuth{}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if err := auth.AuthenticateRequest(req); err != nil {
		t.Fatalf("DisabledWorkflowAuth.AuthenticateRequest() = %v, want nil", err)
	}
}

func TestBearerTokenAuthAllowsValidToken(t *testing.T) {
	const token = "supersecrettoken"
	auth := NewBearerTokenAuth(token)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	if err := auth.AuthenticateRequest(req); err != nil {
		t.Fatalf("BearerTokenAuth.AuthenticateRequest() = %v, want nil", err)
	}
}

func TestBearerTokenAuthRejectsInvalidToken(t *testing.T) {
	auth := NewBearerTokenAuth("correct")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	if err := auth.AuthenticateRequest(req); err == nil {
		t.Fatal("BearerTokenAuth.AuthenticateRequest() = nil, want error for wrong token")
	}
}

func TestBearerTokenAuthRejectsMissingHeader(t *testing.T) {
	auth := NewBearerTokenAuth("token")
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	if err := auth.AuthenticateRequest(req); err == nil {
		t.Fatal("BearerTokenAuth.AuthenticateRequest() = nil, want error for missing header")
	}
}

func TestBearerTokenAuthRejectsMalformedHeader(t *testing.T) {
	auth := NewBearerTokenAuth("token")
	for _, hdr := range []string{
		"Basic dXNlcjpwYXNz", // Basic auth
		"Bearer",             // missing token value
		"token",              // no scheme
	} {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		if hdr != "Bearer" { // avoid empty Bearer value that still has space
			req.Header.Set("Authorization", hdr)
		} else {
			req.Header.Set("Authorization", "Bearer ")
		}
		if err := auth.AuthenticateRequest(req); err == nil {
			t.Errorf("BearerTokenAuth.AuthenticateRequest(%q) = nil, want error", hdr)
		}
	}
}

func TestWorkflowControlModuleEnforcesAuth(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	// Use a BearerTokenAuth that rejects all requests.
	auth := NewBearerTokenAuth("secret")
	m := &workflowControlModule{eng: f, auth: auth}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	// No token → 401
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 without token", rec.Code)
	}

	// Wrong token → 401
	req = httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	req.Header.Set("Authorization", "Bearer wrongtoken")
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 with wrong token", rec.Code)
	}
}

func TestWorkflowControlModulePassesWithValidAuth(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	auth := NewBearerTokenAuth("secret")
	m := &workflowControlModule{eng: f, auth: auth}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	// Valid token on a GET that doesn't need a body → should reach the handler
	// and return 404 (execution not found) rather than 401.
	req := httptest.NewRequest(http.MethodGet, "/v1/executions/missing", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code == http.StatusUnauthorized {
		t.Fatalf("status = %d, want non-401 with valid token (auth should pass)", rec.Code)
	}
}

func TestAPIServerRequireWorkflowAuthRejectsNilAuth(t *testing.T) {
	_, err := New(Config{
		Concurrency:         1,
		RequireWorkflowAuth: true,
		WorkflowAuth:        nil, // missing → should fail closed
	})
	if err == nil {
		t.Fatal("New() = nil error, want error when RequireWorkflowAuth=true and WorkflowAuth=nil")
	}
}

func TestAPIServerRequireWorkflowAuthAcceptsConfiguredAuth(t *testing.T) {
	srv, err := New(Config{
		Concurrency:         1,
		RequireWorkflowAuth: true,
		WorkflowAuth:        NewBearerTokenAuth("sometoken"),
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil when WorkflowAuth is set", err)
	}
	if srv == nil {
		t.Fatal("New() returned nil server")
	}
}

func TestAPIServerNilWorkflowAuthWithoutRequireIsPermitted(t *testing.T) {
	srv, err := New(Config{
		Concurrency:  1,
		WorkflowAuth: nil, // nil without RequireWorkflowAuth → dev mode, allowed
	})
	if err != nil {
		t.Fatalf("New() error = %v, want nil for dev mode (no RequireWorkflowAuth)", err)
	}
	if srv == nil {
		t.Fatal("New() returned nil server")
	}
}
