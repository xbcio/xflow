package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/service/control"
)

// fakeControlPlaneForAuthz builds a minimal memory-backed control plane.
func fakeControlPlaneForAuthz(t *testing.T) *control.ControlPlane {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{
		Backend: local.New(),
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	return cp
}

func authzModule(t *testing.T, auth PrincipalAuthenticator, authorizer Authorizer, audit AuditSink) *workflowControlModule {
	t.Helper()
	m := newWorkflowControlModule(fakeControlPlaneForAuthz(t), nil, nil)
	m.principalAuth = auth
	m.authorizer = authorizer
	m.audit = audit
	return m
}

// staticPrincipalAuth returns a fixed principal for tests.
type staticPrincipalAuth struct {
	principal Principal
	err      error
}

func (s staticPrincipalAuth) Authenticate(*http.Request) (Principal, error) {
	return s.principal, s.err
}

func TestAuthzAllowsPrincipalWithRequiredScope(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}
	audit := NewInMemoryAuditSink()
	m := authzModule(t, auth, ScopeAuthorizer{}, audit)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)

	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)
	mux.ServeHTTP(rec, req)

	// The submit handler runs after authz allows; it returns 400 (workflow
	// required) rather than 401/403, proving the request was authorized and
	// reached the handler.
	if rec.Code == http.StatusUnauthorized || rec.Code == http.StatusForbidden {
		t.Fatalf("status = %d, want request to reach handler (authz should allow)", rec.Code)
	}
	// An admission audit event was recorded for the mutation.
	events := audit.Events()
	if len(events) != 1 || events[0].Decision != DecisionAllow || events[0].Outcome != "admitted" {
		t.Fatalf("audit = %+v, want one allow/admitted admission", events)
	}
	if events[0].Principal != "alice" {
		t.Fatalf("audit principal = %q, want alice (server-injected)", events[0].Principal)
	}
}

func TestAuthzDeniesPrincipalMissingScope(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "mallory", Scopes: []string{}}}
	audit := NewInMemoryAuditSink()
	m := authzModule(t, auth, ScopeAuthorizer{}, audit)
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (deny)", rec.Code)
	}
	events := audit.Events()
	if len(events) != 1 || events[0].Decision != DecisionDeny {
		t.Fatalf("audit = %+v, want one deny event", events)
	}
}

func TestAuthzRejectsUnauthenticatedRequest(t *testing.T) {
	auth := staticPrincipalAuth{err: ErrWorkflowUnauthenticated}
	audit := NewInMemoryAuditSink()
	m := authzModule(t, auth, ScopeAuthorizer{}, audit)
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	// Unauthenticated deny audit is best-effort (no principal yet).
	if len(audit.Events()) != 1 || audit.Events()[0].Reason != "unauthenticated" {
		t.Fatalf("audit = %+v, want one unauthenticated deny", audit.Events())
	}
}

// failingAuditSink always returns an error, simulating an unavailable durable
// audit sink. Mutations must fail-closed (503) rather than execute unaudited.
type failingAuditSink struct{}

func (failingAuditSink) Append(context.Context, AuditEvent) error { return ErrAuditUnavailable }

func TestAuthzMutationFailClosedWhenAuditUnavailable(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}
	m := authzModule(t, auth, ScopeAuthorizer{}, failingAuditSink{})
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 (audit unavailable → mutation fail-closed)", rec.Code)
	}
}

func TestScopeAuthorizerDeniesUnknownOperation(t *testing.T) {
	dec, err := ScopeAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", Scopes: []string{"*"}},
		Operation: "bogus.operation",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("unknown operation decision = %q, want deny (default-deny)", dec)
	}
}

func TestBearerPrincipalAuthMapsTokenToPrincipal(t *testing.T) {
	a := NewBearerPrincipalAuth("tok-123", "operator-1", []string{"workflow", "execution"})
	p, err := a.Authenticate(httptest.NewRequest(http.MethodGet, "/", nil))
	if err != ErrWorkflowUnauthenticated {
		t.Fatalf("missing token: err=%v, want ErrWorkflowUnauthenticated", err)
	}
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer tok-123")
	p, err = a.Authenticate(req)
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if p.Subject != "operator-1" || !p.HasScope("workflow") {
		t.Fatalf("principal = %+v, want operator-1 with workflow scope", p)
	}

	// Wrong token must not authenticate and must not leak principal.
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	if _, err := a.Authenticate(req2); err != ErrWorkflowUnauthenticated {
		t.Fatalf("wrong token: err=%v, want ErrWorkflowUnauthenticated", err)
	}
}

func TestNewFailsClosedWhenPrincipalAuthMissingAuthorizerOrAudit(t *testing.T) {
	auth := NewBearerPrincipalAuth("tok", "op", []string{"workflow"})

	// PrincipalAuth without Authorizer.
	if _, err := New(Config{
		Concurrency:  1,
		PrincipalAuth: auth,
		AuditSink:    NewInMemoryAuditSink(),
	}); err == nil {
		t.Fatal("New() = nil, want error when PrincipalAuth set without Authorizer")
	}
	// PrincipalAuth without AuditSink.
	if _, err := New(Config{
		Concurrency:  1,
		PrincipalAuth: auth,
		Authorizer:    ScopeAuthorizer{},
	}); err == nil {
		t.Fatal("New() = nil, want error when PrincipalAuth set without AuditSink")
	}
	// All three present: OK.
	srv, err := New(Config{
		Concurrency:  1,
		PrincipalAuth: auth,
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("New() with all three: %v", err)
	}
	_ = srv
}
