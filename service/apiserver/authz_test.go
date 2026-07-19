package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store/memstore"
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
	m := newWorkflowControlModule(fakeControlPlaneForAuthz(t), nil, nil, nil)
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
	// A mutation records an admission audit event (admitted) plus a reconcile
	// outcome (failed, because the handler returned 400 for the missing
	// workflow body). The admission row carries the server-injected principal.
	events := audit.Events()
	if len(events) != 2 {
		t.Fatalf("audit = %+v, want 2 events (admission + reconcile)", events)
	}
	if events[0].Decision != DecisionAllow || events[0].Outcome != "admitted" {
		t.Fatalf("admission = %+v, want allow/admitted", events[0])
	}
	if events[1].Outcome != "failed" {
		t.Fatalf("reconcile outcome = %q, want failed (handler returned 400)", events[1].Outcome)
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
	// Single-token constructor maps to the default tenant so downstream code
	// always sees a non-empty, key-safe tenant.
	if p.TenantID != string(tenant.DefaultTenant) {
		t.Fatalf("single-token tenant = %q, want %q (default)", p.TenantID, tenant.DefaultTenant)
	}

	// Wrong token must not authenticate and must not leak principal.
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	if _, err := a.Authenticate(req2); err != ErrWorkflowUnauthenticated {
		t.Fatalf("wrong token: err=%v, want ErrWorkflowUnauthenticated", err)
	}
}

// TestBearerPrincipalAuthMultiMapsTokenToTenant proves the multi-tenant token
// registry (design §2.3 scheme A) binds each token to its own tenant, and that
// the tenant is server-issued from the token (never self-reported).
func TestBearerPrincipalAuthMultiMapsTokenToTenant(t *testing.T) {
	a := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", TenantID: "tenantA", Scopes: []string{"workflow", "execution", "management.read"}},
		{Token: "tok-b", Subject: "op-b", TenantID: "tenantB", Scopes: []string{"workflow", "execution", "management.read"}},
	})

	reqA := httptest.NewRequest(http.MethodGet, "/", nil)
	reqA.Header.Set("Authorization", "Bearer tok-a")
	pa, err := a.Authenticate(reqA)
	if err != nil {
		t.Fatalf("tok-a: %v", err)
	}
	if pa.Subject != "op-a" || pa.TenantID != "tenantA" {
		t.Fatalf("tok-a principal = %+v, want op-a/tenantA", pa)
	}

	reqB := httptest.NewRequest(http.MethodGet, "/", nil)
	reqB.Header.Set("Authorization", "Bearer tok-b")
	pb, err := a.Authenticate(reqB)
	if err != nil {
		t.Fatalf("tok-b: %v", err)
	}
	if pb.Subject != "op-b" || pb.TenantID != "tenantB" {
		t.Fatalf("tok-b principal = %+v, want op-b/tenantB", pb)
	}

	// A token not in the registry must not authenticate and must not leak which
	// tokens exist (same error as a missing token).
	reqX := httptest.NewRequest(http.MethodGet, "/", nil)
	reqX.Header.Set("Authorization", "Bearer tok-x")
	if _, err := a.Authenticate(reqX); err != ErrWorkflowUnauthenticated {
		t.Fatalf("unknown token: err=%v, want ErrWorkflowUnauthenticated", err)
	}

	// Empty TenantID in a mapping normalizes to default so every authenticated
	// principal carries a non-empty tenant (TenantAwareAuthorizer requires it).
	aDef := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-d", Subject: "op-d", Scopes: []string{"workflow"}},
	})
	reqD := httptest.NewRequest(http.MethodGet, "/", nil)
	reqD.Header.Set("Authorization", "Bearer tok-d")
	pd, err := aDef.Authenticate(reqD)
	if err != nil {
		t.Fatalf("tok-d: %v", err)
	}
	if pd.TenantID != string(tenant.DefaultTenant) {
		t.Fatalf("empty tenant mapping normalized to %q, want %q", pd.TenantID, tenant.DefaultTenant)
	}
}

// TestBearerPrincipalAuthImplementsWorkflowAuthenticator proves the multi-token
// registry can also gate the outer management middleware: any registered token
// passes AuthenticateRequest, and an unknown token fails with the same error as
// a missing token.
func TestBearerPrincipalAuthImplementsWorkflowAuthenticator(t *testing.T) {
	var auth WorkflowAuthenticator = NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", TenantID: "tenantA", Scopes: []string{"management.read"}},
		{Token: "tok-b", Subject: "op-b", TenantID: "tenantB", Scopes: []string{"management.read"}},
	})

	for _, tok := range []string{"tok-a", "tok-b"} {
		req := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
		req.Header.Set("Authorization", "Bearer "+tok)
		if err := auth.AuthenticateRequest(req); err != nil {
			t.Fatalf("%s: AuthenticateRequest = %v, want nil", tok, err)
		}
	}

	reqUnknown := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	reqUnknown.Header.Set("Authorization", "Bearer tok-x")
	if err := auth.AuthenticateRequest(reqUnknown); err != ErrWorkflowUnauthenticated {
		t.Fatalf("unknown token: err = %v, want ErrWorkflowUnauthenticated", err)
	}

	reqMissing := httptest.NewRequest(http.MethodGet, "/v1/management/leader", nil)
	if err := auth.AuthenticateRequest(reqMissing); err != ErrWorkflowUnauthenticated {
		t.Fatalf("missing token: err = %v, want ErrWorkflowUnauthenticated", err)
	}
}

func TestTenantAwareAuthorizerDeniesEmptyTenant(t *testing.T) {
	// A principal with scopes but no tenant must be denied — fail-closed.
	dec, err := TenantAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", TenantID: "", Scopes: []string{"workflow"}},
		Operation: OpWorkflowCreate,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("empty tenant decision = %q, want deny (fail-closed)", dec)
	}
}

func TestTenantAwareAuthorizerDeniesMissingScope(t *testing.T) {
	dec, err := TenantAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", TenantID: "tenantA", Scopes: []string{}},
		Operation: OpWorkflowCreate,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("missing scope decision = %q, want deny", dec)
	}
}

func TestTenantAwareAuthorizerAllowsMatchingTenantAndScope(t *testing.T) {
	dec, err := TenantAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", TenantID: "tenantA", Scopes: []string{"management.read"}},
		Operation: OpManagementRead,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionAllow {
		t.Fatalf("matching decision = %q, want allow", dec)
	}
}

func TestTenantAwareAuthorizerDeniesCrossTenantResource(t *testing.T) {
	// ResourceTenant resolved by the route layer disagrees with the principal's
	// tenant → deny (defense in depth; the authoritative IDOR path is the
	// tenant-scoped store read, exercised in tenant_idor_test.go).
	dec, err := TenantAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal:      Principal{Subject: "x", TenantID: "tenantA", Scopes: []string{"management.read"}},
		Operation:      OpManagementRead,
		ResourceTenant: "tenantB",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("cross-tenant resource decision = %q, want deny", dec)
	}
}

func TestAuthzWrapInjectsPrincipalTenantIntoContext(t *testing.T) {
	// The authz wrapper injects the principal's TenantID into the request
	// context so downstream store reads are tenant-scoped. A stub handler
	// observes the context.
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", TenantID: "tenantA", Scopes: []string{"workflow"}}}
	m := authzModule(t, auth, TenantAwareAuthorizer{}, NewInMemoryAuditSink())

	var observed tenant.TenantID
	wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
		observed = tenant.FromContext(r.Context())
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	wrapped.ServeHTTP(rec, req)

	if observed != "tenantA" {
		t.Fatalf("context tenant = %q, want tenantA (principal-injected)", observed)
	}
}

func TestAuthzWrapAuditCarriesPrincipalTenant(t *testing.T) {
	// Tenant boundary (Task 7.4): the authz wrapper injects tenant into context
	// before audit append, and the SQL audit sink reads tenant from context. The
	// persisted audit record must carry the principal's tenant.
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", TenantID: "tenantA", Scopes: []string{"workflow"}}}
	db := memstore.New()
	audit := NewSQLAuditSink(db)
	m := authzModule(t, auth, TenantAwareAuthorizer{}, audit)

	wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	wrapped.ServeHTTP(rec, req)

	records := db.AuditRecords()
	if len(records) == 0 {
		t.Fatal("no audit records persisted")
	}
	for _, r := range records {
		if r.TenantID != "tenantA" {
			t.Fatalf("audit TenantID = %q, want tenantA; record=%+v", r.TenantID, r)
		}
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

// TestAuthzMutationAppendsReconcileOutcome proves the authz wrapper appends a
// second audit row after a mutation handler settles: reconciled on 2xx,
// failed on non-2xx. The admission row (admitted) and the reconcile row share
// the RequestID so audit reconciliation can join them.
func TestAuthzMutationAppendsReconcileOutcome(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}

	t.Run("reconciled on 2xx", func(t *testing.T) {
		audit := NewInMemoryAuditSink()
		m := authzModule(t, auth, ScopeAuthorizer{}, audit)
		// Mount a mutation handler that succeeds (writes 200).
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/workflows", func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		})
		// Re-register through the authz wrapper by reaching into the module's
		// helper: the production path is registerAuthzRoutes; here we exercise
		// the same authz closure directly via a minimal mux mount.
		wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		}, nil)
		mux2 := http.NewServeMux()
		mux2.HandleFunc("/v1/workflows", wrapped)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
		mux2.ServeHTTP(rec, req)

		events := audit.Events()
		// admission (admitted) + reconcile (reconciled)
		if len(events) != 2 {
			t.Fatalf("audit events = %d, want 2 (admission + reconcile)", len(events))
		}
		if events[0].Outcome != "admitted" {
			t.Fatalf("first event outcome = %q, want admitted", events[0].Outcome)
		}
		if events[1].Outcome != "reconciled" {
			t.Fatalf("second event outcome = %q, want reconciled", events[1].Outcome)
		}
		if events[0].RequestID != events[1].RequestID {
			t.Fatalf("admission and reconcile RequestIDs differ: %q vs %q", events[0].RequestID, events[1].RequestID)
		}
	})

	t.Run("failed on 5xx", func(t *testing.T) {
		audit := NewInMemoryAuditSink()
		m := authzModule(t, auth, ScopeAuthorizer{}, audit)
		wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
			writeJSON(w, http.StatusInternalServerError, errorResponse{Error: "boom"})
		}, nil)
		mux := http.NewServeMux()
		mux.HandleFunc("/v1/workflows", wrapped)

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
		mux.ServeHTTP(rec, req)

		events := audit.Events()
		if len(events) != 2 {
			t.Fatalf("audit events = %d, want 2 (admission + reconcile)", len(events))
		}
		if events[1].Outcome != "failed" {
			t.Fatalf("second event outcome = %q, want failed", events[1].Outcome)
		}
	})
}
