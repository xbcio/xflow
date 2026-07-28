package apiserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/namespace"
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
	err       error
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
	// Single-token constructor maps to the default namespace so downstream code
	// always sees a non-empty, key-safe namespace.
	if p.Namespace != string(namespace.Default) {
		t.Fatalf("single-token namespace = %q, want %q (default)", p.Namespace, namespace.Default)
	}

	// Wrong token must not authenticate and must not leak principal.
	req2 := httptest.NewRequest(http.MethodGet, "/", nil)
	req2.Header.Set("Authorization", "Bearer wrong")
	if _, err := a.Authenticate(req2); err != ErrWorkflowUnauthenticated {
		t.Fatalf("wrong token: err=%v, want ErrWorkflowUnauthenticated", err)
	}
}

// TestBearerPrincipalAuthMultiMapsTokenToNamespace proves the multi-namespace token
// registry (design §2.3 scheme A) binds each token to its own namespace, and that
// the namespace is server-issued from the token (never self-reported).
func TestBearerPrincipalAuthMultiMapsTokenToNamespace(t *testing.T) {
	a := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"workflow", "execution", "management.read"}},
		{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"workflow", "execution", "management.read"}},
	})

	reqA := httptest.NewRequest(http.MethodGet, "/", nil)
	reqA.Header.Set("Authorization", "Bearer tok-a")
	pa, err := a.Authenticate(reqA)
	if err != nil {
		t.Fatalf("tok-a: %v", err)
	}
	if pa.Subject != "op-a" || pa.Namespace != "namespaceA" {
		t.Fatalf("tok-a principal = %+v, want op-a/namespaceA", pa)
	}

	reqB := httptest.NewRequest(http.MethodGet, "/", nil)
	reqB.Header.Set("Authorization", "Bearer tok-b")
	pb, err := a.Authenticate(reqB)
	if err != nil {
		t.Fatalf("tok-b: %v", err)
	}
	if pb.Subject != "op-b" || pb.Namespace != "namespaceB" {
		t.Fatalf("tok-b principal = %+v, want op-b/namespaceB", pb)
	}

	// A token not in the registry must not authenticate and must not leak which
	// tokens exist (same error as a missing token).
	reqX := httptest.NewRequest(http.MethodGet, "/", nil)
	reqX.Header.Set("Authorization", "Bearer tok-x")
	if _, err := a.Authenticate(reqX); err != ErrWorkflowUnauthenticated {
		t.Fatalf("unknown token: err=%v, want ErrWorkflowUnauthenticated", err)
	}

	// Empty Namespace in a mapping normalizes to default so every authenticated
	// principal carries a non-empty namespace (NamespaceAwareAuthorizer requires it).
	aDef := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-d", Subject: "op-d", Scopes: []string{"workflow"}},
	})
	reqD := httptest.NewRequest(http.MethodGet, "/", nil)
	reqD.Header.Set("Authorization", "Bearer tok-d")
	pd, err := aDef.Authenticate(reqD)
	if err != nil {
		t.Fatalf("tok-d: %v", err)
	}
	if pd.Namespace != string(namespace.Default) {
		t.Fatalf("empty namespace mapping normalized to %q, want %q", pd.Namespace, namespace.Default)
	}
}

// TestBearerPrincipalAuthImplementsWorkflowAuthenticator proves the multi-token
// registry can also gate the outer management middleware: any registered token
// passes AuthenticateRequest, and an unknown token fails with the same error as
// a missing token.
func TestBearerPrincipalAuthImplementsWorkflowAuthenticator(t *testing.T) {
	var auth WorkflowAuthenticator = NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-a", Subject: "op-a", Namespace: "namespaceA", Scopes: []string{"management.read"}},
		{Token: "tok-b", Subject: "op-b", Namespace: "namespaceB", Scopes: []string{"management.read"}},
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

func TestNamespaceAwareAuthorizerDeniesEmptyNamespace(t *testing.T) {
	// A principal with scopes but no namespace must be denied — fail-closed.
	dec, err := NamespaceAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", Namespace: "", Scopes: []string{"workflow"}},
		Operation: OpWorkflowCreate,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("empty namespace decision = %q, want deny (fail-closed)", dec)
	}
}

func TestNamespaceAwareAuthorizerDeniesMissingScope(t *testing.T) {
	dec, err := NamespaceAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", Namespace: "namespaceA", Scopes: []string{}},
		Operation: OpWorkflowCreate,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("missing scope decision = %q, want deny", dec)
	}
}

func TestNamespaceAwareAuthorizerAllowsMatchingNamespaceAndScope(t *testing.T) {
	dec, err := NamespaceAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal: Principal{Subject: "x", Namespace: "namespaceA", Scopes: []string{"management.read"}},
		Operation: OpManagementRead,
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionAllow {
		t.Fatalf("matching decision = %q, want allow", dec)
	}
}

func TestNamespaceAwareAuthorizerDeniesCrossNamespaceResource(t *testing.T) {
	// ResourceNamespace resolved by the route layer disagrees with the principal's
	// namespace → deny (defense in depth; the authoritative IDOR path is the
	// namespace-scoped store read, exercised in namespaceor_test.go).
	dec, err := NamespaceAwareAuthorizer{}.Authorize(context.Background(), AuthorizationRequest{
		Principal:         Principal{Subject: "x", Namespace: "namespaceA", Scopes: []string{"management.read"}},
		Operation:         OpManagementRead,
		ResourceNamespace: "namespaceB",
	})
	if err != nil {
		t.Fatalf("Authorize: %v", err)
	}
	if dec != DecisionDeny {
		t.Fatalf("cross-namespace resource decision = %q, want deny", dec)
	}
}

func TestAuthzWrapInjectsPrincipalNamespaceIntoContext(t *testing.T) {
	// The authz wrapper injects the principal's Namespace into the request
	// context so downstream store reads are namespace-scoped. A stub handler
	// observes the context.
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: "namespaceA", Scopes: []string{"workflow"}}}
	m := authzModule(t, auth, NamespaceAwareAuthorizer{}, NewInMemoryAuditSink())

	var observed namespace.Namespace
	wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
		observed = namespace.FromContext(r.Context())
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)
	wrapped.ServeHTTP(rec, req)

	if observed != "namespaceA" {
		t.Fatalf("context namespace = %q, want namespaceA (principal-injected)", observed)
	}
}

func TestAuthzWrapAuditCarriesPrincipalNamespace(t *testing.T) {
	// Namespace boundary (Task 7.4): the authz wrapper injects namespace into context
	// before audit append, and the SQL audit sink reads namespace from context. The
	// persisted audit record must carry the principal's namespace.
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Namespace: "namespaceA", Scopes: []string{"workflow"}}}
	db := memstore.New()
	audit := NewSQLAuditSink(db)
	m := authzModule(t, auth, NamespaceAwareAuthorizer{}, audit)

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
		if r.Namespace != "namespaceA" {
			t.Fatalf("audit Namespace = %q, want namespaceA; record=%+v", r.Namespace, r)
		}
	}
}

func TestNewFailsClosedWhenPrincipalAuthMissingAuthorizerOrAudit(t *testing.T) {
	auth := NewBearerPrincipalAuth("tok", "op", []string{"workflow"})

	// PrincipalAuth without Authorizer.
	if _, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: auth,
		AuditSink:     NewInMemoryAuditSink(),
	}); err == nil {
		t.Fatal("New() = nil, want error when PrincipalAuth set without Authorizer")
	}
	// PrincipalAuth without AuditSink.
	if _, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: auth,
		Authorizer:    ScopeAuthorizer{},
	}); err == nil {
		t.Fatal("New() = nil, want error when PrincipalAuth set without AuditSink")
	}
	// All three present: OK.
	srv, err := New(Config{
		Concurrency:   1,
		PrincipalAuth: auth,
		Authorizer:    ScopeAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("New() with all three: %v", err)
	}
	_ = srv
}

// TestAuthzMutationOutcomeIsSynchronousNotDefer proves the post-handler
// outcome audit is appended inline after the handler returns, not deferred.
// If the handler panics, the reconcile row must NOT be written by a deferred
// cleanup (crash-safety for that gap is T9's scope).
func TestAuthzMutationOutcomeIsSynchronousNotDefer(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"workflow"}}}
	audit := NewInMemoryAuditSink()
	m := authzModule(t, auth, ScopeAuthorizer{}, audit)

	wrapped := m.wrapForTest(OpWorkflowCreate, true, func(w http.ResponseWriter, r *http.Request) {
		panic("expected handler panic")
	}, nil)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows", nil)

	func() {
		defer func() {
			if rec := recover(); rec == nil {
				t.Fatal("expected handler panic")
			}
		}()
		wrapped.ServeHTTP(rec, req)
	}()

	events := audit.Events()
	if len(events) != 1 || events[0].Outcome != "admitted" {
		t.Fatalf("audit = %+v, want exactly one admission event (outcome is synchronous, not deferred)", events)
	}
}

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

// TestInMemoryAuditSinkConcurrentAppendIsRaceFree exercises the L fix: Append
// and Events() must be safe under concurrent use. Run with -race to detect the
// previously unsynchronized slice append.
func TestInMemoryAuditSinkConcurrentAppendIsRaceFree(t *testing.T) {
	sink := NewInMemoryAuditSink()
	const writers = 8
	const perWriter = 128

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := sink.Append(context.Background(), AuditEvent{Operation: "op"}); err != nil {
					t.Errorf("Append() error = %v", err)
					return
				}
				// Concurrent reader path must also be synchronized.
				_ = sink.Events()
			}
		}()
	}
	wg.Wait()

	if got := len(sink.Events()); got != writers*perWriter {
		t.Fatalf("recorded %d events, want %d", got, writers*perWriter)
	}
}
