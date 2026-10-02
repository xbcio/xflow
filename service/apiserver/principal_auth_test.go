package apiserver

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/store/memstore"
	"github.com/xbcio/xflow/store/objectstore"
)

func issuedIdentityForPrincipalAuth(t *testing.T, namespaces []string) *control.MemoryIssuedIdentityStore {
	t.Helper()
	store := control.NewMemoryIssuedIdentityStore()
	if err := store.Issue(context.Background(), control.IssuedIdentity{PoolID: "test-pool",
		RunnerID:  "runner-issued",
		TokenHash: control.HashSecret("issued-token"),
		Scope: control.RunnerPolicy{
			Name:              "runner-issued",
			IDPrefix:          "runner-",
			AllowedNamespaces: namespaces,
			AllowedNodeTypes:  []string{"xflow.function"},
		},
		IssuedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("Issue: %v", err)
	}
	return store
}

func issuedPrincipalRequest() *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil)
	req.Header.Set(protocol.RunnerIDHeader, "runner-issued")
	req.Header.Set("Authorization", "Bearer issued-token")
	req.Header.Set(objectstore.NamespaceHeader, "team-a")
	return req
}

func TestContextPrincipalAuthenticatorUsesOnlyTrustedContext(t *testing.T) {
	auth := ContextPrincipalAuthenticator()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("X-Xflow-Principal", "attacker")
	if _, err := auth.Authenticate(req); !errors.Is(err, ErrPrincipalNotApplicable) {
		t.Fatalf("header-only request error = %v, want ErrPrincipalNotApplicable", err)
	}

	original := Principal{Subject: "embedded-host", Namespace: "team-a", Scopes: []string{"workflow"}}
	req = req.WithContext(ContextWithPrincipal(req.Context(), original))
	original.Scopes[0] = "mutated"
	principal, err := auth.Authenticate(req)
	if err != nil {
		t.Fatalf("Authenticate trusted context: %v", err)
	}
	if principal.Subject != "embedded-host" || principal.Namespace != "team-a" || !principal.HasScope("workflow") {
		t.Fatalf("principal = %+v, want copied host principal", principal)
	}
}

func TestHTTPTransportInfoFromRequestUsesPeerAddress(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.RemoteAddr = "127.0.0.9:43120"
	info := httpTransportInfoFromRequest(req)
	if info.Kind != control.TransportKindHTTP {
		t.Fatalf("Kind = %q, want %q", info.Kind, control.TransportKindHTTP)
	}
	if got := info.SourceIP; got != "127.0.0.9" {
		t.Fatalf("SourceIP = %q, want 127.0.0.9", got)
	}
	req.Header.Set("X-Forwarded-For", "198.51.100.7")
	if got := httpTransportInfoFromRequest(req).SourceIP; got != "127.0.0.9" {
		t.Fatalf("SourceIP trusted X-Forwarded-For: %q", got)
	}
}

func TestIssuedIdentityPrincipalAuthenticatorChecksIdentityLifecycleAndScope(t *testing.T) {
	auth := NewIssuedIdentityPrincipalAuthenticator(issuedIdentityForPrincipalAuth(t, []string{"team-a"}))
	principal, err := auth.Authenticate(issuedPrincipalRequest())
	if err != nil {
		t.Fatalf("Authenticate issued identity: %v", err)
	}
	if principal.Subject != "runner-issued" || principal.Namespace != "team-a" || !principal.HasScope(ScopeRunnerResource) {
		t.Fatalf("principal = %+v, want issued runner resource principal", principal)
	}

	outOfScope := issuedPrincipalRequest()
	outOfScope.Header.Set(objectstore.NamespaceHeader, "team-b")
	if _, err := auth.Authenticate(outOfScope); err != ErrWorkflowUnauthenticated {
		t.Fatalf("out-of-scope namespace error = %v, want ErrWorkflowUnauthenticated", err)
	}

	expired := control.NewMemoryIssuedIdentityStore()
	if err := expired.Issue(context.Background(), control.IssuedIdentity{PoolID: "test-pool",
		RunnerID: "runner-expired", TokenHash: control.HashSecret("expired-token"),
		Scope:     control.RunnerPolicy{IDPrefix: "runner-", AllowedNamespaces: []string{"team-a"}},
		ExpiresAt: time.Now().UTC().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("issue expired identity: %v", err)
	}
	expiredReq := issuedPrincipalRequest()
	expiredReq.Header.Set(protocol.RunnerIDHeader, "runner-expired")
	expiredReq.Header.Set("Authorization", "Bearer expired-token")
	if _, err := NewIssuedIdentityPrincipalAuthenticator(expired).Authenticate(expiredReq); err != ErrWorkflowUnauthenticated {
		t.Fatalf("expired identity error = %v, want ErrWorkflowUnauthenticated", err)
	}
}

// countingIssuedIdentityStore records the store reads one authentication
// performs. Lookup is the only method this path uses; everything else is
// forwarded to the embedded store.
type countingIssuedIdentityStore struct {
	control.IssuedIdentityStore
	lookups int
}

func (s *countingIssuedIdentityStore) Lookup(ctx context.Context, runnerID string) (control.IssuedIdentity, bool, error) {
	s.lookups++
	return s.IssuedIdentityStore.Lookup(ctx, runnerID)
}

// TestIssuedIdentityPrincipalAuthenticatorLooksUpTheIdentityOnce pins the
// single-read contract on the hottest path a runner has: the authenticator must
// probe existence to decide whether a request carries an issued credential at
// all, and must then authenticate against that same row rather than fetching it
// a second time. Against a remote SQL store the duplicate read doubled the
// round trips of every runner request — measured in the SAS deployment as ~26
// identity reads/sec serving ~8.5 admission requests/sec.
func TestIssuedIdentityPrincipalAuthenticatorLooksUpTheIdentityOnce(t *testing.T) {
	counted := &countingIssuedIdentityStore{
		IssuedIdentityStore: issuedIdentityForPrincipalAuth(t, []string{"team-a"}),
	}
	auth := NewIssuedIdentityPrincipalAuthenticator(counted)

	if _, err := auth.Authenticate(issuedPrincipalRequest()); err != nil {
		t.Fatalf("Authenticate issued identity: %v", err)
	}
	if counted.lookups != 1 {
		t.Fatalf("store lookups for an accepted credential = %d, want 1", counted.lookups)
	}

	counted.lookups = 0
	unknown := issuedPrincipalRequest()
	unknown.Header.Set(protocol.RunnerIDHeader, "runner-absent")
	if _, err := auth.Authenticate(unknown); !errors.Is(err, ErrPrincipalNotApplicable) {
		t.Fatalf("unknown runner id error = %v, want ErrPrincipalNotApplicable", err)
	}
	if counted.lookups != 1 {
		t.Fatalf("store lookups for an unknown runner id = %d, want 1: the existence probe "+
			"is the only read this path may issue", counted.lookups)
	}
}

// TestIssuedIdentityPrincipalAuthenticatorRejectsRowMutations proves the
// single-read path still enforces every check the lookup path enforces: the
// store row handed to the authenticator is the only input, and a wrong token,
// a revoked row, an expired row, or a mismatched ID prefix must each be refused
// with the terminal error rather than falling through to another authenticator.
func TestIssuedIdentityPrincipalAuthenticatorRejectsRowMutations(t *testing.T) {
	const good = "issued-token"
	mutations := []struct {
		name   string
		change func(*control.IssuedIdentity)
	}{
		{"wrong token", func(*control.IssuedIdentity) {}},
		{"revoked", func(id *control.IssuedIdentity) { id.RevokedAt = time.Now().UTC() }},
		{"expired", func(id *control.IssuedIdentity) { id.ExpiresAt = time.Now().UTC().Add(-time.Minute) }},
		{"no pool", func(id *control.IssuedIdentity) { id.PoolID = "" }},
		{"prefix mismatch", func(id *control.IssuedIdentity) { id.Scope.IDPrefix = "other-" }},
	}

	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			store := control.NewMemoryIssuedIdentityStore()
			id := control.IssuedIdentity{
				PoolID: "test-pool", RunnerID: "runner-mutated", TokenHash: control.HashSecret(good),
				Scope:    control.RunnerPolicy{Name: "runner-mutated", IDPrefix: "runner-", AllowedNamespaces: []string{"team-a"}},
				IssuedAt: time.Now().UTC(),
			}
			tc.change(&id)
			if err := store.Issue(context.Background(), id); err != nil {
				t.Fatalf("Issue: %v", err)
			}
			req := issuedPrincipalRequest()
			req.Header.Set(protocol.RunnerIDHeader, id.RunnerID)
			if tc.name == "wrong token" {
				req.Header.Set("Authorization", "Bearer not-"+good)
			}
			if _, err := NewIssuedIdentityPrincipalAuthenticator(store).Authenticate(req); !errors.Is(err, ErrWorkflowUnauthenticated) {
				t.Fatalf("mutated identity error = %v, want terminal ErrWorkflowUnauthenticated", err)
			}
		})
	}
}

func TestMultiPrincipalAuthenticatorDoesNotDowngradeInvalidIssuedIdentity(t *testing.T) {
	issued := NewIssuedIdentityPrincipalAuthenticator(issuedIdentityForPrincipalAuth(t, []string{"team-a"}))
	static := NewBearerPrincipalAuth("static-token", "operator", []string{"workflow"})
	auth := NewMultiPrincipalAuthenticator(ContextPrincipalAuthenticator(), issued, static)

	invalidIssued := issuedPrincipalRequest()
	invalidIssued.Header.Set("Authorization", "Bearer static-token")
	if _, err := auth.Authenticate(invalidIssued); err != ErrWorkflowUnauthenticated {
		t.Fatalf("invalid issued credential error = %v, want terminal ErrWorkflowUnauthenticated", err)
	}

	// SDK resource clients declare RunnerID for static runners too. An ID not
	// known to the issued store must therefore continue to the regular auth
	// path, while the preceding assertion pins that a known issued ID cannot.
	staticRunner := httptest.NewRequest(http.MethodGet, "/", nil)
	staticRunner.Header.Set(protocol.RunnerIDHeader, "runner-static")
	staticRunner.Header.Set("Authorization", "Bearer static-token")
	principal, err := auth.Authenticate(staticRunner)
	if err != nil || principal.Subject != "operator" {
		t.Fatalf("static runner fallback = (%+v, %v), want operator / nil", principal, err)
	}

	staticOnly := httptest.NewRequest(http.MethodGet, "/", nil)
	staticOnly.Header.Set("Authorization", "Bearer static-token")
	principal, err = auth.Authenticate(staticOnly)
	if err != nil || principal.Subject != "operator" {
		t.Fatalf("static fallback = (%+v, %v), want operator / nil", principal, err)
	}
}

func TestIssuedPrincipalHasOnlyRunnerResourceAuthorization(t *testing.T) {
	principal, err := NewIssuedIdentityPrincipalAuthenticator(issuedIdentityForPrincipalAuth(t, []string{"team-a"})).Authenticate(issuedPrincipalRequest())
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	for _, operation := range []string{OpSupplyRead, OpArtifactRead, OpExecutionSeed} {
		if decision, err := (ScopeAuthorizer{}).Authorize(context.Background(), AuthorizationRequest{Principal: principal, Operation: operation}); err != nil || decision != DecisionAllow {
			t.Fatalf("%s authorization = (%q, %v), want allow", operation, decision, err)
		}
	}
	if decision, err := (ScopeAuthorizer{}).Authorize(context.Background(), AuthorizationRequest{Principal: principal, Operation: OpWorkflowRead}); err != nil || decision != DecisionDeny {
		t.Fatalf("workflow authorization = (%q, %v), want deny", decision, err)
	}
}

func TestAPIServerComposesEnrollmentIssuedPrincipalAuth(t *testing.T) {
	ids := issuedIdentityForPrincipalAuth(t, []string{"team-a"})
	server, err := New(Config{
		RegistrationCodes: control.NewMemoryRegistrationCodeStore(),
		IssuedIdentities:  ids,
		PrincipalAuth:     NewBearerPrincipalAuth("static-token", "operator", []string{"workflow"}),
		Authorizer:        ScopeAuthorizer{},
		AuditSink:         NewInMemoryAuditSink(),
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	principal, err := server.cfg.PrincipalAuth.Authenticate(issuedPrincipalRequest())
	if err != nil || principal.Subject != "runner-issued" {
		t.Fatalf("composed issued authentication = (%+v, %v), want runner-issued / nil", principal, err)
	}
}

func TestEnrollmentIssuedPrincipalCanFetchItsEntitledSupply(t *testing.T) {
	ctx := context.Background()
	ids := issuedIdentityForPrincipalAuth(t, []string{"team-a"})
	supplies := memstore.New()
	if _, err := supplies.PutSupply(ctx, &store.SupplyResource{
		Namespace: "team-a", Name: "rules", Content: []byte("issued-content"), ContentType: "text/plain",
	}, nil); err != nil {
		t.Fatalf("seed supply: %v", err)
	}
	server, err := New(Config{
		RegistrationCodes: control.NewMemoryRegistrationCodeStore(),
		IssuedIdentities:  ids,
		PrincipalAuth:     NewBearerPrincipalAuth("static-token", "operator", []string{"workflow"}),
		Authorizer:        NamespaceAwareAuthorizer{},
		AuditSink:         NewInMemoryAuditSink(),
		Supplies:          supplies,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(server.Handler())
	defer ts.Close()
	request, err := http.NewRequest(http.MethodGet, ts.URL+"/v1/supplies/rules", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	request.Header.Set(protocol.RunnerIDHeader, "runner-issued")
	request.Header.Set("Authorization", "Bearer issued-token")
	request.Header.Set(objectstore.NamespaceHeader, "team-a")
	response, err := ts.Client().Do(request)
	if err != nil {
		t.Fatalf("supply request: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("supply status = %d, want 200", response.StatusCode)
	}
}
