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
	if err := store.Issue(context.Background(), control.IssuedIdentity{
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
	if err := expired.Issue(context.Background(), control.IssuedIdentity{
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
