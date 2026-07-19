package apiserver

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// newMgmtAuthzModule builds a managementModule whose authzHolder is wired with a
// principal authenticator + authorizer + audit sink, so the leader/runner/
// executions routes go through the B3 authz wrapper (the production path).
func newMgmtAuthzModule(t *testing.T, auth PrincipalAuthenticator) *managementModule {
	t.Helper()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.principalAuth = auth
	m.authorizer = TenantAwareAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	return m
}

func mgmtMux(t *testing.T, m *managementModule) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

// TestManagementLeaderRunnerIndependentScope proves Task 8 blocker 2: the
// leader and runner routes get independent scopes (management.leader.read /
// management.runner.read). A token with only leader-read can hit leader but is
// denied (403) on runner; a token with only runner-read can hit runner but is
// denied on leader. Neither rides on a blanket management.read.
func TestManagementLeaderRunnerIndependentScope(t *testing.T) {
	principalAuth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-leader", Subject: "op-leader", TenantID: "tenantA", Scopes: []string{"management.leader.read"}},
		{Token: "tok-runner", Subject: "op-runner", TenantID: "tenantA", Scopes: []string{"management.runner.read"}},
		{Token: "tok-both", Subject: "op-both", TenantID: "tenantA", Scopes: []string{"management.leader.read", "management.runner.read"}},
	})
	mux := mgmtMux(t, newMgmtAuthzModule(t, principalAuth))

	// tok-leader: leader 200, runner 403.
	if code := doMgmt(mux, "tok-leader", "/v1/management/leader"); code != http.StatusOK {
		t.Fatalf("tok-leader /leader = %d, want 200", code)
	}
	if code := doMgmt(mux, "tok-leader", "/v1/management/runners/r1"); code != http.StatusForbidden {
		t.Fatalf("tok-leader /runners/r1 = %d, want 403 (independent scope)", code)
	}

	// tok-runner: runner 404 (authorized, not-found), leader 403.
	if code := doMgmt(mux, "tok-runner", "/v1/management/leader"); code != http.StatusForbidden {
		t.Fatalf("tok-runner /leader = %d, want 403 (independent scope)", code)
	}
	// Runner lookup is authorized (scope present) but the runner does not
	// exist → 404 (not 403), proving the scope check passed and the handler ran.
	if code := doMgmt(mux, "tok-runner", "/v1/management/runners/r1"); code != http.StatusNotFound {
		t.Fatalf("tok-runner /runners/r1 = %d, want 404 (authorized, not-found)", code)
	}

	// tok-both: both pass authz; leader 200, runner 404.
	if code := doMgmt(mux, "tok-both", "/v1/management/leader"); code != http.StatusOK {
		t.Fatalf("tok-both /leader = %d, want 200", code)
	}
	if code := doMgmt(mux, "tok-both", "/v1/management/runners/r1"); code != http.StatusNotFound {
		t.Fatalf("tok-both /runners/r1 = %d, want 404 (authorized, not-found)", code)
	}

	// No token → 401 on both (authenticate fails before authorize).
	if code := doMgmt(mux, "", "/v1/management/leader"); code != http.StatusUnauthorized {
		t.Fatalf("no-token /leader = %d, want 401", code)
	}
}

// TestManagementLeaderRunnerUnknownOpDenies is the unknown-op default-deny for
// the management surface: a token with no management scopes at all is denied
// (403) on leader and runner.
func TestManagementLeaderRunnerUnknownOpDenies(t *testing.T) {
	principalAuth := NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
		{Token: "tok-noscope", Subject: "op-noscope", TenantID: "tenantA", Scopes: []string{"workflow"}},
	})
	mux := mgmtMux(t, newMgmtAuthzModule(t, principalAuth))

	if code := doMgmt(mux, "tok-noscope", "/v1/management/leader"); code != http.StatusForbidden {
		t.Fatalf("tok-noscope /leader = %d, want 403 (default-deny)", code)
	}
	if code := doMgmt(mux, "tok-noscope", "/v1/management/runners/r1"); code != http.StatusForbidden {
		t.Fatalf("tok-noscope /runners/r1 = %d, want 403 (default-deny)", code)
	}
}

func doMgmt(mux http.Handler, token, path string) int {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Code
}
