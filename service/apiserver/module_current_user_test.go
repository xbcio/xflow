package apiserver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

// currentUserBody decodes the envelope's data as the raw map, so a test can
// assert on the wire shape (field names, and whether scopes is null or []).
func currentUserBody(t *testing.T, rec *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var envelope struct {
		Success bool           `json:"success"`
		Data    map[string]any `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("decode body %q: %v", rec.Body.String(), err)
	}
	if !envelope.Success {
		t.Fatalf("body = %q, want success envelope", rec.Body.String())
	}
	return envelope.Data
}

func getCurrentUser(t *testing.T, auth PrincipalAuthenticator) *httptest.ResponseRecorder {
	t.Helper()
	m := authzModule(t, auth, ScopeAuthorizer{}, NewInMemoryAuditSink())
	mux := http.NewServeMux()
	m.registerAuthzRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathCurrentUser, nil))
	return rec
}

// TestCurrentUserReportsTheVerifiedPrincipal pins the endpoint's whole purpose:
// the three fields are the principal the authenticator verified, reported
// verbatim. Nothing about the request participates — there is no parameter that
// could name a principal, which is what makes this endpoint incapable of
// disclosing another one.
func TestCurrentUserReportsTheVerifiedPrincipal(t *testing.T) {
	rec := getCurrentUser(t, staticPrincipalAuth{principal: Principal{
		Subject:   "alice",
		Namespace: "namespaceA",
		Scopes:    []string{"workflow", "execution"},
	}})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body := currentUserBody(t, rec)
	if body["subject"] != "alice" {
		t.Errorf("subject = %#v, want alice", body["subject"])
	}
	if body["namespace"] != "namespaceA" {
		t.Errorf("namespace = %#v, want namespaceA", body["namespace"])
	}
	scopes, ok := body["scopes"].([]any)
	if !ok {
		t.Fatalf("scopes = %#v (%T), want an array", body["scopes"], body["scopes"])
	}
	if len(scopes) != 2 || scopes[0] != "workflow" || scopes[1] != "execution" {
		t.Errorf("scopes = %v, want [workflow execution] in issue order", scopes)
	}
	if len(body) != 3 {
		t.Errorf("body has %d fields (%v), want exactly subject/namespace/scopes — "+
			"an extra field is a disclosure nobody reviewed", len(body), body)
	}
}

// TestCurrentUserRequiresAuthenticationOnly pins the deliberate choice in
// scopeForOperation (B5): the route needs no grant, because the response is the
// caller's own credential and a scope here could only refuse a caller the
// ability to read what it already holds. A console credential that cannot read
// its own scopes has its capabilities cleared (fail-closed) and loses controls
// it is allowed to use, so this must stay authentication-only rather than
// moving to workflow.read.
func TestCurrentUserRequiresAuthenticationOnly(t *testing.T) {
	if got := scopeForOperation(OpCurrentUserRead); got != ScopeAuthenticatedOnly {
		t.Fatalf("scopeForOperation(%s) = %q, want %q: the endpoint discloses "+
			"only the caller's own credential, so requiring any grant would "+
			"refuse a credential its own scopes",
			OpCurrentUserRead, got, ScopeAuthenticatedOnly)
	}
	// Behavior, not just the mapping: a principal with no scopes at all is
	// admitted by the shipped authorizers.
	principal := Principal{Subject: "scopeless", Namespace: "namespaceA"}
	if dec, err := (ScopeAuthorizer{}).Authorize(context.Background(), AuthorizationRequest{
		Principal: principal, Operation: OpCurrentUserRead,
	}); err != nil || dec != DecisionAllow {
		t.Fatalf("ScopeAuthorizer(scopeless, %s) = (%q, %v), want allow", OpCurrentUserRead, dec, err)
	}
	if dec, err := (NamespaceAwareAuthorizer{}).Authorize(context.Background(), AuthorizationRequest{
		Principal: principal, Operation: OpCurrentUserRead,
	}); err != nil || dec != DecisionAllow {
		t.Fatalf("NamespaceAwareAuthorizer(scopeless, %s) = (%q, %v), want allow", OpCurrentUserRead, dec, err)
	}
}

// TestCurrentUserAdmitsAuthenticatedPrincipalWithoutTheWriteScope is the other
// half of the binding above: an authenticated caller whose credential carries
// unrelated scopes (here only "execution") still reads its own identity. Before
// B5 this route demanded the workflow scope, so exactly this population lost its
// capabilities projection to a 403.
func TestCurrentUserAdmitsAuthenticatedPrincipalWithoutTheWriteScope(t *testing.T) {
	rec := getCurrentUser(t, staticPrincipalAuth{principal: Principal{
		Subject: "mallory", Namespace: "namespaceA", Scopes: []string{"execution"},
	}})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body := currentUserBody(t, rec)
	scopes, ok := body["scopes"].([]any)
	if !ok || len(scopes) != 1 || scopes[0] != "execution" {
		t.Errorf("scopes = %#v, want [execution]: the endpoint reports the credential verbatim", body["scopes"])
	}
}

func TestCurrentUserRejectsUnauthenticated(t *testing.T) {
	rec := getCurrentUser(t, staticPrincipalAuth{err: ErrWorkflowUnauthenticated})

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

// TestCurrentUserEncodesNoScopesAsAnEmptyList drives the handler directly: a
// scope-less principal now reaches this route through the authorizer too (it is
// authentication-only), but Principal is built by a PLUGGABLE authenticator, so
// a host can hand one over with no scopes. The handler is the boundary that
// promises a wire shape, and "no scopes" must reach the client as [] — a client
// that iterates the list should not have to guard against null.
func TestCurrentUserEncodesNoScopesAsAnEmptyList(t *testing.T) {
	m := authzModule(t, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, PathCurrentUser, nil)
	req = req.WithContext(context.WithValue(req.Context(), authzContextKey{},
		Principal{Subject: "scopeless", Namespace: "namespaceA"}))

	rec := httptest.NewRecorder()
	m.handleCurrentUser(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", rec.Code, rec.Body.String())
	}
	body := currentUserBody(t, rec)
	if body["scopes"] == nil {
		t.Fatalf("scopes = null, want []")
	}
	scopes, ok := body["scopes"].([]any)
	if !ok || len(scopes) != 0 {
		t.Fatalf("scopes = %#v, want an empty array", body["scopes"])
	}
}

// TestCurrentUserRefusesAnEmptySubject covers the other pluggable-authenticator
// hazard: a principal that verified to nobody. An empty subject would reach a
// client as a successful answer naming no one, which reads as "authenticated as
// the empty string" rather than "not authenticated".
func TestCurrentUserRefusesAnEmptySubject(t *testing.T) {
	m := authzModule(t, nil, nil, nil)
	req := httptest.NewRequest(http.MethodGet, PathCurrentUser, nil)
	req = req.WithContext(context.WithValue(req.Context(), authzContextKey{}, Principal{}))

	rec := httptest.NewRecorder()
	m.handleCurrentUser(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body = %s", rec.Code, rec.Body.String())
	}
}

// TestCurrentUserIsNotMountedWithoutPrincipalAuth pins the registration
// asymmetry: the legacy branch serves no identity at all, so the route is absent
// there rather than answering a caller something the server never verified.
func TestCurrentUserIsNotMountedWithoutPrincipalAuth(t *testing.T) {
	m := authzModule(t, nil, nil, nil)
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathCurrentUser, nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404: without a PrincipalAuthenticator there is no "+
			"verified identity to report; body = %s", rec.Code, rec.Body.String())
	}
}
