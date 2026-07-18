package apiserver

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/backend/local"
	"github.com/xbcio/xflow/service/control"
)

// newMgmtAuthzServer builds an apiserver management module with the B3 authz
// path enabled (PrincipalAuth + Authorizer + AuditSink), backed by an in-memory
// control plane (its StateStore does NOT implement DeadLetterStore, so the
// dead-letter routes surface 503 for the backend — but the authz wrapper still
// runs authenticate/authorize/audit before the handler).
func newMgmtAuthzServer(t *testing.T, principalAuth PrincipalAuthenticator, authorizer Authorizer, audit AuditSink) (*managementModule, http.Handler) {
	t.Helper()
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	m.principalAuth = principalAuth
	m.authorizer = authorizer
	m.audit = audit
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return m, mux
}

func TestDeadLetterRoutesRejectWithoutPrincipalAuth(t *testing.T) {
	// No PrincipalAuthenticator → the route is not exposed (404), so a dev
	// server without authz never serves the privileged replay path.
	cp, err := control.NewControlPlane(control.Config{Backend: local.New()})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	m := newManagementModule(cp)
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/management/dead-letters/exec-1", nil)
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (no authz configured)", rec.Code)
	}
}

func TestDeadLetterReplayDeniesMissingScope(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "mallory", Scopes: []string{}}}
	audit := NewInMemoryAuditSink()
	_, mux := newMgmtAuthzServer(t, auth, ScopeAuthorizer{}, audit)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/management/dead-letters/exec-1/replay", nil)
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403 (missing deadletter.replay scope)", rec.Code)
	}
	events := audit.Events()
	if len(events) != 1 || events[0].Decision != DecisionDeny {
		t.Fatalf("audit = %+v, want one deny", events)
	}
	if events[0].Operation != OpDeadLetterReplay {
		t.Fatalf("audit operation = %q, want %q", events[0].Operation, OpDeadLetterReplay)
	}
}

func TestDeadLetterListAllowsAndReachesBackend(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"deadletter.list"}}}
	audit := NewInMemoryAuditSink()
	// The in-memory backend DOES implement DeadLetterStore (local dead-letter
	// list returns an empty page). This proves the request passed authz and
	// reached the manager → store, returning 200.
	_, mux := newMgmtAuthzServer(t, auth, ScopeAuthorizer{}, audit)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/v1/management/dead-letters/exec-1?limit=10", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (list reached backend)", rec.Code)
	}
	events := audit.Events()
	if len(events) != 1 || events[0].Decision != DecisionAllow || events[0].Outcome != "admitted" {
		t.Fatalf("audit = %+v, want one allow/admitted (read path)", events)
	}
	if events[0].Operation != OpDeadLetterList {
		t.Fatalf("audit operation = %q, want %q", events[0].Operation, OpDeadLetterList)
	}
}

func TestDeadLetterReplayAllowsAndReachesBackend(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"deadletter.replay"}}}
	audit := NewInMemoryAuditSink()
	_, mux := newMgmtAuthzServer(t, auth, ScopeAuthorizer{}, audit)

	body := `{"entry_id":"e1","reason":"retry after fix"}`
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/management/dead-letters/exec-1/replay", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)

	// authz allowed → reached manager → store. With no dead-letter entry for a
	// nonexistent execution the store returns not_found, surfaced as 200 with
	// the outcome body (replay is idempotent; the outcome carries the verdict).
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (replay reached backend)", rec.Code)
	}
	events := audit.Events()
	if len(events) != 2 {
		t.Fatalf("audit = %d events, want 2 (admission + reconcile)", len(events))
	}
	if events[0].Outcome != "admitted" {
		t.Fatalf("admission outcome = %q, want admitted", events[0].Outcome)
	}
	// The reconcile row records reconciled (handler returned 2xx).
	if events[1].Outcome != "reconciled" {
		t.Fatalf("reconcile outcome = %q, want reconciled", events[1].Outcome)
	}
}

func TestDeadLetterReplayRejectsMissingFields(t *testing.T) {
	auth := staticPrincipalAuth{principal: Principal{Subject: "alice", Scopes: []string{"deadletter.replay"}}}
	audit := NewInMemoryAuditSink()
	_, mux := newMgmtAuthzServer(t, auth, ScopeAuthorizer{}, audit)

	// Missing reason → 400 before reaching the backend.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/management/dead-letters/exec-1/replay", strings.NewReader(`{"entry_id":"e1"}`))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 (entry_id+reason required)", rec.Code)
	}
}
