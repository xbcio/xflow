package apiserver

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store/memstore"
)

// newSupplyTestServer wires a supplyModule with a stub principal that carries
// scopes and namespace, plus an in-memory store.
func newSupplyTestServer(t *testing.T, scopes []string, ns string) (*http.ServeMux, *memstore.Store) {
	t.Helper()
	st := memstore.New()
	m := newSupplyModule(st)
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "test-user", Namespace: ns, Scopes: scopes}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux, st
}

func TestSupplyPutThenGet(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")

	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules",
		bytes.NewReader([]byte(`{"rules":[1]}`)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, body=%s", rec.Code, rec.Body)
	}
	var put struct {
		Revision    uint64 `json:"revision"`
		ContentHash string `json:"content_hash"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &put); err != nil {
		t.Fatalf("decode PUT response: %v", err)
	}
	if put.Revision != 1 || !strings.HasPrefix(put.ContentHash, "sha256:") {
		t.Fatalf("PUT response = %+v", put)
	}

	get := httptest.NewRecorder()
	mux.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if get.Code != http.StatusOK {
		t.Fatalf("GET = %d", get.Code)
	}
	if got := get.Body.String(); got != `{"rules":[1]}` {
		t.Fatalf("GET body = %q", got)
	}
	if got := get.Header().Get("ETag"); got != put.ContentHash {
		t.Fatalf("ETag = %q, want %q", got, put.ContentHash)
	}
}

func TestSupplyGetNotModified(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`{"rules":[]}`))))

	first := httptest.NewRecorder()
	mux.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	etag := first.Header().Get("ETag")

	req := httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil)
	req.Header.Set("If-None-Match", etag)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotModified {
		t.Fatalf("conditional GET = %d, want 304", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("304 must carry no body, got %d bytes", rec.Body.Len())
	}
}

func TestSupplyPutIfMatchConflict(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`a`))))

	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`b`)))
	req.Header.Set("If-Match", "99")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusConflict {
		t.Fatalf("stale If-Match = %d, want 409", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "revision_conflict") {
		t.Fatalf("409 body = %s", rec.Body)
	}
}

func TestSupplyPutTooLarge(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write"}, "ns1")
	big := bytes.Repeat([]byte("x"), maxSupplyContentBytes+1)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader(big)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized PUT = %d, want 413", rec.Code)
	}
}

// The namespace must come from the authenticated principal, never from the
// client. Two principals in different namespaces writing the same name must not
// see each other's content.
func TestSupplyNamespaceIsolation(t *testing.T) {
	st := memstore.New()

	mk := func(ns string) *http.ServeMux {
		m := newSupplyModule(st)
		m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "user", Namespace: ns, Scopes: []string{"supply.write", "supply.read"}}}
		m.authorizer = ScopeAuthorizer{}
		m.audit = NewInMemoryAuditSink()
		mux := http.NewServeMux()
		m.RegisterHTTP(mux)
		return mux
	}
	a, b := mk("tenant-a"), mk("tenant-b")

	a.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`from-a`))))

	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant-b read of tenant-a supply = %d, want 404", rec.Code)
	}

	b.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`from-b`))))
	back := httptest.NewRecorder()
	a.ServeHTTP(back, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if got := back.Body.String(); got != "from-a" {
		t.Fatalf("tenant-a content = %q, tenant-b clobbered it", got)
	}
}

func TestSupplyRequiresScope(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"workflow"}, "ns1")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(
		http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`x`))))
	if rec.Code != http.StatusForbidden {
		t.Fatalf("PUT without supply.write = %d, want 403", rec.Code)
	}
}

// A supply operation must never fall through to the default-deny "" scope: that
// would make the route silently unreachable rather than authorizable.
func TestSupplyOperationsHaveScopes(t *testing.T) {
	for _, op := range []string{OpSupplyWrite, OpSupplyRead} {
		if scopeForOperation(op) == "" {
			t.Fatalf("scopeForOperation(%q) is empty — the route would be unreachable", op)
		}
	}
}

func TestSupplyMethodNotAllowed(t *testing.T) {
	mux, _ := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodDelete, "/v1/supplies/rules", nil))
	if rec.Code != http.StatusNotFound && rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE = %d, want 404 or 405", rec.Code)
	}
}

// Without PrincipalAuth the supply routes must not exist. Serving them through
// the legacy allow-all path would expose rule rewriting to any caller.
func TestSupplyNotRegisteredWithoutPrincipalAuth(t *testing.T) {
	m := newSupplyModule(memstore.New())
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unauthenticated GET = %d, want 404 (route must not be registered)", rec.Code)
	}
}
