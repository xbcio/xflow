package apiserver

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/xbcio/xflow/store"
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

// PUT /v1/supplies/{name} 已被封（Z.5）。这不是「暂时没权限」，是这个动词不存在：
// 唯一的写入口是嵌入方进程内的 sdk/xflow.Server.UpdateSupply(IfMatch)，它前面
// 由嵌入方（SAS）套自己的 session + 授权 + 审计。
//
// 关键点：这个测试故意给主体配上 "supply.write" scope。若哪天有人把 PUT 分支
// 加回 resolver，鉴权会放行，写会成功，这条测试就红——这才是它的意义。断言的是
// 「动词不存在」，不是「权限不足」。
func TestSupplyWriteVerbIsSealed(t *testing.T) {
	mux, supplies := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPut, "/v1/supplies/rules", bytes.NewReader([]byte(`{"v":1}`)))
	req.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(rec, req)

	// 404 而不是 405/403：module_supply.go 的既定反存在性探测约定。
	if rec.Code != http.StatusNotFound {
		t.Fatalf("PUT = %d, want 404；写动词必须不存在，body=%s", rec.Code, rec.Body)
	}
	if !strings.Contains(rec.Body.String(), "route_not_found") {
		t.Fatalf("body = %s, want route_not_found（403/权限类错误说明分支还在）", rec.Body)
	}

	// 反向断言必须真的反向：确认这次 PUT 一个字节都没落地。
	if _, err := supplies.GetSupply(context.Background(), "ns1", "rules"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("PUT 被 404 之后 GetSupply = %v, want ErrNotFound；说明字节还是写进去了", err)
	}

	// 读路径必须仍然可达——本次只封写，不封整个 supply API。
	if _, err := supplies.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "ns1", Name: "rules", Content: []byte(`{"v":1}`), ContentType: "application/json",
	}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	getRec := httptest.NewRecorder()
	mux.ServeHTTP(getRec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if getRec.Code != http.StatusOK {
		t.Fatalf("GET = %d, want 200；封写不得连读一起封掉", getRec.Code)
	}
}

func TestSupplyGetNotModified(t *testing.T) {
	mux, supplies := newSupplyTestServer(t, []string{"supply.write", "supply.read"}, "ns1")
	if _, err := supplies.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "ns1", Name: "rules", Content: []byte(`{"rules":[]}`), ContentType: "application/json",
	}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}

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

	if _, err := st.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "tenant-a", Name: "rules", Content: []byte(`from-a`), ContentType: "application/json",
	}, nil); err != nil {
		t.Fatalf("seed tenant-a: %v", err)
	}

	rec := httptest.NewRecorder()
	b.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("tenant-b read of tenant-a supply = %d, want 404", rec.Code)
	}

	if _, err := st.PutSupply(context.Background(), &store.SupplyResource{
		Namespace: "tenant-b", Name: "rules", Content: []byte(`from-b`), ContentType: "application/json",
	}, nil); err != nil {
		t.Fatalf("seed tenant-b: %v", err)
	}
	back := httptest.NewRecorder()
	a.ServeHTTP(back, httptest.NewRequest(http.MethodGet, "/v1/supplies/rules", nil))
	if got := back.Body.String(); got != "from-a" {
		t.Fatalf("tenant-a content = %q, tenant-b clobbered it", got)
	}
}

// A supply operation must never fall through to the default-deny "" scope: that
// would make the route silently unreachable rather than authorizable.
//
// supply 只剩读 op：写动词已封（Z.5），OpSupplyWrite 常量不再存在。
func TestSupplyOperationsHaveScopes(t *testing.T) {
	for _, op := range []string{OpSupplyRead} {
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
