package apiserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

type runnerPoolAPITestServer struct {
	srv   *httptest.Server
	pools *control.MemoryRunnerPoolStore
	codes *control.MemoryRegistrationCodeStore
}

func newRunnerPoolAPITestServer(t *testing.T, global bool) *runnerPoolAPITestServer {
	t.Helper()
	pools := control.NewMemoryRunnerPoolStore()
	codes := control.NewMemoryRegistrationCodeStore()
	scopes := []string{scopeForOperation(OpManagementRunnerPoolRead), scopeForOperation(OpManagementRunnerPoolWrite)}
	if global {
		scopes = append(scopes, ScopeManagementRunnerPoolReadGlobal, ScopeManagementRunnerPoolWriteGlobal)
	}
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.pools = pools
	m.codes = codes
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "ops", Namespace: "tenant-a", Scopes: scopes}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &runnerPoolAPITestServer{srv: httptest.NewServer(mux), pools: pools, codes: codes}
}

func (h *runnerPoolAPITestServer) request(t *testing.T, method, path, body string, want int) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != want {
		t.Fatalf("%s %s status=%d want=%d body=%s", method, path, resp.StatusCode, want, data)
	}
	return resp, data
}

func decodeRunnerPoolEnvelopeData(t *testing.T, body []byte, out any) {
	t.Helper()
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil {
		t.Fatalf("decode envelope: %v: %s", err, body)
	}
	if err := json.Unmarshal(envelope.Data, out); err != nil {
		t.Fatalf("decode data: %v: %s", err, body)
	}
}

func TestRunnerPoolOpsHaveScopesAndPathsAreUserFacing(t *testing.T) {
	for _, op := range []string{OpManagementRunnerPoolRead, OpManagementRunnerPoolWrite} {
		if scopeForOperation(op) == "" {
			t.Fatalf("scopeForOperation(%q) is empty", op)
		}
	}
	want := map[string]bool{
		PathManagementRunnerPools: false, PathManagementRunnerPoolByID: false,
		PathManagementRunnerPoolTokens: false, PathManagementRunnerPoolTokenByID: false,
		PathManagementRunnerPoolTokenAudit: false, PathManagementRunnerPoolRunners: false,
	}
	for _, path := range UserFacingPaths {
		if _, ok := want[path]; ok {
			want[path] = true
		}
	}
	for path, found := range want {
		if !found {
			t.Errorf("%s missing from UserFacingPaths", path)
		}
	}
}

func TestRunnerPoolManagementAPI(t *testing.T) {
	h := newRunnerPoolAPITestServer(t, false)
	defer h.srv.Close()

	_, body := h.request(t, http.MethodPost, PathManagementRunnerPools,
		`{"name":"workers","owner_kind":"tenant","allowed_namespaces":["tenant-a"],"allowed_node_types":["xflow.http","xflow.wait"],"labels":{"zone":"a"},"max_instances":3,"inherit_namespaces":true}`, http.StatusCreated)
	var created runnerPoolView
	decodeRunnerPoolEnvelopeData(t, body, &created)
	if created.ID == "" || created.OwnerNamespace != "tenant-a" || created.Name != "workers" {
		t.Fatalf("created=%+v", created)
	}
	base := "/v1/management/runner-pools/" + created.ID

	h.request(t, http.MethodGet, PathManagementRunnerPools, "", http.StatusOK)
	h.request(t, http.MethodGet, base, "", http.StatusOK)
	_, body = h.request(t, http.MethodPatch, base,
		`{"name":"workers-v2","allowed_node_types":["xflow.http"],"paused":true}`, http.StatusOK)
	var updated runnerPoolView
	decodeRunnerPoolEnvelopeData(t, body, &updated)
	if updated.Name != "workers-v2" || !updated.Paused || len(updated.AllowedNodeTypes) != 1 {
		t.Fatalf("updated=%+v", updated)
	}
	// Widening a scope after creation is forbidden.
	h.request(t, http.MethodPatch, base, `{"allowed_node_types":["xflow.http","xflow.wait"]}`, http.StatusBadRequest)

	resp, tokenBody := h.request(t, http.MethodPost, base+"/tokens", `{}`, http.StatusOK)
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("Cache-Control=%q", resp.Header.Get("Cache-Control"))
	}
	var token runnerPoolTokenCreateResponse
	decodeRunnerPoolEnvelopeData(t, tokenBody, &token)
	if token.ID == "" || token.Code == "" {
		t.Fatalf("token=%+v", token)
	}
	_, listBody := h.request(t, http.MethodGet, base+"/tokens", "", http.StatusOK)
	if strings.Contains(string(listBody), token.Code) || strings.Contains(strings.ToLower(string(listBody)), "hash") {
		t.Fatalf("token list leaked secret material: %s", listBody)
	}
	codes, err := h.codes.List(context.Background(), store.OwnerScope{Namespace: "tenant-a"})
	if err != nil || len(codes) != 1 || codes[0].PoolID != created.ID || codes[0].MaxUses != 0 {
		t.Fatalf("pool token persisted incorrectly: codes=%+v err=%v", codes, err)
	}
	if err := h.codes.AppendEnrollAudit(context.Background(), control.EnrollAuditRecord{
		CodeID: token.ID, Success: true, Reason: "first", RunnerID: "runner-1", SourceIP: "192.0.2.1", At: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEnrollAudit: %v", err)
	}
	_, auditBody := h.request(t, http.MethodGet, base+"/tokens/"+token.ID+"/audit", "", http.StatusOK)
	if !strings.Contains(string(auditBody), "runner-1") || !strings.Contains(string(auditBody), "first") {
		t.Fatalf("token audit=%s", auditBody)
	}
	h.request(t, http.MethodGet, base+"/tokens/no-such-token/audit", "", http.StatusNotFound)
	h.request(t, http.MethodDelete, base+"/tokens/"+token.ID, "", http.StatusOK)

	_, err = h.pools.EnrollInstance(context.Background(), store.EnrollInstanceRequest{
		PoolID: created.ID, SystemID: "pod-1", InstanceUID: "uid-1", CandidateRunnerID: "runner-1", Now: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("EnrollInstance: %v", err)
	}
	_, runnersBody := h.request(t, http.MethodGet, base+"/runners", "", http.StatusOK)
	if !strings.Contains(string(runnersBody), "runner-1") || !strings.Contains(string(runnersBody), "pod-1") {
		t.Fatalf("runners=%s", runnersBody)
	}

	h.request(t, http.MethodDelete, base, "", http.StatusOK)
	h.request(t, http.MethodGet, base, "", http.StatusNotFound)
}

func TestRunnerPoolCrossTenantIsNotFound(t *testing.T) {
	h := newRunnerPoolAPITestServer(t, false)
	defer h.srv.Close()
	if err := h.pools.CreatePool(context.Background(), store.RunnerPool{
		ID: "foreign", Name: "foreign", OwnerKind: store.PoolOwnerTenant,
		OwnerNamespace: "tenant-b", AllowedNamespaces: []string{"tenant-b"},
		AllowedNodeTypes: []string{"xflow.http"}, CreatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	base := "/v1/management/runner-pools/foreign"
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodGet, base, ""}, {http.MethodPatch, base, `{}`}, {http.MethodDelete, base, ""},
		{http.MethodPost, base + "/tokens", `{}`}, {http.MethodGet, base + "/tokens", ""},
		{http.MethodDelete, base + "/tokens/token-1", ""}, {http.MethodGet, base + "/tokens/token-1/audit", ""},
		{http.MethodGet, base + "/runners", ""},
	} {
		h.request(t, tc.method, tc.path, tc.body, http.StatusNotFound)
	}
}

func TestRunnerPoolPlatformAndWildcardRequireGlobal(t *testing.T) {
	h := newRunnerPoolAPITestServer(t, false)
	defer h.srv.Close()
	h.request(t, http.MethodPost, PathManagementRunnerPools,
		`{"name":"platform","owner_kind":"platform","allowed_namespaces":["tenant-a"],"allowed_node_types":["xflow.http"]}`, http.StatusForbidden)
	h.request(t, http.MethodPost, PathManagementRunnerPools,
		`{"name":"wild","owner_kind":"tenant","allowed_namespaces":["tenant-a"],"allowed_node_types":["*"]}`, http.StatusForbidden)

	global := newRunnerPoolAPITestServer(t, true)
	defer global.srv.Close()
	_, body := global.request(t, http.MethodPost, PathManagementRunnerPools,
		`{"name":"platform","owner_kind":"platform","allowed_namespaces":["*"],"allowed_node_types":["*"]}`, http.StatusCreated)
	var pool runnerPoolView
	decodeRunnerPoolEnvelopeData(t, body, &pool)
	if pool.OwnerKind != store.PoolOwnerPlatform || pool.OwnerNamespace != "" {
		t.Fatalf("platform pool=%+v", pool)
	}
}

func TestRunnerPoolRoutesUnavailableWithoutStore(t *testing.T) {
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.principalAuth = staticPrincipalAuth{principal: Principal{Subject: "ops", Namespace: "tenant-a", Scopes: []string{
		scopeForOperation(OpManagementRunnerPoolRead), scopeForOperation(OpManagementRunnerPoolWrite),
	}}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, PathManagementRunnerPools, nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
