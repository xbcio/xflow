package apiserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/store"
)

func TestRegistrationCodeOpsHaveScopes(t *testing.T) {
	// A new Op without a scopeForOperation case makes the route silently
	// unreachable: the wrapper resolves an empty scope, no principal can hold
	// it, and every request 403s with no hint why (repo hard rule).
	for _, op := range []string{
		OpRegistrationCodeCreate,
		OpRegistrationCodeList,
		OpRegistrationCodeRevoke,
		OpRegistrationCodeAudit,
	} {
		if got := scopeForOperation(op); got == "" {
			t.Fatalf("scopeForOperation(%q) = \"\"; the route is unreachable", op)
		}
	}
}

func TestRegistrationCodePathsAreInUserFacingPaths(t *testing.T) {
	// Task 8 addendum Ruling A: PathManagementRunners does not exist yet (it is
	// Task 9's constant); asserting it here would make this package fail to
	// compile. Only the three Path* constants this task defines are asserted.
	want := map[string]bool{
		PathManagementRegistrationCodes:     false,
		PathManagementRegistrationCodeByID:  false,
		PathManagementRegistrationCodeAudit: false,
	}
	for _, p := range UserFacingPaths {
		if _, ok := want[p]; ok {
			want[p] = true
		}
	}
	for p, found := range want {
		if !found {
			t.Fatalf("%q missing from UserFacingPaths; the dead-constant guard exists for this", p)
		}
	}
}

// newRegistrationCodeTestServer builds a managementModule wired with an
// in-memory registration-code store and a principal holding all four
// registration-code scopes, mirroring newMgmtAuthzServer /
// newMgmtAuthzModule's shape elsewhere in this package. It serves over a real
// httptest.Server (not httptest.NewRecorder) so doJSON can exercise a full
// request/response round trip including JSON body encoding/decoding.
type registrationCodeTestServer struct {
	srv   *httptest.Server
	codes control.RegistrationCodeStore
}

func newRegistrationCodeTestServer(t *testing.T) *registrationCodeTestServer {
	t.Helper()
	codes := control.NewMemoryRegistrationCodeStore()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.codes = codes
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject:   "ops",
		Namespace: "namespaceA",
		Scopes: []string{
			"management.registration_code.create",
			"management.registration_code.list",
			"management.registration_code.revoke",
			"management.registration_code.audit",
		},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &registrationCodeTestServer{srv: httptest.NewServer(mux), codes: codes}
}

// newRegistrationCodeTestServerNoAuth builds the same module WITHOUT a
// PrincipalAuthenticator, so RegisterHTTP's `if m.principalAuth != nil` guard
// never mounts the registration-code routes at all.
func newRegistrationCodeTestServerNoAuth(t *testing.T) *registrationCodeTestServer {
	t.Helper()
	codes := control.NewMemoryRegistrationCodeStore()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.codes = codes
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &registrationCodeTestServer{srv: httptest.NewServer(mux), codes: codes}
}

// newGlobalRegistrationCodeTestServer builds the same module as
// newRegistrationCodeTestServer, but the principal additionally holds all
// four `_global` scopes. _global is additive, not an alternative
// route-admission scope (authz.go's ScopeRegistrationCodeCreateGlobal
// comment): the base scope is still required to reach the handler at all, so
// both the base and the _global scopes must be present.
func newGlobalRegistrationCodeTestServer(t *testing.T) *registrationCodeTestServer {
	t.Helper()
	codes := control.NewMemoryRegistrationCodeStore()
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.codes = codes
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject:   "platform-op",
		Namespace: "namespaceA",
		Scopes: []string{
			"management.registration_code.create",
			"management.registration_code.list",
			"management.registration_code.revoke",
			"management.registration_code.audit",
			"management.registration_code.create_global",
			"management.registration_code.list_global",
			"management.registration_code.revoke_global",
			"management.registration_code.audit_global",
		},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return &registrationCodeTestServer{srv: httptest.NewServer(mux), codes: codes}
}

// doJSON sends method/path (optionally with a JSON body), asserts the response
// status is wantStatus, and returns the raw response body.
func (h *registrationCodeTestServer) doJSON(t *testing.T, method, path, body string, wantStatus int) string {
	t.Helper()
	var reader *strings.Reader
	if body != "" {
		reader = strings.NewReader(body)
	} else {
		reader = strings.NewReader("")
	}
	req, err := http.NewRequest(method, h.srv.URL+path, reader)
	if err != nil {
		t.Fatalf("NewRequest %s %s: %v", method, path, err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := h.srv.Client().Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body for %s %s: %v", method, path, err)
	}
	got := string(data)
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s: status = %d, want %d; body = %q", method, path, resp.StatusCode, wantStatus, got)
	}
	return got
}

// recordAudit appends an enroll audit record directly through the store the
// module was built with, bypassing HTTP — the audit trail is written by the
// enroll endpoint (Task 7), not by this management API, so the test drives it
// straight through the store as brief step 1 specifies.
func (h *registrationCodeTestServer) recordAudit(t *testing.T, codeID string, success bool, reason, runnerID, sourceIP string) {
	t.Helper()
	if err := h.codes.AppendEnrollAudit(context.Background(), control.EnrollAuditRecord{
		CodeID:   codeID,
		Success:  success,
		Reason:   reason,
		RunnerID: runnerID,
		SourceIP: sourceIP,
		At:       time.Now().UTC(),
	}); err != nil {
		t.Fatalf("AppendEnrollAudit: %v", err)
	}
}

func TestCreateRegistrationCodeReturnsPlaintextExactlyOnce(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	defer h.srv.Close()

	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes,
		`{"allowed_namespaces":["namespaceA"],"allowed_node_types":["kafka.trigger"]}`, http.StatusOK)

	var created struct {
		Data struct {
			ID   string `json:"id"`
			Code string `json:"code"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if created.Data.ID == "" || created.Data.Code == "" {
		t.Fatalf("create response = %q, want both id and code", body)
	}

	// The list endpoint must never echo it back — the plaintext exists exactly
	// once, in this response, and is not recoverable afterwards.
	listBody := h.doJSON(t, http.MethodGet, PathManagementRegistrationCodes, "", http.StatusOK)
	if strings.Contains(listBody, created.Data.Code) {
		t.Fatalf("list echoed the plaintext code: %q", listBody)
	}
	if !strings.Contains(listBody, created.Data.ID) {
		t.Fatalf("list did not contain the created code id: %q", listBody)
	}
	// Nor may it leak the hash: publishing sha256(code) makes the code offline-
	// crackable by anyone who can read the list.
	if strings.Contains(strings.ToLower(listBody), "hash") {
		t.Fatalf("list exposed a hash field: %q", listBody)
	}
}

func TestRevokeRegistrationCode(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	defer h.srv.Close()

	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{"allowed_namespaces":["namespaceA"]}`, http.StatusOK)
	var created struct {
		Data struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	h.doJSON(t, http.MethodDelete, "/v1/management/registration-codes/"+created.Data.ID, "", http.StatusOK)

	listBody := h.doJSON(t, http.MethodGet, PathManagementRegistrationCodes, "", http.StatusOK)
	if !strings.Contains(listBody, `"revoked":true`) {
		t.Fatalf("list does not reflect the revocation: %q", listBody)
	}
	// Revoking an unknown id is a 404, not a silent success.
	h.doJSON(t, http.MethodDelete, "/v1/management/registration-codes/no-such-id", "", http.StatusNotFound)
}

func TestRegistrationCodeAuditIsReadable(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	defer h.srv.Close()

	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{"allowed_namespaces":["namespaceA"]}`, http.StatusOK)
	var created struct {
		Data struct {
			ID   string `json:"id"`
			Code string `json:"code"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Drive one failed and one successful enroll straight through the store the
	// module was built with, then read the audit back through HTTP.
	h.recordAudit(t, created.Data.ID, false, "unknown code", "", "10.0.0.1")
	h.recordAudit(t, created.Data.ID, true, "", "runner-x", "10.0.0.2")

	auditBody := h.doJSON(t, http.MethodGet,
		"/v1/management/registration-codes/"+created.Data.ID+"/audit", "", http.StatusOK)
	if !strings.Contains(auditBody, "10.0.0.1") || !strings.Contains(auditBody, "runner-x") {
		t.Fatalf("audit body missing records: %q", auditBody)
	}
	if !strings.Contains(auditBody, `"success":false`) {
		t.Fatalf("audit body must include the failed attempt: %q", auditBody)
	}
}

func TestRegistrationCodeRoutesAbsentWithoutPrincipalAuth(t *testing.T) {
	// Without a PrincipalAuthenticator these routes must not exist at all.
	// This is STRICTER than the other management routes, not "the same rule":
	// leader/runner/exec degrade to a bare (unwrapped) handler in the else
	// branch when principalAuth is nil (module_management.go RegisterHTTP), and
	// dead-letters mount unconditionally and self-wrap authz inside the
	// handler. Neither precedent applies to a route that mints runner
	// credentials — this one has no else branch and no unconditional mount, so
	// principalAuth == nil leaves it genuinely unregistered (404), full stop
	// (Task 8 addendum Ruling Y).
	h := newRegistrationCodeTestServerNoAuth(t)
	defer h.srv.Close()
	h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{}`, http.StatusNotFound)
	h.doJSON(t, http.MethodGet, PathManagementRegistrationCodes, "", http.StatusNotFound)
}

// TestCreateRegistrationCodeRejectsNamespacesAbovePrincipal is the regression
// test for the H1 privilege-escalation chain: a tenant principal holding only
// management.registration_code.create could mint a code whose
// AllowedNamespaces said "*", enroll a runner with it, and reach every
// namespace on the server.
func TestCreateRegistrationCodeRejectsNamespacesAbovePrincipal(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"wildcard", `{"allowed_namespaces":["*"]}`},
		{"another tenant", `{"allowed_namespaces":["namespaceB"]}`},
		{"own plus another", `{"allowed_namespaces":["namespaceA","namespaceB"]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRegistrationCodeTestServer(t)
			defer h.srv.Close()
			body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, tc.body, http.StatusForbidden)
			if strings.Contains(body, "\"code\"") && strings.Contains(body, "eyJ") {
				t.Fatalf("rejected request still returned a plaintext code: %s", body)
			}
		})
	}
}

// TestCreateRegistrationCodeFillsEmptyNamespacesWithPrincipal pins the empty-set
// trap. RunnerPolicy.AllowsNamespace treats an empty AllowedNamespaces as "the
// default namespace ONLY" — which for a principal in namespaceA is a grant it
// does not hold. Empty must therefore be filled in explicitly, never stored as
// empty.
func TestCreateRegistrationCodeFillsEmptyNamespacesWithPrincipal(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	defer h.srv.Close()

	h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{}`, http.StatusOK)

	list, err := h.codes.List(context.Background(), control.OwnerScope{All: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("stored %d codes, want 1", len(list))
	}
	if got := list[0].AllowedNamespaces; len(got) != 1 || got[0] != "namespaceA" {
		t.Fatalf("AllowedNamespaces = %v, want [namespaceA] — an empty slice would\n"+
			"grant the default namespace, which this principal does not hold", got)
	}
	if list[0].OwnerNamespace != "namespaceA" {
		t.Fatalf("OwnerNamespace = %q, want namespaceA", list[0].OwnerNamespace)
	}
}

// TestRegistrationCodeReadsAreNamespaceScoped covers the other three
// endpoints. There is no /foreign/revoke route; cross-namespace revocation is
// exercised the same way any other revoke is (DELETE .../{id}), the only
// difference being that {id} names a code minted by a different namespace.
func TestRegistrationCodeReadsAreNamespaceScoped(t *testing.T) {
	h := newRegistrationCodeTestServer(t)
	defer h.srv.Close()

	if err := h.codes.Create(context.Background(), control.RegistrationCode{
		ID:             "foreign",
		CodeHash:       control.HashSecret("foreign-plaintext"),
		OwnerNamespace: "namespaceB",
		CreatedAt:      time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	listBody := h.doJSON(t, http.MethodGet, PathManagementRegistrationCodes, "", http.StatusOK)
	if strings.Contains(listBody, "foreign") {
		t.Fatalf("list leaked another namespace's code: %s", listBody)
	}

	h.doJSON(t, http.MethodDelete, "/v1/management/registration-codes/foreign", "", http.StatusNotFound)
	h.doJSON(t, http.MethodGet, "/v1/management/registration-codes/foreign/audit", "", http.StatusNotFound)
}

// failingRegistrationCodeStoreListErr is a control.RegistrationCodeStore test
// double whose List always fails, wrapping store.ErrEnrollScopeCorrupted to
// mimic the real failure mode a corrupted scope column produces (Task 7). Its
// other methods are never exercised by the one test that uses it and return
// zero values.
type failingRegistrationCodeStoreListErr struct {
	// uniqueDetail is baked into the wrapped error text. The test asserts this
	// exact string never reaches the HTTP response body — proving the handler
	// does not forward err.Error() to the caller, not merely that it returns
	// 500 (a 500 that echoes err.Error() is still a 500).
	uniqueDetail string
}

func (s failingRegistrationCodeStoreListErr) Create(context.Context, control.RegistrationCode) error {
	return nil
}

func (s failingRegistrationCodeStoreListErr) ResolveByPlaintext(context.Context, string) (control.RegistrationCode, error) {
	return control.RegistrationCode{}, nil
}

func (s failingRegistrationCodeStoreListErr) List(context.Context, control.OwnerScope) ([]control.RegistrationCode, error) {
	return nil, fmt.Errorf("registrationCodeStore: scope column decode failed (%s): %w", s.uniqueDetail, store.ErrEnrollScopeCorrupted)
}

func (s failingRegistrationCodeStoreListErr) Revoke(context.Context, string, control.OwnerScope) error {
	return nil
}

func (s failingRegistrationCodeStoreListErr) AppendEnrollAudit(context.Context, control.EnrollAuditRecord) error {
	return nil
}

func (s failingRegistrationCodeStoreListErr) EnrollAudit(context.Context, string, control.OwnerScope) ([]control.EnrollAuditRecord, error) {
	return nil, nil
}

// TestListRegistrationCodesStoreFailureIsGeneric pins fix1 Important-1: a List
// failure (e.g. store.ErrEnrollScopeCorrupted, the "one bad scope row poisons
// the whole page" contract Task 7 gave List) must surface as a generic 500,
// never as an empty list (that would hide storage corruption from the one
// surface an operator could act on it from — see the handler's own comment)
// and never with the underlying error text in the body (org security policy
// §7: production exceptions return a generic message, detail stays
// server-side).
func TestListRegistrationCodesStoreFailureIsGeneric(t *testing.T) {
	const uniqueDetail = "xzq-9f3c1b-storage-detail-that-must-not-leak"
	m := newManagementModule(fakeControlPlaneForAuthz(t))
	m.codes = failingRegistrationCodeStoreListErr{uniqueDetail: uniqueDetail}
	m.principalAuth = staticPrincipalAuth{principal: Principal{
		Subject:   "ops",
		Namespace: "namespaceA",
		Scopes:    []string{"management.registration_code.list"},
	}}
	m.authorizer = ScopeAuthorizer{}
	m.audit = NewInMemoryAuditSink()
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodGet, srv.URL+PathManagementRegistrationCodes, strings.NewReader(""))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", PathManagementRegistrationCodes, err)
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	body := string(data)

	// 1. Status is 500.
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500; body = %q", resp.StatusCode, body)
	}

	// 2. The envelope's stable code is internal_error.
	var env struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if env.Code != "internal_error" {
		t.Fatalf("code = %q, want internal_error; body = %q", env.Code, body)
	}

	// 3. The response body must not contain the original error text — neither
	// the sentinel's own message nor the unique detail this test's double
	// baked into the wrapped error. A body that echoed err.Error() would still
	// be a 500 with code internal_error, so this is the assertion that
	// actually has teeth (fix1 Important-1's own framing).
	if strings.Contains(body, store.ErrEnrollScopeCorrupted.Error()) {
		t.Fatalf("response body leaked the sentinel error text: %q", body)
	}
	if strings.Contains(body, uniqueDetail) {
		t.Fatalf("response body leaked the store's internal error detail: %q", body)
	}
}

// TestRegistrationCodeHandlersAnswer404WithoutStore pins fix1 Important-2:
// registrationCodeUnavailable (the nil-store branch each of the four handlers
// opens with) must answer 404 route_not_found, for all four handlers. The
// server here has PrincipalAuth configured (so RegisterHTTP's `if
// m.principalAuth != nil` mounts the four routes) but leaves
// RegistrationCodes/IssuedIdentities unset — the same shape
// paths_test.go's newFullGuardMux builds for the dead-constant/behavioral
// guards, but constructed independently here through the public New +
// WithManagement API so this test does not touch that shared fixture (fix1
// instructions explicitly forbid editing newFullGuardMux itself).
//
// The request is authenticated AND fully scoped, not anonymous: an
// unauthenticated request would be rejected by authzWrap's 401 before ever
// reaching the handler, which would prove authn works but say nothing about
// this nil-store branch. Reaching 404 here requires passing straight through
// authzWrap into registrationCodeUnavailable.
func TestRegistrationCodeHandlersAnswer404WithoutStore(t *testing.T) {
	srv, err := New(Config{
		PrincipalAuth: staticPrincipalAuth{principal: Principal{
			Subject:   "ops",
			Namespace: "namespaceA",
			Scopes: []string{
				"management.registration_code.create",
				"management.registration_code.list",
				"management.registration_code.revoke",
				"management.registration_code.audit",
			},
		}},
		Authorizer: ScopeAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
		// RegistrationCodes / IssuedIdentities deliberately left unset (nil).
	}, WithManagement())
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	ts := httptest.NewServer(srv.Handler())
	defer ts.Close()

	cases := []struct {
		name   string
		method string
		path   string
	}{
		{"create", http.MethodPost, PathManagementRegistrationCodes},
		{"list", http.MethodGet, PathManagementRegistrationCodes},
		{"revoke", http.MethodDelete, "/v1/management/registration-codes/rc-1"},
		{"audit", http.MethodGet, "/v1/management/registration-codes/rc-1/audit"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequest(tc.method, ts.URL+tc.path, strings.NewReader(""))
			if err != nil {
				t.Fatalf("NewRequest %s %s: %v", tc.method, tc.path, err)
			}
			resp, err := ts.Client().Do(req)
			if err != nil {
				t.Fatalf("%s %s: %v", tc.method, tc.path, err)
			}
			defer func() { _ = resp.Body.Close() }()
			data, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatalf("read body for %s %s: %v", tc.method, tc.path, err)
			}
			body := string(data)
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s: status = %d, want 404; body = %q", tc.method, tc.path, resp.StatusCode, body)
			}
			var env struct {
				Code string `json:"code"`
			}
			if err := json.Unmarshal(data, &env); err != nil {
				t.Fatalf("unmarshal %q: %v", body, err)
			}
			if env.Code != "route_not_found" {
				t.Fatalf("%s %s: code = %q, want route_not_found; body = %q", tc.method, tc.path, env.Code, body)
			}
		})
	}
}

// TestResolveRequestedNamespacesRejectsWildcardPrincipalNamespace is the
// regression test for the H1 reopening this fix (1a) closes: a principal
// namespace of "*" — reachable only through a misconfigured token file, since
// namespace.Validate rejects "*" everywhere else it is checked — must not
// reach RunnerPolicy.AllowsNamespace's policy side, where "*" means match
// everything. If the guard in resolveRequestedNamespaces is removed or
// weakened to a bare `p.Namespace == "*"` string check (which would miss every
// OTHER illegal character), this test fails: a tenant principal with an
// illegal namespace of "*" would get back the full requested set instead of
// an error.
func TestResolveRequestedNamespacesRejectsWildcardPrincipalNamespace(t *testing.T) {
	p := Principal{
		Subject:   "tenant",
		Namespace: "*",
		Scopes:    []string{"management.registration_code.create"},
	}
	got, err := resolveRequestedNamespaces(p, []string{"namespaceA", "namespaceB"})
	if err == nil {
		t.Fatalf("resolveRequestedNamespaces(namespace=*) error = nil, want error; got namespaces = %v", got)
	}
	if got != nil {
		t.Fatalf("resolveRequestedNamespaces(namespace=*) namespaces = %v, want nil on error", got)
	}
}

// TestResolveRequestedNamespacesRejectsWildcardPrincipalNamespaceGlobal covers
// the create_global branch named explicitly in the fix1 brief: a _global
// operator never builds a ceiling from p.Namespace, but
// handleCreateRegistrationCode still stamps OwnerNamespace: p.Namespace
// regardless of branch, so an illegal owner value must be rejected here too,
// before it can be persisted.
func TestResolveRequestedNamespacesRejectsWildcardPrincipalNamespaceGlobal(t *testing.T) {
	p := Principal{
		Subject:   "platform-op",
		Namespace: "*",
		Scopes:    []string{"management.registration_code.create_global"},
	}
	got, err := resolveRequestedNamespaces(p, []string{"namespaceA"})
	if err == nil {
		t.Fatalf("resolveRequestedNamespaces(namespace=*, global) error = nil, want error; got namespaces = %v", got)
	}
	if got != nil {
		t.Fatalf("resolveRequestedNamespaces(namespace=*, global) namespaces = %v, want nil on error", got)
	}
}

// TestCreateRegistrationCodeGlobalRejectsEmptyNamespaces is the regression
// test for fix2: the create_global branch must not silently persist an empty
// AllowedNamespaces when the operator omits allowed_namespaces from the
// request body. Unlike the tenant branch (which has a safe default — the
// caller's own namespace), a _global creator has no such default, so an
// omission must be a 400 bad_request naming what is missing, never a 200 that
// stores an empty (default-namespace-only) grant.
func TestCreateRegistrationCodeGlobalRejectsEmptyNamespaces(t *testing.T) {
	h := newGlobalRegistrationCodeTestServer(t)
	defer h.srv.Close()
	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{}`, http.StatusBadRequest)

	var env struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("unmarshal %q: %v", body, err)
	}
	if env.Code != "bad_request" {
		t.Fatalf("code = %q, want bad_request; body = %q", env.Code, body)
	}

	list, err := h.codes.List(context.Background(), control.OwnerScope{All: true})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 0 {
		t.Fatalf("stored %d codes, want 0 — the rejected request must not persist anything", len(list))
	}
}

// TestListRegistrationCodesGlobalScopeDisclosesForeignAllowedNamespaces pins a
// DELIBERATE property, not a bug: RUNNER-IDENTITY-LIFECYCLE-TODO.md §6 records
// that registrationCodeView's list projection has historically disclosed
// other tenants' scope, that the H1 namespace-scoping fix made this harmless
// ONLY because "list returns exclusively the codes the caller owns" now holds
// for every non-_global caller (TestRegistrationCodeReadsAreNamespaceScoped
// pins that half), and that nothing pinned the other half: a caller holding
// list_global is intentionally handed every owner's codes, AllowedNamespaces
// included, in plaintext.
//
// This test exists so that the day someone either (a) widens list_global-like
// visibility to a broader role "for an operations view", or (b) tries to
// delete AllowedNamespaces from registrationCodeView because it looks like a
// leak, they hit a red test first — one whose comment already explains why
// the disclosure is here and what §6 says it costs to remove the "list_global
// only" guard. It must NOT be "fixed" by narrowing what list_global returns:
// that scope's entire purpose is a platform-wide view across tenants.
func TestListRegistrationCodesGlobalScopeDisclosesForeignAllowedNamespaces(t *testing.T) {
	h := newGlobalRegistrationCodeTestServer(t)
	defer h.srv.Close()

	// The caller's own namespace (namespaceA, per
	// newGlobalRegistrationCodeTestServer) gets one code; a different tenant
	// (namespaceB) gets another, with an AllowedNamespaces value distinctive
	// enough that finding it in the response body cannot be a coincidence.
	if err := h.codes.Create(context.Background(), control.RegistrationCode{
		ID:                "code-tenant-a",
		CodeHash:          control.HashSecret("plaintext-a"),
		OwnerNamespace:    "namespaceA",
		AllowedNamespaces: []string{"namespaceA"},
		CreatedAt:         time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed namespaceA code: %v", err)
	}
	if err := h.codes.Create(context.Background(), control.RegistrationCode{
		ID:                "code-tenant-b",
		CodeHash:          control.HashSecret("plaintext-b"),
		OwnerNamespace:    "namespaceB",
		AllowedNamespaces: []string{"namespaceB-secret-scope"},
		CreatedAt:         time.Now().UTC(),
	}); err != nil {
		t.Fatalf("seed namespaceB code: %v", err)
	}

	listBody := h.doJSON(t, http.MethodGet, PathManagementRegistrationCodes, "", http.StatusOK)

	if !strings.Contains(listBody, "code-tenant-a") {
		t.Fatalf("global list is missing the caller's own code: %s", listBody)
	}
	if !strings.Contains(listBody, "code-tenant-b") {
		t.Fatalf("global list is missing the foreign tenant's code — a list_global "+
			"caller must see every owner's codes, not just its own: %s", listBody)
	}
	// The load-bearing assertion: the foreign code's AllowedNamespaces appears
	// in plaintext in a global list response. This is the exact cross-tenant
	// scope disclosure §6 describes, made visible on purpose so the cost of
	// ever widening list's audience beyond list_global is not invisible.
	if !strings.Contains(listBody, "namespaceB-secret-scope") {
		t.Fatalf("global list did not disclose the foreign code's allowed_namespaces: %s\n"+
			"either newRegistrationCodeView stopped populating AllowedNamespaces, or "+
			"handleListRegistrationCodes stopped granting list_global the full store — "+
			"re-read RUNNER-IDENTITY-LIFECYCLE-TODO.md §6 before changing either one", listBody)
	}
}
