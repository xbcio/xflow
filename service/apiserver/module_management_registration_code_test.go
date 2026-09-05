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
		`{"allowed_namespaces":["sas"],"allowed_node_types":["kafka.trigger"]}`, http.StatusOK)

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

	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{"allowed_namespaces":["sas"]}`, http.StatusOK)
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

	body := h.doJSON(t, http.MethodPost, PathManagementRegistrationCodes, `{"allowed_namespaces":["sas"]}`, http.StatusOK)
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
