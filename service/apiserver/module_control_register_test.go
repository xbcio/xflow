package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/service/control"
)

// newRegisterTestServer builds an apiserver over an in-memory local backend
// control plane with a bearer PrincipalAuthenticator so unauthenticated
// requests are rejected before the register handler runs. It returns the httptest
// server and the ControlPlane so the test can assert the persisted registry
// record directly.
func newRegisterTestServer(t *testing.T) (*httptest.Server, *control.ControlPlane) {
	t.Helper()
	backend := local.New()
	cp, err := control.NewControlPlane(control.Config{Backend: backend})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	cfg := Config{
		Concurrency: 1,
		PrincipalAuth: NewBearerPrincipalAuthMulti([]TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		}),
		Authorizer: NamespaceAwareAuthorizer{},
		AuditSink:  NewInMemoryAuditSink(),
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv, cp
}

func postRegister(t *testing.T, base, token string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		var buf bytes.Buffer
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
		r = &buf
	}
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/workflows/register", r)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows/register: %v", err)
	}
	return resp
}

func TestRegisterWorkflowPersistsCompiledGraph(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	resp := postRegister(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out registerWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.WorkflowID == "" {
		t.Fatal("workflow_id must be non-empty")
	}

	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("GetWorkflow: %v", err)
	}
	if rec.Graph == nil {
		t.Fatal("persisted record must carry a compiled Graph")
	}
	if rec.ID != out.WorkflowID {
		t.Fatalf("record ID = %q, want %q", rec.ID, out.WorkflowID)
	}
	// Namespace must be resolved server-side from the principal, never the body.
	if rec.Namespace != "namespaceA" {
		t.Fatalf("record Namespace = %q, want namespaceA (server-side)", rec.Namespace)
	}
}

func TestRegisterWorkflowRequiresAuth(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	// No token → 401.
	resp := postRegister(t, srv.URL, "", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 for unauthenticated register", resp.StatusCode)
	}
}

// TestDeregisterWorkflowRemovesRecord registers a workflow then deletes it via
// DELETE /v1/workflows/register/{id} and asserts the registry no longer holds it.
func TestDeregisterWorkflowRemovesRecord(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	resp := postRegister(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.WorkflowID == "" {
		t.Fatal("workflow_id must be non-empty")
	}

	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/workflows/register/"+string(out.WorkflowID), nil)
	req.Header.Set("Authorization", "Bearer tok-full")
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = delResp.Body.Close() }()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", delResp.StatusCode)
	}

	if _, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID); err == nil {
		t.Fatalf("record still present after deregister; want removed")
	}
}
