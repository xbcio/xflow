package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
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

// clearFailingEntryStore wraps a real EntryActivationStore but fails any Upsert
// that clears desired-state (Desired=false), simulating a partial deregister
// failure. All other operations delegate so the register-time derivation still
// succeeds.
type clearFailingEntryStore struct {
	engine.EntryActivationStore
}

func (s clearFailingEntryStore) Upsert(ctx context.Context, act engine.EntryActivation) error {
	if !act.Desired {
		return errors.New("injected clear failure")
	}
	return s.EntryActivationStore.Upsert(ctx, act)
}

// triggerWorkflow is a workflow whose entry is a single remote-hosted trigger
// node carrying a RunnerSelector, so registering it derives an EntryActivation
// and deregistering it must clear that desired-state.
func triggerWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "trig-wf",
		Version: "v1",
		Nodes: []types.NodeDef{
			{
				Name: "trig", Type: "kafka.source", Version: 1, Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"zone": "a"}},
				Parameters:     map[string]any{"topic": "orders"},
			},
			{Name: "work", Type: "http.request", Version: 1, Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": {{Node: "work", Input: "main"}}}},
	}
}

// TestDeregisterClearsActivationsBeforeRemovingRecord verifies the fix-round-1
// ordering: the desired-state clear runs BEFORE the registry record is removed,
// so a clear failure leaves the registry record present (independently
// retryable) rather than stranding an orphaned Desired activation. A workflow
// whose clear fails must return 500 AND keep the registry record, so a retry
// can re-fetch the graph and re-clear.
func TestDeregisterClearsActivationsBeforeRemovingRecord(t *testing.T) {
	backend := local.New()
	store := clearFailingEntryStore{EntryActivationStore: control.NewMemoryEntryActivationStore()}
	cp, err := control.NewControlPlane(control.Config{
		Backend:              backend,
		EntryActivationStore: store,
	})
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

	// Register the trigger workflow (derivation writes a Desired activation).
	resp := postRegister(t, httpSrv.URL, "tok-full", triggerWorkflow())
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("register status = %d, want 200", resp.StatusCode)
	}
	var out registerWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}

	// Deregister: the clear fails → 500, and the registry record MUST survive so
	// a retry can re-clear.
	req, _ := http.NewRequest(http.MethodDelete, httpSrv.URL+"/v1/workflows/register/"+string(out.WorkflowID), nil)
	req.Header.Set("Authorization", "Bearer tok-full")
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = delResp.Body.Close() }()
	if delResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("delete status = %d, want 500 on clear failure", delResp.StatusCode)
	}

	// The registry record must still be present (removal did not run).
	if _, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID); err != nil {
		t.Fatalf("registry record must survive a clear failure (independently retryable), got err=%v", err)
	}

	// The desired activation must still be Desired=true (never cleared) so it is
	// not orphaned — a retry would re-clear it.
	act, ok, err := store.EntryActivationStore.Get(context.Background(), engine.EntryActivationKey{
		Namespace:       namespace.Namespace("namespaceA"),
		WorkflowID:      out.WorkflowID,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
	})
	if err != nil || !ok {
		t.Fatalf("Get activation: ok=%v err=%v", ok, err)
	}
	if !act.Desired {
		t.Fatal("activation must remain Desired after a failed clear (retryable, not orphaned)")
	}
}
