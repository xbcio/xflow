package apiserver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// postWorkflows posts a body to POST /v1/workflows (the REGISTER route after
// the §9.1 semantic inversion) with the given bearer token.
func postWorkflows(t *testing.T, base, token string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		var buf strings.Builder
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
		r = strings.NewReader(buf.String())
	}
	req, _ := http.NewRequest(http.MethodPost, base+"/v1/workflows", r)
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST /v1/workflows: %v", err)
	}
	return resp
}

func putWorkflow(t *testing.T, base, token, id string, body any) *http.Response {
	t.Helper()
	return putWorkflowWithRequestID(t, base, token, id, body, "")
}

func putWorkflowWithRequestID(t *testing.T, base, token, id string, body any, requestID string) *http.Response {
	t.Helper()
	var buf strings.Builder
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, _ := http.NewRequest(http.MethodPut, base+"/v1/workflows/"+id, strings.NewReader(buf.String()))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /v1/workflows/%s: %v", id, err)
	}
	return resp
}

func assertRequestIDEcho(t *testing.T, resp *http.Response, want string) {
	t.Helper()
	if got := resp.Header.Get("X-Request-Id"); got != want {
		t.Fatalf("X-Request-Id = %q, want %q", got, want)
	}
}

func newWorkflowReplaceIdentityTestServer(t *testing.T) (*httptest.Server, *control.ControlPlane) {
	t.Helper()
	return newWorkflowReplaceDependencyTestServer(t, nil, nil, []TokenPrincipalMapping{
		{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		{Token: "tok-other", Subject: "op-other", Namespace: "namespaceB", Scopes: []string{"workflow", "execution"}},
	})
}

func newWorkflowReplaceDependencyTestServer(t *testing.T, registry backend.WorkflowRegistry, activations engine.EntryActivationStore, principals []TokenPrincipalMapping) (*httptest.Server, *control.ControlPlane) {
	t.Helper()
	provider := local.New()
	if registry == nil {
		registry = provider.WorkflowRegistry()
	}
	cp, err := control.NewControlPlane(control.Config{
		Backend:              provider,
		WorkflowRegistry:     registry,
		EntryActivationStore: activations,
	})
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}
	if principals == nil {
		principals = []TokenPrincipalMapping{
			{Token: "tok-full", Subject: "op-full", Namespace: "namespaceA", Scopes: []string{"workflow", "execution"}},
		}
	}
	cfg := Config{
		Concurrency:   8,
		PrincipalAuth: NewBearerPrincipalAuthMulti(principals),
		Authorizer:    NamespaceAwareAuthorizer{},
		AuditSink:     NewInMemoryAuditSink(),
	}
	srv, err := New(cfg, WithControlPlane(cp))
	if err != nil {
		t.Fatalf("apiserver.New: %v", err)
	}
	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	return httpSrv, cp
}

type failingDesiredUpsertStore struct {
	engine.EntryActivationStore
	mu                 sync.Mutex
	failDesiredUpserts int
	err                error
}

func (s *failingDesiredUpsertStore) failNextDesiredUpsert(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failDesiredUpserts++
	s.err = err
}

func (s *failingDesiredUpsertStore) Upsert(ctx context.Context, act engine.EntryActivation) error {
	if act.Desired {
		s.mu.Lock()
		if s.failDesiredUpserts > 0 {
			s.failDesiredUpserts--
			err := s.err
			s.mu.Unlock()
			return err
		}
		s.mu.Unlock()
	}
	return s.EntryActivationStore.Upsert(ctx, act)
}

func (s *failingDesiredUpsertStore) AdvanceWorkflowRevision(ctx context.Context, ns namespace.Namespace, workflowID types.WorkflowID, revision uint64) error {
	revisions, ok := s.EntryActivationStore.(engine.EntryActivationRevisionStore)
	if !ok {
		return errors.New("wrapped activation store does not support revision fencing")
	}
	return revisions.AdvanceWorkflowRevision(ctx, ns, workflowID, revision)
}

// decodeEnvelope decodes a response body into the envelope and, when data is
// non-nil, the data field into the typed target. Data is read as
// json.RawMessage first because envelope.Data is `any` — a direct unmarshal
// yields a map[string]any, which cannot be re-decoded into a typed struct.
func decodeEnvelope(t *testing.T, resp *http.Response, data any) envelope {
	t.Helper()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var partial struct {
		Success bool            `json:"success"`
		Code    string          `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`
		TraceID string          `json:"trace_id"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		t.Fatalf("decode envelope: %v (body=%s)", err, raw)
	}
	if data != nil && len(partial.Data) > 0 && string(partial.Data) != "null" {
		if err := json.Unmarshal(partial.Data, data); err != nil {
			t.Fatalf("decode data: %v (data=%s)", err, partial.Data)
		}
	}
	return envelope{
		Success: partial.Success,
		Code:    partial.Code,
		Message: partial.Message,
		Data:    nil,
		TraceID: partial.TraceID,
	}
}

// TestPostWorkflowsRegistersRatherThanExecutes is the §9.1 semantic-inversion
// pin. POST /v1/workflows used to compile-and-execute; after this task it
// registers a definition and returns workflow_id. It must NOT start an
// execution — a caller on the old semantics gets silently different behavior
// (no error, no 404, just a different outcome), so this test is the guard.
func TestPostWorkflowsRegistersRatherThanExecutes(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want 201 (registered, not executed)", resp.StatusCode)
	}

	var out registerWorkflowResponse
	env := decodeEnvelope(t, resp, &out)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success=true code=200", env)
	}
	if out.WorkflowID == "" {
		t.Fatal("data must carry workflow_id; POST /v1/workflows now registers")
	}
	if resp.Header.Get("Location") == "" {
		t.Fatal("201 must carry a Location header (spec §4.2)")
	}

	// The registry must hold the record (register persisted it).
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("registry must hold the registered workflow: %v", err)
	}
	if rec.Graph == nil {
		t.Fatal("registered record must carry a compiled graph")
	}
}

// TestPostWorkflowsExecuteRunsAnInlineDefinition pins the new inline direct-run
// route: POST /v1/workflows/execute compiles an inline definition and submits
// it, returning execution_id. This merges the old submit + invoke.
func TestPostWorkflowsExecuteRunsAnInlineDefinition(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{
		Workflow: validWorkflow(),
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out executeWorkflowResponse
	env := decodeEnvelope(t, resp, &out)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if out.ExecutionID != "exec-1" {
		t.Fatalf("execution_id = %q, want exec-1", out.ExecutionID)
	}
}

// TestPostWorkflowsExecuteWithEntryInvokes pins the merged invoke shape: when
// Entry is set, the engine is driven via the explicit-entry path.
func TestPostWorkflowsExecuteWithEntryInvokes(t *testing.T) {
	f := &fakeControlFacade{invokeID: "exec-invoke"}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{
		Workflow: validWorkflow(),
		Entry:    "start",
		Input:    map[string]any{"k": "v"},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out executeWorkflowResponse
	decodeEnvelope(t, resp, &out)
	if out.ExecutionID != "exec-invoke" {
		t.Fatalf("execution_id = %q, want exec-invoke", out.ExecutionID)
	}
	if f.invokedEnt != "start" {
		t.Fatalf("invoked entry = %q, want start", f.invokedEnt)
	}
}

// TestPostWorkflowsExecuteRejectsNilWorkflow pins the stable failure code.
func TestPostWorkflowsExecuteRejectsNilWorkflow(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_invalid" {
		t.Fatalf("code = %q, want workflow_invalid", env.Code)
	}
}

// TestPostWorkflowsExecuteCompileErrorReturnsStableCode pins that a compile
// failure maps to workflow_compile_failed (400), and the message carries only
// node names / referenced node names / parameter names — never node output.
func TestPostWorkflowsExecuteCompileErrorReturnsStableCode(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	wf := &types.WorkflowDef{
		Name: "cycle",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "test.a"},
			{Name: "B", Type: "test.b"},
		},
		Connections: types.Connections{
			"A": {"main": types.PortConnections{Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": types.PortConnections{Targets: []types.Connection{{Node: "A", Input: "main"}}}},
		},
	}
	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{Workflow: wf, Entry: "A"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_compile_failed" {
		t.Fatalf("code = %q, want workflow_compile_failed", env.Code)
	}
}

// TestDeleteWorkflowByIDDeregisters pins the migrated DELETE path: the id is
// read via the mux {id} pattern (no TrimPrefix), and the response is enveloped.
func TestDeleteWorkflowByIDDeregisters(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	// Register first.
	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)
	if out.WorkflowID == "" {
		t.Fatal("workflow_id must be non-empty")
	}

	// DELETE /v1/workflows/{id} (the migrated path).
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/v1/workflows/"+string(out.WorkflowID), nil)
	req.Header.Set("Authorization", "Bearer tok-full")
	delResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("DELETE: %v", err)
	}
	defer func() { _ = delResp.Body.Close() }()
	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("delete status = %d, want 200", delResp.StatusCode)
	}
	env := decodeEnvelope(t, delResp, nil)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if _, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID); err == nil {
		t.Fatal("record still present after deregister; want removed")
	}
}

// TestGetWorkflowByIDReturnsDefinition pins the new GET /v1/workflows/{id}
// route (§7 + Addition 1): it returns the stored record's definition, enveloped.
func TestGetWorkflowByIDReturnsDefinition(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	// GET /v1/workflows/{id}.
	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/workflows/"+string(out.WorkflowID), nil)
	getReq.Header.Set("Authorization", "Bearer tok-full")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", getResp.StatusCode)
	}
	var def types.WorkflowDef
	env := decodeEnvelope(t, getResp, &def)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if def.Name != "test" {
		t.Fatalf("definition Name = %q, want test", def.Name)
	}
}

// TestGetWorkflowByIDNotFoundReturnsStableCode pins the 404 + workflow_not_found.
func TestGetWorkflowByIDNotFoundReturnsStableCode(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/workflows/nonexistent", nil)
	req.Header.Set("Authorization", "Bearer tok-full")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_not_found" {
		t.Fatalf("code = %q, want workflow_not_found", env.Code)
	}
}

// TestPutWorkflowByIDReplaces pins PUT /v1/workflows/{id}: the path id is the
// authoritative resource identity, while the body is the replacement
// definition. A changed definition under the same key is stored under the same
// workflow id rather than deleting whichever record the body key names.
func TestPutWorkflowByIDReplaces(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	// Register a workflow under (namespaceA, replaced, v1).
	first := validWorkflow()
	first.Name = "replaced"
	resp := postWorkflows(t, srv.URL, "tok-full", first)
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	// PUT a changed definition under the same {id}. ReplaceWorkflow deregisters
	// the conflicting key and re-registers, returning a new workflow_id.
	changed := validWorkflow()
	changed.ID = string(out.WorkflowID)
	changed.Namespace = "forged-namespace"
	changed.Name = "replaced"
	changed.Nodes = []types.NodeDef{
		{Name: "start", Type: "xflow.start"},
		{Name: "work", Type: "test.work"},
		{Name: "extra", Type: "test.work"},
	}
	changed.Connections = types.Connections{
		"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		"work":  {"main": types.PortConnections{Targets: []types.Connection{{Node: "extra", Input: "main"}}}},
	}
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(out.WorkflowID), changed, "req-put-200")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", putResp.StatusCode)
	}
	assertRequestIDEcho(t, putResp, "req-put-200")
	var repl registerWorkflowResponse
	env := decodeEnvelope(t, putResp, &repl)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if repl.WorkflowID == "" {
		t.Fatal("PUT must return a workflow_id")
	}
	if repl.WorkflowID != out.WorkflowID {
		t.Fatalf("workflow_id = %q, want path id %q", repl.WorkflowID, out.WorkflowID)
	}
	// The new record must exist and carry the changed graph.
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), repl.WorkflowID)
	if err != nil {
		t.Fatalf("registry must hold the replaced workflow: %v", err)
	}
	if rec.Graph == nil {
		t.Fatal("replaced record must carry a compiled graph")
	}
	if rec.Definition.ID != string(out.WorkflowID) {
		t.Fatalf("stored definition id = %q, want path id %q", rec.Definition.ID, out.WorkflowID)
	}
	if rec.Namespace != "namespaceA" || rec.Definition.Namespace != "namespaceA" {
		t.Fatalf("stored namespaces = record %q, definition %q; want authenticated namespaceA", rec.Namespace, rec.Definition.Namespace)
	}
	if got := len(rec.Definition.Nodes); got != 3 {
		t.Fatalf("stored node count = %d, want changed definition with 3 nodes", got)
	}
}

func TestPutWorkflowByIDMissingTargetDoesNotDeleteBodyKey(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	victim := validWorkflow()
	victim.Name = "victim"
	resp := postWorkflows(t, srv.URL, "tok-full", victim)
	defer func() { _ = resp.Body.Close() }()
	var victimOut registerWorkflowResponse
	decodeEnvelope(t, resp, &victimOut)

	changed := validWorkflow()
	changed.Name = "victim"
	changed.Description = "changed definition under the victim key"
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", "missing-workflow-id", changed, "req-put-404")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", putResp.StatusCode)
	}
	assertRequestIDEcho(t, putResp, "req-put-404")
	if env := decodeEnvelope(t, putResp, nil); env.Code != "workflow_not_found" {
		t.Fatalf("code = %q, want workflow_not_found", env.Code)
	}
	if _, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), victimOut.WorkflowID); err != nil {
		t.Fatalf("victim workflow was deleted while replacing a missing path id: %v", err)
	}
}

func TestPutWorkflowByIDBodyIDMustMatchPath(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	changed := validWorkflow()
	changed.ID = "different-workflow-id"
	changed.Description = "should be rejected before mutation"
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(out.WorkflowID), changed, "req-put-400")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", putResp.StatusCode)
	}
	assertRequestIDEcho(t, putResp, "req-put-400")
	if env := decodeEnvelope(t, putResp, nil); env.Code != "workflow_id_mismatch" {
		t.Fatalf("code = %q, want workflow_id_mismatch", env.Code)
	}
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), out.WorkflowID)
	if err != nil {
		t.Fatalf("target workflow missing after rejected id mismatch: %v", err)
	}
	if rec.Definition.Description == changed.Description {
		t.Fatal("target workflow was mutated despite body/path id mismatch")
	}
}

func TestPutWorkflowByIDAllowsRenameToFreeKeyPreservingID(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	first := validWorkflow()
	first.Name = "rename-source"
	first.Version = "v1"
	resp := postWorkflows(t, srv.URL, "tok-full", first)
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	replacement := validWorkflow()
	replacement.Name = "rename-destination"
	replacement.Version = "v2"
	putResp := putWorkflow(t, srv.URL, "tok-full", string(out.WorkflowID), replacement)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", putResp.StatusCode)
	}
	var repl registerWorkflowResponse
	decodeEnvelope(t, putResp, &repl)
	if repl.WorkflowID != out.WorkflowID {
		t.Fatalf("workflow_id = %q, want preserved path id %q", repl.WorkflowID, out.WorkflowID)
	}
	if _, err := cp.WorkflowRegistry().GetWorkflowByKey(context.Background(), workflowRegistryKey("namespaceA", "rename-source", "v1")); !errors.Is(err, backend.ErrWorkflowNotFound) {
		t.Fatalf("old key lookup err = %v, want ErrWorkflowNotFound", err)
	}
	rec, err := cp.WorkflowRegistry().GetWorkflowByKey(context.Background(), workflowRegistryKey("namespaceA", "rename-destination", "v2"))
	if err != nil {
		t.Fatalf("new key lookup: %v", err)
	}
	if rec.ID != out.WorkflowID {
		t.Fatalf("new key id = %q, want preserved path id %q", rec.ID, out.WorkflowID)
	}
}

func TestPutWorkflowByIDCannotReplaceIntoAnotherWorkflowKey(t *testing.T) {
	srv, cp := newRegisterTestServer(t)

	target := validWorkflow()
	target.Name = "target"
	targetResp := postWorkflows(t, srv.URL, "tok-full", target)
	defer func() { _ = targetResp.Body.Close() }()
	var targetOut registerWorkflowResponse
	decodeEnvelope(t, targetResp, &targetOut)

	occupied := validWorkflow()
	occupied.Name = "occupied"
	occupiedResp := postWorkflows(t, srv.URL, "tok-full", occupied)
	defer func() { _ = occupiedResp.Body.Close() }()
	var occupiedOut registerWorkflowResponse
	decodeEnvelope(t, occupiedResp, &occupiedOut)

	replacement := validWorkflow()
	replacement.Name = "occupied"
	replacement.Description = "attempted overwrite"
	putResp := putWorkflowWithRequestID(t, srv.URL, "tok-full", string(targetOut.WorkflowID), replacement, "req-put-409")
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", putResp.StatusCode)
	}
	assertRequestIDEcho(t, putResp, "req-put-409")
	if env := decodeEnvelope(t, putResp, nil); env.Code != "workflow_conflict" {
		t.Fatalf("code = %q, want workflow_conflict", env.Code)
	}
	if _, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), targetOut.WorkflowID); err != nil {
		t.Fatalf("target workflow missing after destination conflict: %v", err)
	}
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), occupiedOut.WorkflowID)
	if err != nil {
		t.Fatalf("occupied workflow missing after destination conflict: %v", err)
	}
	if rec.Definition.Description == replacement.Description {
		t.Fatal("occupied workflow was overwritten by a body-key replace")
	}
}

func TestPutWorkflowByIDCrossNamespaceTargetIsNotFound(t *testing.T) {
	srv, _ := newWorkflowReplaceIdentityTestServer(t)

	foreign := validWorkflow()
	foreign.Name = "foreign"
	foreignResp := postWorkflows(t, srv.URL, "tok-other", foreign)
	defer func() { _ = foreignResp.Body.Close() }()
	var foreignOut registerWorkflowResponse
	decodeEnvelope(t, foreignResp, &foreignOut)

	replacement := validWorkflow()
	replacement.Name = "foreign-replacement"
	putResp := putWorkflow(t, srv.URL, "tok-full", string(foreignOut.WorkflowID), replacement)
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", putResp.StatusCode)
	}
	if env := decodeEnvelope(t, putResp, nil); env.Code != "workflow_not_found" {
		t.Fatalf("code = %q, want workflow_not_found", env.Code)
	}

	getReq, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/workflows/"+string(foreignOut.WorkflowID), nil)
	getReq.Header.Set("Authorization", "Bearer tok-other")
	getResp, err := http.DefaultClient.Do(getReq)
	if err != nil {
		t.Fatalf("GET foreign as owner: %v", err)
	}
	defer func() { _ = getResp.Body.Close() }()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("foreign owner GET status = %d, want 200", getResp.StatusCode)
	}
}

func TestPutWorkflowByIDRetriesActivationProjectionAfterTransientFailure(t *testing.T) {
	activationStore := &failingDesiredUpsertStore{EntryActivationStore: control.NewMemoryEntryActivationStore()}
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, activationStore, nil)

	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-a"))
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	activationStore.failNextDesiredUpsert(errors.New("injected activation projection failure"))
	putResp := putWorkflow(t, srv.URL, "tok-full", string(out.WorkflowID), configuredTriggerWorkflow("topic-b"))
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 after projection retry", putResp.StatusCode)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), out.WorkflowID, "topic-b")
	assertWorkflowActivationDesired(t, cp.EntryActivationStore(), out.WorkflowID, "topic-b")
}

func TestPutWorkflowByIDKeepsCommittedRevisionWhenActivationProjectionFails(t *testing.T) {
	activationStore := &failingDesiredUpsertStore{EntryActivationStore: control.NewMemoryEntryActivationStore()}
	srv, cp := newWorkflowReplaceDependencyTestServer(t, nil, activationStore, nil)

	resp := postWorkflows(t, srv.URL, "tok-full", configuredTriggerWorkflow("topic-a"))
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	for range 3 {
		activationStore.failNextDesiredUpsert(errors.New("injected persistent activation projection failure"))
	}
	putResp := putWorkflow(t, srv.URL, "tok-full", string(out.WorkflowID), configuredTriggerWorkflow("topic-b"))
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", putResp.StatusCode)
	}
	assertWorkflowTopic(t, cp.WorkflowRegistry(), out.WorkflowID, "topic-b")
	assertWorkflowActivationNotDesired(t, cp.EntryActivationStore(), out.WorkflowID)
}

func assertWorkflowTopic(t *testing.T, registry backend.WorkflowRegistry, id types.WorkflowID, want string) {
	t.Helper()
	rec, err := registry.GetWorkflow(context.Background(), id)
	if err != nil {
		t.Fatalf("GetWorkflow(%q): %v", id, err)
	}
	if len(rec.Definition.Nodes) == 0 {
		t.Fatal("stored workflow has no nodes")
	}
	if got := rec.Definition.Nodes[0].Parameters["topic"]; got != want {
		t.Fatalf("stored topic = %v, want %s", got, want)
	}
}

func assertWorkflowActivationDesired(t *testing.T, store engine.EntryActivationStore, id types.WorkflowID, wantTopic string) {
	t.Helper()
	act, ok, err := store.Get(context.Background(), engine.EntryActivationKey{
		Namespace:       "namespaceA",
		WorkflowID:      id,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
	})
	if err != nil {
		t.Fatalf("Get activation: %v", err)
	}
	if !ok {
		t.Fatal("activation missing after failed replace rollback")
	}
	if !act.Desired {
		t.Fatal("activation is not desired after failed replace rollback")
	}
	if got := act.Params["topic"]; got != wantTopic {
		t.Fatalf("activation topic = %v, want %s", got, wantTopic)
	}
}

func assertWorkflowActivationNotDesired(t *testing.T, store engine.EntryActivationStore, id types.WorkflowID) {
	t.Helper()
	act, ok, err := store.Get(context.Background(), engine.EntryActivationKey{
		Namespace:       "namespaceA",
		WorkflowID:      id,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
	})
	if err != nil {
		t.Fatalf("Get activation: %v", err)
	}
	if ok && act.Desired {
		t.Fatal("orphan activation is still desired after the workflow record was removed")
	}
}

// TestPostWorkflowExecuteByIDRunsRegistered pins the new
// POST /v1/workflows/{id}/execute route (§7): run an already-registered
// workflow by id.
func TestPostWorkflowExecuteByIDRunsRegistered(t *testing.T) {
	srv, _ := newRegisterTestServer(t)

	// Register a workflow.
	resp := postWorkflows(t, srv.URL, "tok-full", validWorkflow())
	defer func() { _ = resp.Body.Close() }()
	var out registerWorkflowResponse
	decodeEnvelope(t, resp, &out)

	// POST /v1/workflows/{id}/execute.
	body, _ := json.Marshal(executeRegisteredRequest{})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/workflows/"+string(out.WorkflowID)+"/execute", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer tok-full")
	execResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	defer func() { _ = execResp.Body.Close() }()
	if execResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", execResp.StatusCode)
	}
	var execOut executeWorkflowResponse
	env := decodeEnvelope(t, execResp, &execOut)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if execOut.ExecutionID == "" {
		t.Fatal("execute must return an execution_id")
	}
}

// TestWorkflowsEnvelopesEchoRequestID pins that the migrated workflows family
// passes the real *http.Request to the enveloped writer (spec §5.2) — the
// property Task 1's writeError shim could not provide (it passes a nil request,
// so no X-Request-Id is echoed and trace_id stays empty). The X-Request-Id echo
// is the testable distinction in a unit test without a tracer configured: the
// shim never echoes it, writeData/writeFail always do.
func TestWorkflowsEnvelopesEchoRequestID(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	mux := newControlMux(f)

	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/execute", encodeBody(t, executeWorkflowRequest{Workflow: validWorkflow()}))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-Id", "abc-123")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("X-Request-Id"); got != "abc-123" {
		t.Fatalf("X-Request-Id echo = %q, want abc-123; the workflows handler is "+
			"still on the writeError shim (nil request) which never echoes it", got)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.TraceID == "" {
		// trace_id is server-authoritative and may be empty when no tracer is
		// configured (unit test). That is acceptable here; the X-Request-Id echo
		// above is the load-bearing assertion that the migrated writer is in use.
		t.Logf("trace_id empty (no tracer configured in bare unit test) — acceptable")
	}
}

func encodeBody(t *testing.T, body any) *strings.Reader {
	t.Helper()
	var buf strings.Builder
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	return strings.NewReader(buf.String())
}
