package apiserver

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

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
		Success bool              `json:"success"`
		Code    string            `json:"code"`
		Message string            `json:"message"`
		Data    json.RawMessage   `json:"data"`
		TraceID string            `json:"trace_id"`
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

// TestPutWorkflowByIDReplaces pins the new PUT /v1/workflows/{id} route
// (§7 + Addition 1): it maps onto APIServer.ReplaceWorkflow — a full update that
// deregisters a conflicting definition under the same key and re-registers.
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
	body, _ := json.Marshal(changed)
	putReq, _ := http.NewRequest(http.MethodPut, srv.URL+"/v1/workflows/"+string(out.WorkflowID), strings.NewReader(string(body)))
	putReq.Header.Set("Content-Type", "application/json")
	putReq.Header.Set("Authorization", "Bearer tok-full")
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		t.Fatalf("PUT: %v", err)
	}
	defer func() { _ = putResp.Body.Close() }()
	if putResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", putResp.StatusCode)
	}
	var repl registerWorkflowResponse
	env := decodeEnvelope(t, putResp, &repl)
	if !env.Success {
		t.Fatalf("envelope not success: %+v", env)
	}
	if repl.WorkflowID == "" {
		t.Fatal("PUT must return a workflow_id")
	}
	// The new record must exist and carry the changed graph.
	rec, err := cp.WorkflowRegistry().GetWorkflow(context.Background(), repl.WorkflowID)
	if err != nil {
		t.Fatalf("registry must hold the replaced workflow: %v", err)
	}
	if rec.Graph == nil {
		t.Fatal("replaced record must carry a compiled graph")
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
