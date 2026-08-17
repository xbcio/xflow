package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/types"
)

// fakeControlFacade is a minimal control.EngineFacade for unit-testing the
// workflow-control module without a real backend. Only the control-API
// methods (Submit/Invoke/Inspect/DeliverSignal/RevokeSignal/Cancel) carry
// behavior; the lease/routing surface from execution.Engine is stubbed.
type fakeControlFacade struct {
	submitID   types.ExecutionID
	submitErr  error
	invokeID   types.ExecutionID
	invokeErr  error
	inspect    engine.ExecutionDetail
	inspectErr error
	signalErr  error
	cancelErr  error
	revokeErr  error

	signalName string
	signalData map[string]any
	canceledID types.ExecutionID
	revokedSig string
	invokedEnt string

	seedErr error
}

func (f *fakeControlFacade) Submit(ctx context.Context, _ *graph.Graph, _ map[string]any, _ ...*types.Runtime) (types.ExecutionID, error) {
	// R3.1: mirror the real engine — when the authz wrapper pre-allocated an
	// execution id into the submission context, echo it back so the response
	// execution_id matches the admission audit row. Falls back to the fixed
	// submitID for tests that don't go through the authz path.
	if id, ok := engine.ExecutionIDFromContext(ctx); ok {
		return id, f.submitErr
	}
	if f.submitID == "" {
		return "exec-submit", f.submitErr
	}
	return f.submitID, f.submitErr
}

func (f *fakeControlFacade) Invoke(ctx context.Context, _ *graph.Graph, entry string, _ map[string]any, _ ...*types.Runtime) (types.ExecutionID, error) {
	f.invokedEnt = entry
	if f.invokeErr != nil {
		return "", f.invokeErr
	}
	if id, ok := engine.ExecutionIDFromContext(ctx); ok {
		return id, nil
	}
	if f.invokeID == "" {
		return "exec-invoke", nil
	}
	return f.invokeID, nil
}

func (f *fakeControlFacade) Inspect(context.Context, types.ExecutionID, ...string) (engine.ExecutionDetail, error) {
	return f.inspect, f.inspectErr
}

func (f *fakeControlFacade) DeliverSignal(_ context.Context, _ types.ExecutionID, name string, data map[string]any) error {
	f.signalName = name
	f.signalData = data
	return f.signalErr
}

func (f *fakeControlFacade) RevokeSignal(_ context.Context, _ types.ExecutionID, name string) error {
	f.revokedSig = name
	return f.revokeErr
}

func (f *fakeControlFacade) Cancel(_ context.Context, id types.ExecutionID) error {
	f.canceledID = id
	return f.cancelErr
}

func (f *fakeControlFacade) BuildTaskLease(context.Context, *engine.Task) (*engine.TaskLease, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeControlFacade) CommitTaskResult(context.Context, *engine.TaskLease, engine.TaskResult) error {
	return errors.New("not implemented")
}
func (f *fakeControlFacade) CommitTaskResultWithOutcome(context.Context, *engine.TaskLease, engine.TaskResult) (engine.CommitOutcome, error) {
	return engine.CommitOutcomeAccepted, errors.New("not implemented")
}
func (f *fakeControlFacade) TaskRouting(context.Context, *engine.Task) (engine.TaskRouting, error) {
	return engine.TaskRouting{}, nil
}
func (f *fakeControlFacade) ReclaimLease(context.Context, engine.ExpiredLease) (bool, error) {
	return false, nil
}

func (f *fakeControlFacade) SeedExecutionFromEntry(context.Context, engine.SeedExecutionFromEntryRequest) (engine.SeedExecutionFromEntryResponse, error) {
	return engine.SeedExecutionFromEntryResponse{}, f.seedErr
}

// compile-time assertion that fakeControlFacade satisfies the widened facade.
var _ control.EngineFacade = (*fakeControlFacade)(nil)

func newControlMux(f *fakeControlFacade) http.Handler {
	m := &workflowControlModule{eng: f}
	mux := http.NewServeMux()
	m.RegisterHTTP(mux)
	return mux
}

func doJSON(t *testing.T, mux http.Handler, method, path string, body any) *http.Response {
	t.Helper()
	return doJSONWithRequestID(t, mux, method, path, body, "")
}

// doJSONWithRequestID is doJSON with an X-Request-Id header. The echoed value
// is the migration tell: writeEnvelope echoes it only when the handler passed
// *http.Request; the writeError shim (nil request) drops it. An empty echo on a
// failure response means the site is still on the shim.
func doJSONWithRequestID(t *testing.T, mux http.Handler, method, path string, body any, requestID string) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	if requestID != "" {
		req.Header.Set("X-Request-Id", requestID)
	}
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	return rec.Result()
}

func validWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "test",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "xflow.start"},
			{Name: "work", Type: "test.work"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "work", Input: "main"}}}},
		},
	}
}

func TestWorkflowControlExecute(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{Workflow: validWorkflow()})
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

func TestWorkflowControlExecuteRejectsNilWorkflow(t *testing.T) {
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

func TestWorkflowControlInspect(t *testing.T) {
	f := &fakeControlFacade{inspect: engine.ExecutionDetail{
		ExecutionID: "exec-1",
		Status:      types.ExecutionStatusRunning,
	}}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodGet, "/v1/executions/exec-1", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var detail engine.ExecutionDetail
	env := decodeEnvelope(t, resp, &detail)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success/code=200", env)
	}
	if detail.ExecutionID != "exec-1" || detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("detail = %+v, want exec-1 running", detail)
	}
}

func TestWorkflowControlInspectNotFound(t *testing.T) {
	f := &fakeControlFacade{inspectErr: engine.ErrExecutionNotFound}
	mux := newControlMux(f)

	resp := doJSONWithRequestID(t, mux, http.MethodGet, "/v1/executions/missing", nil, "req-inspect-1")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "execution_not_found" {
		t.Fatalf("code = %q, want execution_not_found", env.Code)
	}
	// The X-Request-Id echo is the migration tell: writeEnvelope echoes it only
	// when the handler passed *http.Request; the writeError shim (nil request)
	// drops it. An empty echo here means the failure site is still on the shim.
	if got := resp.Header.Get("X-Request-Id"); got != "req-inspect-1" {
		t.Fatalf("X-Request-Id = %q, want %q (failure site must pass *http.Request)", got, "req-inspect-1")
	}
}

func TestWorkflowControlInspectGenericNotFoundTextReturns500(t *testing.T) {
	f := &fakeControlFacade{inspectErr: errors.New("upstream cache said not found while reading projection")}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodGet, "/v1/executions/exec-1", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 for unclassified error text", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "internal_error" {
		t.Fatalf("code = %q, want internal_error", env.Code)
	}
}

func TestWorkflowControlSignalAndCancel(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signals", signalRequest{
		Name: "approve",
		Data: map[string]any{"ok": true},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if !env.Success || env.Code != "200" {
		t.Fatalf("signal envelope = %+v, want success/code=200", env)
	}
	if f.signalName != "approve" || f.signalData["ok"] != true {
		t.Fatalf("signal = %q %v, want approve ok=true", f.signalName, f.signalData)
	}

	resp = doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/cancel", map[string]any{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}
	env = decodeEnvelope(t, resp, nil)
	if !env.Success || env.Code != "200" {
		t.Fatalf("cancel envelope = %+v, want success/code=200", env)
	}
	if f.canceledID != "exec-1" {
		t.Fatalf("canceled id = %q, want exec-1", f.canceledID)
	}
}

func TestWorkflowControlExecuteWithEntryInvokes(t *testing.T) {
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

func TestWorkflowControlExecuteMissingEntryReturns400(t *testing.T) {
	f := &fakeControlFacade{invokeErr: engine.ErrEntryNotFound}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{
		Workflow: validWorkflow(),
		Entry:    "missing",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unknown entry", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_entry_not_found" {
		t.Fatalf("code = %q, want workflow_entry_not_found", env.Code)
	}
}

func TestWorkflowControlExecuteGenericNotFoundTextReturns500(t *testing.T) {
	f := &fakeControlFacade{invokeErr: errors.New("registry not found while compiling runtime metadata")}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/execute", executeWorkflowRequest{
		Workflow: validWorkflow(),
		Entry:    "start",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 for unclassified invoke error text", resp.StatusCode)
	}
}

func TestWorkflowControlExecuteCompileErrorReturns400(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	// A workflow with a cycle fails to compile.
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
		t.Fatalf("status = %d, want 400 for compile error", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "workflow_compile_failed" {
		t.Fatalf("code = %q, want workflow_compile_failed", env.Code)
	}
}

// TestWorkflowControlRevokeSignal exercises the spec §7 target shape
// DELETE /v1/executions/{id}/signals/{name}: the signal name moves from the
// request body into the path segment, so the handler reads it via
// r.PathValue("name") and the body is unused. This test would fail (404 via
// the /v1/executions/{id}/ catch) if the DELETE route were removed.
func TestWorkflowControlRevokeSignal(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodDelete, "/v1/executions/exec-1/signals/approve", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success/code=200", env)
	}
	if f.revokedSig != "approve" {
		t.Fatalf("revoked signal = %q, want approve", f.revokedSig)
	}
}

// TestWorkflowControlRevokeSignalConsumedReturns409 asserts the consumed-or-
// not-found case maps to 409 with a stable snake_case code per spec §3.2.
func TestWorkflowControlRevokeSignalConsumedReturns409(t *testing.T) {
	f := &fakeControlFacade{revokeErr: engine.ErrSignalConsumed}
	mux := newControlMux(f)

	resp := doJSONWithRequestID(t, mux, http.MethodDelete, "/v1/executions/exec-1/signals/approve", nil, "req-revoke-409")
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	env := decodeEnvelope(t, resp, nil)
	if env.Code != "signal_consumed" {
		t.Fatalf("code = %q, want signal_consumed", env.Code)
	}
	if got := resp.Header.Get("X-Request-Id"); got != "req-revoke-409" {
		t.Fatalf("X-Request-Id = %q, want %q (failure site must pass *http.Request)", got, "req-revoke-409")
	}
}

// TestWorkflowControlRevokeSignalOldPostRouteRemoved proves the §9.1 migration
// removed POST /v1/executions/{id}/revoke-signal: a POST to that path now falls
// to the /v1/executions/{id}/ 404 catch (no existence leak, no 405). This guards
// against a regression that re-registers the old verb-stuck path.
func TestWorkflowControlRevokeSignalOldPostRouteRemoved(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/revoke-signal", signalRequest{Name: "approve"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("old POST revoke-signal status = %d, want 404 (route removed)", resp.StatusCode)
	}
	if f.revokedSig != "" {
		t.Fatalf("old POST revoke-signal reached the engine (revokedSig=%q); the removed route must not invoke RevokeSignal", f.revokedSig)
	}
}

// TestWorkflowControlRevokeSignalShapeMismatches404 verifies the §4.2 status
// table for the new route's wrong-method / wrong-shape neighbours. With both
// POST /v1/executions/{id}/signals and DELETE /v1/executions/{id}/signals/{name}
// registered, the /v1/executions/{id}/ subtree catch must answer method/shape
// mismatches with 404 (not 405) so the authorization boundary does not leak
// that the route exists. spec §4.2 has no 405 row.
func TestWorkflowControlRevokeSignalShapeMismatches404(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	cases := []struct {
		name   string
		method string
		path   string
	}{
		// DELETE without the {name} segment: the POST .../signals exact pattern
		// matches the path but the method mismatches; the subtree catch wins → 404.
		{"delete_signals_no_name", http.MethodDelete, "/v1/executions/exec-1/signals"},
		// POST to the DELETE route's path: method mismatch on the DELETE exact
		// pattern; the subtree catch wins → 404.
		{"post_signals_named", http.MethodPost, "/v1/executions/exec-1/signals/approve"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			resp := doJSON(t, mux, c.method, c.path, nil)
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusNotFound {
				t.Fatalf("%s %s: status = %d, want 404 (no 405 leak; §4.2 has no 405 row)", c.method, c.path, resp.StatusCode)
			}
		})
	}
}

func TestWorkflowControlWaitTerminal(t *testing.T) {
	f := &fakeControlFacade{inspect: engine.ExecutionDetail{
		ExecutionID: "exec-1",
		Status:      types.ExecutionStatusSuccess,
	}}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodGet, "/v1/executions/exec-1/wait?timeout=1s", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var detail engine.ExecutionDetail
	env := decodeEnvelope(t, resp, &detail)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success/code=200", env)
	}
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", detail.Status)
	}
}

func TestWorkflowControlWaitTimeoutReturns202(t *testing.T) {
	f := &fakeControlFacade{inspect: engine.ExecutionDetail{
		ExecutionID: "exec-1",
		Status:      types.ExecutionStatusRunning,
	}}
	mux := newControlMux(f)

	start := time.Now()
	resp := doJSON(t, mux, http.MethodGet, "/v1/executions/exec-1/wait?timeout=300ms", nil)
	defer func() { _ = resp.Body.Close() }()
	elapsed := time.Since(start)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", resp.StatusCode)
	}
	var out waitTimeoutResponse
	env := decodeEnvelope(t, resp, &out)
	if !env.Success || env.Code != "200" {
		t.Fatalf("envelope = %+v, want success/code=200", env)
	}
	if !out.TimedOut {
		t.Fatal("timed_out = false, want true")
	}
	if out.Status != types.ExecutionStatusRunning {
		t.Fatalf("status = %s, want running", out.Status)
	}
	if elapsed < 250*time.Millisecond {
		t.Fatalf("wait returned after %v, want >= ~300ms", elapsed)
	}
}
