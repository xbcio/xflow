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
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
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
			"start": {"main": []types.Connection{{Node: "work", Input: "main"}}},
		},
	}
}

func TestWorkflowControlSubmit(t *testing.T) {
	f := &fakeControlFacade{submitID: "exec-1"}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows", submitWorkflowRequest{Workflow: validWorkflow()})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out submitWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ExecutionID != "exec-1" {
		t.Fatalf("execution_id = %q, want exec-1", out.ExecutionID)
	}
}

func TestWorkflowControlSubmitRejectsNilWorkflow(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows", submitWorkflowRequest{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
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
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	if detail.ExecutionID != "exec-1" || detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("detail = %+v, want exec-1 running", detail)
	}
}

func TestWorkflowControlInspectNotFound(t *testing.T) {
	f := &fakeControlFacade{inspectErr: errors.New("inspect execution \"missing\": not found")}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodGet, "/v1/executions/missing", nil)
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

func TestWorkflowControlSignalAndCancel(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/signal", signalRequest{
		Name: "approve",
		Data: map[string]any{"ok": true},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("signal status = %d, want 200", resp.StatusCode)
	}
	if f.signalName != "approve" || f.signalData["ok"] != true {
		t.Fatalf("signal = %q %v, want approve ok=true", f.signalName, f.signalData)
	}

	resp = doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/cancel", map[string]any{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("cancel status = %d, want 200", resp.StatusCode)
	}
	if f.canceledID != "exec-1" {
		t.Fatalf("canceled id = %q, want exec-1", f.canceledID)
	}
}

func TestWorkflowControlInvoke(t *testing.T) {
	f := &fakeControlFacade{invokeID: "exec-invoke"}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/invoke", invokeRequest{
		Workflow: validWorkflow(),
		Entry:    "start",
		Input:    map[string]any{"k": "v"},
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var out invokeResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ExecutionID != "exec-invoke" {
		t.Fatalf("execution_id = %q, want exec-invoke", out.ExecutionID)
	}
	if f.invokedEnt != "start" {
		t.Fatalf("invoked entry = %q, want start", f.invokedEnt)
	}
}

func TestWorkflowControlInvokeMissingEntryReturns400(t *testing.T) {
	f := &fakeControlFacade{invokeErr: errors.New("entry node \"missing\" not found")}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/invoke", invokeRequest{
		Workflow: validWorkflow(),
		Entry:    "missing",
	})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for unknown entry", resp.StatusCode)
	}
}

func TestWorkflowControlInvokeRejectsMissingEntryField(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/invoke", invokeRequest{Workflow: validWorkflow()})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for empty entry", resp.StatusCode)
	}
}

func TestWorkflowControlInvokeCompileErrorReturns400(t *testing.T) {
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
			"A": {"main": []types.Connection{{Node: "B", Input: "main"}}},
			"B": {"main": []types.Connection{{Node: "A", Input: "main"}}},
		},
	}
	resp := doJSON(t, mux, http.MethodPost, "/v1/workflows/invoke", invokeRequest{Workflow: wf, Entry: "A"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400 for compile error", resp.StatusCode)
	}
}

func TestWorkflowControlRevokeSignal(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/revoke-signal", signalRequest{Name: "approve"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if f.revokedSig != "approve" {
		t.Fatalf("revoked signal = %q, want approve", f.revokedSig)
	}
}

func TestWorkflowControlRevokeSignalConsumedReturns409(t *testing.T) {
	f := &fakeControlFacade{revokeErr: engine.ErrSignalConsumed}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/revoke-signal", signalRequest{Name: "approve"})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
}

func TestWorkflowControlRevokeSignalMissingNameReturns400(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	resp := doJSON(t, mux, http.MethodPost, "/v1/executions/exec-1/revoke-signal", signalRequest{})
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestWorkflowControlRevokeSignalAcceptsQueryName(t *testing.T) {
	f := &fakeControlFacade{}
	mux := newControlMux(f)

	// Empty body, name supplied via query parameter.
	req := httptest.NewRequest(http.MethodPost, "/v1/executions/exec-1/revoke-signal?name=approve", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	resp := rec.Result()
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if f.revokedSig != "approve" {
		t.Fatalf("revoked signal = %q, want approve", f.revokedSig)
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
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
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
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
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
