package control

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

func TestHTTPRunnerRegisterPollAndResult(t *testing.T) {
	fake := &fakeControlEngine{}
	pool := NewRunnerPool()
	server := httptest.NewServer(NewServer(fake, pool).Handler())
	defer server.Close()

	register := protocol.RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Concurrency:  2,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}
	var registerResp protocol.RegisterRunnerResponse
	postJSON(t, server.URL+protocol.RegisterRunnerPath, register, http.StatusOK, &registerResp)
	if registerResp.RunnerID != "runner-1" {
		t.Fatalf("runner id = %q, want runner-1", registerResp.RunnerID)
	}

	lease := engine.TaskLease{
		LeaseID:     engine.LeaseID("lease-1"),
		LeaseToken:  engine.LeaseToken("token-1"),
		Task:        engine.Task{ExecutionID: types.ExecutionID("exec-1"), NodeName: "start"},
		NodeType:    "xflow.function",
		NodeVersion: 1,
	}
	if !pool.Assign(lease) {
		t.Fatal("assign lease")
	}

	var pollResp protocol.PollTaskResponse
	postJSON(t, server.URL+protocol.PollTaskPath, protocol.PollTaskRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Capabilities: []protocol.Capability{{NodeType: "xflow.function"}},
	}, http.StatusOK, &pollResp)
	if pollResp.Lease == nil || pollResp.Lease.LeaseID != lease.LeaseID {
		t.Fatalf("polled lease = %+v, want %+v", pollResp.Lease, lease)
	}

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    pollResp.Lease,
		Result:   engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusOK, &resultResp)
	if !resultResp.Accepted {
		t.Fatalf("result accepted = false, response %+v", resultResp)
	}
	if fake.committedLease == nil || fake.committedLease.LeaseID != lease.LeaseID {
		t.Fatalf("committed lease = %+v, want %+v", fake.committedLease, lease)
	}
}

func TestHTTPReportResultRejectsStaleLeaseToken(t *testing.T) {
	fake := &fakeControlEngine{commitErr: engine.ErrInvalidLeaseToken}
	server := httptest.NewServer(NewServer(fake, NewRunnerPool()).Handler())
	defer server.Close()

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    &engine.TaskLease{LeaseID: engine.LeaseID("lease-1")},
		Result:   engine.TaskResult{Output: &types.Output{Data: map[string]any{"ok": true}}},
	}, http.StatusConflict, &resultResp)
	if resultResp.Accepted {
		t.Fatal("stale lease result should not be accepted")
	}
}

func TestHTTPReportResultPreservesRunnerError(t *testing.T) {
	fake := &fakeControlEngine{}
	server := httptest.NewServer(NewServer(fake, NewRunnerPool()).Handler())
	defer server.Close()

	var resultResp protocol.ReportResultResponse
	postJSON(t, server.URL+protocol.ReportResultPath, protocol.ReportResultRequest{
		RunnerID: "runner-1",
		Lease:    &engine.TaskLease{LeaseID: engine.LeaseID("lease-1")},
		Result:   engine.TaskResult{Error: errors.New("handler failed")},
	}, http.StatusOK, &resultResp)
	if !resultResp.Accepted {
		t.Fatalf("result accepted = false, response %+v", resultResp)
	}
	if fake.committedResult.Error == nil || fake.committedResult.Error.Error() != "handler failed" {
		t.Fatalf("committed error = %v, want handler failed", fake.committedResult.Error)
	}
}

func TestHTTPInspectSignalAndCancel(t *testing.T) {
	fake := &fakeControlEngine{
		inspectDetail: engine.ExecutionDetail{
			ExecutionID: types.ExecutionID("exec-1"),
			Status:      types.ExecutionStatusRunning,
		},
	}
	server := httptest.NewServer(NewServer(fake, NewRunnerPool()).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/executions/exec-1")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("inspect status = %d, want 200", resp.StatusCode)
	}
	var detail engine.ExecutionDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	if detail.ExecutionID != "exec-1" || detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("detail = %+v, want exec-1 running", detail)
	}

	postJSON(t, server.URL+"/v1/executions/exec-1/signal", signalRequest{
		Name: "approve",
		Data: map[string]any{"approved": true},
	}, http.StatusOK, nil)
	if fake.signalName != "approve" || fake.signalData["approved"] != true {
		t.Fatalf("signal = %q %v, want approve approved=true", fake.signalName, fake.signalData)
	}

	postJSON(t, server.URL+"/v1/executions/exec-1/cancel", map[string]any{}, http.StatusOK, nil)
	if fake.canceledID != "exec-1" {
		t.Fatalf("canceled id = %q, want exec-1", fake.canceledID)
	}
}

func TestHTTPInspectUnknownExecutionReturnsNotFound(t *testing.T) {
	fake := &fakeControlEngine{inspectErr: errExecutionNotFound}
	server := httptest.NewServer(NewServer(fake, NewRunnerPool()).Handler())
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/executions/missing")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

type fakeControlEngine struct {
	inspectDetail   engine.ExecutionDetail
	inspectErr      error
	commitErr       error
	committedLease  *engine.TaskLease
	committedResult engine.TaskResult
	signalName      string
	signalData      map[string]any
	canceledID      types.ExecutionID
}

func (f *fakeControlEngine) Submit(context.Context, *graph.Graph, map[string]any, ...*types.Runtime) (types.ExecutionID, error) {
	return types.ExecutionID("exec-1"), nil
}

func (f *fakeControlEngine) Inspect(context.Context, types.ExecutionID, ...string) (engine.ExecutionDetail, error) {
	if f.inspectErr != nil {
		return engine.ExecutionDetail{}, f.inspectErr
	}
	return f.inspectDetail, nil
}

func (f *fakeControlEngine) DeliverSignal(_ context.Context, _ types.ExecutionID, name string, data map[string]any) error {
	f.signalName = name
	f.signalData = data
	return nil
}

func (f *fakeControlEngine) Cancel(_ context.Context, id types.ExecutionID) error {
	f.canceledID = id
	return nil
}

func (f *fakeControlEngine) CommitTaskResult(_ context.Context, lease *engine.TaskLease, result engine.TaskResult) error {
	if f.commitErr != nil {
		return f.commitErr
	}
	f.committedLease = lease
	f.committedResult = result
	return nil
}

func postJSON(t *testing.T, url string, body any, wantStatus int, out any) {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(url, "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		t.Fatalf("status = %d, want %d", resp.StatusCode, wantStatus)
	}
	if out != nil {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
			t.Fatal(err)
		}
	}
}

var errExecutionNotFound = errors.New("execution not found")
