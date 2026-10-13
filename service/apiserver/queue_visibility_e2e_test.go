package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// queueModeWorkflow is a definition that admits one execution at a time and
// parks the rest — the bare-int shape is QUEUE mode (reject has to be asked for
// explicitly).
func queueModeWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "concurrency-queued",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
		},
		Settings: &types.WorkflowSettings{
			Concurrency: &types.ConcurrencySettings{
				Limit: 1,
				Mode:  types.ConcurrencyModeQueue,
			},
		},
	}
}

// inspectExecutionE2E reads one execution's audit detail over the HTTP surface,
// which is the form a waiting caller actually uses.
func inspectExecutionE2E(t *testing.T, baseURL string, id types.ExecutionID) engine.ExecutionDetail {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/executions/" + string(id))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("inspect status = %d (body=%s), want 200", resp.StatusCode, body)
	}
	bodyBytes, _ := io.ReadAll(resp.Body)
	var detail engine.ExecutionDetail
	if err := json.Unmarshal(extractData(t, bodyBytes), &detail); err != nil {
		t.Fatal(err)
	}
	return detail
}

// TestQueuedExecutionIsVisibleWithItsPosition is the visibility half of queue
// mode end to end: a submission that was parked is readable, says so, and says
// where it stands — over the same route an operator uses for any other
// execution. The position is the one piece of queue state a waiting caller can
// act on, and nothing else in the API exposes it.
//
// The holder is inspected too: it must report no position, because 1 would read
// as "next in line" for an execution that is already running.
func TestQueuedExecutionIsVisibleWithItsPosition(t *testing.T) {
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	wf := queueModeWorkflow()
	holder := submitWorkflowE2E(t, httpSrv.URL, wf, nil)
	waiting := submitWorkflowE2E(t, httpSrv.URL, wf, nil)

	detail := inspectExecutionE2E(t, httpSrv.URL, waiting)
	if detail.Status != types.ExecutionStatusQueued {
		t.Fatalf("parked execution status = %q, want %q", detail.Status, types.ExecutionStatusQueued)
	}
	if detail.QueuePosition != 1 {
		t.Fatalf("parked execution queue_position = %d, want 1", detail.QueuePosition)
	}

	holderDetail := inspectExecutionE2E(t, httpSrv.URL, holder)
	if holderDetail.Status != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want %q", holderDetail.Status, types.ExecutionStatusRunning)
	}
	if holderDetail.QueuePosition != 0 {
		t.Fatalf("holder queue_position = %d, want 0: it is not waiting for anything",
			holderDetail.QueuePosition)
	}
}

// submitWorkflowRawE2E posts an inline definition and returns the decoded
// execute response together with its `data` object as raw fields, so a test can
// tell an absent field from a zero one.
func submitWorkflowRawE2E(t *testing.T, baseURL string, wf *types.WorkflowDef) (executeWorkflowResponse, map[string]any) {
	t.Helper()
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(executeWorkflowRequest{Workflow: wf}); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+"/v1/workflows/execute", "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	bodyBytes, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("execute status = %d (body=%s), want 200", resp.StatusCode, bodyBytes)
	}
	dataBytes := extractData(t, bodyBytes)
	var out executeWorkflowResponse
	if err := json.Unmarshal(dataBytes, &out); err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(dataBytes, &raw); err != nil {
		t.Fatal(err)
	}
	return out, raw
}

// TestExecuteResponseSaysWhereTheSubmissionLanded answers, at the moment of the
// request, the question the response used to leave to a follow-up read: a 200
// now says whether the work started or is waiting for a slot, and a queued
// submission carries its place in line. "Accepted" alone is not an answer — a
// parked submission has not been accepted by anything that will run it, and a
// client that cannot tell the two apart has to poll to find out which one it
// got.
func TestExecuteResponseSaysWhereTheSubmissionLanded(t *testing.T) {
	srv, err := New(Config{Concurrency: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer func() { _ = srv.Shutdown(context.Background()) }()

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	wf := queueModeWorkflow()
	holder, holderRaw := submitWorkflowRawE2E(t, httpSrv.URL, wf)
	if holder.Status != types.ExecutionStatusRunning {
		t.Fatalf("holder status = %q, want %q", holder.Status, types.ExecutionStatusRunning)
	}
	// The field has to be absent, not zero: a client cannot tell 0 from "not
	// queued" in a decoded struct, and only one of those is a place in line it
	// could show.
	if _, present := holderRaw["queue_position"]; present {
		t.Fatalf("holder response carries queue_position = %v, want the field absent", holderRaw["queue_position"])
	}

	waiting, waitingRaw := submitWorkflowRawE2E(t, httpSrv.URL, wf)
	if waiting.Status != types.ExecutionStatusQueued {
		t.Fatalf("parked status = %q, want %q", waiting.Status, types.ExecutionStatusQueued)
	}
	if waiting.QueuePosition != 1 {
		t.Fatalf("parked queue_position = %d, want 1", waiting.QueuePosition)
	}
	if _, present := waitingRaw["queue_position"]; !present {
		t.Fatalf("parked response has no queue_position field; body fields = %v", waitingRaw)
	}
}
