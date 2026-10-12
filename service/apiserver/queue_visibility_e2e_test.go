package apiserver

import (
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
