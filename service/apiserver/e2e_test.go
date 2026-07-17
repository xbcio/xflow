package apiserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

// TestAPIServerRunnerE2ECompletesSimpleWorkflow is the migrated control-plane
// E2E: the APIServer hosts both the runner protocol (runner-protocol module)
// and the workflow/control API (workflow-control module). A real runner polls
// the server, executes a handler, and commits the result; the test submits a
// workflow and long-polls the wait endpoint until the execution reaches a
// terminal state.
func TestAPIServerRunnerE2ECompletesSimpleWorkflow(t *testing.T) {
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

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.e2e", e2eHandler{})
	runner := runnersvc.New(protocol.NewClient(httpSrv.URL, httpSrv.Client()), registry, runnersvc.Config{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "test.e2e"}},
		PollWait:     5 * time.Millisecond,
	})
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()

	execID := submitWorkflowE2E(t, httpSrv.URL, &types.WorkflowDef{
		Name: "server-runner-e2e",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.e2e"},
		},
	}, map[string]any{"claim_id": "c-1"})

	detail := waitExecutionE2E(t, httpSrv.URL, execID, 5*time.Second)
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", detail.Status)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
}

func submitWorkflowE2E(t *testing.T, baseURL string, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := submitWorkflowRequest{Workflow: wf, Params: params}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+"/v1/workflows", "application/json", &buf)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit status = %d, want 200", resp.StatusCode)
	}
	var out submitWorkflowResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.ExecutionID == "" {
		t.Fatal("empty execution_id")
	}
	return out.ExecutionID
}

func waitExecutionE2E(t *testing.T, baseURL string, id types.ExecutionID, timeout time.Duration) engine.ExecutionDetail {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, baseURL+"/v1/executions/"+string(id)+"/wait?timeout="+timeout.String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	var detail engine.ExecutionDetail
	if err := json.NewDecoder(resp.Body).Decode(&detail); err != nil {
		t.Fatal(err)
	}
	return detail
}

type e2eHandler struct{}

func (e2eHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.e2e"}
}

func (e2eHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}
