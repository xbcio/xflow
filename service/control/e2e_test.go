package control

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
)

func TestServerRunnerE2ECompletesSimpleWorkflow(t *testing.T) {
	backend := memory.New(memory.WithConcurrency(1))
	eng := engine.New(backend.State(), backend.Queue())
	runners := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(eng, runners)
	stopBackend := backend.BindHandler(dispatcher.HandleTask)
	defer stopBackend()

	server := httptest.NewServer(NewServer(eng, runners).Handler())
	defer server.Close()

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.e2e", e2eHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(protocol.NewClient(server.URL, server.Client()), registry, runnersvc.Config{
		RunnerID:     "runner-1",
		Concurrency:  1,
		Capabilities: []protocol.Capability{{NodeType: "test.e2e"}},
		PollWait:     5 * time.Millisecond,
	})
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForRunner(t, runners, "runner-1")

	execID := submitWorkflow(t, server.URL, &types.WorkflowDef{
		Name: "server-runner-e2e",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.e2e"},
		},
	}, map[string]any{"claim_id": "c-1"})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer waitCancel()
	result, err := backend.WaitDone(waitCtx, execID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", result.Status)
	}
	output, ok := result.Output["start"].(map[string]any)
	if !ok {
		t.Fatalf("output = %T, want map", result.Output["start"])
	}
	if output["handled_by"] != "runner" || output["claim_id"] != "c-1" {
		t.Fatalf("output = %v, want runner output with claim_id", output)
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

func waitForRunner(t *testing.T, runners RunnerDirectory, runnerID string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		if _, ok := runners.Runner(context.Background(), runnerID); ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for runner %q to register", runnerID)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func submitWorkflow(t *testing.T, baseURL string, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := submitWorkflowRequest{Workflow: wf, Params: params}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(baseURL+SubmitWorkflowPath, "application/json", &buf)
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
