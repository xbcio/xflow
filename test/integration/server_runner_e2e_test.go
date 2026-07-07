//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/nodes/node"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	redis "github.com/redis/go-redis/v9"
)

type e2eSubmitReq struct {
	Workflow *types.WorkflowDef `json:"workflow"`
	Params   map[string]any     `json:"params"`
}

type e2eSubmitResp struct {
	ExecutionID types.ExecutionID `json:"execution_id"`
}

// Finding 1: accept *http.Client so server.Close() cleans up idle connections.
func submitWorkflowHTTP(t *testing.T, baseURL string, client *http.Client, wf *types.WorkflowDef, params map[string]any) types.ExecutionID {
	t.Helper()
	body := e2eSubmitReq{Workflow: wf, Params: params}
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		t.Fatalf("encode: %v", err)
	}
	resp, err := client.Post(baseURL+control.SubmitWorkflowPath, "application/json", &buf)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("submit status = %d, want 200", resp.StatusCode)
	}
	var out e2eSubmitResp
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if out.ExecutionID == "" {
		t.Fatal("empty execution_id")
	}
	return out.ExecutionID
}

type e2eRealHandler struct{}

func (e2eRealHandler) Descriptor() node.Descriptor { return node.Descriptor{Type: "test.e2e.real"} }
func (e2eRealHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

// Finding 2: ticker + select instead of bare time.Sleep.
func waitForE2ERunner(t *testing.T, pool *control.RunnerPool, id string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := pool.Runner(id); ok {
			return
		}
		select {
		case <-ticker.C:
		case <-time.After(time.Until(deadline)):
			t.Fatalf("timeout waiting for runner %q to register", id)
		}
	}
}

func TestServerRunnerE2ERealRedis(t *testing.T) {
	addr := requireRedis(t)

	b, err := asynq.New(addr, nil, asynq.WithConcurrency(1), asynq.WithConsumer(true))
	if err != nil {
		t.Fatalf("asynq.New: %v", err)
	}

	// Finding 3: flush stale asynq tasks from previous (crashed) runs.
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatalf("flushdb: %v", err)
	}
	_ = rdb.Close()

	eng := engine.New(b.State(), b.Queue())
	runners := control.NewRunnerPool()
	dispatcher := control.NewDispatcher(eng, runners)
	stopBackend := b.BindHandler(eng, dispatcher.HandleTask)
	t.Cleanup(stopBackend)

	server := httptest.NewServer(control.NewServer(eng, runners).Handler())
	t.Cleanup(server.Close)

	registry := execution.NewRegistry()
	registry.RegisterGlobal("test.e2e.real", e2eRealHandler{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewClient(server.URL, server.Client()),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-real-1",
			Concurrency:  1,
			Capabilities: []protocol.Capability{{NodeType: "test.e2e.real"}},
			PollWait:     5 * time.Millisecond,
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, runners, "runner-real-1")

	// Finding 1: pass server.Client() so idle connections are cleaned up on server.Close.
	execID := submitWorkflowHTTP(t, server.URL, server.Client(), &types.WorkflowDef{
		Name: "server-runner-e2e-real",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.e2e.real"},
		},
	}, map[string]any{"claim_id": "c-real"})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	result := waitForCompletion(waitCtx, t, b.State(), execID, "start")
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", result.Status)
	}
	out, ok := result.Output["start"].(map[string]any)
	if !ok {
		t.Fatalf("output[start] = %T, want map", result.Output["start"])
	}
	if out["handled_by"] != "runner" || out["claim_id"] != "c-real" {
		t.Fatalf("output = %v, want handled_by=runner claim_id=c-real", out)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	// Finding 4: extend timeout to 3s and use t.Fatal instead of t.Log.
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}
