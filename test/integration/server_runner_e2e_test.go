//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/distributed"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/service/control"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/service/protocol/runnerpb"
	runnersvc "github.com/xbcio/xflow/service/runner"
	"github.com/xbcio/xflow/types"
	redis "github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

func (e2eRealHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.e2e.real"} }
func (e2eRealHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

// Finding 2: ticker + select instead of bare time.Sleep.
func waitForE2ERunner(t *testing.T, runners control.RunnerDirectory, id string) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.Now().Add(3 * time.Second)
	for {
		if _, ok := runners.Runner(context.Background(), id); ok {
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

	b, err := distributed.New(addr, nil, distributed.WithConcurrency(1), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}

	// Finding 3: flush stale asynq tasks from previous (crashed) runs.
	// Scoped to the "asynq:*" namespace (not FlushDB) so it does not clear
	// keys other integration tests (e.g. leader election) may hold in the
	// same Redis DB.
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	eng := engine.New(b.State(), b.Queue())
	runners := control.NewRedisRunnerDirectory(b.RedisClient())
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

// e2eGRPCHandler records completed executions for the gRPC integration
// test. The test verifies durable gRPC handoff; concurrent credit-flow is a
// separate experimental transport capability and is intentionally not part of
// this release-gate assertion.
type e2eGRPCHandler struct {
	executions atomic.Int32
}

func (*e2eGRPCHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.e2e.grpc"}
}

func (h *e2eGRPCHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	h.executions.Add(1)
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

// TestServerRunnerE2EgRPCStreamRealRedis drives the production gRPC Connect
// bidi-stream path end-to-end against a real Redis/Asynq backend, mirroring
// TestServerRunnerE2ERealRedis but swapping the HTTP fallback transport for
// gRPC. It verifies that independent assignments survive the durable
// server-to-runner handoff and complete through the gRPC protocol. It does not
// assert concurrent credit-flow, which remains experimental.
func TestServerRunnerE2EgRPCStreamRealRedis(t *testing.T) {
	addr := requireRedis(t)

	b, err := distributed.New(addr, nil, distributed.WithConcurrency(2), distributed.WithConsumer(true))
	if err != nil {
		t.Fatalf("distributed.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	eng := engine.New(b.State(), b.Queue())
	runners := control.NewRedisRunnerDirectory(b.RedisClient())
	dispatcher := control.NewDispatcher(eng, runners)
	stopBackend := b.BindHandler(eng, dispatcher.HandleTask)
	t.Cleanup(stopBackend)

	// HTTP server is used for workflow submission; the runner itself connects
	// through the gRPC production transport.
	httpSrv := httptest.NewServer(control.NewServer(eng, runners).Handler())
	t.Cleanup(httpSrv.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	grpcSrv := grpc.NewServer()
	runnerpb.RegisterRunnerProtocolServer(grpcSrv, control.NewGRPCServer(eng, runners))
	go func() { _ = grpcSrv.Serve(lis) }()
	t.Cleanup(grpcSrv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial gRPC: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	registry := execution.NewRegistry()
	handler := &e2eGRPCHandler{}
	registry.RegisterGlobal("test.e2e.grpc", handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runner := runnersvc.New(
		protocol.NewGRPCClient(conn),
		registry,
		runnersvc.Config{
			RunnerID:     "runner-grpc-1",
			Concurrency:  2,
			Capabilities: []protocol.Capability{{NodeType: "test.e2e.grpc"}},
		},
	)
	errCh := make(chan error, 1)
	go func() { errCh <- runner.Run(ctx) }()
	waitForE2ERunner(t, runners, "runner-grpc-1")

	execIDs := make([]types.ExecutionID, 0, 2)
	for _, claim := range []string{"c-grpc-1", "c-grpc-2"} {
		id := submitWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), &types.WorkflowDef{
			Name: "server-runner-e2e-grpc-stream",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.e2e.grpc"},
			},
		}, map[string]any{"claim_id": claim})
		execIDs = append(execIDs, id)
	}

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer waitCancel()
	results := make([]types.Result, 0, 2)
	for _, id := range execIDs {
		results = append(results, waitForCompletion(waitCtx, t, b.State(), id, "start"))
	}
	for i, r := range results {
		if r.Status != types.ExecutionStatusSuccess {
			t.Fatalf("workflow %d status = %s, want success", i, r.Status)
		}
		out, ok := r.Output["start"].(map[string]any)
		if !ok {
			t.Fatalf("workflow %d output[start] = %T, want map", i, r.Output["start"])
		}
		if out["handled_by"] != "runner" {
			t.Fatalf("workflow %d output = %v, want handled_by=runner", i, out)
		}
	}
	if got := handler.executions.Load(); got != 2 {
		t.Fatalf("gRPC handler executions = %d, want 2", got)
	}

	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runner error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runner did not stop in time")
	}
}
