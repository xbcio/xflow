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

	"github.com/xbcio/xflow/backend/asynq"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/node"
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
	// Scoped to the "asynq:*" namespace (not FlushDB) so it does not clear
	// keys other integration tests (e.g. leader election) may hold in the
	// same Redis DB.
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
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

// e2eCountingHandler records concurrent executions so the gRPC streaming
// integration test can assert the runner's worker pool actually runs tasks in
// parallel under real asynq dispatch (credit-based flow control lets the
// server push >1 TASK over the Connect stream; the worker pool must execute
// them concurrently).
type e2eCountingHandler struct {
	nodeType  string
	active    atomic.Int32
	maxActive atomic.Int32
}

func (h *e2eCountingHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: h.nodeType} }
func (h *e2eCountingHandler) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	cur := h.active.Add(1)
	for {
		m := h.maxActive.Load()
		if cur <= m || h.maxActive.CompareAndSwap(m, cur) {
			break
		}
	}
	// Sleep long enough that two concurrently-dispatched tasks must overlap
	// if the runner's worker pool honors Concurrency > 1.
	time.Sleep(150 * time.Millisecond)
	h.active.Add(-1)
	return &types.Output{Data: map[string]any{
		"handled_by": "runner",
		"claim_id":   input.Data["claim_id"],
	}}, nil
}

// TestServerRunnerE2EgRPCStreamRealRedis drives the production gRPC Connect
// bidi-stream path end-to-end against a real Redis/asynq backend, mirroring
// TestServerRunnerE2ERealRedis but swapping the HTTP fallback transport for
// gRPC. It also verifies credit-based parallel dispatch: two independent
// workflows are submitted while the runner advertises Concurrency=2, and the
// counting handler asserts both tasks executed concurrently — proving the
// server pushed two TASK frames over one Connect stream and the runner's
// worker pool drained them in parallel (not serially, as the pre-streaming
// concurrency=1 bug would have).
func TestServerRunnerE2EgRPCStreamRealRedis(t *testing.T) {
	addr := requireRedis(t)

	// asynq consumer concurrency = 2 so two queued tasks can be dispatched
	// in parallel to the runner pool (otherwise tasks arrive serially and
	// runner-side Concurrency is moot).
	b, err := asynq.New(addr, nil, asynq.WithConcurrency(2), asynq.WithConsumer(true))
	if err != nil {
		t.Fatalf("asynq.New: %v", err)
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	flushAsynqKeys(context.Background(), t, rdb)
	_ = rdb.Close()

	eng := engine.New(b.State(), b.Queue())
	runners := control.NewRunnerPool()
	dispatcher := control.NewDispatcher(eng, runners)
	stopBackend := b.BindHandler(eng, dispatcher.HandleTask)
	t.Cleanup(stopBackend)

	// HTTP server for workflow submission only.
	httpSrv := httptest.NewServer(control.NewServer(eng, runners).Handler())
	t.Cleanup(httpSrv.Close)

	// gRPC server for the runner's Connect bidi stream — the production
	// transport under test. Shares the same eng/runners as the HTTP server,
	// exactly like cmd/server's dual-listener setup.
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
	handler := &e2eCountingHandler{nodeType: "test.e2e.grpc"}
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

	// Submit two independent single-node workflows. Each lands as one
	// asynq task; with asynq concurrency=2 and runner Concurrency=2, both
	// should dispatch over the Connect stream and execute in parallel.
	execIDs := make([]types.ExecutionID, 0, 2)
	for i, claim := range []string{"c-grpc-1", "c-grpc-2"} {
		id := submitWorkflowHTTP(t, httpSrv.URL, httpSrv.Client(), &types.WorkflowDef{
			Name: "server-runner-e2e-grpc-stream",
			Nodes: []types.NodeDef{
				{Name: "start", Type: "test.e2e.grpc"},
			},
		}, map[string]any{"claim_id": claim})
		_ = i
		execIDs = append(execIDs, id)
	}

	// Wait for both to reach a terminal status and collect "start" outputs.
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

	// The core streaming assertion: with Concurrency=2 and two concurrently-
	// dispatched tasks, the runner's worker pool must have executed both at
	// once at some point. If this drops to 1, either the server didn't push
	// a second TASK (credit regression) or the worker pool serialized
	// (concurrency=1 regression).
	if got := handler.maxActive.Load(); got < 2 {
		t.Fatalf("max concurrent handler executions = %d, want >= 2 (credit push or worker-pool concurrency regression)", got)
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
