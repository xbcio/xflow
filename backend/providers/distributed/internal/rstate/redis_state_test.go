package rstate

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func newRedisStateTestClient(t *testing.T) *redis.Client {
	t.Helper()

	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(redisServer.Close)

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

func testGraphOneNode() *graph.Graph {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "test-one-node",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		panic(err)
	}
	return g
}

type StoreTestQueue struct {
	tasks []*engine.Task
}

func (q *StoreTestQueue) Enqueue(_ context.Context, task *engine.Task) error {
	q.tasks = append(q.tasks, task)
	return nil
}

func (q *StoreTestQueue) EnqueueDelayed(_ context.Context, task *engine.Task, _ time.Duration) error {
	q.tasks = append(q.tasks, task)
	return nil
}

func TestBuildExecutionRecordPersistsWorkflowAuditContext(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "vulnerability-approval",
		Nodes: []types.NodeDef{{
			Name:       "SecurityApproval",
			Type:       "xflow.approval",
			Parameters: map[string]any{"mode": "all", "approvers": []any{"alice", "bob"}},
		}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	params := map[string]any{"vuln_id": "VULN-2026-001", "severity": "critical"}
	ctx := engine.WithWorkflowDef(context.Background(), def)

	rec, err := buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID:      "exec-1",
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		Params:  params,
		Runtime: &types.Runtime{Vars: map[string]any{"namespace_id": "namespace-a"}},
	}, time.Unix(100, 0))
	if err != nil {
		t.Fatalf("buildExecutionRecord() error = %v", err)
	}
	if rec.WorkflowName != "vulnerability-approval" {
		t.Fatalf("WorkflowName = %q, want vulnerability-approval", rec.WorkflowName)
	}

	var gotDef types.WorkflowDef
	if err := json.Unmarshal(rec.WorkflowDef, &gotDef); err != nil {
		t.Fatalf("WorkflowDef unmarshal error = %v", err)
	}
	if gotDef.Name != "vulnerability-approval" || len(gotDef.Nodes) != 1 {
		t.Fatalf("WorkflowDef = %#v, want submitted definition", gotDef)
	}

	var gotParams map[string]any
	if err := json.Unmarshal(rec.Params, &gotParams); err != nil {
		t.Fatalf("Params unmarshal error = %v", err)
	}
	if gotParams["vuln_id"] != "VULN-2026-001" || gotParams["severity"] != "critical" {
		t.Fatalf("Params = %#v, want submitted params", gotParams)
	}

	var gotRuntime types.Runtime
	if err := json.Unmarshal(rec.Runtime, &gotRuntime); err != nil {
		t.Fatalf("Runtime unmarshal error = %v", err)
	}
	if gotRuntime.Vars["namespace_id"] != "namespace-a" {
		t.Fatalf("Runtime.Vars = %#v, want namespace_id namespace-a", gotRuntime.Vars)
	}
	if rec.TraceID != "" || rec.SpanID != "" {
		t.Fatalf("trace metadata = %q/%q, want empty by default", rec.TraceID, rec.SpanID)
	}

	rec, err = buildExecutionRecord(ctx, &engine.ExecutionSnapshot{
		ID:      "exec-2",
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		TraceID: "trace-123",
		SpanID:  "span-456",
	}, time.Unix(100, 0))
	if err != nil {
		t.Fatalf("buildExecutionRecord() with trace metadata error = %v", err)
	}
	if rec.TraceID != "trace-123" {
		t.Fatalf("TraceID = %q, want trace-123", rec.TraceID)
	}
	if rec.SpanID != "span-456" {
		t.Fatalf("SpanID = %q, want span-456", rec.SpanID)
	}
}

func TestRedisStatePersistsTraceMetadata(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	def := &types.WorkflowDef{
		Name:  "trace-context",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	state := New(rdb, nil, time.Hour)
	ctx := context.Background()
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:      "exec-trace",
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		TraceID: "trace-123",
		SpanID:  "span-456",
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	snap, err := state.GetExecution(ctx, "exec-trace")
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap.TraceID != "trace-123" {
		t.Fatalf("TraceID = %q, want trace-123", snap.TraceID)
	}
	if snap.SpanID != "span-456" {
		t.Fatalf("SpanID = %q, want span-456", snap.SpanID)
	}
}

func TestUpsertNodeStoresStatusString(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Minute)
	snapshot := &engine.NodeSnapshot{
		ExecutionID: "exec-1",
		Name:        "start",
		Status:      types.NodeStatusSuccess,
	}
	if err := state.UpsertNode(context.Background(), snapshot); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	got, err := rdb.Get(context.Background(), nodeStatusKey(namespace.Default, snapshot.ExecutionID, snapshot.Name)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got != string(types.NodeStatusSuccess) {
		t.Fatalf("node status = %q, want %q", got, types.NodeStatusSuccess)
	}
}

func TestTransientModeSetsTransientTTLWithoutPerMutationRefresh(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-ttl")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Params: map[string]any{"k": "v"},
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Optimization 4: structural keys are written with transientTTL at creation,
	// so they outlive the run under the documented constraint (transientTTL > max
	// wall-clock) WITHOUT any per-mutation refresh.
	for _, key := range []string{execKey(namespace.Default, id, "status"), execKey(namespace.Default, id, "graph"), execKey(namespace.Default, id, "params")} {
		got := rdb.TTL(ctx, key).Val()
		if got < 55*time.Second {
			t.Fatalf("creation TTL for %q = %s, want close to %s", key, got, state.transientTTL)
		}
	}

	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusRunning,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	// The per-node Lua write carries transientTTL directly; no refresh needed.
	nodeTTL := rdb.TTL(ctx, nodeStatusKey(namespace.Default, id, "start")).Val()
	if nodeTTL < 55*time.Second {
		t.Fatalf("node TTL after mutation = %s, want close to %s", nodeTTL, state.transientTTL)
	}
}

func TestTransientModeShortensTTLOnTerminalExecutionStatus(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 15 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-complete")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusSuccess,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	for _, key := range []string{execKey(namespace.Default, id, "status"), execKey(namespace.Default, id, "graph"), nodeStatusKey(namespace.Default, id, "start")} {
		got := rdb.TTL(ctx, key).Val()
		if got < 10*time.Second || got > 15*time.Second {
			t.Fatalf("TTL for %q = %s, want close to %s", key, got, state.transientCompletionTTL)
		}
	}
}

func TestTransientClaimTaskLeaseFencesCanceledExecutionWithStaleGraphCache(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 15 * time.Second

	ctx := context.Background()
	controlQueue := &StoreTestQueue{}
	control := engine.New(state, controlQueue)
	worker := engine.New(state, &StoreTestQueue{})

	id, err := control.Submit(ctx, testGraphOneNode(), map[string]any{"msg": "hello"})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if len(controlQueue.tasks) != 1 {
		t.Fatalf("queued tasks = %d, want 1", len(controlQueue.tasks))
	}
	task := controlQueue.tasks[0]

	lease, err := worker.BuildTaskLease(ctx, task)
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}
	if err := control.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}

	if err := worker.CommitTaskResult(ctx, lease, engine.TaskResult{
		Output: &types.Output{Data: map[string]any{"stale": true}},
	}); err != nil {
		t.Fatalf("CommitTaskResult() after cancel error = %v", err)
	}

	out, err := state.GetOutput(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetOutput() error = %v", err)
	}
	if out != nil {
		t.Fatalf("output = %#v, want nil after canceled execution", out)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("execution status = %q, want canceled", snap.Status)
	}
	node, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusRunning {
		t.Fatalf("node status = %+v, want original running state not overwritten by stale commit", node)
	}
}
