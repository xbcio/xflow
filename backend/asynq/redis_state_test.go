package asynq

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
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
		Runtime: &types.Runtime{Vars: map[string]any{"tenant_id": "tenant-a"}},
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
	if gotRuntime.Vars["tenant_id"] != "tenant-a" {
		t.Fatalf("Runtime.Vars = %#v, want tenant_id tenant-a", gotRuntime.Vars)
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
	state := newRedisState(rdb, nil, time.Hour)
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

func TestQueuedTaskPayloadKeepsSchedulerMetadataPrivate(t *testing.T) {
	task := &engine.Task{
		ExecutionID:  types.ExecutionID("exec-1"),
		NodeName:     "Review",
		NodeIdx:      3,
		Type:         engine.TaskTypeNodeExec,
		AutoDepth:    8,
		ActivationID: 13,
	}

	payload, err := marshalQueuedTask(task)
	if err != nil {
		t.Fatalf("marshalQueuedTask() error = %v", err)
	}
	var wire map[string]any
	if err := json.Unmarshal(payload, &wire); err != nil {
		t.Fatalf("payload unmarshal error = %v", err)
	}
	for _, key := range []string{"auto_depth", "activation_id"} {
		if _, ok := wire[key]; ok {
			t.Fatalf("public scheduler key %q leaked into queue payload: %s", key, payload)
		}
	}
	for _, key := range []string{"_auto_depth", "_activation_id"} {
		if _, ok := wire[key]; !ok {
			t.Fatalf("internal queue key %q missing from payload: %s", key, payload)
		}
	}

	got, err := unmarshalQueuedTask(payload)
	if err != nil {
		t.Fatalf("unmarshalQueuedTask() error = %v", err)
	}
	if got.AutoDepth != task.AutoDepth || got.ActivationID != task.ActivationID {
		t.Fatalf("scheduler metadata = %d/%d, want %d/%d", got.AutoDepth, got.ActivationID, task.AutoDepth, task.ActivationID)
	}
}

func TestUpsertNodeStoresStatusString(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer rdb.Close()

	state := newRedisState(rdb, nil, time.Minute)
	snapshot := &engine.NodeSnapshot{
		ExecutionID: "exec-1",
		Name:        "start",
		Status:      types.NodeStatusSuccess,
	}
	if err := state.UpsertNode(context.Background(), snapshot); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	got, err := rdb.Get(context.Background(), nodeStatusKey(snapshot.ExecutionID, snapshot.Name)).Result()
	if err != nil {
		t.Fatal(err)
	}
	if got != string(types.NodeStatusSuccess) {
		t.Fatalf("node status = %q, want %q", got, types.NodeStatusSuccess)
	}
}

func TestTransientModeRefreshesTTLOnNodeMutation(t *testing.T) {
	redisServer, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer redisServer.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
	defer rdb.Close()

	state := newRedisState(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-ttl")

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	rdb.Set(ctx, nodeStatusKey(id, "start"), string(types.NodeStatusPending), 20*time.Second)
	before := rdb.TTL(ctx, execKey(id, "status")).Val()
	if before <= 0 {
		t.Fatalf("initial TTL = %s, want positive", before)
	}
	redisServer.FastForward(20 * time.Second)
	before = rdb.TTL(ctx, execKey(id, "status")).Val()
	if before > 45*time.Second {
		t.Fatalf("aged TTL = %s, want below refresh target", before)
	}

	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusRunning,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}

	after := rdb.TTL(ctx, execKey(id, "status")).Val()
	if after <= before {
		t.Fatalf("TTL after mutation = %s, before = %s; want refreshed", after, before)
	}

	nodeTTL := rdb.TTL(ctx, nodeStatusKey(id, "start")).Val()
	if nodeTTL < 55*time.Second {
		t.Fatalf("node TTL after mutation = %s, want close to %s", nodeTTL, state.transientTTL)
	}
}

func TestTransientModeShortensTTLOnTerminalExecutionStatus(t *testing.T) {
	rdb := newRedisStateTestClient(t)
	state := newRedisState(rdb, nil, time.Hour)
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

	for _, key := range []string{execKey(id, "status"), execKey(id, "graph"), nodeStatusKey(id, "start")} {
		got := rdb.TTL(ctx, key).Val()
		if got < 10*time.Second || got > 15*time.Second {
			t.Fatalf("TTL for %q = %s, want close to %s", key, got, state.transientCompletionTTL)
		}
	}
}
