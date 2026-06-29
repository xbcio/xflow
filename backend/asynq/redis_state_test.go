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
