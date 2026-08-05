package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/service/protocol"
	"github.com/xbcio/xflow/types"
)

// batchRoutingWorkflow is a single xflow.map node carrying a required
// runnerSelector. Mode is workflow-level (compile rejects it on a node), the
// match labels are the map node's own. The body is the minimum shape
// validateMapBody accepts: the selector, not the body, is what this file is
// about.
func batchRoutingWorkflow() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:           "batch-routing",
		RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired},
		Nodes: []types.NodeDef{{
			Name:           "m",
			Type:           "xflow.map",
			RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"zone": "datahub"}},
			Parameters: map[string]any{
				"items": "$input.rows",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "echo", "type": "test.echo"},
						},
					},
				},
			},
		}},
	}
}

// batchTaskFor builds the batch task the engine's own expansion would enqueue.
// The parent lease id/token are deliberately not live: routing must not depend
// on them, and a non-matching fence keeps ExecuteBatch a no-op if the escape
// hatch is missing, so the failure this test reports is "swallowed", not
// "errored".
func batchTaskFor(execID types.ExecutionID, nodeIdx int) *engine.Task {
	return &engine.Task{
		ExecutionID: execID,
		NodeName:    "m/_batch/0",
		NodeIdx:     nodeIdx,
		Type:        engine.TaskTypeNodeBatch,
		Payload: &types.SignalPayload{Data: map[string]any{
			"_batch_exec":          true,
			"parent_exec_id":       string(execID),
			"parent_node":          "m",
			"parent_node_idx":      nodeIdx,
			"parent_lease_id":      "lease-not-live",
			"parent_lease_token":   "token-not-live",
			"parent_attempt":       1,
			"parent_activation_id": 0,
			"parent_auto_depth":    0,
			"child_exec_id":        string(execID) + "/sub/m/lease-not-live/0",
			"batch_index":          0,
			"items":                []any{map[string]any{"id": 1}},
		}},
	}
}

// A batch task is consumed unconditionally by handleSystemTask today, so it
// never reaches TaskRouting and the runnerSelector configured on the map node
// is silently inert — it never reaches the directory that matches labels.
// Asserting on the selector rather than only on "an assignment exists" is the
// point: routing that drops the selector would place body work on any runner,
// which is the same production outcome as not routing at all.
func TestDispatcherRoutesBatchTaskCarryingTheMapNodeSelector(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	g, err := graph.Compile(batchRoutingWorkflow())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	execID, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	mapIdx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}

	dir := NewMemoryRunnerDirectory()
	dispatcher := NewDispatcher(eng, dir)

	if err := dispatcher.HandleTask(ctx, batchTaskFor(execID, mapIdx)); err != nil {
		t.Fatalf("HandleTask() error = %v", err)
	}

	caps := []protocol.Capability{{NodeType: "xflow.map"}}
	session, err := dir.Register(ctx, RegisterRunnerRequest{
		RunnerID:     "runner-1",
		Capacity:     1,
		Labels:       map[string]string{"zone": "datahub"},
		Capabilities: caps,
		Policy:       RunnerPolicy{AllowedNodeTypes: []string{"*"}},
	})
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	claim, ok, err := dir.ClaimForRunner(ctx, ClaimRequest{
		RunnerID:     "runner-1",
		SessionID:    session.SessionID,
		Capacity:     1,
		Labels:       map[string]string{"zone": "datahub"},
		Capabilities: caps,
	})
	if err != nil {
		t.Fatalf("ClaimForRunner() error = %v", err)
	}
	if !ok {
		t.Fatal("batch task was swallowed by handleSystemTask: no assignment reached the directory")
	}
	got := claim.Assignment.Routing.RunnerSelector
	if got == nil {
		t.Fatal("assignment lost the map node's runnerSelector entirely")
	}
	if got.Mode != types.RunnerSelectorModeRequired || got.MatchLabels["zone"] != "datahub" {
		t.Errorf("assignment routing selector = %+v, want required/zone=datahub", got)
	}
}
