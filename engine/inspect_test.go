package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestInspectReturnsExecutionAndGraphNodeDetails(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	g, err := graph.Compile(&types.WorkflowDef{
		Name: "audit-query",
		Nodes: []types.NodeDef{
			{
				Name: "Approve",
				Type: "test.audit",
				Kind: types.NodeKindAction,
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	id := types.ExecutionID("exec-audit")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusSuccess,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	if err := state.UpsertNode(ctx, &NodeSnapshot{
		ExecutionID: id,
		Name:        "Approve",
		Status:      types.NodeStatusSuccess,
		Attempt:     2,
		Port:        "main",
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	if err := state.PutOutput(ctx, id, "Approve", map[string]any{
		"approved": true,
		"ticket":   "VULN-1",
	}); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}

	if detail.ExecutionID != id {
		t.Fatalf("ExecutionID = %q, want %q", detail.ExecutionID, id)
	}
	if detail.Status != types.ExecutionStatusSuccess {
		t.Fatalf("Status = %q, want %q", detail.Status, types.ExecutionStatusSuccess)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}

	node := detail.Nodes[0]
	if node.Name != "Approve" {
		t.Fatalf("node name = %q, want Approve", node.Name)
	}
	if node.Status != types.NodeStatusSuccess {
		t.Fatalf("node status = %q, want %q", node.Status, types.NodeStatusSuccess)
	}
	if node.Attempt != 2 {
		t.Fatalf("node attempt = %d, want 2", node.Attempt)
	}
	if node.Port != "main" {
		t.Fatalf("node port = %q, want main", node.Port)
	}
	if got := node.Output["approved"]; got != true {
		t.Fatalf("node output approved = %#v, want true", got)
	}
	if got := node.Output["ticket"]; got != "VULN-1" {
		t.Fatalf("node output ticket = %#v, want VULN-1", got)
	}
}

func TestInspectDefaultsMissingRequestedNodeToPending(t *testing.T) {
	ctx := context.Background()
	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)

	id := types.ExecutionID("exec-pending")
	if err := state.CreateExecution(ctx, &ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id, "Review")
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}

	if detail.ExecutionID != id {
		t.Fatalf("ExecutionID = %q, want %q", detail.ExecutionID, id)
	}
	if detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("Status = %q, want %q", detail.Status, types.ExecutionStatusRunning)
	}
	if len(detail.Nodes) != 1 {
		t.Fatalf("Nodes len = %d, want 1", len(detail.Nodes))
	}

	node := detail.Nodes[0]
	if node.Name != "Review" {
		t.Fatalf("node name = %q, want Review", node.Name)
	}
	if node.Status != types.NodeStatusPending {
		t.Fatalf("node status = %q, want %q", node.Status, types.NodeStatusPending)
	}
	if node.Attempt != 0 {
		t.Fatalf("node attempt = %d, want 0", node.Attempt)
	}
	if node.Port != "" {
		t.Fatalf("node port = %q, want empty", node.Port)
	}
	if node.Output != nil {
		t.Fatalf("node output = %#v, want nil", node.Output)
	}
}
