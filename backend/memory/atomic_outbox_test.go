package memory

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestMemoryAtomicOutboxTransitionsPreserveTaskPayload(t *testing.T) {
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "memory-atomic-outbox",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("memory-outbox")
	root := engine.OutboxEntry{
		ID:   "root/memory-outbox/start/0",
		Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}, []engine.OutboxEntry{root}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}
	if entries, err := state.ListOutbox(ctx, id, time.Now(), 4); err != nil || len(entries) != 1 || entries[0].ID != root.ID {
		t.Fatalf("initial outbox entries=%+v err=%v", entries, err)
	}
	if err := state.AckOutbox(ctx, id, root.ID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}

	lease := &engine.TaskLease{
		LeaseID:    "lease-1",
		LeaseToken: "token-1",
		Task:       engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeResume},
		IssuedAt:   time.Now().UTC(),
		TTL:        time.Minute,
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	payload := map[string]any{"approved": true}
	requeue := engine.OutboxEntry{
		ID: "requeue/memory-outbox/start/0/lease-1",
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "start",
			NodeIdx:     0,
			Type:        engine.TaskTypeNodeResume,
			Payload:     &types.SignalPayload{Name: "approval", Data: payload},
		},
	}
	revoked, err := state.RevokeLeaseWithOutbox(ctx, id, "start", lease.LeaseToken, requeue)
	if err != nil || !revoked {
		t.Fatalf("RevokeLeaseWithOutbox() revoked=%v err=%v, want true/nil", revoked, err)
	}
	payload["approved"] = false

	node, err := state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusPending || node.LeaseToken != "" {
		t.Fatalf("node after revoke=%+v err=%v, want pending without token", node, err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now(), 4)
	if err != nil || len(entries) != 1 || entries[0].ID != requeue.ID {
		t.Fatalf("requeue entries=%+v err=%v", entries, err)
	}
	if got := entries[0].Task.Payload.Data["approved"]; got != true {
		t.Fatalf("stored resume payload mutated to %v, want true", got)
	}
}
