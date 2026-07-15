package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestEngineLoopSplitStaleBatchCannotFinalizeReclaimedParent(t *testing.T) {
	def := &types.WorkflowDef{
		Name:    "loop-stale-batch-fence",
		Options: &types.WorkflowOptions{ExperimentalExpand: true},
		Nodes: []types.NodeDef{
			{Name: "loop", Type: "xflow.loop"},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"loop": {"main": []types.Connection{{Node: "done", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := newTestEngine(state, queue, &fakeRegistry{})
	ctx := context.Background()
	id, err := eng.Submit(ctx, g, nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 {
		t.Fatalf("root tasks = %d, want 1", len(roots))
	}

	loopOutput := TaskResult{Output: &types.Output{Data: map[string]any{
		"_loop":   true,
		"batches": [][]any{{"old"}},
	}}}
	firstLease, err := eng.BuildTaskLease(ctx, roots[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(first) error = %v", err)
	}
	if err := eng.CommitTaskResult(ctx, firstLease, loopOutput); err != nil {
		t.Fatalf("CommitTaskResult(first) error = %v", err)
	}
	oldBatches := queue.Drain()
	if len(oldBatches) != 1 {
		t.Fatalf("old batch tasks = %d, want 1", len(oldBatches))
	}

	// Simulate lease recovery after the first process created batches but died
	// before they completed. The recovered task gets a new parent generation.
	revoked, err := state.RevokeLease(ctx, id, "loop", firstLease.LeaseToken)
	if err != nil || !revoked {
		t.Fatalf("RevokeLease(first) = %v, %v; want true, nil", revoked, err)
	}
	if err := queue.Enqueue(ctx, roots[0]); err != nil {
		t.Fatalf("enqueue recovered root: %v", err)
	}
	recovered := queue.Drain()
	if len(recovered) != 1 {
		t.Fatalf("recovered tasks = %d, want 1", len(recovered))
	}
	secondLease, err := eng.BuildTaskLease(ctx, recovered[0])
	if err != nil {
		t.Fatalf("BuildTaskLease(second) error = %v", err)
	}
	if secondLease.LeaseToken == firstLease.LeaseToken {
		t.Fatal("recovered task reused the original lease token")
	}
	if err := eng.CommitTaskResult(ctx, secondLease, loopOutput); err != nil {
		t.Fatalf("CommitTaskResult(second) error = %v", err)
	}
	newBatches := queue.Drain()
	if len(newBatches) != 1 {
		t.Fatalf("new batch tasks = %d, want 1", len(newBatches))
	}

	if err := eng.ExecuteBatch(ctx, oldBatches[0]); err != nil {
		t.Fatalf("ExecuteBatch(stale) error = %v", err)
	}
	node, err := state.GetNode(ctx, id, "loop")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if node == nil || node.Status != types.NodeStatusWaiting || node.LeaseToken != secondLease.LeaseToken {
		t.Fatalf("node after stale batch = %+v, want second waiting generation", node)
	}
	if queued := queue.Drain(); len(queued) != 0 {
		t.Fatalf("stale batch queued downstream tasks: %v", taskNames(queued))
	}

	if err := eng.ExecuteBatch(ctx, newBatches[0]); err != nil {
		t.Fatalf("ExecuteBatch(current) error = %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		queued := queue.Drain()
		for _, task := range queued {
			if task.NodeName == "done" {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("current batch did not enqueue downstream node")
		}
		time.Sleep(time.Millisecond)
	}
}
