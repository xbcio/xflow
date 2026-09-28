package local

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// A node's private state is stored inside the map that is also its output, so
// every read that hands an output to somebody has to strip the slot. The wait
// result is assembled without going through Inspect, which makes it the one
// projection this backend has to apply itself.
func TestMemoryWaitResultOmitsNodePrivateState(t *testing.T) {
	ctx := context.Background()
	backend := New()
	state := backend.state
	id := types.ExecutionID("node-state-output")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	lease := privateOutputLease(id, "stateful", "lease-1", "token-1", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	committed, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "stateful",
		NodeIdx:     0,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
		Output: map[string]any{
			"visible":             "downstream",
			engine.NodeStateKey: map[string]any{"count": 3},
		},
		StoreOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !committed.ExecutionDone {
		t.Fatal("CommitNode() did not complete the one-unit execution")
	}

	// The store keeps the slot: a resumption reads the node's state back from
	// this map and nowhere else, so the raw read has to retain it.
	raw, err := state.GetOutput(ctx, id, "stateful")
	if err != nil {
		t.Fatalf("GetOutput() error = %v", err)
	}
	rawState, ok := raw[engine.NodeStateKey].(map[string]any)
	if !ok || !reflect.DeepEqual(rawState, map[string]any{"count": 3}) {
		t.Fatalf("stored state = %#v, want it kept for the node's next resumption", raw[engine.NodeStateKey])
	}

	result, err := backend.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("WaitDone() status = %q, want success", result.Status)
	}
	got, ok := result.Output["stateful"].(map[string]any)
	if !ok {
		t.Fatalf("WaitDone() output = %#v, want the node's public output", result.Output)
	}
	if state, leaked := got[engine.NodeStateKey]; leaked {
		t.Fatalf("WaitDone() leaked node-private state: %#v", state)
	}
	if got["visible"] != "downstream" {
		t.Fatalf("WaitDone() output = %#v, want the public keys kept", got)
	}

	// Stripping must not be in place: the result would otherwise have taken the
	// slot out of the very map the next resumption reads.
	if again, err := state.GetOutput(ctx, id, "stateful"); err != nil {
		t.Fatalf("GetOutput() after wait error = %v", err)
	} else if _, kept := again[engine.NodeStateKey]; !kept {
		t.Fatal("WaitDone() stripped the slot from the stored output")
	}
}

func nodeStateGraph(t *testing.T) *graph.Graph {
	t.Helper()
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name:  "node-state-output",
		Nodes: []types.NodeDef{{Name: "stateful", Type: "test.stateful"}},
	})
	if err != nil {
		t.Fatalf("Compile(node state graph) error = %v", err)
	}
	return compiled
}
