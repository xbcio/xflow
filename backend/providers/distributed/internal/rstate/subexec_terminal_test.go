package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestSubExecutionCompletionWaitsForANonTerminalChild pins what "all done"
// means on the store's own completion read.
//
// The answer decides whether the parent consumes its batch or keeps waiting, so
// a child counted as done but holding no result puts a silent hole in that
// batch — an empty result is a legitimate result. Only a terminal child is
// finished; testing for 'running' instead counts every other status as
// finished, including the ones a child can be parked in.
func TestSubExecutionCompletionWaitsForANonTerminalChild(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	parent := types.ExecutionID("parent-subexec-terminal")
	const node = "subgraph"

	if err := state.CreateSubExecution(ctx, &engine.SubExecution{
		ParentExecID: parent, ParentNode: node, ChildExecID: "child-0",
		BatchIndex: 0, Status: types.ExecutionStatusQueued,
	}); err != nil {
		t.Fatalf("CreateSubExecution(child-0) error = %v", err)
	}
	if err := state.CreateSubExecution(ctx, &engine.SubExecution{
		ParentExecID: parent, ParentNode: node, ChildExecID: "child-1",
		BatchIndex: 1, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateSubExecution(child-1) error = %v", err)
	}

	allDone, err := state.CompleteSubExecution(ctx, parent, node, "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("CompleteSubExecution() error = %v", err)
	}
	if allDone {
		t.Fatal("allDone = true with a child still queued: the parent would consume a batch missing that child's result")
	}

	// Terminal children are the other half of the rule: once the parked child
	// finishes, the batch is complete.
	if err := state.CreateSubExecution(ctx, &engine.SubExecution{
		ParentExecID: parent, ParentNode: node, ChildExecID: "child-0",
		BatchIndex: 0, Status: types.ExecutionStatusSuccess, Result: map[string]any{"ok": true},
	}); err != nil {
		t.Fatalf("CreateSubExecution(child-0 terminal) error = %v", err)
	}
	allDone, err = state.CompleteSubExecution(ctx, parent, node, "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("CompleteSubExecution() error = %v", err)
	}
	if !allDone {
		t.Fatal("allDone = false with every child terminal, want true")
	}
}

// TestExpandedSubExecutionCompletionWaitsForANonTerminalChild is the same rule
// on the expansion barrier, which decides it inside the Lua transition rather
// than in Go. The barrier is what the engine reads to know the batch is
// complete, and it has to agree with the Go-side read: two answers to one
// question is how a parent ends up waiting on a batch it already consumed, or
// consuming one it is still waiting for.
func TestExpandedSubExecutionCompletionWaitsForANonTerminalChild(t *testing.T) {
	state, _, _ := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("expansion-subexec-terminal")
	const node = "foreach"

	lease := &engine.TaskLease{
		LeaseID:    "lease-terminal",
		LeaseToken: "token-terminal",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID: id, NodeName: node, NodeIdx: 1, Type: engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v", claimed, err)
	}
	if started, err := state.BeginTaskExpansion(ctx, lease); err != nil || !started {
		t.Fatalf("BeginTaskExpansion() started=%v err=%v", started, err)
	}
	// child-0 is parked in a status that neither runs nor finished; child-1 is
	// the one this call reports.
	statuses := []types.ExecutionStatus{types.ExecutionStatusQueued, types.ExecutionStatusRunning}
	for i, cid := range []types.ExecutionID{"child-0", "child-1"} {
		if accepted, err := state.CreateExpandedSubExecution(ctx, lease, &engine.SubExecution{
			ParentExecID: id, ParentNode: node, ChildExecID: cid, BatchIndex: i, Status: statuses[i],
		}); err != nil || !accepted {
			t.Fatalf("CreateExpandedSubExecution(%d) accepted=%v err=%v", i, accepted, err)
		}
	}

	allDone, accepted, _, err := state.CompleteExpandedSubExecution(ctx, lease, "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil || !accepted {
		t.Fatalf("CompleteExpandedSubExecution() accepted=%v err=%v", accepted, err)
	}
	if allDone {
		t.Fatal("allDone = true with a child still queued: the barrier released a batch missing that child's result")
	}
}
