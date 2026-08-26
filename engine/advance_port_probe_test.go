package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// nodeReaderState counts the node reads the advance path used to charge to
// every completed node.
type nodeReaderState struct {
	*fakeState
	nodeReads int
}

func (s *nodeReaderState) GetNode(ctx context.Context, id types.ExecutionID, name string) (*NodeSnapshot, error) {
	s.nodeReads++
	return s.fakeState.GetNode(ctx, id, name)
}

// compileTwoPortGraph builds a source with two output ports going to different
// downstream nodes.
//
// Two ports, not one, so the assertions can tell "a port was carried" apart
// from "the right port was carried". With a single main port, dropping the
// value entirely and carrying it correctly both end with the same node
// scheduled, and a test built on that shape would pass either way.
func compileTwoPortGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "advance-port-probe",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "on_main", Type: "test.echo"},
			{Name: "on_other", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {
				"main":  {Targets: []types.Connection{{Node: "on_main", Input: "main"}}},
				"other": {Targets: []types.Connection{{Node: "on_other", Input: "main"}}},
			},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// startLeaseOnTwoPortGraph submits the graph and leases its root task.
func startLeaseOnTwoPortGraph(t *testing.T, eng *Engine, state *nodeReaderState, queue *fakeQueue) (types.ExecutionID, *TaskLease) {
	t.Helper()
	ctx := context.Background()
	id, err := eng.Submit(ctx, compileTwoPortGraph(t), map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	tasks := queue.Drain()
	if len(tasks) != 1 || tasks[0].NodeName != "start" {
		t.Fatalf("root delivery = %+v, want one task for start", tasks)
	}
	lease, err := eng.BuildTaskLease(ctx, tasks[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}
	return id, lease
}

// TestAdvanceDoesNotReadBackTheNodeItJustCommitted pins that a node advance
// gets the source port from the task rather than from the store.
//
// The committing side already holds the port -- it is the value it is about to
// write -- so reading the node back to learn it was a round trip per completed
// node that carried no information the store did not already have. GetNode's
// own doc comment named this branch as the reason it is hot-path traffic.
//
// Nothing about the resulting state changes, which is the whole difficulty:
// the scheduling outcome is identical whether the port arrives on the task or
// via a read. Counting the reads is the only assertion that can see it, and
// the delivery check below is what stops the count from being satisfied by an
// advance that simply did nothing.
func TestAdvanceDoesNotReadBackTheNodeItJustCommitted(t *testing.T) {
	ctx := context.Background()
	state := &nodeReaderState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)

	_, lease := startLeaseOnTwoPortGraph(t, eng, state, queue)

	state.nodeReads = 0
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "other", Data: map[string]any{"out": 1}},
	})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeAccepted)
	}

	if state.nodeReads != 0 {
		t.Errorf("node reads during commit + advance = %d, want 0 "+
			"(the port now rides on the advance task)", state.nodeReads)
	}

	// Teeth. Zero reads is trivially satisfiable by an advance that routed
	// nowhere, so the port has to be shown to have arrived intact: "other",
	// not "" and not "main".
	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "on_other" {
		t.Fatalf("delivered = %+v, want exactly the on_other task", delivered)
	}
}

// TestAdvanceWithoutCarriedPortFallsBackToTheStore covers the other side of the
// same change: an advance task that carries no port.
//
// That is not a hypothetical shape. Advance entries sitting in a durable outbox
// across the deploy that added Task.Port decode with it empty, and they have to
// keep routing correctly or a rolling upgrade silently sends live executions
// down the wrong branch. The fallback is the only thing standing between those
// entries and that outcome, and without this test it would be an untested
// branch that a later reader could remove as dead on the reasoning that every
// task obviously carries a port now.
func TestAdvanceWithoutCarriedPortFallsBackToTheStore(t *testing.T) {
	ctx := context.Background()
	state := &nodeReaderState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, lease := startLeaseOnTwoPortGraph(t, eng, state, queue)
	task := lease.Task

	if _, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Port: "other", Data: map[string]any{"out": 1}},
	}); err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	// Drop what the carried-port advance already scheduled; this test is about
	// what a second, port-less delivery of the same advance does.
	queue.Drain()

	state.nodeReads = 0
	legacy := &Task{
		ExecutionID:  id,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		UnitIdx:      task.UnitIdx,
		Type:         TaskTypeNodeAdvance,
		ActivationID: task.ActivationID,
		// Port deliberately absent: this is the pre-upgrade wire shape.
	}
	handled, err := eng.handleSystemTask(ctx, legacy, true)
	if err != nil {
		t.Fatalf("handleSystemTask() error = %v", err)
	}
	if !handled {
		t.Fatal("handleSystemTask() reported the advance unhandled")
	}

	if state.nodeReads != 1 {
		t.Errorf("node reads for a port-less advance = %d, want 1 "+
			"(the fallback read that recovers the port)", state.nodeReads)
	}
}

// TestAdvanceFallbackRoutesToTheSamePortAsTheCarriedPath is the assertion the
// count above cannot make: that the fallback recovers the RIGHT port.
//
// Reading the node once and then routing on "main" would satisfy the read count
// and still be the bug the fallback exists to prevent. This runs the port-less
// advance as the only advance for the execution, so the branch it schedules is
// attributable to the fallback alone.
func TestAdvanceFallbackRoutesToTheSamePortAsTheCarriedPath(t *testing.T) {
	ctx := context.Background()
	state := &nodeReaderState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, lease := startLeaseOnTwoPortGraph(t, eng, state, queue)
	task := lease.Task

	// Commit the node terminal on "other" with no advance intent attached, so
	// the port-less advance below is the only one this execution ever sees and
	// the branch it schedules is attributable to the fallback alone.
	//
	// A fatal commit would suppress the advance too, but it also terminates the
	// execution, and the advance branch returns early on an inactive one -- the
	// fallback would never run and the test would pass by not testing it.
	if _, err := eng.commitNode(ctx, CommitNodeRequest{
		ExecutionID:  id,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		ActivationID: task.ActivationID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       types.NodeStatusSuccess,
		Output:       map[string]any{"out": 1},
		StoreOutput:  true,
		Port:         "other",
	}); err != nil {
		t.Fatalf("commitNode() error = %v", err)
	}
	if delivered := queue.Drain(); len(delivered) != 0 {
		t.Fatalf("commit without an advance intent delivered %+v, want nothing "+
			"(the fallback below must be the only thing that routes)", delivered)
	}

	legacy := &Task{
		ExecutionID:  id,
		NodeName:     task.NodeName,
		NodeIdx:      task.NodeIdx,
		UnitIdx:      task.UnitIdx,
		Type:         TaskTypeNodeAdvance,
		ActivationID: task.ActivationID,
	}
	if _, err := eng.handleSystemTask(ctx, legacy, true); err != nil {
		t.Fatalf("handleSystemTask() error = %v", err)
	}

	delivered := queue.Drain()
	if len(delivered) != 1 || delivered[0].NodeName != "on_other" {
		t.Fatalf("delivered = %+v, want exactly the on_other task "+
			"(the fallback must recover the committed port, not default to main)", delivered)
	}
}
