package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func compileCommitProbeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "commit-probe",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestAcyclicCommitDoesNotProbeExecutionStatus pins that the ordinary node
// commit reaches CommitNode without first asking the store whether the
// execution is alive.
//
// CommitNode decides that itself, inside the transition that would write, from
// the same key a probe here would read -- and it has to, because a probe cannot
// hold its answer across the round trip to the write. The probe was a second
// opinion charged to every commit to arrive one round trip earlier at the
// answer the write already carries.
//
// Nothing about the committed state changes, so no correctness assertion can
// see this. Counting the reads is the only way it stays gone.
func TestAcyclicCommitDoesNotProbeExecutionStatus(t *testing.T) {
	ctx := context.Background()
	state := &statusReaderState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), map[string]any{"a": 1})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	state.statusReads = 0
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Output: &types.Output{Data: map[string]any{"out": 1}},
	})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	if outcome != CommitOutcomeAccepted {
		t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeAccepted)
	}

	// One, not zero: the advance intent this commit produced is handled in the
	// same call and still probes on its own path. Asserting zero would be
	// asserting a change this test's subject did not make.
	if state.statusReads != 1 {
		t.Errorf("execution status reads during an accepted commit = %d, want 1 "+
			"(the advance handler's, and none for the commit itself)", state.statusReads)
	}
	// The count means nothing unless the commit actually landed.
	node, err := state.GetNode(ctx, id, "start")
	if err != nil || node == nil || node.Status != types.NodeStatusSuccess {
		t.Fatalf("committed node = %+v, %v, want success", node, err)
	}
	if delivered := queue.Drain(); len(delivered) != 1 || delivered[0].NodeName != "next" {
		t.Fatalf("delivered = %+v, want the downstream task", delivered)
	}
}

// TestSuspendCommitStillProbesExecutionStatus is the other half. The suspend
// commit does not go through CommitNode, so nothing downstream of it re-decides
// liveness -- dropping the probe there would let a suspend land on a canceled
// execution.
func TestSuspendCommitStillProbesExecutionStatus(t *testing.T) {
	ctx := context.Background()
	state := &statusReaderState{fakeState: newFakeState()}
	queue := &fakeQueue{}
	eng := New(state, queue)

	id, err := eng.Submit(ctx, compileCommitProbeGraph(t), nil)
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if err := eng.FlushOutbox(ctx, id); err != nil {
		t.Fatalf("FlushOutbox() error = %v", err)
	}
	task := queue.Drain()[0]
	lease, err := eng.BuildTaskLease(ctx, task)
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceled, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}
	state.statusReads = 0
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{
		Suspend: &types.SuspendSpec{Signals: []string{"approval"}},
	})
	if err != nil {
		t.Fatalf("CommitTaskResultWithOutcome() error = %v", err)
	}
	if outcome != CommitOutcomeExecutionInactive {
		t.Fatalf("suspend on a canceled execution = %s, want %s", outcome, CommitOutcomeExecutionInactive)
	}
	if state.statusReads == 0 {
		t.Error("suspend commit made no status read: it has no CommitNode behind it, " +
			"so the probe is the only thing standing between it and a canceled execution")
	}
}
