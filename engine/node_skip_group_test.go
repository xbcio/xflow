package engine

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestNotifySkip_GroupCommitReportsEachSkippedNode covers the third skip site.
//
// A group's downstream fan-in is counted inside the commit transition rather
// than by a subsequent advance, so a skip decided there reaches the engine
// through GroupCommitResult instead of AdvanceNodeResult. Nothing about the
// engine's dispatch is group-specific, but the two sites are separate call
// paths, and a mutation that dropped one would leave the other's test green.
//
// The backend result is supplied explicitly rather than induced from the
// fixture. Making this double reproduce the DAG arithmetic would test the
// double; what has to be pinned here is that the engine turns a reported skip
// list into exactly one observation per node, with the labels the metric
// partitions on and the per-node counts intact. Both real backends' arithmetic
// is covered where it lives, including its own skip reporting.
func TestNotifySkip_GroupCommitReportsEachSkippedNode(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingSkipObserver{}
	state := &fakeStateWithGroup{
		fakeState: newFakeState(),
		groupState: &fakeGroupState{commitResult: &GroupCommitResult{
			Outcome: CommitOutcomeAccepted,
			Applied: true,
			Skipped: []SkippedUnit{
				{NodeName: "out_a", Count: 2},
				{NodeName: "out_b", Count: 1},
			},
		}},
	}
	execID := types.ExecutionID("exec-observer-group-skip")
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	eng := New(state, &fakeQueue{},
		WithGroupExecutor(&fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}}}),
		WithNodeSkipObserver(obs))
	eng.cacheExecutionGraph(execID, g)

	if _, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true); err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}

	if len(obs.take()) != 2 {
		t.Fatalf("skips = %+v, want exactly two observations (one per skipped node)", obs.take())
	}
	byNode := map[string]recordedSkip{}
	for _, s := range obs.take() {
		byNode[s.node] = s
	}
	for node, want := range map[string]int{"out_a": 2, "out_b": 1} {
		got, ok := byNode[node]
		if !ok {
			t.Fatalf("no skip observed for %q; got %+v", node, obs.take())
		}
		if got.flow != flowGroup {
			t.Fatalf("skip flow for %q = %q, want %q", node, got.flow, flowGroup)
		}
		if got.count != want {
			t.Fatalf("skip count for %q = %d, want %d", node, got.count, want)
		}
	}
}

// TestNotifySkip_GroupCommitWithNoSkipsReportsNothing is the group site's
// negative control, mirroring the advance path's: an accepted commit that
// skipped nothing must produce no observation, so the series keeps meaning
// "work was lost" rather than "a group committed".
func TestNotifySkip_GroupCommitWithNoSkipsReportsNothing(t *testing.T) {
	g := buildSingleGroupGraph(t)
	groupUnitIdx := findGroupUnit(t, g)

	obs := &recordingSkipObserver{}
	groupObs := &recordingGroupObserver{}
	state := &fakeStateWithGroup{
		fakeState: newFakeState(),
		groupState: &fakeGroupState{commitResult: &GroupCommitResult{
			Outcome: CommitOutcomeAccepted,
			Applied: true,
		}},
	}
	execID := types.ExecutionID("exec-observer-group-no-skip")
	state.fakeState.CreateExecution(context.Background(), &ExecutionSnapshot{
		ID:     execID,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	})

	eng := New(state, &fakeQueue{},
		WithGroupExecutor(&fakeGroupExecutor{exits: []GroupExit{{NodeName: "g.sink", Port: "main", Data: map[string]any{"ok": true}}}}),
		WithGroupObserver(groupObs),
		WithNodeSkipObserver(obs))
	eng.cacheExecutionGraph(execID, g)

	if _, err := eng.handleSystemTask(context.Background(), &Task{
		ExecutionID: execID,
		NodeName:    "g",
		UnitIdx:     groupUnitIdx,
		Type:        TaskTypeGroupExec,
	}, true); err != nil {
		t.Fatalf("handleSystemTask: %v", err)
	}

	if got := obs.take(); len(got) != 0 {
		t.Fatalf("skips = %+v, want none for a commit that skipped nothing", got)
	}
	// Teeth: the commit has to have happened, or "no skips" is vacuous. The
	// group observer is the independent witness that it did — asserted through
	// a different observer than the one under test, so a change that stopped
	// calling notifySkip and a change that stopped committing cannot both be
	// satisfied by the same broken wiring.
	if len(groupObs.commitOutcomes) != 1 {
		t.Fatalf("commit outcomes = %v, want exactly one — the commit did not run, "+
			"so a zero skip count proves nothing", groupObs.commitOutcomes)
	}
}
