package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// groupPhaseFakeState adds the group-unit readback to the package's fakeState.
// The phase table is set by the test rather than derived from a group protocol
// the fake does not implement: Inspect only asks "does a record exist", and the
// test wants to control that answer on both sides.
type groupPhaseFakeState struct {
	*fakeState
	phases map[string]GroupUnitPhase
}

func newGroupPhaseFakeState() *groupPhaseFakeState {
	return &groupPhaseFakeState{fakeState: newFakeState(), phases: map[string]GroupUnitPhase{}}
}

func phaseKey(id types.ExecutionID, unitIdx int) string {
	return fmt.Sprintf("%s/%d", id, unitIdx)
}

func (f *groupPhaseFakeState) GetGroupUnitPhase(_ context.Context, id types.ExecutionID, unitIdx int) (GroupUnitPhase, bool, error) {
	phase, ok := f.phases[phaseKey(id, unitIdx)]
	return phase, ok, nil
}

// inspectGroupGraph compiles the group-only workflow every case below inspects:
// one co-location group {ingest -> analyze}, in the given mode.
func inspectGroupGraph(t *testing.T, mode string) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "inspect-group",
		Nodes: []types.NodeDef{
			{Name: "ingest", Type: "test.echo", Kind: types.NodeKindTrigger},
			{Name: "analyze", Type: "test.echo", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"ingest": {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}, Mode: mode}},
	})
	if err != nil {
		t.Fatalf("compile group graph: %v", err)
	}
	return g
}

func nodeDetailByName(t *testing.T, detail ExecutionDetail, name string) NodeDetail {
	t.Helper()
	for _, n := range detail.Nodes {
		if n.Name == name {
			return n
		}
	}
	t.Fatalf("node %q missing from inspect detail %+v", name, detail.Nodes)
	return NodeDetail{}
}

// TestInspectReportsExpiredTransientGroupMember is the case groups[].mode:
// transient adds to Inspect: the execution is terminal, the group's unit record
// is gone (its completion TTL lapsed), and the member must not be reported as
// "pending" — the one reading the mode makes false, since the group either ran
// or its records are the reason nothing is readable.
func TestInspectReportsExpiredTransientGroupMember(t *testing.T) {
	ctx := context.Background()
	state := newGroupPhaseFakeState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("exec-expired-group")
	g := inspectGroupGraph(t, types.GroupModeTransient)

	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusSuccess, Graph: g}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	for _, name := range []string{"ingest", "analyze"} {
		if got := nodeDetailByName(t, detail, name).Status; got != types.NodeStatusExpired {
			t.Errorf("node %q status = %q, want %q: a terminal execution plus a transient group "+
				"with no unit record means the artifacts expired", name, got, types.NodeStatusExpired)
		}
	}
}

// TestInspectKeepsPendingWhileExecutionRuns is the other side of the same
// absence: nothing shortens a transient group's keys before the execution is
// terminal, so a member with no record during a live run is simply not started
// yet, and "expired" would be the false claim in that direction.
func TestInspectKeepsPendingWhileExecutionRuns(t *testing.T) {
	ctx := context.Background()
	state := newGroupPhaseFakeState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("exec-running-group")
	g := inspectGroupGraph(t, types.GroupModeTransient)

	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusRunning, Graph: g}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if got := nodeDetailByName(t, detail, "analyze").Status; got != types.NodeStatusPending {
		t.Errorf("node status = %q, want %q while the execution runs", got, types.NodeStatusPending)
	}
}

// TestInspectShowsTransientGroupMemberWithinRetention is the "正常展示" half:
// while the unit's record is still there, Inspect reports the ordinary readback
// — the boundary exit output — and does not rewrite the status to expired.
func TestInspectShowsTransientGroupMemberWithinRetention(t *testing.T) {
	ctx := context.Background()
	state := newGroupPhaseFakeState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("exec-live-group")
	g := inspectGroupGraph(t, types.GroupModeTransient)

	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusSuccess, Graph: g}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	state.phases[phaseKey(id, g.Groups()[0].UnitIdx)] = GroupUnitPhaseDone
	if err := state.PutOutput(ctx, id, "analyze", map[string]any{"k": "v"}); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	node := nodeDetailByName(t, detail, "analyze")
	if node.Status == types.NodeStatusExpired {
		t.Fatalf("node status = expired while the unit record and its exit are still readable")
	}
	if node.Output["k"] != "v" {
		t.Fatalf("node output = %+v, want the committed exit value", node.Output)
	}
}

// TestInspectDoesNotExpireDurableGroupMember is the negative control: a durable
// group's artifacts live as long as the execution's, so an absent node record
// there is the pre-existing group-readback gap and must not be relabeled as the
// transient mode's doing.
func TestInspectDoesNotExpireDurableGroupMember(t *testing.T) {
	ctx := context.Background()
	state := newGroupPhaseFakeState()
	eng := New(state, &fakeQueue{})
	id := types.ExecutionID("exec-durable-group")
	g := inspectGroupGraph(t, types.GroupModeDurable)

	if err := state.CreateExecution(ctx, &ExecutionSnapshot{ID: id, Status: types.ExecutionStatusSuccess, Graph: g}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if got := nodeDetailByName(t, detail, "analyze").Status; got != types.NodeStatusPending {
		t.Errorf("durable group member status = %q, want %q: the transient expiry verdict "+
			"must not apply to a durable group", got, types.NodeStatusPending)
	}
}
