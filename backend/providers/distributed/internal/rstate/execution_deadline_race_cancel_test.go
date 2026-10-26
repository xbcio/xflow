package rstate

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// submitParkedParent submits a parent whose only node is a sub-workflow node and
// drives that node far enough to start its child, so the parent is parked on a
// child that is really running. The rig has no queue consumer, so the sub-
// workflow task is handed to the engine's system-task handler by hand, which is
// what a queue worker does with it.
func (r *deadlineRaceRig) submitParkedParent(ctx context.Context, parentGraph *graph.Graph) (parent, child types.ExecutionID) {
	r.t.Helper()
	parent = r.submit(ctx, parentGraph)
	tasks := r.queue.take(parent)
	if len(tasks) != 1 || tasks[0].Type != engine.TaskTypeSubWorkflow {
		r.t.Fatalf("parent %s queued %+v, want exactly its sub-workflow task", parent, tasks)
	}
	if handled, err := r.eng.HandleSystemTask(ctx, tasks[0]); err != nil || !handled {
		r.t.Fatalf("HandleSystemTask(sub-workflow) = (%v, %v), want handled", handled, err)
	}
	link, found, err := r.state.GetSubWorkflowLink(ctx, parent, "sub")
	if err != nil || !found {
		r.t.Fatalf("GetSubWorkflowLink() = (%+v, %v, %v), want the node parked on its child", link, found, err)
	}
	if got := r.snapshot(ctx, link.ChildExecutionID).Status; got != types.ExecutionStatusRunning {
		r.t.Fatalf("child before the race = %q, want running", got)
	}
	if got := r.snapshot(ctx, parent).Status; got != types.ExecutionStatusRunning {
		r.t.Fatalf("parent before the race = %q, want running: the deadline must be reached by the race, not before it", got)
	}
	return parent, link.ChildExecutionID
}

// raceParentGraph compiles the parent of the sub-workflow races: a workflow
// holding settings.timeout whose only node runs a child that never finishes by
// itself.
func raceParentGraph(t *testing.T) (*graph.Graph, deadlineChildResolver) {
	t.Helper()
	child, err := graph.Compile(&types.WorkflowDef{
		Name:    "deadline-race-child",
		Version: "v1",
		Nodes:   []types.NodeDef{{Name: "work", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile(child) error = %v", err)
	}
	resolver := deadlineChildResolver{target: graph.SubWorkflowTarget{
		Version: "v1", DefinitionHash: "sha256:child", Graph: child,
	}}
	parent, err := graph.CompileWithResolver(context.Background(), &types.WorkflowDef{
		Name:     "deadline-race-parent",
		Version:  "v1",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: raceTimeout},
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: map[string]any{"workflow": "child"},
		}},
	}, resolver)
	if err != nil {
		t.Fatalf("CompileWithResolver(parent) error = %v", err)
	}
	return parent, resolver
}

// TestExpireRacingCancelLeavesOneTerminalStateAndNoLiveChildOnRedis replays the
// race between the deadline sweep and a user cancel on a parent that is parked
// on a running child, on a real Engine over the Redis store. Whichever side
// reaches the execution first owns it: the other must back off without leaving
// the execution in canceling, without leaving the deadline entry behind, and
// without leaving the child running.
func TestExpireRacingCancelLeavesOneTerminalStateAndNoLiveChildOnRedis(t *testing.T) {
	ctx := context.Background()
	var canceled, timedOut int
	for round := 0; round < raceRounds(); round++ {
		parentGraph, resolver := raceParentGraph(t)
		rig := newDeadlineRaceRig(t, engine.WithSubWorkflowResolver(resolver))
		parentID, childID := rig.submitParkedParent(ctx, parentGraph)

		var cancelErr, expireErr error
		var outcome engine.ExpireOutcome
		raceTogether(
			func() { cancelErr = rig.eng.Cancel(ctx, parentID) },
			func() { outcome, expireErr = rig.eng.ExpireExecution(ctx, parentID) },
		)
		// A cancel that arrives once the expiry has already concluded the
		// execution is refused as inactive; that is the expiry winning, not a
		// failure.
		if cancelErr != nil && !errors.Is(cancelErr, engine.ErrExecutionInactive) {
			t.Fatalf("round %d: Cancel() error = %v", round, cancelErr)
		}
		if expireErr != nil {
			t.Fatalf("round %d: ExpireExecution() error = %v", round, expireErr)
		}

		snap := rig.snapshot(ctx, parentID)
		switch snap.Status {
		case types.ExecutionStatusCanceled:
			canceled++
			if snap.Error == engine.ExecutionDeadlineExceededError {
				t.Fatalf("round %d: canceled execution carries the timeout reason", round)
			}
			if outcome != engine.ExpireOutcomeCanceling && outcome != engine.ExpireOutcomeAlreadyTerminal {
				t.Fatalf("round %d: the cancel won but ExpireExecution reported %q", round, outcome)
			}
		case types.ExecutionStatusTimeout:
			timedOut++
			if snap.Error != engine.ExecutionDeadlineExceededError {
				t.Fatalf("round %d: timeout reason = %q, want %q", round, snap.Error, engine.ExecutionDeadlineExceededError)
			}
			if outcome != engine.ExpireOutcomeExpired {
				t.Fatalf("round %d: the expiry won but ExpireExecution reported %q, want expired", round, outcome)
			}
		default:
			t.Fatalf("round %d: parent ended %q, want canceled or timeout (canceling must not outlive a finished cancel)", round, snap.Status)
		}
		// Whoever won announced the end; a loser may announce too in the narrow
		// window two concurrent enders share, but nothing outside the two
		// candidates may ever appear.
		announced := rig.hooks.completionsOf(parentID)
		sawFinal := false
		for _, status := range announced {
			if status != types.ExecutionStatusCanceled && status != types.ExecutionStatusTimeout {
				t.Fatalf("round %d: parent announced %v, want only canceled or timeout", round, announced)
			}
			sawFinal = sawFinal || status == snap.Status
		}
		if !sawFinal {
			t.Fatalf("round %d: parent ended %q but its completion was announced as %v", round, snap.Status, announced)
		}

		if got := rig.snapshot(ctx, childID).Status; got != types.ExecutionStatusCanceled {
			t.Fatalf("round %d: child = %q after the parent ended %q, want canceled", round, got, snap.Status)
		}
		node, err := rig.state.GetNode(ctx, parentID, "sub")
		if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
			t.Fatalf("round %d: parked sub-workflow node = (%+v, %v), want canceled", round, node, err)
		}
		rig.requireDrained(round, parentGraph.Name(), parentID, childID)
		rig.close()
	}
	t.Logf("%d rounds: %d ended canceled, %d ended timeout", raceRounds(), canceled, timedOut)
}
