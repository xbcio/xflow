package rstate

import (
	"context"
	"errors"
	"fmt"
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

// raceParkedNodes is how many independent nodes the cancel race parks at once.
// One is the case every other race covers; the cascade over a set of suspended
// nodes only shows its shape from the second node on, so the number is above
// two.
const raceParkedNodes = 3

// raceParallelParkGraph compiles a workflow holding settings.timeout whose n
// nodes have no connections, so every one of them is an entry node and Submit
// queues a task for each.
func raceParallelParkGraph(t *testing.T, n int) *graph.Graph {
	t.Helper()
	nodes := make([]types.NodeDef, n)
	for i := range nodes {
		nodes[i] = types.NodeDef{Name: fmt.Sprintf("park%d", i), Type: "test.race_park"}
	}
	return raceGraph(t, "deadline-race-parallel-park", 0, nodes...)
}

// parkEveryNode drives each of the execution's n queued entry tasks to a
// suspension on a signal of its own ("go-<node>"), the way parkOnSignal does for
// a single node, and returns the node names in the order they were parked.
func (r *deadlineRaceRig) parkEveryNode(ctx context.Context, id types.ExecutionID, n int) []string {
	r.t.Helper()
	tasks := r.queue.take(id)
	if len(tasks) != n {
		r.t.Fatalf("execution %s has %d queued tasks, want %d", id, len(tasks), n)
	}
	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		lease, err := r.eng.BuildTaskLease(ctx, task)
		if err != nil {
			r.t.Fatalf("BuildTaskLease(%s) error = %v", task.NodeName, err)
		}
		err = r.eng.CommitTaskResult(ctx, lease, engine.TaskResult{
			Suspend: &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go-" + task.NodeName}},
		})
		if err != nil {
			r.t.Fatalf("CommitTaskResult(suspend %s) error = %v", task.NodeName, err)
		}
		names = append(names, task.NodeName)
	}
	if got := r.snapshot(ctx, id).Status; got != types.ExecutionStatusRunning {
		r.t.Fatalf("execution after the parks = %q, want running", got)
	}
	nodes, err := r.state.ListSuspendedNodes(ctx, id)
	if err != nil || len(nodes) != n {
		r.t.Fatalf("ListSuspendedNodes after the parks = (%v, %v), want %d", nodes, err, n)
	}
	return names
}

// requireNodesCanceled fails unless every one of the named nodes ended canceled
// and the execution's suspended set is empty. The signal-named waiter keys are
// not looked at, for the reason drainProblem gives.
func (r *deadlineRaceRig) requireNodesCanceled(ctx context.Context, round int, id types.ExecutionID, names []string) {
	r.t.Helper()
	for _, name := range names {
		node, err := r.state.GetNode(ctx, id, name)
		if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
			r.t.Fatalf("round %d: parked node %s = (%+v, %v), want canceled", round, name, node, err)
		}
	}
	listed, err := r.state.ListSuspendedNodes(ctx, id)
	if err != nil || len(listed) != 0 {
		r.t.Fatalf("round %d: ListSuspendedNodes = (%v, %v), want none left", round, listed, err)
	}
}

// TestExpireRacingCancelStopsEveryParkedNodeOnRedis replays the race between the
// deadline sweep and a user cancel on an execution with raceParkedNodes nodes
// parked on signals, on a real Engine over the Redis store. Whichever side owns
// the execution, its cascade has to reach all of them: a cascade that stopped
// after the first node would leave the others suspended on an execution that is
// already over.
func TestExpireRacingCancelStopsEveryParkedNodeOnRedis(t *testing.T) {
	ctx := context.Background()
	var canceled, timedOut int
	for round := 0; round < raceRounds(); round++ {
		rig := newDeadlineRaceRig(t)
		g := raceParallelParkGraph(t, raceParkedNodes)
		id := rig.submit(ctx, g)
		names := rig.parkEveryNode(ctx, id, raceParkedNodes)

		var cancelErr, expireErr error
		raceTogether(
			func() { cancelErr = rig.eng.Cancel(ctx, id) },
			func() { _, expireErr = rig.eng.ExpireExecution(ctx, id) },
		)
		if cancelErr != nil && !errors.Is(cancelErr, engine.ErrExecutionInactive) {
			t.Fatalf("round %d: Cancel() error = %v", round, cancelErr)
		}
		if expireErr != nil {
			t.Fatalf("round %d: ExpireExecution() error = %v", round, expireErr)
		}

		switch snap := rig.snapshot(ctx, id); snap.Status {
		case types.ExecutionStatusCanceled:
			canceled++
		case types.ExecutionStatusTimeout:
			timedOut++
		default:
			t.Fatalf("round %d: execution ended %q, want canceled or timeout", round, snap.Status)
		}
		rig.requireNodesCanceled(ctx, round, id, names)
		rig.requireDrained(round, g.Name(), id)
		rig.close()
	}
	t.Logf("%d rounds: %d ended canceled, %d ended timeout", raceRounds(), canceled, timedOut)
}

// TestCancelAloneStopsEveryParkedNodeOnRedis is the cancel side of the test
// above with the expiry taken away. The replay lets the expiry win nearly every
// round on Redis, so Cancel's own cascade over several suspended nodes is the
// thing it covers least; here the cancel is the only writer and must reach every
// node by itself.
func TestCancelAloneStopsEveryParkedNodeOnRedis(t *testing.T) {
	ctx := context.Background()
	rig := newDeadlineRaceRig(t)
	g := raceParallelParkGraph(t, raceParkedNodes)
	id := rig.submit(ctx, g)
	names := rig.parkEveryNode(ctx, id, raceParkedNodes)

	if err := rig.eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if snap := rig.snapshot(ctx, id); snap.Status != types.ExecutionStatusCanceled {
		t.Fatalf("execution = %q, want canceled", snap.Status)
	}
	rig.requireNodesCanceled(ctx, 0, id, names)
	rig.requireDrained(0, g.Name(), id)
}
