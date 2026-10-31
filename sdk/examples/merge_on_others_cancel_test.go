package examples_test

import (
	"context"
	"strings"
	"testing"
	"time"

	enginecore "github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
	"github.com/xbcio/xflow/types"
)

const (
	mergeCancelGateA  = "MergeCancelGateA"
	mergeCancelGateB  = "MergeCancelGateB"
	mergeCancelWinner = "MergeCancelWinner"
	mergeCancelTail   = "MergeCancelTail"
	mergeCancelJoin   = "MergeCancelJoin"
	mergeCancelAfter  = "MergeCancelAfter"
)

// mergeWithParams overrides the merge builder's raw parameters. MergeNode's own
// RawParams emits only the mode and relies on an absent on_others meaning
// cancel, so this is the only way to express on_others: detach — or to state
// cancel explicitly — from the SDK.
type mergeWithParams struct {
	*node.MergeNode
	params map[string]any
}

func (b mergeWithParams) RawParams() any { return b.params }

// buildMergeCancelWorkflow builds
//
//	      start
//	    /   |   \
//	winner gateA gateB
//	          |     |
//	         tail   |
//	    \     |    /
//	      join (wait_any)
//	         |
//	       after
//
// The winner's handler blocks on release, so the merge cannot fire before both
// gates are parked; that ordering is what makes the cancellation observable.
func buildMergeCancelWorkflow(merge types.Builder, release <-chan struct{}) *xflow.WorkflowBuilder {
	winnerNode := node.Define("examples.merge_cancel.winner",
		func(ctx context.Context, _ *types.Input) (*types.Output, error) {
			select {
			case <-release:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			return &types.Output{Data: map[string]any{"winner": true}}, nil
		},
	)

	wf := xflow.Workflow("merge-on-others-cancel")
	start := wf.Node("start", node.Start())
	winner := wf.Node(mergeCancelWinner, winnerNode.New(nil))
	gateA := wf.Node(mergeCancelGateA, node.Approval([]string{"alice"}, node.ApprovalAll))
	gateB := wf.Node(mergeCancelGateB, node.Approval([]string{"bob"}, node.ApprovalAll))
	tail := wf.Node(mergeCancelTail, node.Set(map[string]any{"from": "gate-tail"}))
	join := wf.Node(mergeCancelJoin, merge)
	after := wf.Node(mergeCancelAfter, node.Set(map[string]any{"after": true}))
	wf.Connect(start, winner).
		Connect(start, gateA).
		Connect(start, gateB).
		Connect(gateA, tail).
		Connect(tail, join).
		Connect(gateB, join).
		Connect(winner, join).
		Connect(join, after)
	return wf
}

// TestMergeOnOthersCancelConverges is the end-to-end cancel case over the real
// local engine. The merge carries no on_others parameter, so this also pins the
// default: an absent policy is cancel. Both parked gates are retired, the
// skipped branch behind one gate never runs, the winner and the merge's
// successor do, and the execution converges on success instead of waiting for
// approvals that will never come.
func TestMergeOnOthersCancelConverges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hooks := newSuspensionHooks()
	eng, err := xflow.NewLocal(xflow.WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	release := make(chan struct{})
	wf := buildMergeCancelWorkflow(node.Merge(node.MergeWaitAny), release)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, xflow.EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	// Both gates must be parked before the winner commits. Without this the
	// merge could fire while a gate is still pending, and the cancellation would
	// be tested against a branch that never suspended.
	hooks.waitForSuspensions(t, ctx, mergeCancelGateA, 1)
	hooks.waitForSuspensions(t, ctx, mergeCancelGateB, 1)
	close(release)

	result, err := eng.Wait(ctx, id)
	if err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %q, want success; workflow error: %s", result.Status, result.Error)
	}

	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	wantStatus := map[string]types.NodeStatus{
		mergeCancelGateA:  types.NodeStatusCanceled,
		mergeCancelGateB:  types.NodeStatusCanceled,
		mergeCancelTail:   types.NodeStatusSkipped,
		mergeCancelWinner: types.NodeStatusSuccess,
		mergeCancelJoin:   types.NodeStatusSuccess,
		mergeCancelAfter:  types.NodeStatusSuccess,
	}
	for name, want := range wantStatus {
		if got := mergeCancelNode(detail, name).Status; got != want {
			t.Fatalf("%s status = %q, want %q", name, got, want)
		}
	}
	if got := mergeCancelNode(detail, mergeCancelGateA).Error; got != "canceled by merge "+mergeCancelJoin {
		t.Fatalf("%s error = %q, want the merge-cancel cause", mergeCancelGateA, got)
	}
	mustNodeOutput(t, result, mergeCancelWinner)
	mustNodeOutput(t, result, mergeCancelAfter)

	// The eliminated branch's successor is skipped, not run, and never appears
	// in the outputs.
	if _, ok := result.Output[mergeCancelTail]; ok {
		t.Fatalf("skipped node %s reported an output", mergeCancelTail)
	}
	if got := hooks.count(mergeCancelGateA); got != 1 {
		t.Fatalf("%s suspended %d times, want exactly 1", mergeCancelGateA, got)
	}
}

// TestMergeOnOthersDetachLeavesBranchesParked is the on_others: detach variant:
// the merge fires, its successor runs, but the parked branches are left alone
// and the execution does not converge.
func TestMergeOnOthersDetachLeavesBranchesParked(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	hooks := newSuspensionHooks()
	eng, err := xflow.NewLocal(xflow.WithHooks(hooks))
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()

	release := make(chan struct{})
	detach := mergeWithParams{
		MergeNode: node.Merge(node.MergeWaitAny),
		params:    map[string]any{"mode": "wait_any", "on_others": "detach"},
	}
	wf := buildMergeCancelWorkflow(detach, release)

	workflowID, err := eng.AddWorkflow(ctx, wf)
	if err != nil {
		t.Fatalf("AddWorkflow() error = %v", err)
	}
	id, err := eng.Invoke(ctx, workflowID, xflow.EntryNode("start"), nil)
	if err != nil {
		t.Fatalf("Invoke() error = %v", err)
	}

	hooks.waitForSuspensions(t, ctx, mergeCancelGateA, 1)
	hooks.waitForSuspensions(t, ctx, mergeCancelGateB, 1)
	close(release)

	// The merge and its successor still complete; only the parked branches are
	// left behind, so the execution cannot finish.
	waitForMergeCancelNode(t, ctx, eng, id, mergeCancelAfter, types.NodeStatusSuccess)
	detail, err := eng.Inspect(ctx, id)
	if err != nil {
		t.Fatalf("Inspect() error = %v", err)
	}
	if detail.Status != types.ExecutionStatusRunning {
		t.Fatalf("execution = %q, want running: detach leaves the parked branches", detail.Status)
	}
	for _, name := range []string{mergeCancelGateA, mergeCancelGateB} {
		if got := mergeCancelNode(detail, name).Status; got != types.NodeStatusSuspended {
			t.Fatalf("%s status = %q, want suspended: detach must not cancel it", name, got)
		}
	}

	// Release the run: the parked gates would otherwise outlive the test.
	if err := eng.Cancel(ctx, id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
}

// TestMergeOnOthersCancelDegradesInCyclicGraphs pins the compile-time degrade
// on the SDK path: a cyclic workflow that explicitly asks for on_others: cancel
// compiles with a warning, resolves to detach, and is still accepted by
// AddWorkflow. The runtime detach behaviour is covered by the test above; this
// one proves the builder carries the parameter into the compiled graph and that
// the cyclic guard overrides it there.
func TestMergeOnOthersCancelDegradesInCyclicGraphs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	wf := xflow.Workflow("merge-on-others-cyclic").AllowCycles(10)
	start := wf.Node("start", node.Start())
	a := wf.Node("a", node.Set(map[string]any{"step": "a"}))
	b := wf.Node("b", node.Set(map[string]any{"step": "b"}))
	join := wf.Node("join", mergeWithParams{
		MergeNode: node.Merge(node.MergeWaitAny),
		params:    map[string]any{"mode": "wait_any", "on_others": "cancel"},
	})
	after := wf.Node("after", node.Set(map[string]any{"step": "after"}))
	wf.Connect(start, a).
		Connect(start, b).
		Connect(a, join).
		Connect(b, join).
		Connect(join, after).
		Connect(after, a) // the back edge makes the graph cyclic

	def, err := wf.Definition()
	if err != nil {
		t.Fatalf("Definition() error = %v", err)
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	idx, ok := g.NodeIndex("join")
	if !ok {
		t.Fatal("compiled graph has no join node")
	}
	if g.MergeCancelsOthers(idx) {
		t.Fatal("MergeCancelsOthers(join) = true in a cyclic graph, want false: cancel degrades to detach")
	}
	warnings := g.Warnings()
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "on_others") && strings.Contains(w, `"join"`) {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %q, want one naming join and on_others", warnings)
	}

	eng, err := xflow.NewLocal()
	if err != nil {
		t.Fatalf("NewLocal() error = %v", err)
	}
	defer eng.Stop()
	if _, err := eng.AddWorkflow(ctx, wf); err != nil {
		t.Fatalf("AddWorkflow() error = %v, want the degraded definition accepted", err)
	}
}

func mergeCancelNode(detail enginecore.ExecutionDetail, name string) enginecore.NodeDetail {
	for _, n := range detail.Nodes {
		if n.Name == name {
			return n
		}
	}
	return enginecore.NodeDetail{Name: name, Status: "missing"}
}

// waitForMergeCancelNode polls Inspect until the node reaches want; ctx bounds
// the wait, so a state that never arrives fails instead of parking the test.
func waitForMergeCancelNode(t *testing.T, ctx context.Context, eng *xflow.Engine, id types.ExecutionID, name string, want types.NodeStatus) {
	t.Helper()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		detail, err := eng.Inspect(ctx, id, name)
		if err != nil {
			t.Fatalf("Inspect(%s) error = %v", name, err)
		}
		if len(detail.Nodes) == 1 && detail.Nodes[0].Status == want {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s to reach %q; last nodes: %+v", name, want, detail.Nodes)
		case <-ticker.C:
		}
	}
}
