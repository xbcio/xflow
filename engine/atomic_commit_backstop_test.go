package engine

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestAcyclicCommitBackstopsANilOutputExpandingNode pins the backstop at
// engine/atomic_commit.go:52, which today has zero test coverage even though
// it is reachable in production.
//
// How it is reached, traced against the current source rather than assumed:
//
//  1. "m" is a map node with a projected body (mapBodyParamsForTest gives it
//     one), so expandsIntoSubExecutions(g, m) — g.BodyAt(idx) != nil — is true
//     for it (engine/expand.go:30).
//  2. The runner reports back TaskResult{} — Output == nil, Error == nil. This
//     is not a contrived shape: execution/runner.go:326 assigns whatever
//     handler.Execute returns straight to Output, and a handler returning
//     (nil, nil) produces exactly this.
//  3. taskResultExpands (engine/expand.go:47) checks result.Output == nil
//     first and returns false before it ever consults the graph — a nil
//     Output can never be an expansion descriptor to decode, so it is treated
//     as "does not expand" regardless of what the node's body says.
//  4. Back in CommitTaskResultWithOutcome (engine/commit.go:56),
//     fencedByCommitNode = !AllowCycles() && Suspend==nil && !taskResultExpands
//     evaluates to true: the graph is acyclic, there is no suspend, and step 3
//     just supplied the "does not expand" half.
//  5. fencedByCommitNode routes to commitAcyclicTaskResult (engine/commit.go:76).
//  6. Inside it (engine/atomic_commit.go:16), the result is neither an error
//     (Error and Output are both nil, so the line 20 check is false) nor a
//     retry-port output (outputPortRetryError on a nil Output returns nil), so
//     control reaches expandsIntoSubExecutions(g, task.NodeIdx) at line 52.
//     THIS call answers true — it reads BodyAt directly off the graph, not off
//     the (already nil) Output that taskResultExpands consulted in step 3. The
//     two calls in steps 3 and 6 read different things (Output shape vs. graph
//     structure), which is exactly how the branch stays reachable: a node that
//     structurally expands can still carry a result that structurally does
//     not, and nothing before line 52 reconciles the mismatch.
//  7. The backstop fires: CommitOutcomeTransientError plus an error naming the
//     node.
//
// What this test asserts is the current behavior, not the correct one. A
// handler returning (nil, nil) for an expanding map node looks like it should
// be an ordinary contract violation (checked once, cleanly, at the runner
// boundary) rather than something that reaches all the way into the atomic
// commit primitive and comes back as "transient" — CommitOutcomeTransientError
// is the label CommitTaskResultWithOutcome's other backend-failure paths use,
// and a transient label invites a caller to retry, but retrying a (nil, nil)
// handler response will produce the identical (nil, nil) forever. Whether that
// is the right classification is exactly the kind of question a merge of
// commitLegacyTaskResult and commitAcyclicTaskResult could accidentally change
// the answer to, which is what this test exists to catch either way.
func TestAcyclicCommitBackstopsANilOutputExpandingNode(t *testing.T) {
	def := &types.WorkflowDef{
		Name:  "nil-output-expanding-map",
		Nodes: []types.NodeDef{{Name: "m", Type: "xflow.map", Parameters: mapBodyParamsForTest()}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("node \"m\" not found in compiled graph")
	}
	if g.BodyAt(idx) == nil {
		t.Fatal("premise broken: the map node declares a body but none was projected")
	}

	state := newFakeState()
	queue := &fakeQueue{}
	eng := New(state, queue)
	ctx := context.Background()

	if _, err := eng.Submit(ctx, g, nil); err != nil {
		t.Fatalf("submit: %v", err)
	}
	roots := queue.Drain()
	if len(roots) != 1 || roots[0].NodeName != "m" {
		t.Fatalf("root tasks = %v, want one \"m\"", taskNames(roots))
	}

	lease, err := eng.BuildTaskLease(ctx, roots[0])
	if err != nil || lease == nil {
		t.Fatalf("BuildTaskLease() = %v, %v", lease, err)
	}

	// The shape that reaches CommitTaskResultWithOutcome for a (nil, nil)
	// handler response: no Output, no Error, no Suspend. Constructed directly
	// rather than via executeTask/a registered handler, because no real
	// handler in this suite returns (nil, nil) — that is exactly the point:
	// the contract violation happens at the runner boundary, not inside a
	// handler this package can register.
	outcome, err := eng.CommitTaskResultWithOutcome(ctx, lease, TaskResult{})

	if outcome != CommitOutcomeTransientError {
		t.Fatalf("outcome = %s, want %s", outcome, CommitOutcomeTransientError)
	}
	if err == nil {
		t.Fatal("err = nil, want the atomic-commit backstop error")
	}
	// Shape only: the node identifier must appear in the error, not the exact
	// wording of atomic_commit.go:52's message — that string is not this
	// test's contract, and pinning it verbatim would make an unrelated wording
	// change fail this test for no reason connected to what it guards.
	if !strings.Contains(err.Error(), "m") {
		t.Errorf("err = %q, want it to name the node (%q)", err.Error(), "m")
	}

	// The node must not have been left mid-commit: nothing here should have
	// written node state, since the backstop fires before any write.
	node, getErr := state.GetNode(ctx, lease.Task.ExecutionID, "m")
	if getErr != nil {
		t.Fatalf("GetNode() error = %v", getErr)
	}
	if node != nil && types.IsTerminalNodeStatus(node.Status) {
		t.Errorf("node status = %q after a backstopped commit, want no terminal write", node.Status)
	}
}
