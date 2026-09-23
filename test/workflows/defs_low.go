package workflows

import (
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/sdk/xflow"
)

// LowWorkflow is the low-complexity tier: a linear chain with one conditional
// branch that is decided entirely from the invocation input, closed by a
// wait-any merge.
//
//	start → seed → tag → branch ─true→  big   ─┐
//	                             └false→ small ─┴→ join → done
//
// The two branch arms are mutually exclusive by construction, so the join with
// MergeWaitAny is what makes either one sufficient to release the end node. With
// WaitAll the workflow would deadlock on the arm the condition did not take —
// that is the property this tier exists to exercise.
//
// Node types: xflow.start, xflow.function, xflow.transform.set, xflow.if,
// xflow.transform.pick, xflow.merge, xflow.end.
//
// Input: `amount` (number) decides the branch — AboveThresholdInput takes the
// true arm, BelowThresholdInput the false one. Runtime vars: none.
func LowWorkflow() *xflow.WorkflowBuilder {
	wf := xflow.Workflow("low-tier")
	start := wf.Node("start", node.Start())
	seed := wf.Node("seed", node.Expr(`{"amount": $input.amount, "rows": [10, 20, 30]}`))
	tag := wf.Node("tag", node.Set(map[string]any{"tier": "low"}).
		SetExpr(map[string]string{"double_amount": "$input.amount * 2"}))
	branch := wf.Node("branch", node.IF(BranchCondition()))
	big := wf.Node("big", node.Pick("amount", "tier", "double_amount"))
	small := wf.Node("small", node.Pick("amount", "tier"))
	join := wf.Node("join", node.Merge(node.MergeWaitAny))
	done := wf.Node("done", node.End())

	wf.Connect(start, seed).
		Connect(seed.Output("main"), tag).
		Connect(tag.Output("main"), branch).
		Connect(branch.Output("true"), big).
		Connect(branch.Output("false"), small).
		Connect(big.Output("main"), join).
		Connect(small.Output("main"), join).
		Connect(join.Output("main"), done)
	return wf
}
