package graph

import (
	"fmt"

	"github.com/xbcio/xflow/types"
)

// On-others policies of an xflow.merge node in wait_any mode. An absent, nil or
// empty on_others means onOthersCancel, the Descriptor default.
const (
	onOthersCancel = "cancel"
	onOthersDetach = "detach"
)

// mergeOnOthers reads the on_others policy out of a merge node's parameters.
// explicit reports whether the author wrote a non-empty value. ok is false when
// the value is neither of the two policies (wrong type, unknown string, or a
// template, which cannot be resolved without running the node).
func mergeOnOthers(params map[string]any) (policy string, explicit, ok bool) {
	raw, present := params["on_others"]
	if !present || raw == nil {
		return onOthersCancel, false, true
	}
	s, isString := raw.(string)
	if !isString {
		return "", true, false
	}
	switch s {
	case "":
		return onOthersCancel, false, true
	case onOthersCancel, onOthersDetach:
		return s, true, true
	}
	return "", true, false
}

// validateMergeOnOthers rejects an on_others value the engine cannot honour.
// Only a wait_any merge reads the parameter (the Descriptor hides it for every
// other mode), so any other node is left alone. A template is rejected too:
// the policy decides graph-level behaviour and is read at compile time, before
// any template could be evaluated. The message names the parameter and the
// rule, never the value.
func validateMergeOnOthers(nd types.NodeDef) error {
	if extractMergeMode(nd) != "wait_any" {
		return nil
	}
	if _, _, ok := mergeOnOthers(nd.Parameters); !ok {
		return fmt.Errorf("node %q: on_others must be the literal string %q or %q",
			nd.Name, onOthersCancel, onOthersDetach)
	}
	return nil
}

// warnCyclicMergeCancel reports a wait_any merge of a cyclic graph that asks
// for on_others: cancel. Cyclic graphs have no per-execution unit counter for a
// cancellation to settle, so the request is ignored and the merge behaves as
// detach (see MergeCancelsOthers).
func warnCyclicMergeCancel(g *Graph) {
	if !g.allowCycles {
		return
	}
	for i := range g.nodes {
		n := &g.nodes[i]
		if n.MergeMode != "wait_any" {
			continue
		}
		if policy, explicit, ok := mergeOnOthers(n.Parameters); ok && explicit && policy == onOthersCancel {
			g.addWarning(fmt.Sprintf(
				"node %q: allow_cycles 工作流不支持 on_others: cancel，该节点按 detach 处理（其余分支继续运行）",
				n.Name))
		}
	}
}

// MergeCancelsOthers reports whether the node at nodeIdx is an xflow.merge in
// wait_any mode whose on_others resolves to cancel, so that firing it must
// cancel the branches that have not finished. An absent or empty on_others is
// cancel.
//
// It is false for every other node, for wait_all, for on_others: detach, for an
// out-of-range index, and for every node of a cyclic graph (allow_cycles),
// where on_others: cancel degrades to detach. A value that is not one of the
// two policies is false as well: Compile rejects it, so only a snapshot written
// by an older compiler can still carry one, and that keeps its old behaviour.
func (g *Graph) MergeCancelsOthers(nodeIdx int) bool {
	if g.allowCycles || nodeIdx < 0 || nodeIdx >= len(g.nodes) {
		return false
	}
	n := &g.nodes[nodeIdx]
	if n.MergeMode != "wait_any" {
		return false
	}
	policy, _, ok := mergeOnOthers(n.Parameters)
	return ok && policy == onOthersCancel
}

// CancelClosure returns every node that can reach nodeIdx through data edges,
// without nodeIdx itself: the nodes whose unfinished work a cancelling merge
// may have to abandon. The result has no duplicates and is in topological
// order, every node after all of its own ancestors, so a caller that walks it
// front to back settles a node's ancestors before the node.
//
// It is nil for a node without ancestors, for an out-of-range index, and for a
// cyclic graph (allow_cycles). The graph is immutable, so the result is a pure
// function of nodeIdx; the returned slice belongs to the caller.
func (g *Graph) CancelClosure(nodeIdx int) []int {
	if g.allowCycles || nodeIdx < 0 || nodeIdx >= len(g.nodes) {
		return nil
	}
	type frame struct{ node, next int }
	var out []int
	seen := make([]bool, len(g.nodes))
	seen[nodeIdx] = true
	stack := []frame{{node: nodeIdx}}
	for len(stack) > 0 {
		top := &stack[len(stack)-1]
		edges := g.inEdges[top.node]
		if top.next < len(edges) {
			src := edges[top.next].SrcIdx
			top.next++
			if !seen[src] {
				seen[src] = true
				stack = append(stack, frame{node: src})
			}
			continue
		}
		if top.node != nodeIdx {
			out = append(out, top.node)
		}
		stack = stack[:len(stack)-1]
	}
	return out
}
