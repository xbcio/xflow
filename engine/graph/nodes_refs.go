package graph

import (
	"fmt"
	"sort"
)

// deriveNodesRefs extracts node references ($nodes['name']) from a node's
// parameters, SKIPPING the body sub-graph if present.
//
// It determines which nodes the OUTER graph must prefetch at runtime. A
// reference inside a body sub-graph resolves against the INNER graph's state,
// not the outer one, so attributing it to the outer node causes two bugs:
//   - Compile-time: the outer graph's index may not contain the inner node
//     name, falsely rejecting a legitimate workflow.
//   - Runtime: buildInput would call GetOutput for a name that only exists in
//     the sub-execution's state, getting nil and silently corrupting $nodes.
//
// The body-skipping itself lives in walkParamsForRefs (group_portability.go),
// shared with extractNodeRefs. This function differs from that one only in what
// it does with the result -- prefetch set versus portability check -- not in
// which references it sees. They used to differ in both, and the divergence was
// the compile-time bug above, reached through the group path: a map node in a
// group was rejected for "referencing" its own body's members.
func deriveNodesRefs(params map[string]any) []string {
	if len(params) == 0 {
		return nil
	}
	seen := map[string]bool{}
	walkParamsForRefs(params, seen)
	if len(seen) == 0 {
		return nil
	}
	refs := make([]string, 0, len(seen))
	for name := range seen {
		refs = append(refs, name)
	}
	sort.Strings(refs)
	return refs
}

// buildNodesRefs is the compile-time pass that extracts $nodes references and
// validates them against the graph topology. It populates g.nodesRefs.
//
// Placement rationale: this pass runs AFTER detectCycle / the allowCycles
// branch. In DAG mode detectCycle guarantees acyclicity, so the reachability
// analysis below can assume a DAG without its own cycle guard. In cyclic mode
// (allowCycles=true) detectCycle is skipped, so the graph may contain cycles;
// however the visited-set in isReachable prevents infinite loops regardless.
// Placing the pass after the cycle check (rather than before) means we inherit
// the DAG guarantee in the common case rather than relying solely on defensive
// code — but the defensive code is still present for the cyclic case.
//
// compileTrusted rationale: this pass IS hooked into compileTrusted (the
// projected-package compilation path for group members). validatePortability
// already guarantees that a group member's $nodes references point only to
// other members of the same group, so existence checks always pass. However,
// the forward-reference check is still valuable (a member referencing a
// downstream peer in the projected graph should still be rejected). Cross-
// branch warnings are SKIPPED in compileTrusted because the projected graph's
// topology is a sub-graph of the outer graph: a reference that is cross-branch
// in the projection may be a deterministic ancestor in the outer graph. Warning
// in the projection would produce noise that confuses the workflow author (they
// didn't write a cross-branch reference; the projection created the appearance
// of one).
//
// visibleOuterNodes is non-empty only for a BODY package, where the same
// "references point only to members" guarantee does NOT hold: a body member may
// read the map node's upstream ancestors, whose names live in the outer graph
// alone. Those names are exempted from the existence check and from every
// topology check below -- the projected graph has no opinion about a node it
// does not contain, and the outer compilation already adjudicated them in
// validateBodyOuterRefs against the topology that can answer. They are also
// left OUT of g.nodesRefs: that set drives the inner engine's prefetch, which
// reads the inner execution's state store, where an outer node has no output.
// The outer snapshot reaches the members through the execution scope instead.
func buildNodesRefs(g *Graph, skipCrossBranchWarning bool, visibleOuterNodes []string) error {
	outer := make(map[string]bool, len(visibleOuterNodes))
	for _, name := range visibleOuterNodes {
		outer[name] = true
	}
	if g.nodesRefs == nil {
		g.nodesRefs = make(map[int][]string)
	}
	for i := range g.nodes {
		// Skip grouped nodes in the outer graph: their $nodes references
		// resolve within the projected package's internal scheduling, not
		// the outer graph's topology. Forward/backward relationships inside
		// a group are meaningful only within the group's own package — which
		// compileTrusted validates separately. Validating them here against
		// the OUTER graph's edges would falsely reject legal intra-group
		// references (e.g. a group member referencing a downstream peer that
		// the group's own scheduler will execute first).
		if g.nodes[i].GroupIdx >= 0 {
			continue
		}
		// A computed subscript ($nodes[$vars.which]) matches no literal, so it
		// contributes nothing to the prefetch set and reads nil at runtime with
		// no diagnostic. Warn rather than reject, unlike the $supplies rule in
		// dependency.go: a missing supply fails the node, but a $nodes miss
		// returns nil, which the spec's ?? guard is meant to absorb.
		//
		// Placed after the grouped-node skip so a group member warns once, in
		// its own projected package's compileTrusted pass, rather than twice.
		if hasDynamicNodesRef(g.nodes[i].Parameters) {
			g.addWarning(fmt.Sprintf("node %q: $nodes 下标不是字面量，该引用不会被预取，运行期恒为 nil",
				g.nodes[i].Name))
		}
		refs := deriveNodesRefs(g.nodes[i].Parameters)
		if len(refs) == 0 {
			continue
		}
		local := refs
		if len(outer) > 0 {
			local = make([]string, 0, len(refs))
			for _, ref := range refs {
				if !outer[ref] {
					local = append(local, ref)
				}
			}
		}
		if len(local) == 0 {
			continue
		}
		g.nodesRefs[i] = local
		for _, ref := range local {
			targetIdx, ok := g.index[ref]
			if !ok {
				return fmt.Errorf("node %q: $nodes reference %q does not exist in the workflow definition",
					g.nodes[i].Name, ref)
			}
			if targetIdx == i {
				return fmt.Errorf("node %q: $nodes reference %q is a self-reference (forward reference)",
					g.nodes[i].Name, ref)
			}
			// Forward reference: target is reachable FROM i (i.e. downstream).
			if isReachable(g, i, targetIdx) {
				return fmt.Errorf("node %q: $nodes reference %q is a forward reference (references a downstream node)",
					g.nodes[i].Name, ref)
			}
			// Deterministic ancestor: target dominates i — every path from a
			// root to i passes through target. Equivalently: removing target
			// makes i unreachable from all roots.
			if skipCrossBranchWarning {
				continue
			}
			if !isDeterministicAncestor(g, targetIdx, i) {
				g.addWarning(fmt.Sprintf("node %q: $nodes['%s'] 可能为 nil，建议使用 ?? 提供默认值",
					g.nodes[i].Name, ref))
			}
		}
	}
	return nil
}

// isReachable returns true if dst is reachable from src via outEdges (BFS).
// Uses a visited set to handle cyclic graphs safely.
func isReachable(g *Graph, src, dst int) bool {
	visited := make([]bool, len(g.nodes))
	queue := []int{src}
	visited[src] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.outEdges[cur] {
			if e.DstIdx == dst {
				return true
			}
			if !visited[e.DstIdx] {
				visited[e.DstIdx] = true
				queue = append(queue, e.DstIdx)
			}
		}
	}
	return false
}

// isDeterministicAncestor returns true if target dominates nodeIdx: every path
// from any in-degree-0 root to nodeIdx passes through target. Equivalently,
// removing target (and its out-edges) from the graph makes nodeIdx unreachable
// from all roots.
func isDeterministicAncestor(g *Graph, target, nodeIdx int) bool {
	n := len(g.nodes)
	// BFS from all roots, excluding target and its out-edges.
	visited := make([]bool, n)
	visited[target] = true // "remove" target from the graph
	var queue []int
	for i := 0; i < n; i++ {
		if len(g.inEdges[i]) == 0 && i != target {
			queue = append(queue, i)
			visited[i] = true
		}
	}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		if cur == nodeIdx {
			// nodeIdx is reachable without passing through target → not a
			// deterministic ancestor.
			return false
		}
		for _, e := range g.outEdges[cur] {
			if !visited[e.DstIdx] {
				visited[e.DstIdx] = true
				queue = append(queue, e.DstIdx)
			}
		}
	}
	// nodeIdx was not reached → target is required on every path → dominates.
	return true
}
