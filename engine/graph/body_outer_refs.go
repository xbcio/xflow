package graph

import "fmt"

// validateBodyOuterRefs adjudicates every cross-domain $nodes reference a body
// makes, against the OUTER graph's topology. It is the pass that makes
// checkPortability's collectExternal=true safe: that call site stops rejecting
// external references, and this one decides which of them are legal.
//
// The rule is the DSL spec's (docs/design/DSL-SPECIFICATION.md, 跨域引用): a
// body may read the loop node's upstream ancestors -- nodes the DAG's
// topological order puts strictly before the loop -- and nothing else. A
// downstream node has not run when the body executes; an unrelated branch has
// no ordering against it at all. Both would read nil, and nil is
// indistinguishable from "it ran and produced nothing", so they are compile
// errors rather than runtime surprises.
//
// # Why this is a separate pass rather than a check inside projectNodeBodies
//
// Two reasons, either sufficient:
//
//   - projectNodeBodies runs BEFORE compileGroups, so every node's GroupIdx is
//     still -1 there and the grouped-map rule below could not be expressed.
//   - projectNodeBodies is shared with compileTrusted, where the enclosing
//     graph is a PROJECTED PACKAGE, not the real outer workflow. An ancestor
//     test run there would be asking about the wrong topology, and would
//     re-reject inside the projection what the outer compilation already
//     permitted. Living only in Compile's pass list is what keeps that from
//     happening -- the collection in compileBodyMembers is topology-free and so
//     is safe on both paths.
//
// # Errors carry names only
//
// Every message here names the map node, the body member, and the referenced
// node, and nothing else. An upstream node's output is routinely an HTTP
// response body carrying credentials; node names are not sensitive, node
// outputs are.
func validateBodyOuterRefs(g *Graph) error {
	for i := range g.nodes {
		refs := g.BodyOuterRefsFor(i)
		if len(refs) == 0 {
			continue
		}
		mapName := g.nodes[i].Name

		// A grouped map's body is run by the GROUP's inner engine, from a
		// projected package whose state store holds only the group's own
		// members. The outer snapshot is assembled when the outer engine
		// schedules the map node, and for a group member the outer engine never
		// schedules it -- the group does. So no cross-domain reference can be
		// delivered here, whether it names a group peer or a node outside the
		// group entirely. Rejecting says so at compile time instead of shipping
		// a reference that reads nil on every item.
		if g.nodes[i].GroupIdx >= 0 {
			return fmt.Errorf("node %q: body member %q references node %q via $nodes, "+
				"but %q is a group member: a grouped node's body runs inside the group's "+
				"own execution, which cannot see the outer graph's node outputs",
				mapName, refs[0].Member, refs[0].Node, mapName)
		}

		for _, ref := range refs {
			targetIdx, ok := g.index[ref.Node]
			if !ok {
				return fmt.Errorf("node %q: body member %q references node %q via $nodes, "+
					"which does not exist in the workflow definition",
					mapName, ref.Member, ref.Node)
			}
			if targetIdx == i {
				return fmt.Errorf("node %q: body member %q references node %q via $nodes, "+
					"which is the body's own map node: its output does not exist until "+
					"every item has run",
					mapName, ref.Member, ref.Node)
			}
			if isReachable(g, i, targetIdx) {
				return fmt.Errorf("node %q: body member %q references node %q via $nodes, "+
					"which is DOWNSTREAM of %q: it has not run when the body executes",
					mapName, ref.Member, ref.Node, mapName)
			}
			if !isReachable(g, targetIdx, i) {
				return fmt.Errorf("node %q: body member %q references node %q via $nodes, "+
					"which is on an unrelated branch: nothing orders it before %q",
					mapName, ref.Member, ref.Node, mapName)
			}
			// Reachable but not dominating: the reference is legal (the spec's
			// rule is about topological order, and there IS one), but a
			// conditional branch may skip the target, so the snapshot can be
			// absent. Warn with the SAME advice buildNodesRefs gives an ordinary
			// node in this position rather than rejecting -- the spec's error
			// clause names downstream and unrelated-branch references, not
			// conditional ancestors, and ?? absorbs the miss identically inside
			// a body.
			if !isDeterministicAncestor(g, targetIdx, i) {
				g.addWarning(fmt.Sprintf("node %q: body 成员 %q 的 $nodes['%s'] 可能为 nil，建议使用 ?? 提供默认值",
					mapName, ref.Member, ref.Node))
				continue
			}
			// The legal case still gets a notice: this dependency exists in no
			// connection and in no edge, so the only place an author can learn
			// their body reads an outer node is here.
			g.addWarning(fmt.Sprintf("node %q: body 成员 %q 跨域读取外层节点 $nodes['%s']（进入 %q 时的快照）",
				mapName, ref.Member, ref.Node, mapName))
		}
	}
	return nil
}
