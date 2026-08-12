package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// bodyRefWorkflow builds "src -> m -> down", where m is a map whose body has one
// member reading $nodes[ref]. It is the smallest shape that can distinguish the
// three cross-domain cases the spec names: ref="src" is an upstream ancestor
// (legal), ref="down" is downstream (rejected), ref="side" is an unrelated
// branch (rejected).
func bodyRefWorkflow(ref string) *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "body-outer-refs",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.echo"},
			{Name: "side", Type: "test.echo"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "worker", "type": "test.echo", "parameters": map[string]any{
								"seen": "${{ $nodes['" + ref + "'].value }}",
							}},
						},
					},
				},
			}},
			{Name: "down", Type: "test.echo"},
		},
		Connections: types.Connections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "m", Input: "main"}}}},
			"m":   {"main": types.PortConnections{Targets: []types.Connection{{Node: "down", Input: "main"}}}},
		},
	}
}

// TestBodyMayReferenceAnUpstreamOuterNode is the case the DSL spec allows:
// "跨域引用仅允许读取 loop 节点的上游祖先节点". Before this was built, EVERY outer
// reference from a body was rejected by validatePortability with
//
//	body "m": non-portable member "worker": references external node "src" via $nodes
//
// which is the right answer for a GROUP member (a group is co-located and its
// members are scheduled as one unit) but the wrong one for a body: a body runs
// as a sub-execution of a node that has already-completed ancestors, and the
// spec promises it can read them.
func TestBodyMayReferenceAnUpstreamOuterNode(t *testing.T) {
	g, err := Compile(bodyRefWorkflow("src"))
	if err != nil {
		t.Fatalf("compile with an upstream body reference: %v", err)
	}
	refs := g.BodyOuterRefsFor(g.index["m"])
	if len(refs) != 1 || refs[0].Node != "src" || refs[0].Member != "worker" {
		t.Fatalf("BodyOuterRefsFor(m) = %#v, want one ref {worker, src} -- the snapshot set is what "+
			"turns a permitted reference into data the body actually receives; without it "+
			"the reference compiles and then reads nil at runtime, absorbed by ??", refs)
	}
}

// TestBodyReferencingADownstreamOuterNodeIsRejected covers the spec's
// "引用不可达的外层节点（如 loop 的下游节点或无关分支节点）编译报错".
//
// Rejection has to happen at compile time because the runtime has no honest
// answer: "down" has not run when the body executes, so a snapshot of it would
// be nil, and nil is indistinguishable from "the node ran and produced nothing".
func TestBodyReferencingADownstreamOuterNodeIsRejected(t *testing.T) {
	_, err := Compile(bodyRefWorkflow("down"))
	if err == nil {
		t.Fatal("compile accepted a body reference to a DOWNSTREAM outer node; want a compile error")
	}
	// The error must name the referenced node -- an operator cannot fix
	// "some reference is invalid". Node names are not sensitive; node OUTPUT is,
	// and must never appear here.
	if !strings.Contains(err.Error(), "down") || !strings.Contains(err.Error(), "worker") {
		t.Fatalf("error = %v; want it to name both the body member and the referenced node", err)
	}
}

// TestBodyReferencingAnUnrelatedOuterNodeIsRejected covers the other half of the
// same spec line: a node on a parallel branch is not an ancestor of the map
// node, so nothing orders it against the body's execution.
func TestBodyReferencingAnUnrelatedOuterNodeIsRejected(t *testing.T) {
	_, err := Compile(bodyRefWorkflow("side"))
	if err == nil {
		t.Fatal("compile accepted a body reference to an UNRELATED outer node; want a compile error")
	}
	if !strings.Contains(err.Error(), "side") {
		t.Fatalf("error = %v; want it to name the referenced node", err)
	}
}

// TestBodyReferencingANonexistentOuterNodeIsRejected guards the case that is
// neither upstream nor downstream because the name matches nothing at all. It
// must not fall through the ancestor test into "allowed".
func TestBodyReferencingANonexistentOuterNodeIsRejected(t *testing.T) {
	_, err := Compile(bodyRefWorkflow("nope"))
	if err == nil {
		t.Fatal("compile accepted a body reference to a nonexistent node; want a compile error")
	}
}

// TestLegalBodyOuterReferenceEmitsAnInfoNotice covers the spec's last line:
// "合法的跨域引用产生 info 级提示，帮助用户意识到隐式依赖". The dependency is
// invisible in the topology -- nothing in connections says the body reads "src"
// -- so the author gets told rather than left to discover it.
func TestLegalBodyOuterReferenceEmitsAnInfoNotice(t *testing.T) {
	g, err := Compile(bodyRefWorkflow("src"))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	found := false
	for _, w := range g.Warnings() {
		if strings.Contains(w, "src") && strings.Contains(w, "m") {
			found = true
		}
	}
	if !found {
		t.Fatalf("warnings = %#v; want an info notice naming the map node and the referenced node", g.Warnings())
	}
}

// TestBodyRefsDoNotLeakIntoTheOuterPrefetchSet keeps the two reference sets
// apart. g.NodesRefsFor(m) drives buildInput's prefetch for the MAP NODE's own
// parameters; the body's references are a separate set with a different
// consumer (the snapshot shipped into the sub-execution). Merging them would
// make the map node itself prefetch names it never mentions, and would let a
// body reference silently satisfy the map node's own $nodes lookups.
func TestBodyRefsDoNotLeakIntoTheOuterPrefetchSet(t *testing.T) {
	g, err := Compile(bodyRefWorkflow("src"))
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if refs := g.NodesRefsFor(g.index["m"]); len(refs) != 0 {
		t.Fatalf("NodesRefsFor(m) = %#v, want empty -- the map node's OWN parameters "+
			"reference no nodes; only its body does", refs)
	}
}
