package graph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// subWorkflowRefDef builds a single-node workflow named name whose one node
// references ref (optionally pinned to version).
func subWorkflowRefDef(name, ref, version string) *types.WorkflowDef {
	params := map[string]any{"workflow": ref}
	if version != "" {
		params["version"] = version
	}
	return &types.WorkflowDef{
		Name: name,
		Nodes: []types.NodeDef{{
			Name:       "sub",
			Type:       types.SubWorkflowNodeType,
			Parameters: params,
		}},
	}
}

// targetMap packs compiled graphs into resolver targets, keyed by name.
func targetMap(graphs map[string]*Graph) map[string]SubWorkflowTarget {
	out := make(map[string]SubWorkflowTarget, len(graphs))
	for name, g := range graphs {
		out[name] = SubWorkflowTarget{Version: "v1", DefinitionHash: "sha256:" + name, Graph: g}
	}
	return out
}

func TestCompileRejectsSelfSubWorkflowReference(t *testing.T) {
	def := subWorkflowRefDef("a", "a", "")
	_, err := CompileWithResolver(context.Background(), def, &fakeSubWorkflowResolver{
		targets: targetMap(map[string]*Graph{"a": subWorkflowTargetGraph(t, "a")}),
	})
	if !errors.Is(err, ErrSubWorkflowCycle) {
		t.Fatalf("error = %v, want an ErrSubWorkflowCycle for a self-reference", err)
	}
}

func TestCompileRejectsSubWorkflowReferenceCycle(t *testing.T) {
	// betaGraph references alpha; alphaGraph references beta. beta compiles
	// fine on its own (its chain revisits nothing), and alpha must then be
	// rejected: alpha -> beta -> alpha.
	betaGraph, err := CompileWithResolver(context.Background(),
		subWorkflowRefDef("beta", "alpha", ""), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"alpha": subWorkflowTargetGraph(t, "alpha")}),
		})
	if err != nil {
		t.Fatalf("compile beta: %v", err)
	}
	_, err = CompileWithResolver(context.Background(),
		subWorkflowRefDef("alpha", "beta", ""), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"beta": betaGraph}),
		})
	if !errors.Is(err, ErrSubWorkflowCycle) {
		t.Fatalf("error = %v, want an ErrSubWorkflowCycle for alpha -> beta -> alpha", err)
	}
	if !strings.Contains(err.Error(), "alpha -> beta -> alpha") {
		t.Fatalf("error %q does not spell out the cycle chain", err)
	}
}

// TestCompileRejectsVersionVariantCycle pins the name-based rule: the version
// is not part of a cycle's identity. What a cycle deadlocks is the concurrency
// slot, which is keyed by namespace+name, so a@v1 -> b -> a@v2 is the same
// cycle as a@v1 -> b -> a@v1 — and the second version is not even registered
// in this fixture, so a version-sensitive walk would resolve it happily.
func TestCompileRejectsVersionVariantCycle(t *testing.T) {
	bGraph, err := CompileWithResolver(context.Background(),
		subWorkflowRefDef("b", "a", "v2"), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"a": subWorkflowTargetGraph(t, "a")}),
		})
	if err != nil {
		t.Fatalf("compile b: %v", err)
	}
	_, err = CompileWithResolver(context.Background(),
		subWorkflowRefDef("a", "b", ""), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"b": bGraph}),
		})
	if !errors.Is(err, ErrSubWorkflowCycle) {
		t.Fatalf("error = %v, want an ErrSubWorkflowCycle for a -> b -> a@v2", err)
	}
}

// TestCompileAllowsDiamondReferences guards the other direction: two paths that
// meet without returning to an ancestor are not a cycle, and the walk must not
// confuse the shared descendant for one.
func TestCompileAllowsDiamondReferences(t *testing.T) {
	dGraph := subWorkflowTargetGraph(t, "d")
	cGraph, err := CompileWithResolver(context.Background(),
		subWorkflowRefDef("c", "d", ""), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"d": dGraph}),
		})
	if err != nil {
		t.Fatalf("compile c: %v", err)
	}
	bGraph, err := CompileWithResolver(context.Background(),
		subWorkflowRefDef("b", "d", ""), &fakeSubWorkflowResolver{
			targets: targetMap(map[string]*Graph{"d": dGraph}),
		})
	if err != nil {
		t.Fatalf("compile b: %v", err)
	}
	def := &types.WorkflowDef{
		Name: "a",
		Nodes: []types.NodeDef{
			{Name: "one", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "b"}},
			{Name: "two", Type: types.SubWorkflowNodeType, Parameters: map[string]any{"workflow": "c"}},
		},
	}
	if _, err := CompileWithResolver(context.Background(), def, &fakeSubWorkflowResolver{
		targets: targetMap(map[string]*Graph{"b": bGraph, "c": cGraph, "d": dGraph}),
	}); err != nil {
		t.Fatalf("a diamond reference graph was rejected as a cycle: %v", err)
	}
}

// TestCompileRejectsSubWorkflowChainAtDepthLimit pins the boundary: a chain of
// exactly DefaultMaxSubWorkflowDepth hops compiles, and one more hop is
// rejected. Graphs are named g17 (leaf) .. g0, each referencing the next.
func TestCompileRejectsSubWorkflowChainAtDepthLimit(t *testing.T) {
	const leaf = DefaultMaxSubWorkflowDepth + 1 // g17: the 17-hop chain's tail
	graphs := map[string]*Graph{}

	for i := leaf; i >= 0; i-- {
		name := fmt.Sprintf("g%d", i)
		def := subWorkflowRefDef(name, fmt.Sprintf("g%d", i+1), "")
		if i == leaf {
			def = &types.WorkflowDef{Name: name, Nodes: []types.NodeDef{{Name: "work", Type: "xflow.noop"}}}
		}
		g, err := CompileWithResolver(context.Background(), def, &fakeSubWorkflowResolver{targets: targetMap(graphs)})
		switch {
		case i == 0:
			if !errors.Is(err, ErrSubWorkflowDepth) {
				t.Fatalf("g0 (a %d-hop chain) error = %v, want ErrSubWorkflowDepth", leaf+1, err)
			}
			continue
		case err != nil:
			t.Fatalf("compile %s (a %d-hop chain): %v", name, leaf-i, err)
		}
		graphs[name] = g
	}
}

// TestCompileWithoutResolverSkipsReferenceWalk pins the nil-resolver contract:
// the walk has no other side to look at, so the node stays unresolved rather
// than failing — the engine's dispatch refusal is what keeps it from running.
func TestCompileWithoutResolverSkipsReferenceWalk(t *testing.T) {
	if _, err := Compile(subWorkflowRefDef("a", "a", "")); err != nil {
		t.Fatalf("Compile without a resolver: %v", err)
	}
}
