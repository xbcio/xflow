package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// TestCompileProjectedPackage_ProjectsAMapMemberBody covers the second of the
// two defects that kept an xflow.map node from running as a group member.
//
// Compile (the top-level path) runs projectNodeBodies, so a top-level map ends
// up with NodeMeta.Body populated. compileTrusted -- the path
// CompileProjectedPackage uses for projected GROUP packages -- was a hand-rolled
// parallel pass list that had drifted: it ran buildEdges and
// buildDependencyEdges but never projectNodeBodies. A map member therefore
// compiled cleanly with Body == nil, and the batch it expanded into died at
// runtime with ErrNoMapBody (engine/expand.go) rather than at compile time --
// the failure surfaced far from its cause, as a batch retry loop.
func TestCompileProjectedPackage_ProjectsAMapMemberBody(t *testing.T) {
	pkg := &SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "m",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{
					"items": "$input.items",
					"body": map[string]any{
						"type": "xflow.subgraph",
						"parameters": map[string]any{
							"nodes": []any{
								map[string]any{"name": "member", "type": "test.noop"},
							},
						},
					},
				}},
			},
		},
		Requirements: []Requirement{{NodeType: "xflow.map", NodeVersion: 1}},
	}

	g, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("compile projected package: %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}
	body := g.BodyAt(idx)
	if body == nil {
		t.Fatal("map member \"m\" compiled with a nil body through the trusted path, " +
			"even though its params declare one -- every batch it expands into will " +
			"fail with ErrNoMapBody at run time")
	}
	// Assert on the body's actual content, not merely non-nil: a projection that
	// produced an empty package would satisfy a nil check just as well.
	if body.Package == nil || body.Package.EntryNode != "member" {
		t.Errorf("projected body entry node = %#v, want \"member\"", body.Package)
	}
}
