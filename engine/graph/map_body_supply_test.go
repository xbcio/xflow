package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

// C1: a map body member that reads $supplies.<name> must be able to run.
//
// ProjectNodeBodyPackage never populated SubgraphPackage.VisibleSupplies, unlike
// the group path (ProjectGroupPackage / buildVisibleSupplies, fixed for
// groups by T3). The parent Compile() succeeds -- validateSupplyUsage sees the
// dependency edge on the OUTER graph -- but CompileProjectedPackage(body.Package)
// is what execution/subgraph.PackageCache.Resolve calls on the first batch, and
// that recompiles the body's Def from scratch with no VisibleSupplies to widen
// validateSupplyUsage's declared set. This is worse than the compile-time
// failure T3 fixed for groups: it passes deploy-time validation entirely and
// only fails at the first batch of the first execution.
func TestProjectNodeBodyPackage_CarriesVisibleSupplyNames(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			mapNode(map[string]any{
				"items": "$input.rows",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "inner", "type": "xflow.noop", "parameters": map[string]any{
								"r": "$supplies.rules",
							}},
						},
					},
				},
			}),
		},
		Connections: types.Connections{
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "m"}},
			}},
		},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatalf("parent compile: %v (a body member reading a supply the parent map node "+
			"declares a dependency edge to must compile at deploy time)", err)
	}

	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}
	body := g.BodyAt(idx)
	if body == nil {
		t.Fatal("compiled graph carries no body package for \"m\"")
	}

	if _, err := CompileProjectedPackage(body.Package); err != nil {
		t.Fatalf("projected body package must compile, got: %v\n"+
			"the projection dropped the visible supply names, so validateSupplyUsage "+
			"cannot see that body member \"inner\" is allowed to read $supplies.rules -- "+
			"this is exactly the failure a runner hits on the FIRST BATCH of the FIRST "+
			"execution, despite Compile() having succeeded at deploy time", err)
	}
}
