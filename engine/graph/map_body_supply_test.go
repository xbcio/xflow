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

// C1 ∩ T3: a map node that is itself a GROUP MEMBER, whose body reads
// $supplies.<name>.
//
// Neither existing test reaches this shape. C1 above compiles the body that
// Compile() projected off the OUTER graph, where g.supplyRefs carries the
// dependency edge. T3 (TestProjectGroupPackage_CarriesVisibleSupplyNames)
// compiles a group whose members read a supply directly, with no body to
// reproject. Both are green; their intersection is not covered.
//
// The intersection has its own failure because the group path projects the
// body a SECOND time. compileTrusted re-runs projectNodeBodies on the group's
// own Def, and that Def cannot carry the supply edge: a supply node may not be
// a group member (group_compile.go rejects it) and buildPackageConnections
// keeps only member-to-member edges, so buildDependencyEdges short-circuits
// with g.supplyRefs empty. The freshly rebuilt body package therefore gets
// VisibleSupplies = nil, and it is that body -- not Compile()'s correct one --
// that expand.go reads at runtime.
//
// The group's own visibleSupplies list is right there in compileTrusted's
// scope and already widens validateSupplyUsage for the members; it just never
// reaches the nested projection.
func TestGroupMemberMapBody_CarriesVisibleSupplyNames(t *testing.T) {
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
			{Name: "b", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"m": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "m"}},
			}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"m", "b"}}},
	}

	g, err := Compile(def)
	if err != nil {
		t.Fatalf("parent compile: %v", err)
	}

	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	// Asserted rather than assumed: if the group package did not carry the name,
	// the failure below would be a different defect one layer up and the fix
	// this test guards would be the wrong one.
	if len(pkg.VisibleSupplies) != 1 || pkg.VisibleSupplies[0] != "rules" {
		t.Fatalf("group package must carry the supply name its member declares an "+
			"edge to; got %v", pkg.VisibleSupplies)
	}

	// The runner's path: the group package is recompiled on its own, and the
	// body it reprojects is what expand.go hands to the map body executor.
	trusted, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("group package must compile: %v", err)
	}
	idx, ok := trusted.NodeIndex("m")
	if !ok {
		t.Fatal("recompiled group graph has no node \"m\"")
	}
	body := trusted.BodyAt(idx)
	if body == nil {
		t.Fatal("recompiled group graph carries no body package for \"m\"")
	}
	// Both checks report: the first names the mechanism, the second the symptom
	// an operator actually sees. Fataling on the first would hide whether the
	// symptom is the one #74 hit.
	if got := body.Package.VisibleSupplies; len(got) != 1 || got[0] != "rules" {
		t.Errorf("the reprojected body package must carry the supply names the "+
			"enclosing group already grants its members; got %v.\n"+
			"Without them the compile below fails, and it fails at RUNTIME on the "+
			"first batch -- deploy-time validation passed.", got)
	}
	if _, err := CompileProjectedPackage(body.Package); err != nil {
		t.Errorf("the reprojected body package must compile, got: %v\n"+
			"this is the exact error a grouped map hits on its first batch: the "+
			"group execution never completes, every batch times out, and the "+
			"failure is reported as a deadline rather than a validation error", err)
	}
}
