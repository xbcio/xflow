package graph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/types"
)

// heterogeneousGroupPackage builds a *SubgraphPackage by hand, not through
// ProjectGroupPackage, on purpose: Compile()'s own validateSupplyUsage already
// requires g.supplyRefs[i] -- THIS node's own edge -- for anything that node's
// OWN Parameters (including a nested "body") reference, with extraAllowedSupplies
// nil on Compile's own pass. That closes the leak for every package
// ProjectGroupPackage can currently produce from an honestly Compile()d graph:
// whatever node ends up referencing $supplies.rules in the projected Def
// already had to hold its own edge to "rules" before projection, because
// Parameters travel unchanged.
//
// CompileProjectedPackage/compileTrusted is documented as its OWN, standalone
// trust boundary (see its doc comment): a runner trusts a package via
// hash-integrity alone (execution/subgraph/cache.go's recomputePackageHash),
// never by re-deriving "was this really produced by ProjectGroupPackage from a
// Compile()-validated graph". Building the package directly here is what
// actually exercises that boundary on the shape it is supposed to reject on
// its own: a member with NO own entitlement to "rules" sitting in a package
// whose FLAT VisibleSupplies grants "rules" because some sibling legitimately
// owns it.
func heterogeneousGroupPackage() *SubgraphPackage {
	owner := types.NodeDef{Name: "owner", Type: "xflow.noop"}

	// "innocent" is a map node (mapNode hardcodes Name "m"; renamed below) whose
	// BODY -- not its own top-level Parameters -- references $supplies.rules.
	// The reference sits inside "body" specifically so this test exercises
	// projectNodeBodies' visibleSuppliesForNode call, not just
	// validateSupplyUsage's own direct per-member check: fixing only the
	// latter would leave the body path open, which is the exact gap the task
	// calls "the real hole".
	innocent := mapNode(map[string]any{
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
	})
	innocent.Name = "innocent"

	return &SubgraphPackage{
		Version:      SubgraphPackageVersion,
		GroupName:    "g",
		EntryNode:    "owner",
		Def:          &types.WorkflowDef{Name: "g", Nodes: []types.NodeDef{owner, innocent}},
		Requirements: []Requirement{{NodeType: "xflow.noop"}, {NodeType: "xflow.map"}},
		// The flat grant: "rules" is visible to the GROUP because some sibling
		// (in a real graph, "owner") legitimately declared the dependency edge.
		// "innocent" declared none of its own.
		VisibleSupplies: []string{"rules"},
		// NodeSupplyRefs deliberately left for the caller to set per variant:
		// nil reproduces the pre-fix, flat-only behavior (compileTrusted always
		// falls back to VisibleSupplies for every node); a populated map with
		// "innocent" mapped to nil/empty is what ProjectGroupPackage now
		// produces for a genuinely heterogeneous group.
	}
}

// TestCompileTrusted_FlatVisibleSuppliesAloneLeaksToUnentitledBody pins the
// PRE-FIX shape of the defect directly: with NodeSupplyRefs absent (nil),
// compileTrusted has only the flat VisibleSupplies list to validate every
// member against, so a member with zero entitlement of its own -- "innocent",
// via its map body -- passes anyway because SOME sibling in the group holds
// the edge. This must keep passing for as long as nodeSupplyRefs is nil: nil
// means "no narrowing information available", and the documented fallback
// contract (visibleSuppliesForNode) is that nil-map falls back to flat for
// EVERY node. This test is the fallback-path witness for that contract, not a
// bug report.
func TestCompileTrusted_FlatVisibleSuppliesAloneLeaksToUnentitledBody(t *testing.T) {
	pkg := heterogeneousGroupPackage()
	pkg.NodeSupplyRefs = nil
	if _, err := CompileProjectedPackage(pkg); err != nil {
		t.Fatalf("nil NodeSupplyRefs must fall back to the flat VisibleSupplies list for every "+
			"member, got: %v", err)
	}
}

// TestCompileTrusted_NodeSupplyRefsBlocksSiblingLeakIntoMapBody is the
// mutation-backed acceptance test (criterion A/C): once NodeSupplyRefs
// records that "innocent" has no own entitlement to "rules", compileTrusted
// must reject the same package TestCompileTrusted_FlatVisibleSuppliesAloneLeaksToUnentitledBody
// accepted. The only difference between the two packages is NodeSupplyRefs;
// everything a naive flat check would see (VisibleSupplies, the Def, the
// body's own $supplies.rules reference) is identical.
//
// The mutation actually performed and reverted to produce this test's
// evidence: engine/graph/subgraph_package.go's visibleSuppliesForNode was
// changed from
//
//	if nodeSupplyRefs == nil { return flat }
//	return nodeSupplyRefs[name]
//
// to unconditionally
//
//	return flat
//
// (i.e. compileTrusted/projectNodeBodies/validateSupplyUsage revert to using
// the flat list for every node regardless of NodeSupplyRefs, matching this
// package's PRE-fix behavior). That mutation lands squarely on the map-body
// path this test exercises: "innocent"'s own top-level Parameters carry no
// $supplies reference at all -- only its body's "inner" member does -- so the
// only way this test can go red is if projectNodeBodies' per-node lookup
// (feeding ProjectNodeBodyPackage's visibleSupplies argument) stops being
// consulted. A mutation that only touched validateSupplyUsage's direct,
// top-level per-member check would not move this test, because "innocent"
// itself never appears in deriveSupplyRefs(innocent.Parameters) -- only the
// body's own re-validation (CompileProjectedPackage(body.Package), which this
// test also asserts fails) does.
func TestCompileTrusted_NodeSupplyRefsBlocksSiblingLeakIntoMapBody(t *testing.T) {
	pkg := heterogeneousGroupPackage()
	pkg.NodeSupplyRefs = map[string][]string{
		"owner":    {"rules"},
		"innocent": nil, // innocent's own edges: none.
	}
	if _, err := CompileProjectedPackage(pkg); err == nil {
		t.Fatal("CompileProjectedPackage must reject a member whose body reads a supply only a " +
			"sibling declared an edge to, once NodeSupplyRefs records that the member's own set " +
			"is empty; got nil error -- the sibling's entitlement leaked into \"innocent\"'s body")
	}
}

// TestProjectGroupPackage_NodeSupplyRefsNilForHomogeneousGroup is the
// production-path complement to the two hand-built tests above: it proves
// ProjectGroupPackage itself -- not just a hand-crafted package -- leaves
// NodeSupplyRefs nil (and therefore the JSON field omitted, and PackageHash
// unmoved) for the overwhelmingly common case where every member's own
// supply-ref set already equals the flat union. Both members here read
// exactly the same single supply.
func TestProjectGroupPackage_NodeSupplyRefsNilForHomogeneousGroup(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "a", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
			{Name: "b", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "a"}, {Node: "b"}},
			}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if pkg.NodeSupplyRefs != nil {
		t.Fatalf("NodeSupplyRefs = %v, want nil: both members read exactly the same supply, so "+
			"the flat VisibleSupplies list is already the correct per-node answer and the map "+
			"must stay unpopulated to keep PackageHash from moving for this, the overwhelmingly "+
			"common, case", pkg.NodeSupplyRefs)
	}
}

// TestProjectGroupPackage_NodeSupplyRefsPopulatedForHeterogeneousGroup is
// ProjectGroupPackage's own positive case: a group where "a" holds the edge to
// "rules" and "b" holds no edge to anything must record "a" and leave "b"
// either absent or mapped to nil -- never fall back silently to the flat list
// for "b".
func TestProjectGroupPackage_NodeSupplyRefsPopulatedForHeterogeneousGroup(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "a", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
			{Name: "b", Type: "xflow.noop"},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "a"}},
			}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if pkg.NodeSupplyRefs == nil {
		t.Fatal("NodeSupplyRefs must be populated: \"b\" has no own edge to \"rules\" while the " +
			"flat VisibleSupplies list grants it group-wide")
	}
	if got := pkg.NodeSupplyRefs["a"]; len(got) != 1 || got[0] != "rules" {
		t.Fatalf(`NodeSupplyRefs["a"] = %v, want [rules]`, got)
	}
	if got := pkg.NodeSupplyRefs["b"]; len(got) != 0 {
		t.Fatalf(`NodeSupplyRefs["b"] = %v, want empty: "b" declared no dependency edge of its own`, got)
	}
}

// legacySubgraphPackageV1 mirrors SubgraphPackage's field set and JSON tags
// exactly as they were before NodeSupplyRefs existed (Version, GroupName,
// EntryNode, Def, Exits, Artifacts, Requirements, VisibleSupplies,
// VisibleOuterNodes, in that order, no node_supply_refs key at all). Keeping
// this frozen copy in the test file -- rather than relying on git history --
// is what lets TestPackageHash_StableForHomogeneousGroup re-derive "what the
// hash used to be" on every future run, without ever needing to check out an
// old commit.
type legacySubgraphPackageV1 struct {
	Version           int                   `json:"version"`
	GroupName         string                `json:"group_name"`
	EntryNode         string                `json:"entry_node"`
	Def               *types.WorkflowDef    `json:"def"`
	Exits             []SubgraphPackageExit `json:"exits"`
	Artifacts         []GroupArtifact       `json:"artifacts,omitempty"`
	Requirements      []Requirement         `json:"requirements"`
	VisibleSupplies   []string              `json:"visible_supplies,omitempty"`
	VisibleOuterNodes []string              `json:"visible_outer_nodes,omitempty"`
}

func legacyHash(pkg *SubgraphPackage) (string, error) {
	legacy := legacySubgraphPackageV1{
		Version:           pkg.Version,
		GroupName:         pkg.GroupName,
		EntryNode:         pkg.EntryNode,
		Def:               pkg.Def,
		Exits:             pkg.Exits,
		Artifacts:         pkg.Artifacts,
		Requirements:      pkg.Requirements,
		VisibleSupplies:   pkg.VisibleSupplies,
		VisibleOuterNodes: pkg.VisibleOuterNodes,
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return packageHashPrefix + hex.EncodeToString(sum[:]), nil
}

// TestPackageHash_StableForHomogeneousGroup is the hash-stability acceptance
// test (criterion B): for a group where every member's own ref set already
// equals the flat union, ComputePackageHash's post-fix output (with the new
// NodeSupplyRefs field present on the struct but nil for this package) must be
// BYTE-FOR-BYTE identical to what the pre-fix struct shape (no NodeSupplyRefs
// field at all) would have produced. omitempty on a nil map is what makes the
// two shapes serialize identically; this test proves that empirically instead
// of asserting it.
func TestPackageHash_StableForHomogeneousGroup(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
			{Name: "a", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
			{Name: "b", Type: "xflow.noop", Parameters: map[string]any{"r": "$supplies.rules"}},
		},
		Connections: types.Connections{
			"a": {"main": {Targets: []types.Connection{{Node: "b", Input: "main"}}}},
			"rules": {"supply": {
				Type:    types.ConnectionTypeDependency,
				Targets: []types.Connection{{Node: "a"}, {Node: "b"}},
			}},
		},
		Groups: []types.GroupDef{{Name: "g", Members: []string{"a", "b"}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	pkg, gotHash, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if pkg.NodeSupplyRefs != nil {
		t.Fatalf("precondition failed: NodeSupplyRefs = %v, want nil for this homogeneous group "+
			"(see TestProjectGroupPackage_NodeSupplyRefsNilForHomogeneousGroup for why)", pkg.NodeSupplyRefs)
	}
	wantHash, err := legacyHash(pkg)
	if err != nil {
		t.Fatalf("legacy hash: %v", err)
	}
	if gotHash != wantHash {
		t.Fatalf("PackageHash moved for a homogeneous group: got %s, want %s (the pre-fix shape's "+
			"hash)\nthis would fence every already-deployed group in every workflow the moment this "+
			"change shipped, not just the ones it actually narrows", gotHash, wantHash)
	}
}

// TestNodeSupplyRefs_JSONKeyOrderIsDeterministic pins the load-bearing fact
// the NodeSupplyRefs field doc calls out: encoding/json sorts map[string]...
// keys before marshaling, so two SubgraphPackage values holding the same
// NodeSupplyRefs entries -- built through different insertion orders, which is
// what Go's own randomized map iteration would otherwise risk leaking into the
// output -- must marshal to byte-identical JSON. If this ever stopped holding,
// PackageHash would become nondeterministic per-process for any heterogeneous
// group, silently fencing groups at random.
func TestNodeSupplyRefs_JSONKeyOrderIsDeterministic(t *testing.T) {
	build := func(order []string) map[string][]string {
		m := make(map[string][]string, len(order))
		for _, k := range order {
			m[k] = []string{"x", "y"}
		}
		return m
	}
	a := &SubgraphPackage{Version: 1, NodeSupplyRefs: build([]string{"zeta", "mu", "alpha", "kappa"})}
	b := &SubgraphPackage{Version: 1, NodeSupplyRefs: build([]string{"alpha", "kappa", "mu", "zeta"})}

	ja, err := json.Marshal(a)
	if err != nil {
		t.Fatalf("marshal a: %v", err)
	}
	jb, err := json.Marshal(b)
	if err != nil {
		t.Fatalf("marshal b: %v", err)
	}
	if string(ja) != string(jb) {
		t.Fatalf("two maps with identical entries inserted in different orders marshaled "+
			"differently:\na = %s\nb = %s", ja, jb)
	}

	// Repeated marshaling of the SAME map value, not just two differently-built
	// ones: Go randomizes map iteration order per-process, so this is the case
	// that would actually catch encoding/json ever dropping its documented
	// key-sort guarantee.
	for i := 0; i < 20; i++ {
		jc, err := json.Marshal(a)
		if err != nil {
			t.Fatalf("marshal a (repeat %d): %v", i, err)
		}
		if string(jc) != string(ja) {
			t.Fatalf("repeated marshal of the same value produced different bytes on call %d:\n"+
				"first = %s\nthis   = %s", i, ja, jc)
		}
	}
}
