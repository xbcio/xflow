package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// helper: build a grouped workflow with a linear chain A→B→C inside a group,
// with A as entry and C having a boundary output to an external node D.
func makeGroupedDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "test-grouped",
		Version: "1",
		Context: &types.WorkflowContext{
			Vars:   map[string]any{"env": "test"},
			Config: map[string]any{"timeout": 30},
		},
		Nodes: []types.NodeDef{
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
			{Name: "C", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://c"}},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups: []types.GroupDef{
			{Name: "grp1", Members: []string{"A", "B", "C"}},
		},
		Connections: types.Connections{
			"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}},
			"B": {"main": {Targets: []types.Connection{{Node: "C", Input: "main"}}}},
			"C": {"result": {Targets: []types.Connection{{Node: "D", Input: "main"}}}},
		},
	}
}

func TestProjectGroupPackage_MembersAndCollectors(t *testing.T) {
	def := makeGroupedDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Find the group unit.
	var groupUnitIdx int = -1
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == UnitGroup {
			groupUnitIdx = i
			break
		}
	}
	if groupUnitIdx == -1 {
		t.Fatal("no group unit found")
	}

	pkg, hash, err := ProjectGroupPackage(g, groupUnitIdx)
	if err != nil {
		t.Fatalf("ProjectGroupPackage: %v", err)
	}

	// Members A, B, C + 1 collector for C:result boundary output.
	wantNodes := 4 // A, B, C, __collector_C_result
	if len(pkg.Def.Nodes) != wantNodes {
		t.Errorf("nodes = %d, want %d", len(pkg.Def.Nodes), wantNodes)
	}

	// Exactly 1 exit (C:result).
	if len(pkg.Exits) != 1 {
		t.Fatalf("exits = %d, want 1", len(pkg.Exits))
	}
	exit := pkg.Exits[0]
	if exit.SrcNode != "C" || exit.Port != "result" {
		t.Errorf("exit = {%s, %s}, want {C, result}", exit.SrcNode, exit.Port)
	}
	if exit.CollectorNode != "__collector_C_result" {
		t.Errorf("collector = %s, want __collector_C_result", exit.CollectorNode)
	}

	// Entry node is A.
	if pkg.EntryNode != "A" {
		t.Errorf("entry = %q, want A", pkg.EntryNode)
	}

	// No entry injection node (D1' spec: no xflow.group_input).
	for _, nd := range pkg.Def.Nodes {
		if nd.Type == "xflow.group_input" {
			t.Error("package contains xflow.group_input node (should not exist per D1')")
		}
	}

	// Hash is non-empty and has correct prefix.
	if !strings.HasPrefix(hash, packageHashPrefix) {
		t.Errorf("hash = %q, want prefix %q", hash, packageHashPrefix)
	}
}

func TestProjectGroupPackage_Deterministic(t *testing.T) {
	def := makeGroupedDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	_, firstHash, _ := ProjectGroupPackage(g, groupUnitIdx)

	for i := 0; i < 100; i++ {
		_, h, err := ProjectGroupPackage(g, groupUnitIdx)
		if err != nil {
			t.Fatalf("iteration %d: %v", i, err)
		}
		if h != firstHash {
			t.Fatalf("iteration %d: hash %q != first %q", i, h, firstHash)
		}
	}
}

func TestProjectGroupPackage_HashExcludesActivationCardinality(t *testing.T) {
	baseDef := makeGroupedDef()
	base, err := Compile(baseDef)
	if err != nil {
		t.Fatalf("compile base: %v", err)
	}
	replicatedDef := makeGroupedDef()
	replicatedDef.Groups[0].ActivationReplicas = 7
	replicated, err := Compile(replicatedDef)
	if err != nil {
		t.Fatalf("compile replicated: %v", err)
	}

	basePkg, baseHash, err := ProjectGroupPackage(base, groupUnitOf(t, base))
	if err != nil {
		t.Fatalf("project base: %v", err)
	}
	replicatedPkg, replicatedHash, err := ProjectGroupPackage(replicated, groupUnitOf(t, replicated))
	if err != nil {
		t.Fatalf("project replicated: %v", err)
	}
	if baseHash != replicatedHash {
		t.Fatalf("activation host cardinality changed sandbox package hash: %q != %q", baseHash, replicatedHash)
	}
	if base.Groups()[0].PackageHash != replicated.Groups()[0].PackageHash {
		t.Fatalf("compiled GroupMeta package hash changed with host cardinality: %q != %q", base.Groups()[0].PackageHash, replicated.Groups()[0].PackageHash)
	}
	for _, pkg := range []*SubgraphPackage{basePkg, replicatedPkg} {
		if len(pkg.Def.Groups) != 0 {
			t.Fatalf("projected package must not carry outer placement groups: %+v", pkg.Def.Groups)
		}
		for _, node := range pkg.Def.Nodes {
			if node.ActivationReplicas != 0 {
				t.Fatalf("projected member %q leaked outer activation cardinality %d", node.Name, node.ActivationReplicas)
			}
		}
	}
}

func TestProjectGroupPackage_NodeOrderDoesNotAffectHash(t *testing.T) {
	// Compile with nodes in order A, B, C, D.
	def1 := makeGroupedDef()
	g1, err := Compile(def1)
	if err != nil {
		t.Fatalf("Compile def1: %v", err)
	}

	// Compile with nodes reordered: C, A, B, D.
	def2 := &types.WorkflowDef{
		Name:    "test-grouped",
		Version: "1",
		Context: def1.Context,
		Nodes: []types.NodeDef{
			{Name: "C", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://c"}},
			{Name: "A", Type: "http.request", Version: 1, Parameters: map[string]any{"url": "http://a"}},
			{Name: "B", Type: "code.python", Version: 2, Parameters: map[string]any{"script": "pass"}},
			{Name: "D", Type: "db.query", Version: 1},
		},
		Groups:      def1.Groups,
		Connections: def1.Connections,
	}
	g2, err := Compile(def2)
	if err != nil {
		t.Fatalf("Compile def2: %v", err)
	}

	findGroup := func(g *Graph) int {
		for i := 0; i < g.UnitCount(); i++ {
			if g.UnitKindAt(i) == UnitGroup {
				return i
			}
		}
		t.Fatal("no group unit")
		return -1
	}

	_, h1, _ := ProjectGroupPackage(g1, findGroup(g1))
	_, h2, _ := ProjectGroupPackage(g2, findGroup(g2))

	if h1 != h2 {
		t.Errorf("hash differs when Nodes reordered:\n  h1=%s\n  h2=%s", h1, h2)
	}
}

func TestProjectGroupPackage_ParameterChangeAffectsHash(t *testing.T) {
	def1 := makeGroupedDef()
	g1, err := Compile(def1)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	def2 := makeGroupedDef()
	def2.Nodes[0].Parameters = map[string]any{"url": "http://different"}
	g2, err := Compile(def2)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	findGroup := func(g *Graph) int {
		for i := 0; i < g.UnitCount(); i++ {
			if g.UnitKindAt(i) == UnitGroup {
				return i
			}
		}
		t.Fatal("no group unit")
		return -1
	}

	_, h1, _ := ProjectGroupPackage(g1, findGroup(g1))
	_, h2, _ := ProjectGroupPackage(g2, findGroup(g2))

	if h1 == h2 {
		t.Error("hash unchanged after parameter change")
	}
}

func TestProjectGroupPackage_VersionChangeAffectsHash(t *testing.T) {
	def1 := makeGroupedDef()
	g1, err := Compile(def1)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	def2 := makeGroupedDef()
	def2.Nodes[1].Version = 99
	g2, err := Compile(def2)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	findGroup := func(g *Graph) int {
		for i := 0; i < g.UnitCount(); i++ {
			if g.UnitKindAt(i) == UnitGroup {
				return i
			}
		}
		t.Fatal("no group unit")
		return -1
	}

	_, h1, _ := ProjectGroupPackage(g1, findGroup(g1))
	_, h2, _ := ProjectGroupPackage(g2, findGroup(g2))

	if h1 == h2 {
		t.Error("hash unchanged after version change")
	}
}

func TestProjectGroupPackage_ExitSetChangeAffectsHash(t *testing.T) {
	// Original: C:result goes to D.
	def1 := makeGroupedDef()
	g1, err := Compile(def1)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Add second boundary output: B:extra goes to D.
	def2 := makeGroupedDef()
	def2.Connections["B"]["extra"] = types.PortConnections{Targets: []types.Connection{{Node: "D", Input: "aux"}}}
	g2, err := Compile(def2)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	findGroup := func(g *Graph) int {
		for i := 0; i < g.UnitCount(); i++ {
			if g.UnitKindAt(i) == UnitGroup {
				return i
			}
		}
		t.Fatal("no group unit")
		return -1
	}

	_, h1, _ := ProjectGroupPackage(g1, findGroup(g1))
	_, h2, _ := ProjectGroupPackage(g2, findGroup(g2))

	if h1 == h2 {
		t.Error("hash unchanged after exit set change")
	}
}

func TestProjectGroupPackage_NonGroupUnitError(t *testing.T) {
	def := makeGroupedDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Unit 0 should be a node unit (D is ungrouped).
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == UnitNode {
			_, _, err := ProjectGroupPackage(g, i)
			if err == nil {
				t.Error("expected error for non-group unit")
			}
			return
		}
	}
	t.Fatal("no node unit found")
}

func TestCompileProjectedPackage_AcceptsReservedTypes(t *testing.T) {
	def := makeGroupedDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	var groupUnitIdx int
	for i := 0; i < g.UnitCount(); i++ {
		if g.UnitKindAt(i) == UnitGroup {
			groupUnitIdx = i
			break
		}
	}

	pkg, _, err := ProjectGroupPackage(g, groupUnitIdx)
	if err != nil {
		t.Fatalf("ProjectGroupPackage: %v", err)
	}

	// The package should contain xflow.group_exit nodes.
	hasReserved := false
	for _, nd := range pkg.Def.Nodes {
		if strings.HasPrefix(nd.Type, ReservedNodeTypePrefix) {
			hasReserved = true
			break
		}
	}
	if !hasReserved {
		t.Fatal("projected package has no reserved-type nodes")
	}

	// CompileProjectedPackage should accept them.
	compiled, err := CompileProjectedPackage(pkg)
	if err != nil {
		t.Fatalf("CompileProjectedPackage: %v", err)
	}
	if compiled.NodeCount() != len(pkg.Def.Nodes) {
		t.Errorf("compiled nodes = %d, want %d", compiled.NodeCount(), len(pkg.Def.Nodes))
	}
}

func TestCompile_RejectsReservedTypes(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "evil",
		Nodes: []types.NodeDef{
			{Name: "fake", Type: "xflow.group_exit", Version: 1},
			{Name: "normal", Type: "http.request", Version: 1},
		},
		Connections: types.Connections{
			"fake": {"main": {Targets: []types.Connection{{Node: "normal", Input: "main"}}}},
		},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected Compile to reject reserved xflow.group_* type")
	}
	if !strings.Contains(err.Error(), ReservedNodeTypePrefix) {
		t.Errorf("error = %v, want mention of %q", err, ReservedNodeTypePrefix)
	}
}

func TestProjectGroupPackage_GraphHashIncludesPackageHash(t *testing.T) {
	def := makeGroupedDef()
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// The graph hash should have been computed with the package hash set.
	groups := g.Groups()
	if len(groups) != 1 {
		t.Fatalf("groups = %d, want 1", len(groups))
	}
	if groups[0].PackageHash == "" {
		t.Error("PackageHash empty after Compile")
	}
	if !strings.HasPrefix(groups[0].PackageHash, packageHashPrefix) {
		t.Errorf("PackageHash = %q, want prefix %q", groups[0].PackageHash, packageHashPrefix)
	}
}

func TestProjectGroupPackage_VarsConfigChangeAffectsHash(t *testing.T) {
	def1 := makeGroupedDef()
	g1, err := Compile(def1)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	def2 := makeGroupedDef()
	def2.Context.Vars["env"] = "production"
	g2, err := Compile(def2)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	findGroup := func(g *Graph) int {
		for i := 0; i < g.UnitCount(); i++ {
			if g.UnitKindAt(i) == UnitGroup {
				return i
			}
		}
		t.Fatal("no group unit")
		return -1
	}

	_, h1, _ := ProjectGroupPackage(g1, findGroup(g1))
	_, h2, _ := ProjectGroupPackage(g2, findGroup(g2))

	if h1 == h2 {
		t.Error("hash unchanged after vars change")
	}
}

// 组成员引用 $supplies 时，投影出的包必须能编译。
//
// 这个用例在修复前就是红的 —— 它不是新功能的测试，是一个既有缺陷的回归测试。
// 根因：ProjectGroupPackage 构造 Def 时不带任何 supply 信息，而
// buildPackageConnections 只读 g.outEdges，依赖边编译后进的是 g.supplyRefs。
func TestProjectGroupPackage_CarriesVisibleSupplyNames(t *testing.T) {
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
		t.Fatalf("parent compile: %v", err)
	}

	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}

	if _, err := CompileProjectedPackage(pkg); err != nil {
		t.Fatalf("projected package must compile, got: %v\n"+
			"the projection dropped the visible supply names, so validateSupplyUsage "+
			"cannot see that member \"a\" is allowed to read $supplies.rules", err)
	}
}

// 名单是名字不是内容 —— 内容由 runner 在激活时取，不进包。
// 若把内容也投影进去，PackageHash 会随 supply 内容变化，编译期投影的全部收益
// （N 批共享一次编译）就没了。
func TestProjectGroupPackage_HashStableAcrossSupplyContentChanges(t *testing.T) {
	build := func() *Graph {
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
		return g
	}

	hashOf := func(g *Graph) string {
		t.Helper()
		_, h, err := ProjectGroupPackage(g, groupUnitOf(t, g))
		if err != nil {
			t.Fatalf("projection: %v", err)
		}
		return h
	}

	// 两次编译之间 supply 的「内容」是运行期概念，编译期根本看不到它。
	// 这个断言锁的是：投影出的包里只有名字，没有任何随内容变化的东西。
	// 若实现顺手把内容也塞进 pkg（例如为了省一次 runner 取值），这条会红。
	if h1, h2 := hashOf(build()), hashOf(build()); h1 != h2 {
		t.Fatalf("PackageHash unstable: %q vs %q\n"+
			"the projection must carry supply NAMES only; anything content-derived "+
			"in the package destroys the one-compile-per-N-batches benefit", h1, h2)
	}

	// 反向：名单本身必须真的在包里，否则上面那条用一个空字段也能通过。
	g := build()
	pkg, _, err := ProjectGroupPackage(g, groupUnitOf(t, g))
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	if len(pkg.VisibleSupplies) != 1 || pkg.VisibleSupplies[0] != "rules" {
		t.Fatalf("VisibleSupplies = %v, want [rules]", pkg.VisibleSupplies)
	}
}

// groupUnitOf 取图里唯一的 group unit 下标。
func groupUnitOf(t *testing.T, g *Graph) int {
	t.Helper()
	for i := range g.units {
		if g.units[i].Kind == UnitGroup {
			return i
		}
	}
	t.Fatal("no group unit found")
	return -1
}
