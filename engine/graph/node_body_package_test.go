package graph

import (
	"testing"

	"github.com/xbcio/xflow/types"
)

func mapNodeWithBody(t *testing.T, members []any, conns map[string]any) types.NodeDef {
	t.Helper()
	body := map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": members,
		},
	}
	if conns != nil {
		body["parameters"].(map[string]any)["connections"] = conns
	}
	return types.NodeDef{
		Name: "m",
		Type: "xflow.map",
		Parameters: map[string]any{
			"items": "$input.rows",
			"body":  body,
		},
	}
}

// The body package is projected at compile time so every batch of the same map
// node shares one package and one hash. That is the whole benefit: the sub-graph
// executor's cache is keyed on the hash, so a stable hash means the body compiles
// once no matter how many batches ran.
func TestProjectNodeBodyPackageIsStoredOnTheCompiledGraph(t *testing.T) {
	nd := mapNodeWithBody(t, []any{
		map[string]any{"name": "step", "type": "test.echo"},
	}, nil)
	g, err := Compile(&types.WorkflowDef{Name: "map-body", Nodes: []types.NodeDef{nd}})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	idx, ok := g.NodeIndex("m")
	if !ok {
		t.Fatal("compiled graph has no node \"m\"")
	}
	body := g.BodyAt(idx)
	if body == nil {
		t.Fatal("compiled graph carries no body package for \"m\", so every batch would have to project its own")
	}
	if body.Hash == "" {
		t.Error("body package has no hash, so the executor's cache cannot key on it")
	}
	if body.Package == nil || body.Package.EntryNode != "step" {
		t.Errorf("body package entry = %+v, want \"step\"", body.Package)
	}
}

// expression 形态的 map 节点不能拿到包。引擎正是靠 BodyAt 为 nil 判定「这个节点
// 不扩展、handler 的返回值原样提交」——而 expression 形态的 handler 返回的是算完
// 的 results/count，不是扩展所需的批次描述符。凭空造一个空包会让引擎去扩展它，
// 把那份最终结果当成描述符。
//
// 两者皆无的 map 节点不在这里测，因为它已经在编译期被拒（见
// expansion_requires_body_test.go）。
func TestCompileLeavesAnExpressionMapNodeWithoutAPackage(t *testing.T) {
	g, err := Compile(&types.WorkflowDef{
		Name: "map-expression",
		Nodes: []types.NodeDef{{Name: "m", Type: "xflow.map", Parameters: map[string]any{
			"items":      "$input.rows",
			"expression": "$item.id",
		}}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	idx, _ := g.NodeIndex("m")
	if body := g.BodyAt(idx); body != nil {
		t.Errorf("expression-form map node got a package: %+v", body)
	}
}

// The hash must not move when only the authoring ORDER of the body's members
// changes. Two workflows whose bodies are identical up to node ordering describe
// the same computation, and a hash that moved would make the executor compile
// and cache the same body twice.
func TestProjectNodeBodyPackageHashIgnoresMemberOrder(t *testing.T) {
	members := []any{
		map[string]any{"name": "a", "type": "test.echo"},
		map[string]any{"name": "b", "type": "test.echo"},
	}
	reversed := []any{members[1], members[0]}
	conns := map[string]any{
		"a": map[string]any{"main": map[string]any{"targets": []any{
			map[string]any{"node": "b", "input": "main"},
		}}},
	}

	first, err := ProjectNodeBodyPackage("m", mapNodeWithBody(t, members, conns).Parameters, nil, nil)
	if err != nil {
		t.Fatalf("ProjectNodeBodyPackage(ordered) error = %v", err)
	}
	second, err := ProjectNodeBodyPackage("m", mapNodeWithBody(t, reversed, conns).Parameters, nil, nil)
	if err != nil {
		t.Fatalf("ProjectNodeBodyPackage(reversed) error = %v", err)
	}
	if first.Hash != second.Hash {
		t.Errorf("hash moved with member order: %q vs %q — the same body would compile and cache twice",
			first.Hash, second.Hash)
	}
}

// A body's terminal nodes are its exits: the body has no boundary declarations
// to consult, so whichever members have no outgoing "main" edge are what one
// item's result is made of. Getting this wrong means an item's result is empty.
func TestProjectNodeBodyPackageCollectsTerminalMembers(t *testing.T) {
	body, err := ProjectNodeBodyPackage("m", mapNodeWithBody(t, []any{
		map[string]any{"name": "a", "type": "test.echo"},
		map[string]any{"name": "b", "type": "test.echo"},
	}, map[string]any{
		"a": map[string]any{"main": map[string]any{"targets": []any{
			map[string]any{"node": "b", "input": "main"},
		}}},
	}).Parameters, nil, nil)
	if err != nil {
		t.Fatalf("ProjectNodeBodyPackage() error = %v", err)
	}
	if len(body.Package.Exits) != 1 {
		t.Fatalf("exits = %+v, want exactly one (only \"b\" is terminal)", body.Package.Exits)
	}
	if got := body.Package.Exits[0].SrcNode; got != "b" {
		t.Errorf("exit src = %q, want \"b\": \"a\" feeds \"b\" so it is not terminal", got)
	}
	// The collector must actually be wired, or it collects nothing.
	targets := body.Package.Def.Connections["b"]["main"].Targets
	if len(targets) != 1 || targets[0].Node != body.Package.Exits[0].CollectorNode {
		t.Errorf("terminal node \"b\" is not connected to its collector: %+v", targets)
	}
}

// A projected package must compile. The projection and the compiler are separate
// code paths, so a package that projects cleanly but fails to compile would only
// surface at runtime, on the first batch.
func TestProjectedNodeBodyPackageCompiles(t *testing.T) {
	body, err := ProjectNodeBodyPackage("m", mapNodeWithBody(t, []any{
		map[string]any{"name": "step", "type": "test.echo"},
	}, nil).Parameters, nil, nil)
	if err != nil {
		t.Fatalf("ProjectNodeBodyPackage() error = %v", err)
	}
	compiled, err := CompileProjectedPackage(body.Package)
	if err != nil {
		t.Fatalf("CompileProjectedPackage() error = %v", err)
	}
	if _, ok := compiled.NodeIndex("step"); !ok {
		t.Error("compiled body has no \"step\" node")
	}
	if _, ok := compiled.NodeIndex(body.Package.Exits[0].CollectorNode); !ok {
		t.Errorf("compiled body has no collector %q, so nothing captures the item's result",
			body.Package.Exits[0].CollectorNode)
	}
}

// Requirements are what a runner's capabilities are matched against. Collector
// nodes are the framework's own and present in every package, so requiring them
// would make every runner advertise an internal type it never implements.
func TestProjectNodeBodyPackageRequirementsExcludeCollectors(t *testing.T) {
	body, err := ProjectNodeBodyPackage("m", mapNodeWithBody(t, []any{
		map[string]any{"name": "step", "type": "test.echo"},
	}, nil).Parameters, nil, nil)
	if err != nil {
		t.Fatalf("ProjectNodeBodyPackage() error = %v", err)
	}
	if len(body.Package.Requirements) != 1 || body.Package.Requirements[0].NodeType != "test.echo" {
		t.Errorf("requirements = %+v, want only test.echo", body.Package.Requirements)
	}
}
