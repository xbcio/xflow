package control

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// taggerDigest is a syntactically valid artifact digest. The value is never
// resolved here — the derivation only copies it — but it is kept in the real
// "sha256:<64 hex>" shape so a test that later feeds it to
// RegisterSupplyConsumerByDigest (which validates the shape) does not have to
// invent a second constant.
const taggerDigest = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

// wasmConsumerDef builds a workflow shaped like the SAS tagging pipeline: a
// remote-hosted kafka trigger feeding a wasm script node that depends on a
// supply. The script node carries artifact_digest (the ScriptFile/artifact
// path), which is what makes it bindable.
func wasmConsumerDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger,
				RunnerSelector: &types.RunnerSelector{MatchLabels: map[string]string{"role": "ingest"}}},
			{Name: "tag", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"language":        "wasm",
					"runtime":         "wazero-reactor",
					"artifact_digest": taggerDigest,
				}},
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{"resource": "shared-rules"}},
		},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tag", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "tag", Supply: "rules"}},
	}
}

// mustCompile compiles def or fails. Every test in this file needs the same
// three lines; a second copy is how they drift. graph.Compile takes a POINTER
// (engine/graph/compile.go:151) and every fixture in this package already
// returns one, so nothing here dereferences.
func mustCompile(t *testing.T, def *types.WorkflowDef) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

// setScriptArtifactDigest rewrites artifact_digest on every xflow.script node in
// def, at the top level and inside any map body. It fails when it rewrites
// nothing, so a change to the fixture's shape cannot turn this into a test that
// asserts the default value.
//
// The map-body branch matches mapBodyConsumerDef's actual raw authoring shape
// (map_body_supply_consumer_test.go): def.Nodes[i].Parameters["body"] is
// map[string]any{"type": "xflow.subgraph", "parameters": map[string]any{"nodes":
// []any{map[string]any{...}}}} -- NOT []types.NodeDef. That only exists after
// compilation, inside the projected NodeBodyPackage; the raw DSL body is still
// untyped any-maps. Task 3's own tests only use wasmConsumerDef(), whose script
// node is top-level, so this branch is insurance rather than load-bearing here.
func setScriptArtifactDigest(t *testing.T, def *types.WorkflowDef, value string) {
	t.Helper()
	n := 0
	rewriteParams := func(params map[string]any) {
		if params == nil {
			return
		}
		if _, ok := params["artifact_digest"]; !ok {
			return
		}
		params["artifact_digest"] = value
		n++
	}
	for i := range def.Nodes {
		if def.Nodes[i].Type == "xflow.script" {
			rewriteParams(def.Nodes[i].Parameters)
		}
		body, _ := def.Nodes[i].Parameters["body"].(map[string]any)
		if body == nil {
			continue
		}
		bodyParams, _ := body["parameters"].(map[string]any)
		if bodyParams == nil {
			continue
		}
		rawNodes, _ := bodyParams["nodes"].([]any)
		for _, raw := range rawNodes {
			nodeMap, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if nodeMap["type"] != "xflow.script" {
				continue
			}
			nodeParams, _ := nodeMap["parameters"].(map[string]any)
			rewriteParams(nodeParams)
		}
	}
	if n == 0 {
		t.Fatal("setScriptArtifactDigest rewrote nothing; the fixture shape changed " +
			"and this test would assert against the fixture's own default")
	}
}

// groupedWasmConsumerDef mirrors wasmConsumerDef's topology (a trigger feeding a
// wasm script node that depends on a supply) but collapses the trigger and the
// script node into a co-location group named "tagging-group". The GroupDef
// shape here is copied verbatim from TestSupplyConsumerBindingsForGroupEntryUnit
// below, which is the one place in this package that already builds a group.
func groupedWasmConsumerDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger},
			{Name: "tag", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"language":        "wasm",
					"runtime":         "wazero-reactor",
					"artifact_digest": taggerDigest,
				}},
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply,
				Parameters: map[string]any{"resource": "shared-rules"}},
		},
		Groups: []types.GroupDef{{
			Name:           "tagging-group",
			Members:        []string{"src", "tag"},
			RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"role": "ingest"}},
		}},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tag", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "tag", Supply: "rules"}},
	}
}

// TestBindingsCarryNodeIdentityNotDigest is the §4.2 change in one assertion:
// what goes on the wire is who consumes what, not which module. A digest read
// at compile time is a template string whenever artifact_digest is an
// expression, and the runner would try to compile it.
func TestBindingsCarryNodeIdentityNotDigest(t *testing.T) {
	g := mustCompile(t, wasmConsumerDef())
	got := SupplyConsumerBindingsForEntryUnit(g, 0)
	if len(got) == 0 {
		t.Fatal("no bindings derived; this test would be asserting nothing")
	}
	for _, b := range got {
		if b.ModuleDigest != "" {
			t.Fatalf("binding %+v still carries a compile-time digest", b)
		}
		if !b.IsDeclaration() {
			t.Fatalf("binding %+v is not a declaration (workflow=%q node=%q)",
				b, b.WorkflowName, b.NodeName)
		}
		if b.DigestExpr == "" {
			t.Fatalf("binding %+v has no DigestExpr; the warm-up consumer has "+
				"nothing to render and §4.5 pre-compilation cannot happen", b)
		}
	}
}

// TestExpressionDigestStillProducesBindings is §9.3 probe (1). Before this
// change an expression-valued artifact_digest produced a binding carrying the
// template string, which made the runner's registerSupplyConsumers fail and the
// whole activation fail closed. It must now produce a clean declaration.
//
// wasmConsumerDef() alone will not compile with this expression: validateSupplyUsage
// (engine/graph/dependency.go:205) statically scans every node's params for
// "$supplies.<name>" tokens and rejects any name that has no declared
// dependency edge -- so the expression's "wasm_versions" name needs a real
// supply node and edge, exactly like TestSupplyConsumerBindingsOnePerSupply
// adds "aaa" for the same reason. This is not part of the fixture's own
// default shape (setScriptArtifactDigest's n==0 guard would still fire without
// it, since it only rewrites the existing artifact_digest param).
func TestExpressionDigestStillProducesBindings(t *testing.T) {
	def := wasmConsumerDef() // returns *types.WorkflowDef
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "wasm_versions", Type: "xflow.supply.external", Kind: types.NodeKindSupply})
	def.DependencyEdges = append(def.DependencyEdges,
		types.DependencyEdge{Node: "tag", Supply: "wasm_versions"})
	const expr = "${{ $supplies.wasm_versions.decode.digest }}"
	setScriptArtifactDigest(t, def, expr)

	g := mustCompile(t, def)
	got := SupplyConsumerBindingsForEntryUnit(g, 0)
	if len(got) == 0 {
		t.Fatal("an expression-valued artifact_digest produced no bindings at all; " +
			"the node would run source-driven with nobody registered")
	}
	for _, b := range got {
		if b.DigestExpr != expr {
			t.Fatalf("DigestExpr = %q, want the verbatim template %q", b.DigestExpr, expr)
		}
	}
}

// TestGroupedNodeAttributedToGroupName is §4.2.2. A wasm node projected into a
// group runs inside the group's inner engine, whose graph name is the GROUP's
// name (graph/subgraph_package.go sets Def.Name = GroupMeta.Name). Attributing
// it to the workflow name would produce a declaration the executing node never
// matches -- it would find no supplies in the runner's table and, with §4.3.1
// in place, fail every execution.
func TestGroupedNodeAttributedToGroupName(t *testing.T) {
	g := mustCompile(t, groupedWasmConsumerDef())
	got := SupplyConsumerBindingsForEntryUnit(g, 0)
	if len(got) == 0 {
		t.Fatal("no bindings derived for the grouped wasm node")
	}
	for _, b := range got {
		if b.WorkflowName != "tagging-group" {
			t.Fatalf("WorkflowName = %q, want the GROUP name %q", b.WorkflowName, "tagging-group")
		}
	}
}

// TestDeriveEntryActivationsCarriesSupplyConsumerBindings is the server half of
// the wiring: Supplies alone tells the runner WHICH content to fetch, and
// nothing downstream retains WHO consumes it — the directive carries a flat
// supply list and a projected group package flattens every member's supply refs
// into one deduplicated name set. Without this derivation the runner fetches the
// content and hands it to nobody, and the wasm module silently evaluates against
// no rules.
func TestDeriveEntryActivationsCarriesSupplyConsumerBindings(t *testing.T) {
	g, err := graph.Compile(wasmConsumerDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1 (the trigger)", len(units))
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "wf", NodeName: "tag", SupplyNode: "rules", DigestExpr: taggerDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want %+v", units[0].SupplyConsumers, want)
	}
}

// A wasm node consuming two supplies must produce one binding per supply: the
// registration key is (module, supply node), so a module that consumes two
// supplies needs two registrations to be notified of both.
func TestSupplyConsumerBindingsOnePerSupply(t *testing.T) {
	def := wasmConsumerDef()
	def.Nodes = append(def.Nodes,
		types.NodeDef{Name: "aaa", Type: "xflow.supply.external", Kind: types.NodeKindSupply})
	def.DependencyEdges = append(def.DependencyEdges,
		types.DependencyEdge{Node: "tag", Supply: "aaa"})
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "wf", NodeName: "tag", SupplyNode: "aaa", DigestExpr: taggerDigest},
		{WorkflowName: "wf", NodeName: "tag", SupplyNode: "rules", DigestExpr: taggerDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want %+v (sorted, one per supply)", units[0].SupplyConsumers, want)
	}
}

// A wasm node carrying inline code has no artifact digest, and computing one
// server-side would create a second module-identity source that could drift
// from the runtime's own. Such a node yields no binding — and, critically, does
// not poison the rest of the unit's bindings.
func TestSupplyConsumerBindingsSkipInlineCodeNode(t *testing.T) {
	def := wasmConsumerDef()
	def.Nodes = append(def.Nodes, types.NodeDef{
		Name: "tag_inline", Type: "xflow.script", Kind: types.NodeKindAction,
		Parameters: map[string]any{
			"language": "wasm",
			"runtime":  "wazero-reactor",
			"code":     "AGFzbQEAAAA=", // inline base64 module, no artifact_digest
		}})
	def.Connections["tag"] = map[string]types.PortConnections{
		"main": {Targets: []types.Connection{{Node: "tag_inline", Input: "main"}}},
	}
	def.DependencyEdges = append(def.DependencyEdges,
		types.DependencyEdge{Node: "tag_inline", Supply: "rules"})

	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "wf", NodeName: "tag", SupplyNode: "rules", DigestExpr: taggerDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want only the artifact-backed node's binding %+v",
			units[0].SupplyConsumers, want)
	}
	// The supply itself is still required: the inline node consumes it through
	// the legacy globals path, so the readiness gate must still fetch it.
	if len(units[0].Supplies) != 1 || units[0].Supplies[0].Node != "rules" {
		t.Fatalf("Supplies = %+v, want the rules requirement to survive", units[0].Supplies)
	}
}

// Only wasm modules have a supply-driven instance pool. A js script node
// depending on a supply reads it as $supplies during evaluation and needs no
// consumer registration; producing a binding for it would try to register a
// non-existent wasm module.
func TestSupplyConsumerBindingsSkipNonWasmScript(t *testing.T) {
	def := wasmConsumerDef()
	def.Nodes[1].Parameters = map[string]any{
		"language":        "js",
		"runtime":         "goja",
		"artifact_digest": taggerDigest,
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if units[0].SupplyConsumers != nil {
		t.Fatalf("SupplyConsumers = %#v, want nil for a js script node", units[0].SupplyConsumers)
	}
}

// A unit with no wasm consumer must produce a nil slice, not an empty one: the
// directive's omitempty and the byte-for-byte stability of existing directives
// both depend on it.
func TestNoWasmConsumerMeansNilSlice(t *testing.T) {
	g, err := graph.Compile(supplyGatedDef()) // its script node has code, not artifact_digest
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if units[0].SupplyConsumers != nil {
		t.Fatalf("SupplyConsumers = %#v, want nil", units[0].SupplyConsumers)
	}
}

// The group path must derive bindings too, and it is the path that NEEDS the
// server-side derivation most: graph.ProjectGroupPackage flattens every
// member's supply refs into one deduplicated name list, so by the time the
// package reaches a runner the consumer-to-supply pairing is gone for good.
func TestSupplyConsumerBindingsForGroupEntryUnit(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf-group",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "kafka.source", Kind: types.NodeKindTrigger},
			{Name: "tag", Type: "xflow.script", Kind: types.NodeKindAction,
				Parameters: map[string]any{
					"language":        "wasm",
					"runtime":         "wazero-reactor",
					"artifact_digest": taggerDigest,
				}},
			{Name: "rules", Type: "xflow.supply.external", Kind: types.NodeKindSupply},
		},
		Groups: []types.GroupDef{{
			Name:           "g",
			Members:        []string{"src", "tag"},
			RunnerSelector: &types.RunnerSelector{Mode: types.RunnerSelectorModeRequired, MatchLabels: map[string]string{"role": "ingest"}},
		}},
		Connections: map[string]map[string]types.PortConnections{
			"src": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tag", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "tag", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	units, err := DeriveEntryActivations(g)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	if len(units) != 1 {
		t.Fatalf("units = %d, want 1 (the group)", len(units))
	}
	if units[0].NodeType != "xflow.group" {
		t.Fatalf("NodeType = %q, want xflow.group", units[0].NodeType)
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "g", NodeName: "tag", SupplyNode: "rules", DigestExpr: taggerDigest},
	}
	if !reflect.DeepEqual(units[0].SupplyConsumers, want) {
		t.Fatalf("SupplyConsumers = %+v, want %+v", units[0].SupplyConsumers, want)
	}
}

// The bindings must survive both writes to the durable record, or the
// reconciler has nothing to put on the directive.
func TestAddOrUpdateAndRemoveWorkflowPreserveSupplyConsumers(t *testing.T) {
	st := NewMemoryEntryActivationStore()
	m := NewEntryActivationManager(st)
	g, err := graph.Compile(wasmConsumerDef())
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	ctx := context.Background()
	key := engine.EntryActivationKey{
		Namespace: namespace.Default, WorkflowID: "wf-1", WorkflowVersion: "v1", EntryUnitID: "src",
	}
	want := []engine.SupplyConsumerBinding{
		{WorkflowName: "wf", NodeName: "tag", SupplyNode: "rules", DigestExpr: taggerDigest},
	}

	if err := m.AddOrUpdateWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("AddOrUpdateWorkflow: %v", err)
	}
	got, ok, err := st.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after add: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(got.SupplyConsumers, want) {
		t.Fatalf("persisted SupplyConsumers = %+v, want %+v", got.SupplyConsumers, want)
	}

	// RemoveWorkflow rewrites the record as non-desired; dropping the bindings
	// there would leave a deactivating record the reconciler cannot describe.
	if err := m.RemoveWorkflow(ctx, namespace.Default, "wf-1", "v1", g); err != nil {
		t.Fatalf("RemoveWorkflow: %v", err)
	}
	got, ok, err = st.Get(ctx, key)
	if err != nil || !ok {
		t.Fatalf("Get after remove: ok=%v err=%v", ok, err)
	}
	if got.Desired {
		t.Fatal("expected Desired=false after RemoveWorkflow")
	}
	if !reflect.DeepEqual(got.SupplyConsumers, want) {
		t.Fatalf("RemoveWorkflow wiped SupplyConsumers: got %+v", got.SupplyConsumers)
	}
}
