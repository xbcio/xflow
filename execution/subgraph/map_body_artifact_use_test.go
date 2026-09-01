package subgraph

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"

	// Blank-imported so its init() registers "xflow.script" with
	// node/registry's global registry -- node/internal/code/script is not
	// importable from here directly (it's an internal package outside
	// node/), so the real handler can only be reached through node/registry
	// after this side effect runs.
	_ "github.com/xbcio/xflow/node"
	noderegistry "github.com/xbcio/xflow/node/registry"
)

// artifactChainBodyParams is the map body both tests share: a two-member
// chain, decode -> clean, each an xflow.script node attesting its own digest.
// decode's output feeds clean's $input; clean does not need to read it for the
// artifact-use assertions, but doing so proves this is a real chain rather
// than two unconnected members that happen to share a batch.
func artifactChainBodyParams() map[string]any {
	return map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{
				map[string]any{"name": "decode", "type": "xflow.script", "parameters": map[string]any{
					"language":        "js",
					"runtime":         "goja",
					"code":            `({value: $item})`,
					"artifact_digest": "sha256:decode-aa",
				}},
				map[string]any{"name": "clean", "type": "xflow.script", "parameters": map[string]any{
					"language":        "js",
					"runtime":         "goja",
					"code":            `({value: $input.value * 2})`,
					"artifact_digest": "sha256:clean-bb",
				}},
			},
			"connections": map[string]any{
				"decode": map[string]any{
					"main": []any{map[string]any{"node": "clean", "input": "main"}},
				},
			},
		},
	}
}

// expectedArtifactUses is the exact, sorted (node-ascending) shape both tests
// must observe: two distinct entries, one per guest in the chain, regardless
// of how many of the 5 items ran through them.
func expectedArtifactUses() []any {
	return []any{
		map[string]any{"node": "clean", "digest": "sha256:clean-bb"},
		map[string]any{"node": "decode", "digest": "sha256:decode-aa"},
	}
}

// scriptHandlerForTest resolves the real xflow.script handler through
// node/registry.Lookup -- the only import-legal seam from this package (see
// node/internal/code/script/artifact_use_test.go for the identical pattern
// one layer down).
func scriptHandlerForTest(t *testing.T) types.ActionHandler {
	t.Helper()
	h, ok := noderegistry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script not registered -- node package side-effect import missing")
	}
	return h
}

// fiveItemMapFanoutHandler is a fake xflow.map node handler that always emits
// the same 5 items as a SINGLE batch. Everything must be in one batch: the
// engine seeds a fresh ArtifactUseCollector per batch (engine.runBatchBody),
// so spreading the items across several batches would never let more than one
// item's Record calls contend on the same collector, making body_concurrency
// (and the -race check it exists for) toothless.
type fiveItemMapFanoutHandler struct{}

func (fiveItemMapFanoutHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (fiveItemMapFanoutHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	items := []any{1, 2, 3, 4, 5}
	return &types.Output{Data: map[string]any{
		"items":       items,
		"batches":     [][]any{items},
		"batch_size":  5,
		"total":       5,
		"batch_count": 1,
	}}, nil
}

// echoOKHandler is a trivial terminal node so the map node is not the
// execution's only member -- matching every other harness in this repo that
// drives a map node to completion.
type echoOKHandler struct{}

func (echoOKHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.echo"} }
func (echoOKHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

// asIntForTest normalizes count-like values that may arrive as int (native
// in-process path) or float64 (a path that round-tripped through JSON). The
// local in-memory backend used here keeps native Go types, but this mirrors
// the same defensive read used by other tests in this repo (see
// sdk/xflow/map_body_artifact_test.go's asFloatForMapBodyTest) rather than
// assuming this test's harness will never change.
func asIntForTest(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	default:
		return 0, false
	}
}

// TestMapBodyChainAccumulatesArtifactUsesAcrossBothGuests pins spec §7.1.2 at
// the one layer that can observe it end to end: a map node with a real
// decode -> clean body, run to completion on a PLAIN top-level engine (the map
// node is NOT a group member here). This deliberately does not cross the
// group/Collector boundary (see withoutExecutionRoots in collector.go) --
// TestMapInGroupArtifactUsesSurviveTheGroupBoundary below is the one that
// does. Keeping them separate means a wrong top-level key name (e.g. "$artifacts")
// could still pass THIS test since no root-stripping happens here, but would
// be caught by the other one.
func TestMapBodyChainAccumulatesArtifactUsesAcrossBothGuests(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", fiveItemMapFanoutHandler{})
	reg.RegisterGlobal("xflow.script", scriptHandlerForTest(t))
	reg.RegisterGlobal("test.echo", echoOKHandler{})

	b := local.New(local.WithRegistry(reg), local.WithConcurrency(4))
	bodies := NewMapBodyExecutor(
		NewExecutor(reg, NewPackageCache(PackageCacheConfig{}),
			func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(5)) }),
		false, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-artifact-chain",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items":            "$input.items",
				"body_concurrency": 5,
				"body":             artifactChainBodyParams(),
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"m": {"main": types.PortConnections{Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, map[string]any{"items": []any{1, 2, 3, 4, 5}})
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	res, err := b.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone: %v", err)
	}
	if res.Status != types.ExecutionStatusSuccess {
		snap, _ := b.State().GetNode(ctx, id, "m")
		errText := ""
		if snap != nil {
			errText = snap.Error
		}
		t.Fatalf("execution status = %v, want success; map node error = %s", res.Status, errText)
	}

	mOut, ok := res.Output["m"].(map[string]any)
	if !ok {
		t.Fatalf("res.Output[%q] = %#v, want map[string]any", "m", res.Output["m"])
	}

	// 1. The body actually ran 5 times.
	count, ok := asIntForTest(mOut["count"])
	if !ok || count != 5 {
		t.Fatalf("count = %#v, want 5", mOut["count"])
	}
	results, ok := mOut["results"].([]any)
	if !ok || len(results) != 5 {
		t.Fatalf("results = %#v, want a 5-element array", mOut["results"])
	}

	// 2. Exactly the two distinct (node, digest) pairs, sorted node-ascending,
	// regardless of the body having run 5 times.
	got := mOut["artifacts"]
	want := expectedArtifactUses()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("artifacts = %#v, want %#v", got, want)
	}

	// 3. O(distinct artifacts), not O(items): no per-item copy of the manifest.
	row0, ok := results[0].(map[string]any)
	if !ok {
		t.Fatalf("results[0] = %#v, want map[string]any", results[0])
	}
	if _, present := row0["artifacts"]; present {
		t.Fatalf("results[0] = %#v carries its own \"artifacts\" key; the manifest must be "+
			"a sibling of results, not copied into every item", row0)
	}
}

// buildArtifactChainGroupPackage builds a projected GROUP package whose single
// member is the same xflow.map + decode/clean body as the test above, wrapped
// in a group exit collector -- SAS's real topology (map node as a group
// member), copied from buildGroupWithMapMemberPackage in map_in_group_test.go.
func buildArtifactChainGroupPackage(t *testing.T) *graph.SubgraphPackage {
	t.Helper()
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "artifact-grp",
		EntryNode: "m",
		Def: &types.WorkflowDef{
			Name: "artifact-grp",
			Nodes: []types.NodeDef{
				{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{
					"items":            "$input.items",
					"body_concurrency": 5,
					"body":             artifactChainBodyParams(),
				}},
				{Name: "__collector_m_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"m": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_m_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_m_main", SrcNode: "m", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "xflow.map", NodeVersion: 1},
		},
	}
}

// TestMapInGroupArtifactUsesSurviveTheGroupBoundary pins SAS's real topology:
// the map node is a GROUP MEMBER, so its output crosses the group exit's
// withoutExecutionRoots (collector.go) on the way out. That function strips
// every "$"-prefixed key -- which is exactly why the plan's naming ruling
// picked the bare key "artifacts" over "$artifacts": a "$"-prefixed manifest
// would never reach the group's exit.
//
// If artifacts disappears crossing this boundary, that is not this test's bug
// to route around -- see the package-level note below for how that is
// reported.
func TestMapInGroupArtifactUsesSurviveTheGroupBoundary(t *testing.T) {
	pkg := buildArtifactChainGroupPackage(t)

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", fiveItemMapFanoutHandler{})
	reg.RegisterGlobal("xflow.script", scriptHandlerForTest(t))

	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(5)) })

	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"items": []any{1, 2, 3, 4, 5}}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v (error: %s), want success", res.Outcome, res.Error)
	}
	if len(res.Exits) == 0 {
		t.Fatal("group produced no exit results")
	}

	// Find the exit's artifacts. Per the plan's ruling this must be the bare
	// key "artifacts" on the exit's Data -- NOT "$artifacts", which
	// withoutExecutionRoots would have stripped before it ever reached here.
	var (
		found    bool
		got      any
		exitData map[string]any
	)
	for _, exit := range res.Exits {
		if v, ok := exit.Data["artifacts"]; ok {
			found = true
			got = v
			exitData = exit.Data
			break
		}
	}
	if !found {
		t.Fatalf("BLOCKED: no group exit carried an \"artifacts\" key; the group boundary "+
			"appears to have dropped the host-attested artifact-use manifest. Observed exits: %#v",
			res.Exits)
	}

	want := expectedArtifactUses()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("group exit artifacts = %#v, want %#v (exit data: %#v)", got, want, exitData)
	}
}
