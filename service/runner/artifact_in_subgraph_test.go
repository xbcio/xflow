package runner

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// The two script sources the artifacts under test carry. js is a language the
// digest path treats identically to wasm: ScriptNode.Execute reads
// Input.ArtifactCode BEFORE it dispatches on language (script.go), so a js
// artifact exercises the same resolver chain a 6.68 MiB wasm guest would --
// without paying a ~10s wasm build per test run.
//
// The doubling is the evidence the code actually RAN: a resolver that silently
// returned empty bytes, or a body that echoed its item through, would satisfy a
// "no error" assertion just as well as a correct one.
//
// They differ only in where the number comes from: a group member reads the
// group's own input, a map body member reads the per-item $item root the
// expansion hands it.
const (
	memberScriptSource   = `({doubled: $input.n * 2})`
	bodyItemScriptSource = `({doubled: $item.n * 2})`
)

// registerScriptForTest puts the real xflow.script handler on reg. The handler
// comes out of the production registry, not a stand-in: the defect under test is
// that the resolver never reaches the node, which a stub node could not observe.
func registerScriptForTest(t *testing.T, reg *execution.Registry) {
	t.Helper()
	h, ok := registry.Lookup("xflow.script")
	if !ok {
		t.Fatal("xflow.script is not registered; the node package's init did not run")
	}
	reg.RegisterGlobal("xflow.script", h)
}

// artifactResolverForTest returns the digest -> bytes resolver for src plus the
// digest the script node must ask for. It resolves EXACTLY one digest and errors
// on any other, so a test that passes by resolving the wrong artifact cannot.
func artifactResolverForTest(t *testing.T, src string) (func(ctx context.Context, digest string) ([]byte, error), string) {
	t.Helper()
	code := []byte(src)
	digest := store.ContentHash(code)
	return func(_ context.Context, want string) ([]byte, error) {
		if want != digest {
			t.Errorf("resolver asked for digest %q, want %q", want, digest)
			return nil, nil
		}
		return code, nil
	}, digest
}

// scriptMemberPackage builds a group package whose only member is an
// xflow.script that carries ONLY artifact_digest -- no inline code. That is the
// shape the SAS cross-env topology takes and the shape that reads
// Input.ArtifactCode.
func scriptMemberPackage(digest string) *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "s",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "s", Type: "xflow.script", Version: 1, Parameters: map[string]any{
					"language":        "js",
					"runtime":         "goja",
					"artifact_digest": digest,
				}},
				{Name: "__collector_s_main", Type: graph.NodeTypeGroupExit, Version: 1},
			},
			Connections: types.Connections{
				"s": {"main": types.PortConnections{Targets: []types.Connection{{Node: "__collector_s_main"}}}},
			},
		},
		Exits: []graph.SubgraphPackageExit{
			{CollectorNode: "__collector_s_main", SrcNode: "s", Port: "main"},
		},
		Requirements: []graph.Requirement{
			{NodeType: "xflow.script", NodeVersion: 1},
		},
	}
}

// mapMemberWithScriptBodyPackage builds a group package whose only member is an
// xflow.map whose BODY is the same artifact-backed script. This is one level
// deeper than scriptMemberPackage: the body runs on a backend the map-body
// executor builds, which is itself built from the group executor's newBackend
// closure. It is the recursion case -- the resolver has to travel through two
// nested backend constructions, not one.
func mapMemberWithScriptBodyPackage(digest string) *graph.SubgraphPackage {
	return &graph.SubgraphPackage{
		Version:   1,
		GroupName: "grp",
		EntryNode: "m",
		Def: &types.WorkflowDef{
			Name: "grp",
			Nodes: []types.NodeDef{
				{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{
					"items":      "$input.rows",
					"batch_size": 1,
					"body": map[string]any{
						"type": "xflow.subgraph",
						"parameters": map[string]any{
							"nodes": []any{
								map[string]any{"name": "s", "type": "xflow.script", "parameters": map[string]any{
									"language":        "js",
									"runtime":         "goja",
									"artifact_digest": digest,
								}},
							},
						},
					},
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
			{NodeType: "xflow.script", NodeVersion: 1},
		},
	}
}

func runGroupPackage(t *testing.T, rt *GroupRuntime, pkg *graph.SubgraphPackage, input *types.Input) engine.GroupResult {
	t.Helper()
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := rt.ExecuteRequest(ctx, subgraph.Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       input,
	})
	if err != nil {
		t.Fatalf("execute group: %v", err)
	}
	return res
}

// TestGroupRuntime_MemberScriptResolvesArtifact is the direct criterion for
// WithGroupArtifactCodeResolver: a group member that is an artifact-backed
// xflow.script must be able to fetch its code.
//
// Without the option wired into NewGroupRuntime's newBackend closure, the
// resolver chain ends at the group boundary -- Config.ArtifactCodeResolver
// reaches only the runner's top-level dispatcher, never the per-attempt inner
// backend this runtime builds -- and the member fails permanently with
// script.artifact_unavailable.
func TestGroupRuntime_MemberScriptResolvesArtifact(t *testing.T) {
	reg := execution.NewRegistry()
	registerScriptForTest(t, reg)

	resolve, digest := artifactResolverForTest(t, memberScriptSource)
	rt := NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSuspendDisabled(), WithGroupArtifactCodeResolver(resolve))

	res := runGroupPackage(t, rt, scriptMemberPackage(digest),
		&types.Input{Data: map[string]any{"n": 21.0}})

	if res.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("outcome = %s, want success; error = %q\n"+
			"a group member script could not fetch its artifact -- the resolver "+
			"did not reach the per-attempt inner backend", res.Outcome, res.Error)
	}
	if len(res.Exits) != 1 {
		t.Fatalf("exits = %d, want 1", len(res.Exits))
	}
	// Assert on the transform, not merely on success: only a script that both
	// fetched and executed its code produces 42.
	if got := asFloatForTest(res.Exits[0].Data["doubled"]); got != 42 {
		t.Fatalf("exit doubled = %v (%T), want 42 -- the artifact code did not run",
			res.Exits[0].Data["doubled"], res.Exits[0].Data["doubled"])
	}
}

// TestGroupRuntime_MapBodyScriptResolvesArtifact covers the recursion: the
// resolver must survive into the map-body executor's own inner backend, which
// execution/subgraph builds by reusing the SAME newBackend closure
// (NewMapBodyExecutor(e, ...) hands the executor to itself). If the closure read
// the resolver at construction rather than at call time, or if the map body
// built its backend from a different constructor, this fails while the
// single-level test above still passes.
func TestGroupRuntime_MapBodyScriptResolvesArtifact(t *testing.T) {
	reg := execution.NewRegistry()
	registerScriptForTest(t, reg)
	reg.RegisterGlobal("xflow.map", node.Map("$input.rows", 1))

	resolve, digest := artifactResolverForTest(t, bodyItemScriptSource)
	rt := NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSuspendDisabled(), WithGroupArtifactCodeResolver(resolve))

	res := runGroupPackage(t, rt, mapMemberWithScriptBodyPackage(digest),
		&types.Input{Data: map[string]any{"rows": []any{
			map[string]any{"n": 1.0},
			map[string]any{"n": 2.0},
		}}})

	if res.Outcome != engine.GroupOutcomeSuccess {
		t.Fatalf("outcome = %s, want success; error = %q\n"+
			"a script inside a map body could not fetch its artifact -- the resolver "+
			"did not survive the nested backend construction", res.Outcome, res.Error)
	}
	got := map[float64]bool{}
	for _, exit := range res.Exits {
		results, ok := exit.Data["results"].([]any)
		if !ok {
			continue
		}
		for _, r := range results {
			row, ok := r.(map[string]any)
			if !ok {
				continue
			}
			got[asFloatForTest(row["doubled"])] = true
		}
	}
	if !got[2] || !got[4] {
		t.Fatalf("map body produced doubled values %v, want 2 and 4 -- the artifact "+
			"code did not run for every item (exits: %#v)", got, res.Exits)
	}
}

// TestGroupRuntime_MemberScriptWithoutResolver pins the failure the fix removes,
// so a future regression is recognizable rather than merely red: with no
// resolver configured, the member must fail with script.artifact_unavailable —
// not hang, not silently succeed on empty code.
func TestGroupRuntime_MemberScriptWithoutResolver(t *testing.T) {
	reg := execution.NewRegistry()
	registerScriptForTest(t, reg)

	_, digest := artifactResolverForTest(t, memberScriptSource)
	rt := NewGroupRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSuspendDisabled())

	res := runGroupPackage(t, rt, scriptMemberPackage(digest),
		&types.Input{Data: map[string]any{"n": 21.0}})

	if res.Outcome != engine.GroupOutcomeFailed {
		t.Fatalf("outcome = %s, want failed: with no resolver the member cannot "+
			"fetch its code, so succeeding means it ran something else", res.Outcome)
	}
	if !strings.Contains(res.Error, "no artifact resolver is configured") {
		t.Fatalf("error = %q, want it to name the missing resolver "+
			"(script.artifact_unavailable)", res.Error)
	}
}

// TestSubgraphRuntime_BodyScriptResolvesArtifact is the batch-lease counterpart:
// a map whose batches are dispatched to a runner as separate leases runs through
// SubgraphRuntime, which builds its own per-item backend and therefore needs its
// own resolver option.
func TestSubgraphRuntime_BodyScriptResolvesArtifact(t *testing.T) {
	reg := execution.NewRegistry()
	registerScriptForTest(t, reg)

	resolve, digest := artifactResolverForTest(t, bodyItemScriptSource)
	rt := NewSubgraphRuntime(reg, NewPackageCache(PackageCacheConfig{MaxEntries: 4}),
		WithSubgraphArtifactCodeResolver(resolve))

	pkg := scriptMemberPackage(digest)
	pkg.GroupName = "m/body"
	pkg.Def.Name = "m/body"
	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	items := []any{map[string]any{"n": 3.0}}
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease-artifact"),
		LeaseToken: engine.LeaseToken("token-artifact"),
		Attempt:    1,
		Task: engine.Task{
			ExecutionID: types.ExecutionID("exec-artifact"),
			NodeName:    "m/_batch/0",
			Type:        engine.TaskTypeNodeBatch,
		},
		NodeType: "xflow.map",
		SubgraphPayload: &engine.SubgraphLeasePayload{
			ProtocolVersion: 1,
			ParentNode:      "m",
			PackageHash:     hash,
			Package:         pkg,
			BatchIndex:      0,
			BatchSize:       1,
			Items:           items,
			AllItems:        items,
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := rt.Execute(ctx, lease)
	if err != nil {
		t.Fatalf("execute batch: %v\n"+
			"the body script could not fetch its artifact -- the resolver did not "+
			"reach the per-item inner backend", err)
	}
	if out.Error != nil {
		t.Fatalf("batch error = %v, want none", out.Error)
	}
	// A batch result reports per-item rows under "items" (the group-level
	// aggregation under "results" happens one level up, at the map node).
	rows, _ := out.Output.Data["items"].([]any)
	if len(rows) != 1 {
		t.Fatalf("items = %#v, want one entry", out.Output.Data)
	}
	row, _ := rows[0].(map[string]any)
	if got := asFloatForTest(row["doubled"]); got != 6 {
		t.Fatalf("doubled = %v (%T), want 6 -- the artifact code did not run",
			row["doubled"], row["doubled"])
	}
}

// asFloatForTest normalizes the numeric type a js engine exports. goja yields
// int64 for integer-valued numbers and a JSON round-trip yields float64, so
// pinning one Go type would make the assertion depend on the transport rather
// than on the value.
func asFloatForTest(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int64:
		return float64(n)
	case int:
		return float64(n)
	default:
		return 0
	}
}
