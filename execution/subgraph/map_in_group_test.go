package subgraph

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/types"
)

// groupMapFanoutHandler is the xflow.map node's own handler: it emits the
// batches descriptor, exactly as the production xflow.map node does. Whether
// those batches expand is not its call -- the engine reads that off the node's
// compiled body. It deliberately does NOT run the body itself; running the body
// is the engine's job, via the batch body executor this test is about.
type groupMapFanoutHandler struct{}

func (groupMapFanoutHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "xflow.map"}
}

func (groupMapFanoutHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	items := []any{1, 2, 3}
	return &types.Output{Data: map[string]any{
		"items":       items,
		"batches":     [][]any{{items[0]}, {items[1]}, {items[2]}},
		"batch_size":  1,
		"total":       3,
		"batch_count": 3,
	}}, nil
}

// tenXHandler is the map body's only member. It applies a recognizable
// transform (item * 10) so the assertions below can tell "the body ran" apart
// from "the items were passed through untouched" -- an expansion that routes
// its batches nowhere and simply echoes items would satisfy a count-only
// assertion just as well as a correct one.
type tenXHandler struct {
	mu   sync.Mutex
	seen []any
}

func (h *tenXHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.tenx"}
}

func (h *tenXHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	item := in.Data["$item"]
	h.mu.Lock()
	h.seen = append(h.seen, item)
	h.mu.Unlock()
	n, ok := item.(int)
	if !ok {
		return nil, fmt.Errorf("body member got $item %#v, want an int from the fan-out", item)
	}
	return &types.Output{Data: map[string]any{"tenx": n * 10}}, nil
}

func (h *tenXHandler) items() []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.seen...)
}

// buildGroupWithMapMemberPackage builds a projected GROUP package whose single
// member is an xflow.map node carrying a body. This is the shape
// SUBGRAPH-ENGINE-TODO P1-3 says cannot run: the group is executed by
// Executor.Execute on an inner engine, and a map member expands into a batch
// task on THAT inner engine.
func buildGroupWithMapMemberPackage(t *testing.T) *graph.SubgraphPackage {
	t.Helper()
	return &graph.SubgraphPackage{
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
								map[string]any{"name": "dbl", "type": "test.tenx"},
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
		},
	}
}

// TestExecutor_RunsAMapMemberBodyInsideAGroup is the P1-3 criterion. Two
// distinct defects had to be fixed for it to pass, and it fails on either one
// alone:
//
//  1. The inner engine Executor.Execute builds got no batch body executor, so a
//     member map's batch died with ErrNoBatchBodyExecutor. A caller-side
//     WithBatchBodyExecutor cannot reach it -- engineOpts is assembled fresh
//     inside Execute.
//  2. compileTrusted (the path CompileProjectedPackage uses for group packages)
//     never ran projectNodeBodies, so even with an executor wired the member's
//     NodeMeta.Body stayed nil and the batch died with ErrNoMapBody instead.
func TestExecutor_RunsAMapMemberBodyInsideAGroup(t *testing.T) {
	pkg := buildGroupWithMapMemberPackage(t)

	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", groupMapFanoutHandler{})
	body := &tenXHandler{}
	reg.RegisterGlobal("test.tenx", body)

	ex := NewExecutor(reg, NewPackageCache(PackageCacheConfig{
		MaxEntries: 4, MaxPackageBytes: 1 << 20,
	}), func() Backend { return local.New(local.WithRegistry(reg), local.WithConcurrency(1)) },
		WithMapConcurrencyLimiter(NewMapConcurrencyLimiter(1, 1)))

	hash, err := graph.ComputePackageHash(pkg)
	if err != nil {
		t.Fatalf("compute package hash: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	res, err := ex.Execute(ctx, Request{
		Package:     pkg,
		PackageHash: hash,
		Input:       &types.Input{Data: map[string]any{"items": []any{1, 2, 3}}},
	})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if res.Outcome != OutcomeSuccess {
		t.Fatalf("outcome = %v (error: %s), want success -- a map member of a group "+
			"could not run its body", res.Outcome, res.Error)
	}

	// The body must have run once per item. Assert on the recognizable transform,
	// not just the count: a pass-through expansion that never entered the body
	// would still report three batches completed.
	ran := body.items()
	if len(ran) != 3 {
		t.Fatalf("body member ran %d time(s) (saw %v), want 3 -- the group's map "+
			"member expanded but its body never executed", len(ran), ran)
	}
	seen := map[int]bool{}
	for _, item := range ran {
		n, ok := item.(int)
		if !ok {
			t.Fatalf("body member saw $item %#v, want an int", item)
		}
		seen[n] = true
	}
	if !seen[1] || !seen[2] || !seen[3] {
		t.Errorf("body member saw items %v, want each of 1, 2, 3 exactly once", ran)
	}

	// The transformed values must survive back out through the group's boundary
	// exit: it is not enough that the body ran somewhere, its output has to be
	// what the map node aggregated and what the collector reported.
	if len(res.Exits) == 0 {
		t.Fatal("group produced no exit results")
	}
	found := map[int]bool{}
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
			switch v := row["tenx"].(type) {
			case int:
				found[v] = true
			case float64:
				found[int(v)] = true
			}
		}
	}
	if !found[10] || !found[20] || !found[30] {
		t.Errorf("group exit carried tenx values %v, want 10, 20 and 30 -- the body's "+
			"transform did not reach the group's boundary output (exits: %#v)", found, res.Exits)
	}
}
