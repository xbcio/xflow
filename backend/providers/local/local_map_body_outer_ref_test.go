package local

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/execution"
	"github.com/xbcio/xflow/execution/subgraph"
	"github.com/xbcio/xflow/types"
)

// outerRefMemberHandler records what each body member's parameters evaluated to.
// The parameters read the outer node through ?? defaults, so a missing snapshot
// produces the sentinel string rather than an evaluation error: the failure then
// says WHAT the body saw instead of only that the node failed.
type outerRefMemberHandler struct {
	mu   sync.Mutex
	seen map[string][]any
}

func newOuterRefMemberHandler() *outerRefMemberHandler {
	return &outerRefMemberHandler{seen: map[string][]any{}}
}

func (h *outerRefMemberHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.outer_ref_member"}
}

func (h *outerRefMemberHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen[in.NodeName] = append(h.seen[in.NodeName], in.Params["saw"])
	h.mu.Unlock()
	// "tag" echoes the evaluated parameter so a SIBLING member can read this
	// member's output through $nodes and get a string back -- a bool would make
	// the sibling's concatenation fail on type rather than on the missing value
	// under test.
	return &types.Output{Data: map[string]any{"ok": true, "tag": in.Params["saw"]}}, nil
}

func (h *outerRefMemberHandler) rows(node string) []any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]any(nil), h.seen[node]...)
}

// sourceHandler produces the outer-graph output a body reads across the domain
// boundary. It also emits the map node's items, so "src" is simultaneously the
// map's upstream edge and the target of the cross-domain reference -- which is
// the shape the DSL spec's rule describes (a deterministic ancestor of the loop
// node).
type sourceHandler struct{}

func (sourceHandler) Descriptor() types.Descriptor { return types.Descriptor{Type: "test.source"} }

func (sourceHandler) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{
		"region": "eu-west-1",
		"items":  []any{map[string]any{"id": 1}, map[string]any{"id": 2}},
	}}, nil
}

// TestMapBodyMemberReadsAnOuterNodeSnapshot is the end-to-end assertion for the
// spec's cross-domain read: a body member writing $nodes['src'] must actually
// receive src's OUTPUT, not nil.
//
// Compiling is only half the feature and the less useful half. Once
// validateBodyOuterRefs permits the reference, a body with no data channel gets
// a nil $nodes entry, the spec's own ?? guard swallows it, and the workflow
// produces a plausible wrong answer with no diagnostic anywhere. That failure
// mode is strictly worse than the compile error it replaced, which is why the
// permission and this channel belong to the same change.
//
// Both members read the reference, not just the entry member: the snapshot has
// to be an EXECUTION-level property of the sub-execution. Delivering it as the
// inner submission's params would reach only the entry member -- the exact
// defect the $item/$index/$items roots hit (see local_map_body_chain_test.go).
func TestMapBodyMemberReadsAnOuterNodeSnapshot(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	reg.RegisterGlobal("test.source", sourceHandler{})
	members := newOuterRefMemberHandler()
	reg.RegisterGlobal("test.outer_ref_member", members)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	memberParams := map[string]any{
		"saw": "${{ ($nodes['src'].region ?? 'MISS-src') }}",
	}
	def := &types.WorkflowDef{
		Name: "map-body-outer-ref",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.source"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "first", "type": "test.outer_ref_member", "parameters": memberParams},
							map[string]any{"name": "second", "type": "test.outer_ref_member", "parameters": memberParams},
						},
						"connections": map[string]any{
							"first": map[string]any{
								"main": []any{map[string]any{"node": "second", "input": "main"}},
							},
						},
					},
				},
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"src": {"main": {Targets: []types.Connection{{Node: "m", Input: "main"}}}},
			"m":   {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
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

	// mapFanoutHandler emits two items, one per batch, so each member runs twice
	// and every one of those runs must have seen the snapshot.
	for _, node := range []string{"first", "second"} {
		rows := members.rows(node)
		if len(rows) != 2 {
			t.Fatalf("body member %q ran %d times (%#v), want 2", node, len(rows), rows)
		}
		for i, got := range rows {
			if got != "eu-west-1" {
				t.Errorf("body member %q run %d saw $nodes['src'].region = %#v, want %q -- "+
					"the reference compiles, so a MISS here means it was permitted without "+
					"a data channel and the ?? guard is hiding it", node, i, got, "eu-west-1")
			}
		}
	}
}

// TestMapBodyMemberReadsBothASiblingAndAnOuterNode covers the one place the two
// $nodes sources meet: a member that reads BOTH a body sibling and an outer
// ancestor.
//
// The two arrive by different routes -- the sibling is fetched from the INNER
// execution's state store by prefetchNodesRefs, the ancestor rides the scope
// from the outer snapshot -- and both land in the same Input.Nodes map. A
// prefetch that assigned that map instead of merging into it silently dropped
// the ancestor for exactly these members, which is invisible in a body where no
// member reads a sibling.
func TestMapBodyMemberReadsBothASiblingAndAnOuterNode(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	reg.RegisterGlobal("test.source", sourceHandler{})
	members := newOuterRefMemberHandler()
	reg.RegisterGlobal("test.outer_ref_member", members)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	def := &types.WorkflowDef{
		Name: "map-body-sibling-and-outer",
		Nodes: []types.NodeDef{
			{Name: "src", Type: "test.source"},
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "first", "type": "test.outer_ref_member",
								"parameters": map[string]any{"saw": "first-ran"}},
							// Both roots in one expression, so a dropped ancestor
							// cannot be masked by the sibling read succeeding.
							map[string]any{"name": "second", "type": "test.outer_ref_member",
								"parameters": map[string]any{
									"saw": "${{ ($nodes['src'].region ?? 'MISS-src') + '/' + ($nodes['first'].tag ?? 'MISS-first') }}",
								}},
						},
						"connections": map[string]any{
							"first": map[string]any{
								"main": []any{map[string]any{"node": "second", "input": "main"}},
							},
						},
					},
				},
			}},
			{Name: "done", Type: "test.echo"},
		},
		Connections: types.Connections{
			"src": {"main": {Targets: []types.Connection{{Node: "m", Input: "main"}}}},
			"m":   {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := eng.Submit(ctx, g, nil)
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

	rows := members.rows("second")
	if len(rows) != 2 {
		t.Fatalf("body member \"second\" ran %d times (%#v), want 2", len(rows), rows)
	}
	for i, got := range rows {
		if got != "eu-west-1/first-ran" {
			t.Errorf("run %d saw %#v, want %q -- a MISS-src means the sibling prefetch "+
				"overwrote the outer snapshot instead of merging with it",
				i, got, "eu-west-1/first-ran")
		}
	}
}
