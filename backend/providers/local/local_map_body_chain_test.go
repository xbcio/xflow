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

// chainMemberHandler records the loop roots each of its invocations was handed,
// keyed by the node name that ran. A body with more than one member needs
// per-member evidence: the defect this file guards showed up as the SECOND
// member never running at all, so an assertion that only counted invocations
// would have to know which member it was counting.
type chainMemberHandler struct {
	mu   sync.Mutex
	seen map[string][]map[string]any
}

func newChainMemberHandler() *chainMemberHandler {
	return &chainMemberHandler{seen: map[string][]map[string]any{}}
}

func (h *chainMemberHandler) Descriptor() types.Descriptor {
	return types.Descriptor{Type: "test.chain_member"}
}

func (h *chainMemberHandler) Execute(_ context.Context, in *types.Input) (*types.Output, error) {
	h.mu.Lock()
	h.seen[in.NodeName] = append(h.seen[in.NodeName], map[string]any{
		"item":  in.Params["saw_item"],
		"index": in.Params["saw_index"],
		"items": in.Params["saw_items"],
	})
	h.mu.Unlock()
	return &types.Output{Data: map[string]any{"ok": true}}, nil
}

func (h *chainMemberHandler) rows(node string) []map[string]any {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]map[string]any(nil), h.seen[node]...)
}

// TestMapBodyNonEntryMemberSeesLoopRoots guards the loop roots for a body member
// that is NOT the body's entry node.
//
// The spec says $item/$index/$items are available "仅在 body 内可用" -- inside the
// body, not at the body's entry. They used to travel as the inner submission's
// PARAMS (bodyItemInput put them in Input.Data, Executor passed Data as params),
// and engine/input.go only reads snap.Params for a node with ZERO in-edges. So
// the entry member saw all three roots and every downstream member saw none:
//
//	node "second" (test.chain_member): template evaluation failed for parameter
//	"saw_index": compile expression: unknown name $index (1:1)
//
// which failed the batch, the map node, and the whole execution. Every existing
// body test used a single-member body, which is the one shape that cannot catch
// this.
//
// The assertions are on the observed parameter VALUES, not on an error string:
// before the fix "second" is never invoked at all, so an error-message assertion
// would be checking a symptom that a later refactor could move.
func TestMapBodyNonEntryMemberSeesLoopRoots(t *testing.T) {
	reg := execution.NewRegistry()
	reg.RegisterGlobal("xflow.map", &mapFanoutHandler{})
	reg.RegisterGlobal("test.echo", &batchEchoHandler{})
	members := newChainMemberHandler()
	reg.RegisterGlobal("test.chain_member", members)

	b := New(WithConcurrency(2), WithRegistry(reg))
	bodies := subgraph.NewMapBodyExecutor(
		subgraph.NewExecutor(reg, subgraph.NewPackageCache(subgraph.PackageCacheConfig{}),
			func() subgraph.Backend { return New(WithRegistry(reg), WithConcurrency(1)) }),
		true, time.Time{})
	eng := engine.New(b.State(), b.Queue(), engine.WithBatchBodyExecutor(bodies))
	stop := b.Bind(eng)
	defer stop()

	// Both members read all three roots through ?? defaults so an absent root
	// yields a self-describing value rather than an evaluation error -- the
	// failure then names WHICH root went missing instead of only reporting that
	// the node failed.
	memberParams := map[string]any{
		"saw_item":  "${{ ($item.id ?? 'MISS-item') }}",
		"saw_index": "${{ ($index ?? 'MISS-index') }}",
		"saw_items": "${{ (len($items ?? [])) }}",
	}
	def := &types.WorkflowDef{
		Name: "map-body-chain",
		Nodes: []types.NodeDef{
			{Name: "m", Type: "xflow.map", Parameters: map[string]any{
				"items": "$input.items",
				"body": map[string]any{
					"type": "xflow.subgraph",
					"parameters": map[string]any{
						"nodes": []any{
							map[string]any{"name": "first", "type": "test.chain_member", "parameters": memberParams},
							map[string]any{"name": "second", "type": "test.chain_member", "parameters": memberParams},
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
			"m": {"main": {Targets: []types.Connection{{Node: "done", Input: "main"}}}},
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

	// mapFanoutHandler emits two items, {"id":1} and {"id":2}, one per batch, so
	// each member runs exactly twice and must see index 0 and index 1.
	for _, node := range []string{"first", "second"} {
		rows := members.rows(node)
		if len(rows) != 2 {
			t.Fatalf("body member %q ran %d times (%#v), want 2 -- a non-entry member "+
				"losing the loop roots fails before it is ever invoked", node, len(rows), rows)
		}
		byIndex := map[any]map[string]any{}
		for _, row := range rows {
			byIndex[row["index"]] = row
		}
		for wantIndex, wantItem := range map[int]int{0: 1, 1: 2} {
			row, ok := byIndex[wantIndex]
			if !ok {
				t.Fatalf("body member %q never saw $index = %d; observed %#v", node, wantIndex, rows)
			}
			if row["item"] != wantItem {
				t.Errorf("body member %q at $index %d saw $item.id = %#v, want %d",
					node, wantIndex, row["item"], wantItem)
			}
			if row["items"] != 2 {
				t.Errorf("body member %q at $index %d saw len($items) = %#v, want 2",
					node, wantIndex, row["items"])
			}
		}
	}
}
