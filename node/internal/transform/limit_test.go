package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestLimit_SlicesToMax(t *testing.T) {
	b := node.Limit("items", 2)

	h, ok := registry.Lookup("xflow.transform.limit")
	if !ok {
		t.Fatal("limit handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"name": "b", "score": 5},
			map[string]any{"name": "c", "score": 3},
			map[string]any{"name": "a", "score": 2},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["name"] != "b" || items[1].(map[string]any)["name"] != "c" {
		t.Fatalf("limited items = %#v, want top two", items)
	}
	// limit.go:58 also publishes the post-truncation count. Nothing read it back
	// here, and only filter_test.go pins the equivalent field for its own node,
	// so limit's copy could report the pre-truncation length (3) and stay green.
	// A downstream expression branching on $input.total would then act on a
	// count for a slice this node just discarded items from.
	if out.Data["total"] != 2 {
		t.Fatalf("total = %v, want 2 (the count AFTER truncation, not the "+
			"input's %d)", out.Data["total"], 3)
	}
}

// TestLimit_MaxAboveLengthReturnsEverything covers the clamp at limit.go:52-54.
// Every existing fixture uses a max below the array length, so the clamp is
// never exercised: without it, items[:limit] slices past the end and the handler
// panics. A panic in a node handler is not an ordinary node failure — it takes
// down the worker goroutine rather than routing to the node's error handling,
// and "max larger than the array" is the ordinary case for a limit node placed
// after a filter that happened to match fewer rows than usual.
func TestLimit_MaxAboveLengthReturnsEverything(t *testing.T) {
	b := node.Limit("items", 10)

	h, ok := registry.Lookup("xflow.transform.limit")
	if !ok {
		t.Fatal("limit handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{"x", "y", "z"}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 3 {
		t.Fatalf("items = %#v, want all three kept when max exceeds the length", items)
	}
	if out.Data["total"] != 3 {
		t.Fatalf("total = %v, want 3", out.Data["total"])
	}
}

// TestLimit_NegativeMaxIsRejected covers limit.go:49-51. Without the guard,
// cast.ToInt of a negative max reaches items[:limit] and panics.
func TestLimit_NegativeMaxIsRejected(t *testing.T) {
	b := node.Limit("items", -1)

	h, ok := registry.Lookup("xflow.transform.limit")
	if !ok {
		t.Fatal("limit handler not registered")
	}
	_, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{"x", "y"}},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want a rejection for a negative max")
	}
}
