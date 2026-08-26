package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestSort_PublishesSortedItemCount covers sort.go:86. Nothing in
// sort_test.go reads out.Data["total"] back, and only limit/filter/
// remove_duplicates pin the equivalent field for their own nodes -- sort is
// the one node in this package whose count went unchecked. Without this
// assertion, "total" could be replaced by any placeholder value (e.g. a
// negative sentinel) while the ordering test above stays green. A downstream
// expression branching on $input.total would then act on a bogus count for
// a slice this node only reordered, never resized.
func TestSort_PublishesSortedItemCount(t *testing.T) {
	b := node.Sort("items", node.SortDesc("score"))

	h, ok := registry.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"name": "a", "score": 2},
			map[string]any{"name": "b", "score": 5},
			map[string]any{"name": "c", "score": 3},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["total"] != 3 {
		t.Fatalf("total = %v, want 3 (the count of sorted items)", out.Data["total"])
	}
}
