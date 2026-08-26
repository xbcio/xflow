package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestSort_OrdersByScoreDesc(t *testing.T) {
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
	items := out.Data["items"].([]any)
	if items[0].(map[string]any)["name"] != "b" || items[2].(map[string]any)["name"] != "a" {
		t.Fatalf("sorted items = %#v, want score desc", items)
	}
}

// TestSort_OrdersByStringFieldLexicographically covers compareValues'
// non-numeric branch (sort.go:103-112). Every other sort fixture in this
// package -- score in sort_test.go/sort_total_test.go, score in
// json_shape_test.go's two sort cases -- sorts on a field that
// cast.ToFloat64E parses successfully, so compareValues never falls through
// past its numeric branch anywhere else in this repo. The dept field in
// json_shape_test.go's secondary-sort case is a string, but every item
// shares the same dept ("eng"), so compareValues always returns the tie (0)
// for it and the ls<rs / ls>rs arms are never reached with a real verdict.
//
// A workflow that sorts by a genuinely string-typed field (a SKU, a status
// name, a department when the batch isn't a single department) exercises
// this branch in production; nothing here did before this test. Swapping the
// `case ls < rs` / `case ls > rs` arms -- or negating cmp for the string path
// only -- would leave every existing sort/aggregate/limit/filter test in this
// package green while reversing the order of every string-keyed sort at
// runtime.
func TestSort_OrdersByStringFieldLexicographically(t *testing.T) {
	b := node.Sort("items", node.SortAsc("sku"))

	h, ok := registry.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"sku": "cherry"},
			map[string]any{"sku": "apple"},
			map[string]any{"sku": "banana"},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	want := []string{"apple", "banana", "cherry"}
	for i, w := range want {
		got := items[i].(map[string]any)["sku"]
		if got != w {
			t.Fatalf("items[%d].sku = %#v, want %v (full order %#v): lexicographic "+
				"string comparison was requested but not honoured -- a reversed or "+
				"scrambled string comparator produces a well-formed sort with the "+
				"wrong order and no error on any path", i, got, w, items)
		}
	}
}
