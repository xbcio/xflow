package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestRemoveDuplicates_KeepsFirstByFields(t *testing.T) {
	b := node.RemoveDuplicates("items", "email")

	h, ok := registry.Lookup("xflow.transform.remove_duplicates")
	if !ok {
		t.Fatal("remove duplicates handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"email": "a@example.com", "name": "first"},
			map[string]any{"email": "b@example.com", "name": "second"},
			map[string]any{"email": "a@example.com", "name": "duplicate"},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["name"] != "first" || items[1].(map[string]any)["name"] != "second" {
		t.Fatalf("items = %#v, want first unique records", items)
	}
	// remove_duplicates.go:68 also publishes the post-dedup count, and nothing
	// read it back. Reporting the input length (3) instead leaves this test
	// green while every downstream expression reading $input.total sees a count
	// for records this node just dropped.
	if out.Data["total"] != 2 {
		t.Fatalf("total = %v, want 2 (the count AFTER dedup, not the input's %d)",
			out.Data["total"], 3)
	}
}

// TestRemoveDuplicates_WithoutFieldsDedupsWholeItem covers the other half of
// dedupKey (remove_duplicates.go:74-80): with no `fields` the unique key is the
// whole item, which the descriptor documents as the default. Every existing
// fixture passes a field, so the len(fields) > 0 branch is the only one any
// test executes — the default path is unverified.
//
// The fixture below is chosen so the two forms disagree: the first two items
// share every field the other test would key on but differ in a second field,
// so a whole-item key keeps both while a key built from any single field would
// collapse them.
func TestRemoveDuplicates_WithoutFieldsDedupsWholeItem(t *testing.T) {
	b := node.RemoveDuplicates("items")

	h, ok := registry.Lookup("xflow.transform.remove_duplicates")
	if !ok {
		t.Fatal("remove duplicates handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"email": "a@example.com", "name": "first"},
			map[string]any{"email": "a@example.com", "name": "second"},
			map[string]any{"email": "a@example.com", "name": "first"},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("items = %#v, want 2: only the third record is a whole-item "+
			"duplicate of the first", items)
	}
	if items[0].(map[string]any)["name"] != "first" || items[1].(map[string]any)["name"] != "second" {
		t.Fatalf("items = %#v, want the first occurrence of each distinct record", items)
	}
	if out.Data["total"] != 2 {
		t.Fatalf("total = %v, want 2", out.Data["total"])
	}
}
