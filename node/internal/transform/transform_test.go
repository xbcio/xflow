package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

func TestSetNodeAssignsLiteralsAndExpressions(t *testing.T) {
	b := node.Set(map[string]any{"status": "approved"}).
		SetExpr(map[string]string{"total": "price * quantity"})

	if got := b.NodeType(); got != "xflow.transform.set" {
		t.Fatalf("NodeType() = %q, want xflow.transform.set", got)
	}

	h, ok := noderuntime.Lookup("xflow.transform.set")
	if !ok {
		t.Fatal("set handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"price": 12, "quantity": 3, "keep": true},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["status"] != "approved" {
		t.Fatalf("status = %v, want approved", out.Data["status"])
	}
	if out.Data["total"] != 36 {
		t.Fatalf("total = %v, want 36", out.Data["total"])
	}
	if out.Data["keep"] != true {
		t.Fatalf("keep = %v, want true", out.Data["keep"])
	}
}

func TestPickNodeKeepsOnlyNamedFields(t *testing.T) {
	b := node.Pick("id", "name")

	h, ok := noderuntime.Lookup("xflow.transform.pick")
	if !ok {
		t.Fatal("pick handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"id": "u1", "name": "Ada", "password": "secret"},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if len(out.Data) != 2 || out.Data["id"] != "u1" || out.Data["name"] != "Ada" {
		t.Fatalf("data = %#v, want only id and name", out.Data)
	}
}

func TestRenameNodeRenamesMappedFields(t *testing.T) {
	b := node.Rename(map[string]string{"first_name": "firstName", "last_name": "lastName"})

	h, ok := noderuntime.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"first_name": "Ada", "last_name": "Lovelace", "age": 36},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["firstName"] != "Ada" || out.Data["lastName"] != "Lovelace" || out.Data["age"] != 36 {
		t.Fatalf("data = %#v, want renamed names and preserved age", out.Data)
	}
	if _, ok := out.Data["first_name"]; ok {
		t.Fatalf("first_name should have been removed: %#v", out.Data)
	}
}

func TestFilterNodeKeepsMatchingItems(t *testing.T) {
	b := node.Filter("items", "item.price >= 100")

	h, ok := noderuntime.Lookup("xflow.transform.filter")
	if !ok {
		t.Fatal("filter handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"sku": "a", "price": 90},
			map[string]any{"sku": "b", "price": 120},
			map[string]any{"sku": "c", "price": 200},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	items := out.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["sku"] != "b" || items[1].(map[string]any)["sku"] != "c" {
		t.Fatalf("items = %#v, want b and c", items)
	}
	if out.Data["total"] != 2 {
		t.Fatalf("total = %v, want 2", out.Data["total"])
	}
}

func TestSortAndLimitNodesOrderAndSliceItems(t *testing.T) {
	sortNode := node.Sort("items", node.SortDesc("score"))
	sortHandler, ok := noderuntime.Lookup("xflow.transform.sort")
	if !ok {
		t.Fatal("sort handler not registered")
	}
	sorted, err := sortHandler.Execute(context.Background(), &types.Input{
		Params: sortNode.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"name": "a", "score": 2},
			map[string]any{"name": "b", "score": 5},
			map[string]any{"name": "c", "score": 3},
		}},
	})
	if err != nil {
		t.Fatalf("sort Execute() error = %v", err)
	}
	items := sorted.Data["items"].([]any)
	if items[0].(map[string]any)["name"] != "b" || items[2].(map[string]any)["name"] != "a" {
		t.Fatalf("sorted items = %#v, want score desc", items)
	}

	limitNode := node.Limit("items", 2)
	limitHandler, ok := noderuntime.Lookup("xflow.transform.limit")
	if !ok {
		t.Fatal("limit handler not registered")
	}
	limited, err := limitHandler.Execute(context.Background(), &types.Input{
		Params: limitNode.RawParams().(map[string]any),
		Data:   sorted.Data,
	})
	if err != nil {
		t.Fatalf("limit Execute() error = %v", err)
	}
	items = limited.Data["items"].([]any)
	if len(items) != 2 || items[0].(map[string]any)["name"] != "b" || items[1].(map[string]any)["name"] != "c" {
		t.Fatalf("limited items = %#v, want top two", items)
	}
}

func TestRemoveDuplicatesNodeKeepsFirstByFields(t *testing.T) {
	b := node.RemoveDuplicates("items", "email")

	h, ok := noderuntime.Lookup("xflow.transform.remove_duplicates")
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
}

func TestAggregateNodeComputesSummary(t *testing.T) {
	b := node.Aggregate("items").
		Count("order_count").
		Sum("amount", "total_amount").
		Average("amount", "average_amount")

	h, ok := noderuntime.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{"items": []any{
			map[string]any{"amount": 10},
			map[string]any{"amount": 20},
			map[string]any{"amount": 30},
		}},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["order_count"] != 3 || out.Data["total_amount"] != float64(60) || out.Data["average_amount"] != float64(20) {
		t.Fatalf("aggregate data = %#v, want count/sum/avg", out.Data)
	}
}
