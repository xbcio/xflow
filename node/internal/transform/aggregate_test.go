package transform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestAggregate_ComputesSummary(t *testing.T) {
	b := node.Aggregate("items").
		Count("order_count").
		Sum("amount", "total_amount").
		Average("amount", "average_amount")

	h, ok := registry.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data: map[string]any{
			// Upstream context that aggregate has no business dropping. The three
			// aggregate results below are written onto a map the node builds
			// itself, so they say nothing about whether that map started as a
			// clone of the input or as a fresh empty one -- the exact regression
			// aggregate.go's own comment records as having happened before.
			"tenant_id": "acme",
			"items": []any{
				map[string]any{"amount": 10},
				map[string]any{"amount": 20},
				map[string]any{"amount": 30},
			},
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["order_count"] != 3 || out.Data["total_amount"] != float64(60) || out.Data["average_amount"] != float64(20) {
		t.Fatalf("aggregate data = %#v, want count/sum/avg", out.Data)
	}
	if got := out.Data["tenant_id"]; got != "acme" {
		t.Errorf("out.Data[\"tenant_id\"] = %#v, want %q: aggregate must layer its "+
			"results on top of the upstream data, not replace it -- a node that "+
			"returns only its own three fields silently truncates every downstream "+
			"node's view of the batch", got, "acme")
	}
	if items, ok := out.Data["items"].([]any); !ok || len(items) != 3 {
		t.Errorf("out.Data[\"items\"] = %#v, want the 3-element input array: the "+
			"aggregated array itself is the field most likely to be needed "+
			"downstream alongside the summary", out.Data["items"])
	}
}

// TestAggregate_RejectsEmptyOutputName covers the guard at aggregate.go:85-87.
// Every existing fixture supplies a non-empty `as` name for every operation,
// so nothing exercises the branch that rejects an empty one. Without the
// guard, an operation with an empty `as` silently writes its result onto
// data[""] and returns success, instead of failing a workflow definition
// that omitted the output field name.
func TestAggregate_RejectsEmptyOutputName(t *testing.T) {
	b := node.Aggregate("items").Count("")

	h, ok := registry.Lookup("xflow.transform.aggregate")
	if !ok {
		t.Fatal("aggregate handler not registered")
	}
	_, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"items": []any{map[string]any{"amount": 1}}},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want a rejection for an operation with an empty output name")
	}
	// Pin the specific rejection: Execute rejects missing/unparsable
	// operations a few lines earlier with a different message, so asserting
	// only err != nil would survive deleting the empty-name guard if the
	// operation happened to be rejected by that earlier check instead.
	const want = "output name is required"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Execute() error = %q, want substring %q", err.Error(), want)
	}
}
