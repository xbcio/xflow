package transform_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/node"
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
