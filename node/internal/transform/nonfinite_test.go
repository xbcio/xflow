package transform_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// requirePermanentCode pins both halves of a classified failure: retrying a
// deterministic non-finite value cannot change it, and the wire code must stay
// stable enough for on-error policy to tell it apart.
func requirePermanentCode(t *testing.T, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatal("Execute() error = nil, want a permanent non-finite failure")
	}
	if !types.IsPermanent(err) {
		t.Fatalf("non-finite failure must be permanent (not retried); got err=%v", err)
	}
	var ce *types.ClassifiedError
	if !errors.As(err, &ce) || ce.Code != code {
		t.Fatalf("expected ClassifiedError code=%q, got %T %v", code, err, err)
	}
}

func TestSet_NonFiniteExpressionIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.transform.set")
	b := node.Set(nil).SetExpr(map[string]string{"total": "1 / 0"})
	_, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{},
	})
	requirePermanentCode(t, err, "set.expr_non_finite")
}

// itemsFromInput feeds the node's output directly, so a non-finite item from
// the items expression must be rejected before any transform runs. Filter is
// one of the six nodes sharing that helper.
func TestFilter_NonFiniteItemsIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.transform.filter")
	b := node.Filter("$input.rows", "true")
	_, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"rows": []any{1.0, math.NaN()}},
	})
	requirePermanentCode(t, err, "transform.items_non_finite")
}

// A finite item field can still overflow the sum: MaxFloat64 + MaxFloat64 is
// +Inf, which is not JSON. The operation and its inputs are fixed, so retrying
// reproduces it; both sum and avg (Inf/2) must be classified permanent.
func TestAggregate_NonFiniteOverflowIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.transform.aggregate")
	items := []any{
		map[string]any{"v": math.MaxFloat64},
		map[string]any{"v": math.MaxFloat64},
	}
	cases := []struct {
		name string
		b    *node.AggregateNode
	}{
		{"sum", node.Aggregate("items").Sum("v", "total")},
		{"avg", node.Aggregate("items").Average("v", "average")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := h.Execute(context.Background(), &types.Input{
				Params: c.b.RawParams().(map[string]any),
				Data:   map[string]any{"items": items},
			})
			requirePermanentCode(t, err, "aggregate.non_finite")
		})
	}
}
