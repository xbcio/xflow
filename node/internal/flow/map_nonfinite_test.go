package flow_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// requirePermanentCode pins both halves of a classified failure: retrying a
// deterministic non-finite expression cannot change its result, and the wire
// code must stay stable enough for on-error policy to tell it apart.
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

// A non-finite value produced by the items expression reaches the node's output
// array unchanged, so it must be rejected where it is evaluated.
func TestMap_NonFiniteItemsIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	_, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{"items": "$input.rows", "expression": "$item"},
		Data:   map[string]any{"rows": []any{1.0, math.Inf(1)}},
	})
	requirePermanentCode(t, err, "map.expr_non_finite")
}

// The per-item expression form commits each evaluated value verbatim, so a
// NaN/±Inf item is a per-item failure and must be permanent.
func TestMapExpression_NonFiniteItemIsPermanent(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	params := map[string]any{"items": "rows", "expression": "1 / 0"}
	data := map[string]any{"rows": []any{1, 2}}

	_, err := h.Execute(context.Background(), &types.Input{Params: params, Data: data})
	requirePermanentCode(t, err, "map.expr_non_finite")

	// continue_on_error means "tolerate a partial failure", not "never fail":
	// with every item non-finite there is no success to tolerate.
	params["continue_on_error"] = true
	_, err = h.Execute(context.Background(), &types.Input{Params: params, Data: data})
	requirePermanentCode(t, err, "map.expr_non_finite")
}

// Under continue_on_error a non-finite item is treated exactly like any other
// failed item: it keeps its {_error, _index} slot instead of failing the node,
// as long as at least one item succeeded.
func TestMapExpression_NonFiniteItemKeepsItsSlotUnderContinueOnError(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	out, err := h.Execute(context.Background(), &types.Input{
		Params: map[string]any{
			"items":             "rows",
			"expression":        "$item == 1 ? $item : 1 / 0",
			"continue_on_error": true,
		},
		Data: map[string]any{"rows": []any{1, 2}},
	})
	if err != nil {
		t.Fatalf("one good item must let continue_on_error tolerate the bad one: %v", err)
	}
	results, ok := out.Data["results"].([]any)
	if !ok || len(results) != 2 || out.Data["count"] != 2 {
		t.Fatalf("results/count = %#v/%v, want two slots (the bad item is not dropped)", out.Data["results"], out.Data["count"])
	}
	if results[0] != 1 {
		t.Fatalf("results[0] = %v, want the successful 1", results[0])
	}
	failed, ok := results[1].(map[string]any)
	if !ok {
		t.Fatalf("results[1] = %#v, want an {_error, _index} placeholder", results[1])
	}
	if _, has := failed["_error"]; !has {
		t.Errorf("placeholder has no _error key: downstream cannot tell a failure from data")
	}
	if failed["_index"] != 1 {
		t.Errorf("placeholder _index = %v, want 1", failed["_index"])
	}
}
