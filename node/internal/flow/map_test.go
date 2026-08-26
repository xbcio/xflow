package flow_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"

	"github.com/xbcio/xflow/node"
)

func TestMap_Factory(t *testing.T) {
	b := node.Map("orders", 5)
	if b.NodeType() != "xflow.map" {
		t.Fatalf("expected xflow.map, got %s", b.NodeType())
	}
	params := b.RawParams().(map[string]any)
	if params["batch_size"] != 5 {
		t.Fatalf("expected batch_size=5, got %v", params["batch_size"])
	}
}

// mapParams builds the parameters a body-form xflow.map actually reaches this
// handler with. The body matters even though the handler never looks inside it:
// a map node carrying neither a body nor an expression is rejected at compile
// time (validateNodeBody rule 3), so calling Execute with bare RawParams() was
// exercising a shape no execution can produce.
func mapParams(b *node.MapNode) map[string]any {
	params := b.RawParams().(map[string]any)
	params["body"] = map[string]any{
		"type": "xflow.subgraph",
		"parameters": map[string]any{
			"nodes": []any{map[string]any{"name": "echo", "type": "xflow.function"}},
		},
	}
	return params
}

func TestMap_BasicIteration(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 2)
	input := &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": []any{1, 2, 3, 4, 5}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["total"] != 5 {
		t.Fatalf("expected total=5, got %v", out.Data["total"])
	}
	if out.Data["batch_count"] != 3 {
		t.Fatalf("expected batch_count=3, got %v", out.Data["batch_count"])
	}
}

func TestMap_SingleBatch(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
	input := &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": []any{"a", "b", "c"}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["batch_count"] != 3 {
		t.Fatalf("expected batch_count=3, got %v", out.Data["batch_count"])
	}
}

// TestMap_EmitsTheBatchesThemselves asserts the two fields the engine's fan-out
// actually reads off a map node's output.
//
// TestMap_BasicIteration and TestMap_SingleBatch check only total and
// batch_count, and both are computed from the local items/batches values rather
// than from the map literal being returned — so writing `"batches": nil,
// "items": nil` into that literal leaves them at 5 and 3 while
// engine/expand.go:107 loopSplitBatches sees a missing key, returns (nil, nil),
// and the map node expands into zero batch tasks: the body never runs once and
// the node terminalizes as a success. Reordering is equally invisible — reverse
// items before the chunking and both counts are unchanged while every batch and
// every $items view is backwards, which for an ordered stream inverts the order
// the downstream side effects are applied in.
//
// Nothing downstream closes this gap either. Every engine test that exercises
// expansion (expand_test.go, batch_body_test.go, expansion_criterion_test.go,
// expand_fencing_test.go) hand-builds its own "batches" value, so the hop from
// MapNode.Execute to what the engine consumes is checked on neither side.
//
// 5 items at batch_size 2 is deliberate: the final batch is short, so the chunk
// layout is asymmetric and order-sensitive.
func TestMap_EmitsTheBatchesThemselves(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 2)
	in := []any{"a", "b", "c", "d", "e"}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": in},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	batches, ok := out.Data["batches"].([][]any)
	if !ok {
		t.Fatalf("out.Data[\"batches\"] = %#v, want [][]any: the engine reads this "+
			"key to build the batch tasks, and a missing or wrongly-typed value "+
			"expands into no tasks at all", out.Data["batches"])
	}
	want := [][]any{{"a", "b"}, {"c", "d"}, {"e"}}
	if len(batches) != len(want) {
		t.Fatalf("batches = %#v, want %#v", batches, want)
	}
	for i := range want {
		if !slices.Equal(batches[i], want[i]) {
			t.Errorf("batches[%d] = %#v, want %#v: this is the exact slice one "+
				"body invocation is handed, short final batch included",
				i, batches[i], want[i])
		}
	}

	// items travels on every batch task (expansionBatchTask) because the body
	// needs $items and the map node's own output does not exist yet at
	// expansion time.
	items, ok := out.Data["items"].([]any)
	if !ok || !slices.Equal(items, in) {
		t.Errorf("out.Data[\"items\"] = %#v, want %#v in that order: each batch "+
			"carries this forward as the body's $items", out.Data["items"], in)
	}
	if out.Data["total"] != 5 || out.Data["batch_count"] != 3 {
		t.Errorf("total/batch_count = %v/%v, want 5/3",
			out.Data["total"], out.Data["batch_count"])
	}
}

func TestMap_MissingItems(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	input := &types.Input{
		Params: map[string]any{"batch_size": 1},
		Data:   map[string]any{},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for missing items")
	}
}

func TestMap_ItemsNotArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
	input := &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": "not_an_array"},
	}
	_, err := h.Execute(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for non-array items")
	}
	// Name the reason: with a bare RawParams() this test also went red, but for
	// the missing body rather than the items type — passing for the wrong reason.
	if !strings.Contains(err.Error(), "must evaluate to an array") {
		t.Fatalf("failed for the wrong reason: %v", err)
	}
}

func TestMap_EmptyArray(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	b := node.Map("items", 1)
	input := &types.Input{
		Params: mapParams(b),
		Data:   map[string]any{"items": []any{}},
	}
	out, err := h.Execute(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Data["total"] != 0 {
		t.Fatalf("expected total=0, got %v", out.Data["total"])
	}
}

// The DSL setter is the only entry a Go caller has to the parameter. A field
// that exists but has no setter is a field that cannot be set -- the exact shape
// of the transient-mode gap, where the compiler read a value nothing could
// write.
func TestMap_BodyConcurrencySetter(t *testing.T) {
	params := node.Map("items", 10).Concurrency(8).RawParams().(map[string]any)
	if params["body_concurrency"] != 8 {
		t.Fatalf("body_concurrency = %v, want 8", params["body_concurrency"])
	}
}

// Absent by default. RawParams is what the SDK serialises into a workflow
// definition, so emitting a non-1 default here would parallelise every existing
// map node built through the builder.
func TestMap_BodyConcurrencyDefaultsToSerial(t *testing.T) {
	params := node.Map("items", 10).RawParams().(map[string]any)
	if got, ok := params["body_concurrency"]; ok {
		if n, _ := got.(int); n > 1 {
			t.Fatalf("body_concurrency = %v by default, want <= 1: concurrency must be opt-in", got)
		}
	}
}

// A cap of zero or below is not a cap. Clamping in the setter -- rather than
// trusting every reader downstream to re-check -- keeps the invalid value out of
// the definition entirely.
func TestMap_BodyConcurrencyRejectsNonPositive(t *testing.T) {
	for _, in := range []int{0, -1, -100} {
		params := node.Map("items", 10).Concurrency(in).RawParams().(map[string]any)
		n, _ := params["body_concurrency"].(int)
		if n > 1 {
			t.Fatalf("Concurrency(%d) produced body_concurrency=%d, want <= 1", in, n)
		}
	}
}

// The descriptor is what an editor, a validator, and the docs read. A parameter
// the handler honours but the descriptor omits is invisible to all three, and
// api-level parameter validation rejects what it does not know.
func TestMap_DescriptorDeclaresBodyConcurrency(t *testing.T) {
	h, _ := registry.Lookup("xflow.map")
	for _, p := range h.Descriptor().Params {
		if p.Name != "body_concurrency" {
			continue
		}
		if p.Type != types.ParamNumber {
			t.Fatalf("body_concurrency declared as %v, want %v", p.Type, types.ParamNumber)
		}
		if p.Required {
			t.Fatal("body_concurrency must not be required: it is opt-in")
		}
		return
	}
	t.Fatal("xflow.map descriptor declares no body_concurrency parameter")
}
