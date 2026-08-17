package flow_test

import (
	"context"
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
