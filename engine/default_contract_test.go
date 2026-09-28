package engine

// Engine-layer Default contract (descriptor-contract design §2.4): the map
// node's body_concurrency and continue_on_error are read here, at expansion
// time, not by the map handler. The readers must treat "absent", "the
// descriptor's Default" and "that Default after a JSON round trip" identically,
// otherwise an HTTP-registered workflow (no Defaults written) and an SDK one
// (Defaults written) would expand differently.
//
// The Default is read from the live xflow.map descriptor, so this test cannot
// drift from TestBuiltinDefaultsGolden. Importing node here is test-only; node
// does not depend on engine, so there is no cycle.

import (
	"encoding/json"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	_ "github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
)

func mapDescriptorDefault(t *testing.T, name string) any {
	t.Helper()
	h, ok := registry.LookupVersion("xflow.map", 1)
	if !ok {
		t.Fatal("xflow.map@1 is not registered")
	}
	for _, p := range h.Descriptor().Params {
		if p.Name == name {
			if p.Default == nil {
				t.Fatalf("xflow.map/%s has no Default", name)
			}
			return p.Default
		}
	}
	t.Fatalf("xflow.map has no param %q", name)
	return nil
}

func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// mapMetaVariants returns NodeMeta with the param absent (nil and empty
// Parameters), set to the Default, and set to the JSON-round-tripped Default.
func mapMetaVariants(t *testing.T, name string) map[string]graph.NodeMeta {
	def := mapDescriptorDefault(t, name)
	base := map[string]any{"items": "items"}
	withValue := func(v any) map[string]any {
		m := map[string]any{"items": "items"}
		m[name] = v
		return m
	}
	return map[string]graph.NodeMeta{
		"absent":       {Type: "xflow.map", Parameters: base},
		"nil_params":   {Type: "xflow.map"},
		"default":      {Type: "xflow.map", Parameters: withValue(def)},
		"default_json": {Type: "xflow.map", Parameters: withValue(jsonRoundTrip(t, def))},
	}
}

func TestDefaultContractMapBodyConcurrency(t *testing.T) {
	variants := mapMetaVariants(t, "body_concurrency")
	want := mapBodyConcurrency(variants["absent"])
	for name, meta := range variants {
		if got := mapBodyConcurrency(meta); got != want {
			t.Errorf("%s: mapBodyConcurrency = %d, want %d (param absent)", name, got, want)
		}
	}
	// Control: the reader does observe the parameter.
	if mapBodyConcurrency(graph.NodeMeta{Parameters: map[string]any{"body_concurrency": 3}}) == want {
		t.Fatal("body_concurrency=3 reads like absent; the probe does not observe the setting")
	}
}

func TestDefaultContractMapContinueOnError(t *testing.T) {
	variants := mapMetaVariants(t, "continue_on_error")
	want := mapContinueOnError(variants["absent"])
	for name, meta := range variants {
		if got := mapContinueOnError(meta); got != want {
			t.Errorf("%s: mapContinueOnError = %v, want %v (param absent)", name, got, want)
		}
	}
	if mapContinueOnError(graph.NodeMeta{Parameters: map[string]any{"continue_on_error": true}}) == want {
		t.Fatal("continue_on_error=true reads like absent; the probe does not observe the setting")
	}
}
