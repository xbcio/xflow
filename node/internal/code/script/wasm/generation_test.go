package wasm

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node/internal/code/script/engine"
	"github.com/xbcio/xflow/node/supply"
)

// A tagged result must carry the content version that produced it, taken from the
// SERVER revision so two runners' outputs are comparable during a rollout skew.
func TestEvalResultCarriesConfigGeneration(t *testing.T) {
	code := testReactorCode(t)
	reg := supply.NewRegistry()
	if err := RegisterSupplyConsumer(code, "rules", reg); err != nil {
		t.Fatalf("register: %v", err)
	}
	ctx := context.Background()
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[]}`), Hash: "h1", Revision: 6,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply v6: %v", err)
	}

	out, err := sharedReactorEngine.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	m, ok := out.(map[string]any)
	if !ok {
		t.Fatalf("output = %T, want map[string]any", out)
	}
	if got := m[ConfigGenerationKey]; got != uint64(6) {
		t.Fatalf("%s = %#v, want uint64 6", ConfigGenerationKey, got)
	}

	// After a swap the SAME assertion must yield the new revision — this is the
	// pair a warehouse query uses to separate pre- and post-rollout rows.
	if err := reg.Apply(ctx, supply.Snapshot{
		Name: "rules", Content: []byte(`{"rules":[{"name":"r1","expr":"x > 0"}]}`), Hash: "h2", Revision: 7,
		FetchedAt: time.Now(),
	}); err != nil {
		t.Fatalf("apply v7: %v", err)
	}
	out2, err := sharedReactorEngine.Execute(ctx, code, map[string]any{"x": 1.0}, engine.DefaultHelpers())
	if err != nil {
		t.Fatalf("Execute after swap: %v", err)
	}
	if got := out2.(map[string]any)[ConfigGenerationKey]; got != uint64(7) {
		t.Fatalf("%s after swap = %#v, want uint64 7", ConfigGenerationKey, got)
	}
}

// annotateGeneration must leave non-object results untouched: injecting a key
// would change the value's shape and break the guest's output contract. Tested
// directly against the pure function rather than through a full Execute round
// trip — no testdata guest returns a non-object result (verified: reactor and
// reactormin both return maps; reactorspin never returns), and adding a
// wasm-shaped fixture just to exercise this pure-function property would be
// disproportionate. This still fails for the right reason if annotateGeneration
// is changed to inject unconditionally.
func TestAnnotateGenerationLeavesNonObjectsUnchanged(t *testing.T) {
	cases := []any{
		[]any{1.0, 2.0},
		float64(3),
		"a string",
		nil,
	}
	for _, v := range cases {
		got := annotateGeneration(v, 9)
		if got == nil && v == nil {
			continue // nil in, nil out: unchanged, as required.
		}
		switch want := v.(type) {
		case []any:
			gotSlice, ok := got.([]any)
			if !ok || len(gotSlice) != len(want) {
				t.Fatalf("annotateGeneration(%#v) = %#v, want unchanged", v, got)
			}
		default:
			if got != v {
				t.Fatalf("annotateGeneration(%#v) = %#v, want unchanged", v, got)
			}
		}
	}
}

// annotateGeneration must inject config_generation into an object result,
// including when the revision is zero (legacy globals path) — a zero must be
// stamped, not treated as "absent", so a warehouse can tell "legacy path" from
// "field missing because this host predates the field".
func TestAnnotateGenerationInjectsIntoObject(t *testing.T) {
	m := map[string]any{"matched": []any{"a"}}
	got := annotateGeneration(m, 0)
	gotMap, ok := got.(map[string]any)
	if !ok {
		t.Fatalf("annotateGeneration(map) = %T, want map[string]any", got)
	}
	if v, ok := gotMap[ConfigGenerationKey]; !ok || v != uint64(0) {
		t.Fatalf("%s = %#v, want uint64(0) present", ConfigGenerationKey, v)
	}
}
