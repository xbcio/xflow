package wasm

import (
	"context"
	"testing"
)

// TestRejectedConfigLeavesActivePoolIntact: rejection must not shrink or replace
// the live pool (spec §7-6). The canary-vs-fan-out split from the original
// brief is NOT implemented here — probed and confirmed the existing serial
// buildPool already returns on the first failed instance, so a bad config
// already costs exactly one configure call (see task-16-brief-addendum.md §2).
// This test only covers the invariant that still applies: a rejected swap must
// not touch the currently active pool.
func TestRejectedConfigLeavesActivePoolIntact(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 4, 1); err != nil {
		t.Fatalf("initial swap: %v", err)
	}
	good := e.active.Load()

	// {"rules":[{"bad":true}]}: the "bad" field is unknown to configRule, so it
	// decodes to a rule with an empty Expr. expr.Compile("") fails with
	// "unexpected token EOF" — the rejection is via expr compile failure, NOT
	// unknown-field validation (there is none). See addendum §3.
	if err := e.swapConfig(ctx, []byte(`{"rules":[{"bad":true}]}`), 4, 2); err == nil {
		t.Fatal("bad config must be rejected")
	}
	if e.active.Load() != good {
		t.Fatal("active pool pointer changed on a rejected config")
	}
	if len(good.free) != 4 {
		t.Fatalf("live pool shrank to %d instances", len(good.free))
	}
}
