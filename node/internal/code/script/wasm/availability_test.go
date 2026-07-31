package wasm

import (
	"context"
	"testing"
)

func TestAvailabilityLadder(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if got := e.availability(); got != AvailUnavailable {
		t.Fatalf("fresh engine availability = %v, want Unavailable", got)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 2, 1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if got := e.availability(); got != AvailFresh {
		t.Fatalf("after a good swap = %v, want Fresh", got)
	}
	for range staleFailureThreshold {
		// {"bad":true} decodes to a rule with an empty Expr, which
		// expr.Compile rejects with "unexpected token EOF" — see
		// addendum §3 for why this is rejected (not unknown-field validation).
		_ = e.swapConfig(ctx, []byte(`{"rules":[{"bad":true}]}`), 2, 2)
	}
	if got := e.availability(); got != AvailStale {
		t.Fatalf("after %d source failures = %v, want Stale", staleFailureThreshold, got)
	}
	// Stale must still SERVE: last-good beats refusing traffic.
	if e.active.Load() == nil {
		t.Fatal("Stale must keep its last-good pool")
	}
	if e.ConfigAge() <= 0 {
		t.Fatal("ConfigAge must be positive once content was installed")
	}
}

// An empty rule set is a LEGAL state, distinct from "no content". Flink's own
// fraud demo got this wrong; the regression is mandatory (spec §7-5).
func TestEmptyRuleSetIsServed(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 2, 1); err != nil {
		t.Fatalf("an empty rule set must configure successfully: %v", err)
	}
	if got := e.availability(); got != AvailFresh {
		t.Fatalf("availability = %v, want Fresh — an empty rule set is not 'unavailable'", got)
	}
}
