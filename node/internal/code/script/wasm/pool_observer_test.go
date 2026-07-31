package wasm

import (
	"context"
	"testing"
)

// swapConfig must report the applied outcome with the RULE COUNT (never
// content) and the revision, and must set the ready instance-count gauge.
func TestSwapConfigNotifiesObserverOnApply(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[{"name":"a","expr":"x>0"},{"name":"b","expr":"x<0"}]}`), 2, 5); err != nil {
		t.Fatalf("swapConfig: %v", err)
	}

	if len(rec.swaps) != 1 {
		t.Fatalf("swap notifications = %d, want 1", len(rec.swaps))
	}
	sw := rec.swaps[0]
	if sw.result != "applied" {
		t.Fatalf("result = %q, want applied", sw.result)
	}
	if sw.ruleCount != 2 {
		t.Fatalf("ruleCount = %d, want 2", sw.ruleCount)
	}
	if sw.revision != 5 {
		t.Fatalf("revision = %d, want 5", sw.revision)
	}

	found := false
	for _, ic := range rec.instances {
		if ic.state == "ready" && ic.n == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("instance count notifications = %#v, want a ready/2 entry", rec.instances)
	}
}

// A rejected config (bad rule) must report "rejected", not "applied" — and
// must NOT publish the rule-count/generation gauges for content that never
// became active.
func TestSwapConfigNotifiesObserverOnReject(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[{"bad":true}]}`), 2, 9); err == nil {
		t.Fatal("expected bad-config error")
	}

	if len(rec.swaps) != 1 {
		t.Fatalf("swap notifications = %d, want 1", len(rec.swaps))
	}
	if rec.swaps[0].result != "rejected" {
		t.Fatalf("result = %q, want rejected", rec.swaps[0].result)
	}
}

// A borrow that must wait for a free instance reports a non-zero wait
// duration. A borrow that succeeds immediately (a free instance was already
// parked) is still a legal near-zero observation.
func TestBorrowNotifiesObserverWithWaitDuration(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 1, 1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	if _, _, err := e.borrow(ctx); err != nil {
		t.Fatalf("borrow: %v", err)
	}
	if len(rec.borrowWait) != 1 {
		t.Fatalf("borrow-wait notifications = %d, want 1", len(rec.borrowWait))
	}
}

// doom must report an instance recycle with cause "eval_error" when the
// context was not the thing that expired, and must report the doomed-state
// instance count.
func TestDoomNotifiesObserverEvalError(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 1, 1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	inst, pool, err := e.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}
	e.doom(ctx, pool, inst)

	if len(rec.recycled) != 1 || rec.recycled[0] != "eval_error" {
		t.Fatalf("recycled = %#v, want [eval_error]", rec.recycled)
	}
	found := false
	for _, ic := range rec.instances {
		if ic.state == "doomed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("instance count notifications = %#v, want a doomed entry", rec.instances)
	}
}

// The other side of doom's classification: an instance doomed while its context
// is already expired is a timeout, not an eval error. The two causes route to
// different operator responses — "timeout" means the guest was too slow for its
// deadline (raise it, or fix the guest), "eval_error" means the guest faulted —
// so conflating them sends an operator down the wrong path.
//
// Without this, only the eval_error branch is load-bearing: inverting the
// condition in doom would leave the suite green.
func TestDoomClassifiesExpiredContextAsTimeout(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 1, 1); err != nil {
		t.Fatalf("swap: %v", err)
	}
	inst, pool, err := e.borrow(ctx)
	if err != nil {
		t.Fatalf("borrow: %v", err)
	}

	// An already-cancelled context is what doom sees when
	// WithCloseOnContextDone closed the module out from under a call.
	expired, cancel := context.WithCancel(ctx)
	cancel()
	e.doom(expired, pool, inst)

	if len(rec.recycled) != 1 || rec.recycled[0] != "timeout" {
		t.Fatalf("recycled = %#v, want [timeout]", rec.recycled)
	}
}

// drainPool tears down every parked instance in a superseded pool and must
// report each one recycled with cause "pool_swapped".
func TestDrainPoolNotifiesObserverPoolSwapped(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 2, 1); err != nil {
		t.Fatalf("swap 1: %v", err)
	}
	old := e.active.Load()
	// A second swap makes `old` unreachable from e.active; drainPool tears it
	// down directly here rather than via the async path swapConfig triggers,
	// so the assertion is deterministic.
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 2, 2); err != nil {
		t.Fatalf("swap 2: %v", err)
	}
	e.drainPool(ctx, old)

	if len(rec.recycled) != 2 {
		t.Fatalf("recycled = %#v, want 2 pool_swapped entries", rec.recycled)
	}
	for _, cause := range rec.recycled {
		if cause != "pool_swapped" {
			t.Fatalf("recycled cause = %q, want pool_swapped", cause)
		}
	}
}

// engineFor must report a compile cache miss on first compile and a hit on a
// subsequent call for the same module bytes.
func TestEngineForNotifiesObserverCompileHitMiss(t *testing.T) {
	rec := &recordingObserver{}
	SetObserver(rec)
	defer SetObserver(nil)

	ctx := context.Background()
	h := newReactorHost()
	if _, err := h.engineFor(ctx, reactorWasm); err != nil {
		t.Fatalf("engineFor #1: %v", err)
	}
	if _, err := h.engineFor(ctx, reactorWasm); err != nil {
		t.Fatalf("engineFor #2: %v", err)
	}

	if len(rec.compiles) != 2 {
		t.Fatalf("compile notifications = %#v, want 2", rec.compiles)
	}
	if rec.compiles[0] != "miss" {
		t.Fatalf("first compile = %q, want miss", rec.compiles[0])
	}
	if rec.compiles[1] != "hit" {
		t.Fatalf("second compile = %q, want hit", rec.compiles[1])
	}
}
