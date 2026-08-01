package wasm

import (
	"context"
	"testing"
	"time"
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

	swaps := rec.swapCalls()
	if len(swaps) != 1 {
		t.Fatalf("swap notifications = %d, want 1", len(swaps))
	}
	sw := swaps[0]
	if sw.result != "applied" {
		t.Fatalf("result = %q, want applied", sw.result)
	}
	if sw.ruleCount != 2 {
		t.Fatalf("ruleCount = %d, want 2", sw.ruleCount)
	}
	if sw.revision != 5 {
		t.Fatalf("revision = %d, want 5", sw.revision)
	}

	instances := rec.instanceCalls()
	found := false
	for _, ic := range instances {
		if ic.state == "ready" && ic.n == 2 {
			found = true
		}
	}
	if !found {
		t.Fatalf("instance count notifications = %#v, want a ready/2 entry", instances)
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

	swaps := rec.swapCalls()
	if len(swaps) != 1 {
		t.Fatalf("swap notifications = %d, want 1", len(swaps))
	}
	if swaps[0].result != "rejected" {
		t.Fatalf("result = %q, want rejected", swaps[0].result)
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
	if waits := rec.borrowWaits(); len(waits) != 1 {
		t.Fatalf("borrow-wait notifications = %d, want 1", len(waits))
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

	causes := rec.recycledCauses()
	if len(causes) != 1 || causes[0] != "eval_error" {
		t.Fatalf("recycled = %#v, want [eval_error]", causes)
	}
	instances := rec.instanceCalls()
	found := false
	for _, ic := range instances {
		if ic.state == "doomed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("instance count notifications = %#v, want a doomed entry", instances)
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

	if causes := rec.recycledCauses(); len(causes) != 1 || causes[0] != "timeout" {
		t.Fatalf("recycled = %#v, want [timeout]", causes)
	}
}

// drainPool tears down every parked instance in a superseded pool and must
// report each one recycled with cause "pool_swapped".
//
// The drain under test is the one swapConfig itself starts — this test must NOT
// call drainPool on the same pool. Two drainers racing for the same p.free
// channel each want `size` instances out of a channel that only ever yields
// `size` total, so the loser blocks forever. An earlier version of this test
// did exactly that and deadlocked: it passed when run alone (the test goroutine
// won both instances and the engine's drainer was left as a zombie) and hung
// for ten minutes under `go test ./...`, where CPU contention let the engine's
// drainer take one. Assert on the engine's drain, don't stage a competing one.
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
	// The second swap supersedes the first pool, and swapConfig hands that pool
	// to a drain goroutine of its own. Both of its instances must come back
	// reported as "pool_swapped".
	if err := e.swapConfig(ctx, []byte(`{"rules":[]}`), 2, 2); err != nil {
		t.Fatalf("swap 2: %v", err)
	}

	// The drain is asynchronous, so poll rather than read once. Polling is safe
	// here in a way it is not for tests that pin an interleaving: this asserts a
	// terminal count, so waiting longer can only let MORE recycles land — it
	// cannot manufacture the expected answer out of a broken drain.
	deadline := time.Now().Add(10 * time.Second)
	var causes []string
	for {
		causes = rec.recycledCauses()
		if len(causes) >= 2 || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	if len(causes) != 2 {
		t.Fatalf("recycled = %#v, want 2 pool_swapped entries", causes)
	}
	for _, cause := range causes {
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

	compiles := rec.compileResults()
	if len(compiles) != 2 {
		t.Fatalf("compile notifications = %#v, want 2", compiles)
	}
	if compiles[0] != "miss" {
		t.Fatalf("first compile = %q, want miss", compiles[0])
	}
	if compiles[1] != "hit" {
		t.Fatalf("second compile = %q, want hit", compiles[1])
	}
}
