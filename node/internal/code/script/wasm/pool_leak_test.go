package wasm

import (
	"context"
	"testing"
	"time"
)

// TestDrainPoolReturnsWhenInstancesWereInFlight is the regression test for a
// goroutine leaked on every config swap.
//
// Mechanism: drainPool waited for exactly p.size receives from p.free. But an
// instance that was borrowed at swap time never arrives there — giveBack sees
// its pool is no longer active and tears it down itself (pool.go's giveBack),
// and doom does the same. So a swap with k borrows in flight left drainPool
// waiting for k receives that could never happen, and the goroutine stayed
// parked for the process lifetime.
//
// The test builds the state directly rather than through swapConfig: a real swap
// starts its own drainer, and two drainers racing for one p.free channel each
// want `size` instances from a channel that yields fewer, so the loser blocks —
// the deadlock TestDrainPoolNotifiesObserverPoolSwapped documents. Here the pool
// is hand-built and this test is its only drainer.
func TestDrainPoolReturnsWhenInstancesWereInFlight(t *testing.T) {
	ctx := context.Background()
	h := newReactorHost()
	e, err := h.engineFor(ctx, reactorWasm)
	if err != nil {
		t.Fatalf("engineFor: %v", err)
	}

	// A retired pool declaring 2 instances but holding only 1: the other is
	// "in flight" and will be reclaimed by its borrower, never parked here.
	inst, err := e.newInstance(ctx, []byte(`{"rules":[]}`))
	if err != nil {
		t.Fatalf("newInstance: %v", err)
	}
	retired := &activePool{
		cfg:  []byte(`{"rules":[]}`),
		free: make(chan *pooledInstance, 2),
		size: 2,
		// Short bound so this probe measures whether drainPool honours ANY bound,
		// not how long the production bound is. With the unfixed code the wait is
		// unbounded, so no value here would let it return.
		drainTimeout: 200 * time.Millisecond,
	}
	retired.free <- inst

	returned := make(chan struct{})
	go func() {
		e.drainPool(ctx, retired)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(10 * time.Second):
		t.Fatal("drainPool never returned for a pool that had an instance in flight " +
			"at swap time. That instance is reclaimed by its own borrower and never " +
			"reaches p.free, so an unbounded wait parks this goroutine for the " +
			"process lifetime — one leak per swap per in-flight borrow.")
	}
}

// TestDoomReportsRebuildFailure pins that a failed replacement is visible.
//
// doom rebuilds asynchronously so the pool returns to full strength. Nothing
// retries that rebuild, so a failure narrows the pool permanently — and pool
// width is what bounds wasm concurrency. Reported as a recycle cause because the
// operator response is the same as for the other causes: the pool is short an
// instance and borrowers will queue.
//
// The rebuild is made to fail by giving the retired pool a config the guest
// rejects, which is the same failure mode a corrupted or newly-incompatible
// supply produces in production.
func TestDoomReportsRebuildFailure(t *testing.T) {
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
	// Poison the live pool's replay config so the async rebuild fails. Safe here
	// and nowhere else: nothing else reads pool.cfg for the rest of this test.
	pool.cfg = []byte(`{"rules":[{"bad":true}]}`)

	e.doom(ctx, pool, inst)

	// The rebuild runs on its own goroutine. Polling a terminal count is safe —
	// waiting longer can only let more causes land, never invent the one wanted.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if hasCause(rec.recycledCauses(), "rebuild_failed") || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	causes := rec.recycledCauses()
	if !hasCause(causes, "rebuild_failed") {
		t.Fatalf("recycled causes = %#v, want a rebuild_failed entry. The pool is now "+
			"permanently one instance narrower and nothing retries the rebuild, so "+
			"without this metric the shrinkage is indistinguishable from contention.",
			causes)
	}
	// The doom itself must still be reported: rebuild_failed is an ADDITIONAL
	// signal, not a replacement for the eval_error that caused the doom.
	if !hasCause(causes, "eval_error") {
		t.Errorf("recycled causes = %#v, want the eval_error entry as well", causes)
	}
}

func hasCause(causes []string, want string) bool {
	for _, c := range causes {
		if c == want {
			return true
		}
	}
	return false
}
