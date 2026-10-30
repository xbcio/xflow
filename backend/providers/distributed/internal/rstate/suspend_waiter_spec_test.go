package rstate

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestRedisSingleSignalParkStoresWaiterSpec pins the suspend script's spec
// write for a single-signal wait. The signal-named waiter keys are only
// reachable through the stored spec: without it, a cancel or a timeout of the
// execution could not enumerate which waiter:<signal> keys to retire, and the
// keys outlived the execution until their TTL. Delivery never reads the stored
// mode for a single-signal wait, and its consume path already deleted the key,
// so storing it changes no delivery behaviour — which the second act asserts:
// delivering the signal still wakes the node and tears the spec down with the
// waiter.
func TestRedisSingleSignalParkStoresWaiterSpec(t *testing.T) {
	ctx := context.Background()
	state, _, rdb := newTestRedisState(t)
	id := types.ExecutionID("mcr-spec-write")
	mcrPark(t, state, id, types.ModeSignal, "approve")

	spec, err := state.loadWaiterSpec(ctx, id, "alpha")
	if err != nil || spec == nil || len(spec.Signals) != 1 || spec.Signals[0] != "approve" {
		t.Fatalf("loadWaiterSpec after the park = %+v, %v; want the parked single-signal spec", spec, err)
	}

	node, payload, err := state.DeliverSignal(ctx, id, "approve", map[string]any{"by": "wake"})
	if err != nil || node != "alpha" {
		t.Fatalf("DeliverSignal = (%q, %+v, %v); want the parked node woken", node, payload, err)
	}
	if n, _ := rdb.Exists(ctx,
		waiterKey(namespace.Default, id, "approve"),
		waiterSpecKey(namespace.Default, id, "alpha"),
	).Result(); n != 0 {
		t.Fatalf("%d waiter/spec keys survived the delivery, want none", n)
	}
}

// TestRedisSingleSignalImmediateSatisfyStoresNoSpec keeps the write on the
// parked path only: a wait whose signal was already delivered returns before
// parking, and must not leave a spec nothing will clean up.
func TestRedisSingleSignalImmediateSatisfyStoresNoSpec(t *testing.T) {
	ctx := context.Background()
	state, _, rdb := newTestRedisState(t)
	id := types.ExecutionID("mcr-spec-satisfied")
	if _, _, err := state.DeliverSignal(ctx, id, "approve", map[string]any{"by": "early"}); err != nil {
		t.Fatalf("DeliverSignal before the park: %v", err)
	}
	mcrPark(t, state, id, types.ModeSignal, "approve")
	if n, _ := rdb.Exists(ctx, waiterSpecKey(namespace.Default, id, "alpha")).Result(); n != 0 {
		t.Fatal("an immediately satisfied wait stored a waiter spec")
	}
}
