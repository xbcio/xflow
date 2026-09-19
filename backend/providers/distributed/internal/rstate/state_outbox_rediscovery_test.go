package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// This file answers the R6 duplicate-dispatch question for the cursor-paged
// outbox discovery added in v0.0.12: can its wrap/rotation semantics rediscover
// an execution and cause the same node to be dispatched twice — which would mint
// a second lease token and make the first runner's report a stale-token 409?
//
// The answer is no, and the reason is that rediscovery and redelivery are
// different questions. The cursor is a resumable keyspace walk over
// `...:outbox:ready`, and a wrap (or a SCAN restart from cursor 0) can enumerate
// the same execution ID again. What it cannot do is hand the same outbox ENTRY
// to two deliverers, because delivery is gated by a per-entry lease
// (LeaseOutbox / OutboxDeliveryLeaseTTL), not by the discovery scan. A second
// discovery of the same execution at worst costs one extra FlushOutbox, which
// finds every entry already leased and delivers nothing.
//
// The distinction matters because the two failure modes look identical in a log:
// both show the same execution ID handled twice.

// TestOutboxRediscoveryCannotRedeemALeasedEntry is the fence: the same execution
// being discovered again must not yield its entry a second time while the first
// delivery still holds it.
func TestOutboxRediscoveryCannotRedeemALeasedEntry(t *testing.T) {
	const execID = types.ExecutionID("exec-rediscover-0")
	state, _ := newOutboxDiscoveryTestStore(t, map[namespace.Namespace][]types.ExecutionID{
		namespace.Default: {execID},
	})
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	first, err := state.LeaseOutbox(ctx, execID, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("first LeaseOutbox() error = %v", err)
	}
	if len(first) != 1 {
		t.Fatalf("first LeaseOutbox() returned %d entries, want 1", len(first))
	}

	for attempt := 0; attempt < 3; attempt++ {
		again, err := state.LeaseOutbox(ctx, execID, time.Now().UTC(), 8)
		if err != nil {
			t.Fatalf("rediscovery attempt %d LeaseOutbox() error = %v", attempt+1, err)
		}
		if len(again) != 0 {
			t.Fatalf("rediscovery attempt %d re-leased %d entries (%v); the discovery scan "+
				"re-running must not become a second delivery",
				attempt+1, len(again), outboxEntryIDs(again))
		}
	}
}

// TestOutboxDiscoveryWrapRediscoveryIsIdempotent walks the discovery scan past a
// full turnover and asserts the property that matters end to end: however many
// times an execution ID is enumerated, only one deliverer holds its entry at a
// time. This is what stops the cursor's wrap from minting a second lease.
func TestOutboxDiscoveryWrapRediscoveryIsIdempotent(t *testing.T) {
	ids := []types.ExecutionID{"exec-wrap-0", "exec-wrap-1"}
	state, hook := newOutboxDiscoveryTestStore(t, map[namespace.Namespace][]types.ExecutionID{
		namespace.Default: ids,
	})
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	// One id per page: with two keys the scan reaches the end of the keyspace,
	// wraps to cursor 0, and enumerates both ids again on the next cycle.
	const limit = 1
	discovered := map[types.ExecutionID]int{}
	for call := 0; call < 4; call++ {
		page, err := state.ListOutboxExecutions(ctx, limit)
		if err != nil {
			t.Fatalf("ListOutboxExecutions() call %d error = %v", call+1, err)
		}
		for _, id := range page {
			discovered[id]++
		}
	}
	cursors := hook.cursorsFor(namespace.Default, "outbox:ready")
	if len(cursors) < 2 {
		t.Fatalf("SCAN cursors = %v, want at least one resume and one wrap", cursors)
	}

	// Every discovery is followed by exactly one delivery attempt, and the
	// second attempt for each id is answered with nothing.
	delivered := map[types.ExecutionID]int{}
	for _, id := range ids {
		for attempt := 0; attempt < 3; attempt++ {
			entries, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
			if err != nil {
				t.Fatalf("LeaseOutbox(%q) attempt %d error = %v", id, attempt+1, err)
			}
			delivered[id] += len(entries)
		}
		if delivered[id] != 1 {
			t.Fatalf("%q was leased for delivery %d times across repeated discovery, want 1",
				id, delivered[id])
		}
	}
	if discovered[ids[0]] < 2 || discovered[ids[1]] < 2 {
		t.Fatalf("expected the wrap to enumerate both ids more than once, got %v "+
			"(cursors %v); this test is not exercising the wrap it claims to",
			discovered, cursors)
	}
}

func outboxEntryIDs(entries []engine.OutboxEntry) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.ID)
	}
	return out
}
