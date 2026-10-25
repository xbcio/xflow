package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestUnparsableDeadlineIsNotRemembered covers the read that succeeds but
// returns something that is not a deadline. It says nothing about whether the
// execution has a time budget, which makes it the same class as a failed read:
// remembering it as "no budget" would pin the replica to the default retention
// for as long as it runs, while the value itself may be repaired or rewritten
// the next moment. The answer is the base retention now and another read next
// time.
func TestUnparsableDeadlineIsNotRemembered(t *testing.T) {
	ctx := context.Background()
	_, rdb, creator := newMiniredisStore(t)
	id := types.ExecutionID("replica-deadline-garbled")
	deadline := time.Now().Add(replicaBudget)
	createWithDeadline(t, creator, ctx, id, deadline)

	key := execKey(namespace.Default, id, "deadline_ms")
	if err := rdb.Set(ctx, key, "not-a-number", time.Hour).Err(); err != nil {
		t.Fatalf("corrupt deadline_ms: %v", err)
	}

	hook := &deadlineReadHook{}
	rdb.AddHook(hook)
	other := newReplica(rdb)

	for i := 0; i < 3; i++ {
		if got := other.getExecTTL(ctx, id); got != replicaDefaultRetention {
			t.Fatalf("getExecTTL #%d with an unparsable deadline_ms = %v, want the %v base retention", i+1, got, replicaDefaultRetention)
		}
		requireMemo(t, other, id, false)
	}
	if got := hook.reads.Load(); got != 3 {
		t.Fatalf("deadline_ms was read %d times over 3 calls, want 3: an unparsable value is retried, not remembered", got)
	}

	// Once the value is a deadline again the replica picks it up and settles.
	if err := rdb.Set(ctx, key, deadline.UnixMilli(), time.Hour).Err(); err != nil {
		t.Fatalf("repair deadline_ms: %v", err)
	}
	requireOutlivesDeadline(t, other.getExecTTL(ctx, id), deadline, "getExecTTL after deadline_ms was repaired")
	if memo := requireMemo(t, other, id, true); memo.UnixMilli() != deadline.UnixMilli() {
		t.Fatalf("memo after the repair = %v, want %v", memo, deadline)
	}
}
