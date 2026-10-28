package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// parkWaitingNode creates a running execution with one node suspended on the
// signal "go", the state an expiring execution is typically found in. Only the
// multi-signal mode stores a waiter spec, which is what lets a cleanup find the
// signal-name keyed waiter again; a single-signal park leaves nothing to
// enumerate it by.
func parkWaitingNode(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, deadline time.Time, mode types.SuspendMode) {
	t.Helper()
	g := errorDetailsRedisGraph(t, "wait")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning, Deadline: deadline,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	lease := privateRedisLease(id, "wait", 0, "lease-park", "token-park")
	lease.Task.ActivationID = 1
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease claimed=%v err=%v", claimed, err)
	}
	payload, _, err := state.SuspendTaskLease(ctx, lease, nil, false, false, &types.SuspendSpec{
		Mode: mode, Signals: []string{"go"}, Quorum: 1, Timeout: time.Hour,
	}, "")
	if err != nil || payload != nil {
		t.Fatalf("SuspendTaskLease payload=%+v err=%v, want parked (nil payload)", payload, err)
	}
	nodes, err := state.ListSuspendedNodes(ctx, id)
	if err != nil || len(nodes) != 1 || nodes[0] != "wait" {
		t.Fatalf("ListSuspendedNodes after the park = (%v, %v), want [wait]", nodes, err)
	}
}

// TestTimeoutWriteKeepsSuspendedNodesButDropsWaiters pins the trimmed cleanup
// the timeout status write runs. The suspended-nodes set must survive it: the
// expiry cascade lists that set after the status write, and a cleanup that
// deleted it (as cancel's does) would leave every suspended node parked. What
// could still drive work -- the waiter and the timeout index -- must go.
func TestTimeoutWriteKeepsSuspendedNodesButDropsWaiters(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	id := types.ExecutionID("timeout-cleanup")
	parkWaitingNode(t, state, ctx, id, time.Now().Add(time.Hour), types.ModeMultiSignal)
	ns := namespace.Default
	if err := rdb.ZAdd(ctx, timeoutZSetKey(ns, id), redis.Z{Score: float64(time.Now().Unix()), Member: timeoutMember(id, "wait")}).Err(); err != nil {
		t.Fatalf("ZAdd timeout: %v", err)
	}
	if n, _ := rdb.Exists(ctx, waiterKey(ns, id, "go"), waiterSpecKey(ns, id, "wait"), timeoutZSetKey(ns, id)).Result(); n != 3 {
		t.Fatalf("precondition: %d of the waiter, waiter spec and timeout keys exist, want all three", n)
	}

	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout): %v", err)
	}

	nodes, err := state.ListSuspendedNodes(ctx, id)
	if err != nil || len(nodes) != 1 || nodes[0] != "wait" {
		t.Fatalf("ListSuspendedNodes after the timeout write = (%v, %v), want [wait] kept for the cascade", nodes, err)
	}
	for name, key := range map[string]string{
		"waiter":       waiterKey(ns, id, "go"),
		"waiter spec":  waiterSpecKey(ns, id, "wait"),
		"timeout zset": timeoutZSetKey(ns, id),
		"lease expiry": leaseExpiryZSetKey(ns, id),
	} {
		if n, err := rdb.Exists(ctx, key).Result(); err != nil || n != 0 {
			t.Fatalf("%s key %q survived the timeout write (exists=%d err=%v)", name, key, n, err)
		}
	}
}

// TestSignalAfterTimeoutWritesNoResume is the reason the waiter keys must go:
// the store's signal delivery only checks that a waiter exists, never the
// execution's status, so a surviving waiter would write a resume for a timed-out
// execution.
func TestSignalAfterTimeoutWritesNoResume(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	id := types.ExecutionID("timeout-signal")
	parkWaitingNode(t, state, ctx, id, time.Now().Add(time.Hour), types.ModeMultiSignal)
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusTimeout, engine.ExecutionDeadlineExceededError); err != nil {
		t.Fatalf("UpdateExecutionStatus(timeout): %v", err)
	}

	node, _, committed, err := state.DeliverSignalWithOutbox(ctx, id, "go", map[string]any{"by": "late"},
		engine.ResumeIntent{NodeName: "wait", NodeIdx: 0, ActivationID: 1})
	if err != nil || committed || node != "" {
		t.Fatalf("DeliverSignalWithOutbox after the timeout = (%q, %v, %v), want no resume", node, committed, err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil || len(entries) != 0 {
		t.Fatalf("ListOutbox after the late signal = (%+v, %v), want empty", entries, err)
	}
}

// TestExpireExecutionCancelsSuspendedNodesOnRedis runs the real cascade against
// the Redis store: it finds the node through the set the timeout write kept,
// cancels it, and acknowledges the deadline entry.
func TestExpireExecutionCancelsSuspendedNodesOnRedis(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	id := types.ExecutionID("timeout-expire")
	parkWaitingNode(t, state, ctx, id, time.Now().Add(-time.Second), types.ModeSignal)
	eng := engine.New(state, &StoreTestQueue{})

	outcome, err := eng.ExpireExecution(ctx, id)
	if err != nil || outcome != engine.ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution = (%q, %v), want expired", outcome, err)
	}

	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("node after the expiry = (%+v, %v), want canceled", node, err)
	}
	if nodes, err := state.ListSuspendedNodes(ctx, id); err != nil || len(nodes) != 0 {
		t.Fatalf("ListSuspendedNodes after the cascade = (%v, %v), want the node removed by its cancel", nodes, err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil || snap.Status != types.ExecutionStatusTimeout {
		t.Fatalf("execution after the expiry = (%+v, %v), want timeout", snap, err)
	}
	if inIndex(t, rdb, namespace.Default, id) {
		t.Fatal("the deadline entry was not acknowledged after the cascade")
	}
}

// TestEngineSignalAfterTimeoutResumesNothing covers the single-signal park,
// whose waiter key the store cleanup cannot enumerate (no waiter spec is stored
// for it, exactly as after a cancel). The engine refuses to resume into an
// inactive execution, and a timed-out one is inactive, so the late signal still
// writes nothing to the outbox.
func TestEngineSignalAfterTimeoutResumesNothing(t *testing.T) {
	ctx := context.Background()
	_, _, state := newMiniredisStore(t)
	id := types.ExecutionID("timeout-engine-signal")
	parkWaitingNode(t, state, ctx, id, time.Now().Add(-time.Second), types.ModeSignal)
	eng := engine.New(state, &StoreTestQueue{})
	if outcome, err := eng.ExpireExecution(ctx, id); err != nil || outcome != engine.ExpireOutcomeExpired {
		t.Fatalf("ExpireExecution = (%q, %v), want expired", outcome, err)
	}

	if err := eng.DeliverSignal(ctx, id, "go", map[string]any{"by": "late"}); err != nil {
		t.Fatalf("DeliverSignal after the timeout: %v", err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 4)
	if err != nil || len(entries) != 0 {
		t.Fatalf("ListOutbox after the late signal = (%+v, %v), want empty", entries, err)
	}
	node, err := state.GetNode(ctx, id, "wait")
	if err != nil || node == nil || node.Status != types.NodeStatusCanceled {
		t.Fatalf("node after the late signal = (%+v, %v), want it still canceled", node, err)
	}
}
