package rstate

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestDeliverSignalWithOutboxEmptyBodyPreservesWaiterState covers M5: on the
// TOCTOU empty-body path (a waiter is present but Go built the call before the
// waiter became visible, so entryBody is empty), the waiter/suspend teardown
// must run AFTER the empty-body guard — never before. The signal is stored and
// the full waiter state (signalKey, waiterKey, resumeLockKey, waiterSpecKey,
// suspended-set membership, timeout ZSET member) is left intact so a later
// delivery or the suspend timeout can still resume the node. Driving the Lua
// directly is the faithful way to reach this branch (the public Go API only
// produces an empty entryBody when NodeName is empty, which would also change
// the derived keys).
func TestDeliverSignalWithOutboxEmptyBodyPreservesWaiterState(t *testing.T) {
	_, _, rdb := newTestRedisState(t)
	ctx := context.Background()
	tns := namespace.Default
	id := types.ExecutionID("m5-empty-body")
	node := "wait"
	sig := "go"
	ttl := 300

	// Seed the full waiter/suspend state that a parked node holds.
	if err := rdb.Set(ctx, waiterKey(tns, id, sig), "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, resumeLockKey(tns, id, node), "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.Set(ctx, waiterSpecKey(tns, id, node), `{"mode":"signal"}`, 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := rdb.SAdd(ctx, suspendedNodesKey(tns, id), node).Err(); err != nil {
		t.Fatal(err)
	}
	tmember := timeoutMember(id, node)
	if err := rdb.ZAdd(ctx, timeoutZSetKey(tns, id), redis.Z{Score: float64(time.Now().Add(time.Hour).UnixMilli()), Member: tmember}).Err(); err != nil {
		t.Fatal(err)
	}

	keys := []string{
		signalKey(tns, id, sig),
		waiterKey(tns, id, sig),
		suspendedNodesKey(tns, id),
		signalBatchKey(tns, id, node),
		waiterSpecKey(tns, id, node),
		resumeLockKey(tns, id, node),
		outboxReadyKey(tns, id),
		outboxBodyKey(tns, id),
		timeoutZSetKey(tns, id),
		nodeMetaKey(tns, id, node),
	}
	// ARGV[5] (entryBody) is empty: no resume entry can be constructed.
	args := []any{
		`{"v":1}`, ttl, node,
		"", "", time.Now().UTC().UnixMilli(), 0, 1, sig, tmember,
		string(id), node, sig,
	}
	res, err := deliverSignalWithOutboxLua.Run(ctx, rdb, keys, args...).Result()
	if err != nil && err != redis.Nil {
		t.Fatalf("deliverSignalWithOutboxLua error = %v", err)
	}
	if s, _ := res.(string); s != "" {
		t.Fatalf("empty-body delivery returned %q, want '' (no resume committed)", s)
	}

	// The signal is stored so a later successful-peek delivery / timeout resumes.
	if got, err := rdb.Get(ctx, signalKey(tns, id, sig)).Result(); err != nil || got != `{"v":1}` {
		t.Fatalf("signalKey = %q err=%v, want the stored signal", got, err)
	}
	// Waiter state must be INTACT — teardown must not precede the empty-body guard.
	if n, _ := rdb.Exists(ctx, waiterKey(tns, id, sig)).Result(); n != 1 {
		t.Fatal("waiterKey torn down on empty-body path (M5 regression)")
	}
	if n, _ := rdb.Exists(ctx, resumeLockKey(tns, id, node)).Result(); n != 1 {
		t.Fatal("resumeLockKey torn down on empty-body path (M5 regression)")
	}
	if n, _ := rdb.Exists(ctx, waiterSpecKey(tns, id, node)).Result(); n != 1 {
		t.Fatal("waiterSpecKey torn down on empty-body path (M5 regression)")
	}
	if isMember, _ := rdb.SIsMember(ctx, suspendedNodesKey(tns, id), node).Result(); !isMember {
		t.Fatal("node removed from suspended set on empty-body path (M5 regression)")
	}
	if _, err := rdb.ZScore(ctx, timeoutZSetKey(tns, id), tmember).Result(); err != nil {
		t.Fatalf("timeout member removed on empty-body path (M5 regression): %v", err)
	}
	// No resume outbox entry may have been written.
	if n, _ := rdb.ZCard(ctx, outboxReadyKey(tns, id)).Result(); n != 0 {
		t.Fatalf("outbox ready has %d entries, want 0 (no resume on empty body)", n)
	}
}

// TestRedisCompleteExpandedSubExecutionPreservesResultFidelity covers M4: the
// expansion complete path splices the raw result JSON via a placeholder rather
// than round-tripping it through cjson (which mutates empty object {} -> [] and
// rounds int64 > 2^53). It also asserts the lowercase json tags let the Lua
// read/write child.status (the transition fires) and that allDone only becomes
// true after ALL batches complete. Uses the real-Redis harness so genuine cjson
// behavior is exercised; skips when XFLOW_TEST_REDIS_ADDR is unset.
func TestRedisCompleteExpandedSubExecutionPreservesResultFidelity(t *testing.T) {
	addr := realRedisAddr(t)
	state := newRealRedisState(t, addr)
	ctx := context.Background()
	id := types.ExecutionID(fmt.Sprintf("expansion-fidelity-%d", time.Now().UnixNano()))
	node := "loop"
	lease := &engine.TaskLease{
		LeaseID:    "lease-fid",
		LeaseToken: "token-fid",
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    node,
			NodeIdx:     2,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	lease.Attempt = 1
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v", claimed, err)
	}
	if started, err := state.BeginTaskExpansion(ctx, lease); err != nil || !started {
		t.Fatalf("BeginTaskExpansion() started=%v err=%v", started, err)
	}
	for i, cid := range []types.ExecutionID{"child-0", "child-1"} {
		if accepted, err := state.CreateExpandedSubExecution(ctx, lease, &engine.SubExecution{
			ParentExecID: id, ParentNode: node, ChildExecID: cid, BatchIndex: i, Status: types.ExecutionStatusRunning,
		}); err != nil || !accepted {
			t.Fatalf("CreateExpandedSubExecution(%d) accepted=%v err=%v", i, accepted, err)
		}
	}

	const bigInt = int64(9007199254740993) // 2^53 + 1: not representable as float64
	result0 := map[string]any{"empty": map[string]any{}, "big": bigInt}

	// Complete child-0: accepted, but allDone must stay false (child-1 running).
	allDone, accepted, _, err := state.CompleteExpandedSubExecution(ctx, lease, "child-0", types.ExecutionStatusSuccess, result0)
	if err != nil || !accepted || allDone {
		t.Fatalf("complete child-0 = allDone=%v accepted=%v err=%v, want false/true/nil", allDone, accepted, err)
	}

	// Inspect the raw stored child JSON for fidelity (splice, not cjson round-trip).
	key := expansionSubExecutionKey(namespace.Default, id, node, lease.LeaseID)
	raw, err := state.rdb.HGet(ctx, key, "child-0").Result()
	if err != nil {
		t.Fatalf("HGet child-0 raw: %v", err)
	}
	if !strings.Contains(raw, "9007199254740993") {
		t.Fatalf("stored result lost int64 precision (cjson round-trip regression): %s", raw)
	}
	if !strings.Contains(raw, `"empty":{}`) {
		t.Fatalf("stored result mutated empty object {}->[] (cjson round-trip regression): %s", raw)
	}
	// The lowercase json tags let the Lua read/write child.status: transition fired.
	if !strings.Contains(raw, `"status":"success"`) {
		t.Fatalf("child status transition did not fire (M4 lowercase-tag regression): %s", raw)
	}

	// Complete child-1: now allDone becomes true and results return in batch order.
	allDone, accepted, results, err := state.CompleteExpandedSubExecution(ctx, lease, "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil || !accepted || !allDone {
		t.Fatalf("complete child-1 = allDone=%v accepted=%v err=%v, want true/true/nil", allDone, accepted, err)
	}
	if len(results) != 2 {
		t.Fatalf("aggregated results = %d, want 2 (both batches)", len(results))
	}
	if _, ok := results[0]["empty"].(map[string]any); !ok {
		t.Fatalf("results[0].empty = %#v, want empty object preserved as a map", results[0]["empty"])
	}
}
