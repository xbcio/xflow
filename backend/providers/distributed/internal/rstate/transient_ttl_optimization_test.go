package rstate

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/alicebob/miniredis/v2/server"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
	"github.com/redis/go-redis/v9"
)

// commandRecorder is a miniredis pre-hook that counts how often each command
// (lowercased) is issued. It is used to verify the transient TTL bookkeeping no
// longer pays a per-mutation SMEMBERS round-trip.
type commandRecorder struct {
	mu       sync.Mutex
	counts   map[string]int
	smembers int
}

func newCommandRecorder() *commandRecorder { return &commandRecorder{counts: map[string]int{}} }

// TestTransientRefreshNoLongerIssuesSMembersOnMutation verifies that the
// transient TTL bookkeeping issues no SMEMBERS and no SADD anywhere: the
// per-mutation refresh is now a no-op (structural keys get transientTTL at
// creation; per-node keys get it from their own Lua), and completion-time TTL
// shortening enumerates keys from the graph instead of a tracked :keys set.
func TestTransientRefreshNoLongerIssuesSMembersOnMutation(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rec := newCommandRecorder()
	mr.Server().SetPreHook(server.Hook(func(_ *server.Peer, cmd string, _ ...string) bool {
		// miniredis delivers commands uppercased; compare case-insensitively.
		c := strings.ToLower(cmd)
		rec.mu.Lock()
		defer rec.mu.Unlock()
		rec.counts[c]++
		if c == "smembers" {
			rec.smembers++
		}
		return false // do not intercept
	}))

	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-cmd-count")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Each UpsertNode is a state mutation that previously triggered
	// refreshTransientTTL → SADD + structural EXPIRE. It is now a no-op.
	for range 5 {
		if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID: id,
			Name:        "start",
			NodeIdx:     0,
			Status:      types.NodeStatusRunning,
		}); err != nil {
			t.Fatalf("UpsertNode() error = %v", err)
		}
	}

	// Completion shortening (triggered by a terminal status) now enumerates keys
	// from the graph — no SMEMBERS.
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus() error = %v", err)
	}

	if rec.smembers != 0 {
		t.Fatalf("SMEMBERS count = %d, want 0 (refresh is a no-op; completion enumerates from graph)", rec.smembers)
	}
	if rec.counts["sadd"] != 0 {
		t.Fatalf("SADD count = %d, want 0 (the per-execution :keys set is gone)", rec.counts["sadd"])
	}
}

// TestTransientStructuralKeysGetTransientTTLAtCreation confirms optimization 4's
// safety property: the structural keys (:status/:graph/:params/...) are written
// with transientTTL at CreateExecution, so they outlive the run under the
// documented constraint (transientTTL > max wall-clock) WITHOUT the per-mutation
// refresh the previous design paid on every state mutation.
func TestTransientStructuralKeysGetTransientTTLAtCreation(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Hour)
	state.transient = true
	state.transientTTL = time.Minute
	state.transientCompletionTTL = 30 * time.Second

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-structural")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  testGraphOneNode(),
		Params: map[string]any{"k": "v"},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	structural := []string{execKey(namespace.Default, id, "status"), execKey(namespace.Default, id, "graph"), execKey(namespace.Default, id, "params")}
	for _, key := range structural {
		got := rdb.TTL(ctx, key).Val()
		if got < 55*time.Second {
			t.Fatalf("creation TTL for %q = %s, want close to transientTTL (%s)", key, got, state.transientTTL)
		}
	}

	// A subsequent mutation neither shortens nor is required to refresh them.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusRunning,
	}); err != nil {
		t.Fatalf("UpsertNode() error = %v", err)
	}
	for _, key := range structural {
		got := rdb.TTL(ctx, key).Val()
		if got < 55*time.Second {
			t.Fatalf("post-mutation TTL for %q = %s, want still close to transientTTL", key, got)
		}
	}
}
