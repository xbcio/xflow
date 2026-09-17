package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func TestTransientSubExecutionTTLUsesParentRemainingLifetime(t *testing.T) {
	state, mr, rdb := newTestRedisState(t)
	ctx := context.Background()
	id := types.ExecutionID("transient-subexec-remaining")
	state.transient = true
	state.transientTTL = 10 * time.Second
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	statusKey := execKey(namespace.Default, id, "status")
	subsKey := subExecutionKey(namespace.Default, id, "start")
	mr.FastForward(3 * time.Second)
	before, err := rdb.PTTL(ctx, statusKey).Result()
	if err != nil {
		t.Fatalf("parent PTTL() error = %v", err)
	}
	if err := state.CreateSubExecution(ctx, &engine.SubExecution{
		ParentExecID: id, ParentNode: "start", ChildExecID: "child-1", Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateSubExecution() error = %v", err)
	}
	got, err := rdb.PTTL(ctx, subsKey).Result()
	if err != nil {
		t.Fatalf("subs PTTL() error = %v", err)
	}
	assertPTTLNear(t, got, before, 100*time.Millisecond, "subs after create")
	if got >= state.transientTTL-time.Second {
		t.Fatalf("subs PTTL = %v, appears renewed to full transient TTL %v", got, state.transientTTL)
	}

	mr.FastForward(2 * time.Second)
	beforeComplete, err := rdb.PTTL(ctx, subsKey).Result()
	if err != nil {
		t.Fatalf("subs PTTL before completion error = %v", err)
	}
	allDone, err := state.CompleteSubExecution(ctx, id, "start", "child-1", types.ExecutionStatusSuccess, map[string]any{"ok": true})
	if err != nil {
		t.Fatalf("CompleteSubExecution() error = %v", err)
	}
	if !allDone {
		t.Fatal("CompleteSubExecution() allDone = false, want true")
	}
	afterComplete, err := rdb.PTTL(ctx, subsKey).Result()
	if err != nil {
		t.Fatalf("subs PTTL after completion error = %v", err)
	}
	assertPTTLNear(t, afterComplete, beforeComplete, 100*time.Millisecond, "subs after completion")
	if afterComplete >= state.transientTTL-time.Second {
		t.Fatalf("subs PTTL after completion = %v, appears renewed to full TTL %v", afterComplete, state.transientTTL)
	}
}

func TestTransientSubExecutionRejectsExpiredOrPermanentParent(t *testing.T) {
	state, mr, rdb := newTestRedisState(t)
	ctx := context.Background()
	state.transient = true
	state.transientTTL = time.Second

	t.Run("expired parent", func(t *testing.T) {
		id := types.ExecutionID("transient-subexec-expired")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: testGraphOneNode(), Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		mr.FastForward(2 * time.Second)
		if err := state.CreateSubExecution(ctx, &engine.SubExecution{ParentExecID: id, ParentNode: "start", ChildExecID: "child", Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateSubExecution() error = %v", err)
		}
		if exists, err := rdb.Exists(ctx, subExecutionKey(namespace.Default, id, "start")).Result(); err != nil || exists != 0 {
			t.Fatalf("expired parent left subs key: exists=%d err=%v", exists, err)
		}
	})

	t.Run("permanent parent", func(t *testing.T) {
		id := types.ExecutionID("transient-subexec-permanent")
		statusKey := execKey(namespace.Default, id, "status")
		if err := rdb.Set(ctx, statusKey, string(types.ExecutionStatusRunning), 0).Err(); err != nil {
			t.Fatalf("seed permanent parent error = %v", err)
		}
		if err := state.CreateSubExecution(ctx, &engine.SubExecution{ParentExecID: id, ParentNode: "start", ChildExecID: "child", Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateSubExecution() error = %v", err)
		}
		if exists, err := rdb.Exists(ctx, subExecutionKey(namespace.Default, id, "start")).Result(); err != nil || exists != 0 {
			t.Fatalf("permanent parent left subs key: exists=%d err=%v", exists, err)
		}
	})
}

func assertPTTLNear(t *testing.T, got, want, tolerance time.Duration, label string) {
	t.Helper()
	if delta := got - want; delta < -tolerance || delta > tolerance {
		t.Fatalf("%s PTTL = %v, want approximately %v (±%v)", label, got, want, tolerance)
	}
}
