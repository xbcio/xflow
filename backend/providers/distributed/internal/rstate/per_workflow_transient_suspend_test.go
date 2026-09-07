package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestPerWorkflowTransient_SuspendUsesWorkflowTTL pins the TTL a suspended node
// gets on a per-workflow transient execution.
//
// The store-wide transient guard in SuspendOrConsume does not fire here --
// engine.WithSuspendDisabled is set only from the store-wide mode, so a
// per-workflow transient execution really parks a waiter. When it does, every
// key it touches must carry the workflow's transient TTL. Falling back to the
// adapter default keeps a declared-ephemeral execution's keys alive for the
// durable retention window instead.
func TestPerWorkflowTransient_SuspendUsesWorkflowTTL(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	// Adapter default is 24h; the workflow declares a 2m transient TTL.
	state := New(rdb, nil, 24*time.Hour)
	state.transient = false

	ctx := context.Background()
	id := types.ExecutionID("exec-transient-suspend")
	tg := testTransientGraph() // TransientTTL = 2m
	tctx := engine.WithExecutionTransient(ctx, engine.TransientHint{
		TTL:           tg.TransientTTL(),
		CompletionTTL: tg.TransientCompletionTTL(),
	})
	if err := state.CreateExecution(tctx, &engine.ExecutionSnapshot{
		ID:     id,
		Status: types.ExecutionStatusRunning,
		Graph:  tg,
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}

	spec := &types.SuspendSpec{Signals: []string{"approval"}}
	payload, err := state.SuspendOrConsume(ctx, id, "start", spec)
	if err != nil {
		t.Fatalf("SuspendOrConsume: %v", err)
	}
	if payload != nil {
		t.Fatalf("expected the node to park, got a pre-delivered signal: %+v", payload)
	}

	// extendExecTTL slides every structural key to suspendTTL. Assert on the
	// status key: it is written by CreateExecution with the transient TTL, so a
	// value above 2m can only have come from the suspend path.
	got := mr.TTL(execKey(namespace.Default, id, "status"))
	if got > 2*time.Minute {
		t.Errorf("suspended structural key TTL = %v, want <= 2m (the workflow's "+
			"transient TTL): the suspend path resolved the adapter default, so a "+
			"declared-ephemeral execution's keys now outlive it by hours", got)
	}

	// The waiter key is created by the suspend path itself and has no other TTL
	// source, so it isolates suspendTTL's answer.
	if got := mr.TTL(waiterKey(namespace.Default, id, "approval")); got > 2*time.Minute {
		t.Errorf("waiter key TTL = %v, want <= 2m", got)
	}
}
