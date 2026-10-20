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

// TestExtendExecTTLKeepsWorkflowIdentity pins the suspend path's renewal of the
// workflow identity. It is written with the execution's creation retention, so
// an execution parked past that retention must have it renewed alongside the
// status — otherwise Inspect answers with an empty identity for a live
// suspended execution.
func TestExtendExecTTLKeepsWorkflowIdentity(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	state := New(rdb, nil, time.Second)
	ctx := context.Background()
	id := types.ExecutionID("exec-suspend-identity")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Status: types.ExecutionStatusRunning, Graph: testDurableGraph(),
		WorkflowID: "wf-suspend", WorkflowVersion: "v9",
	}); err != nil {
		t.Fatalf("CreateExecution: %v", err)
	}
	if err := state.extendExecTTL(ctx, id, "start", &types.SuspendSpec{Signals: []string{"approval"}}, time.Minute); err != nil {
		t.Fatalf("extendExecTTL: %v", err)
	}
	mr.FastForward(30 * time.Second)

	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution = (%+v, %v), want the suspended execution", snap, err)
	}
	if snap.WorkflowID != "wf-suspend" || snap.WorkflowVersion != "v9" {
		t.Fatalf("identity after the renewed retention = (%q, %q), want (%q, %q): "+
			"extendExecTTL did not renew the identity keys", snap.WorkflowID,
			snap.WorkflowVersion, "wf-suspend", "v9")
	}
	// The execution's clock is written once with the execution too, so a wait
	// longer than the creation retention must renew it here or a resumed
	// execution reports no start line and no duration.
	if snap.CreatedAt.IsZero() || snap.UpdatedAt.IsZero() {
		t.Fatalf("clock after the renewed retention = (%v, %v), want both non-zero: "+
			"extendExecTTL did not renew created_at_ms/updated_at_ms",
			snap.CreatedAt, snap.UpdatedAt)
	}
}

// TestWorkflowIdentityInTransientKeys pins that completion-time TTL shortening
// covers the identity keys: they are part of the execution's own key set.
func TestWorkflowIdentityInTransientKeys(t *testing.T) {
	id := types.ExecutionID("transient-identity-1")
	keys := transientExecutionKeys(namespace.Default, id, nil)
	want := []string{
		execKey(namespace.Default, id, "workflow_id"),
		execKey(namespace.Default, id, "workflow_version"),
	}
	for _, w := range want {
		found := false
		for _, k := range keys {
			if k == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("transientExecutionKeys does not include %q", w)
		}
	}
}

// TestExecutionTimingKeysInTransientKeys pins that completion-time TTL
// shortening covers the execution's clock keys. They are written once with the
// execution, so without this they keep their active transient TTL after every
// other key has been shortened — the exact asymmetry the identity keys above
// were added to remove.
func TestExecutionTimingKeysInTransientKeys(t *testing.T) {
	id := types.ExecutionID("transient-timing-1")
	keys := transientExecutionKeys(namespace.Default, id, nil)
	want := []string{
		execKey(namespace.Default, id, "created_at_ms"),
		execKey(namespace.Default, id, "updated_at_ms"),
	}
	for _, w := range want {
		found := false
		for _, k := range keys {
			if k == w {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("transientExecutionKeys does not include %q", w)
		}
	}
}
