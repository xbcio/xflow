package rstate

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestSlotAdmissionIsReentrantPerExecution pins what an idempotent redo relies
// on: a keyed request that crashed after its slot was charged but before its
// execution was written is redone under the same execution ID, and that redo
// must be admitted on the slot it already holds — not refused at the limit it
// is itself occupying, and not charged a second slot.
func TestSlotAdmissionIsReentrantPerExecution(t *testing.T) {
	ctx := context.Background()
	_, rdb, state := newMiniredisStore(t)
	g := sweepGraph(t, "conc-reentrant")
	admission := sweepAdmission(g, 1)
	id := engine.IdempotentExecutionID(namespace.Default, "workflow:conc-reentrant", "k")

	// The crashed attempt: charged, nothing written.
	first, err := state.admitWorkflowSlot(ctx, namespace.Default, id, admission)
	if err != nil || first.code != concurrencyAdmitAdmitted || !first.added {
		t.Fatalf("first admission = (%+v, %v), want admitted and added", first, err)
	}

	snap := &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}
	if _, err := state.CreateExecutionWithConcurrency(ctx, snap, nil, admission); err != nil {
		t.Fatalf("redo on the slot it already holds error = %v, want admission", err)
	}
	slotKey := concurrencySlotKey(namespace.Default, g.Name())
	if n, err := rdb.ZCard(ctx, slotKey).Result(); err != nil || n != 1 {
		t.Fatalf("slot ZSET size = %d (err %v), want 1: the redo must not charge twice", n, err)
	}
	if got, err := state.GetExecution(ctx, id); err != nil || got == nil {
		t.Fatalf("redo did not persist the execution (%+v, %v)", got, err)
	}
}
