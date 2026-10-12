package local

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestListOutboxExecutionsSkipsParkedExecutions pins what the dispatcher's
// discovery read is allowed to report.
//
// The dispatcher's page is a bound on one drain's work, so an execution that can
// produce nothing competes with the executions that can. A parked submission is
// exactly that: its intents are held until promotion, and flushing it returns an
// empty claim every time. The holder — the execution that ran — must still be
// reported, or the fix would be "report nothing" rather than "report work".
func TestListOutboxExecutionsSkipsParkedExecutions(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:    "discovery-hold",
		Version: "1",
		Nodes:   []types.NodeDef{{Name: "start", Type: "test.echo"}},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	admission := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue,
	}
	holderSnap, holderEntries := sweepSubmission(t, g)
	if _, err := state.CreateExecutionWithConcurrency(ctx, holderSnap, holderEntries, admission); err != nil {
		t.Fatalf("holder submission error = %v", err)
	}
	queuedSnap, queuedEntries := sweepSubmission(t, g)
	res, err := state.CreateExecutionWithConcurrency(ctx, queuedSnap, queuedEntries, admission)
	if err != nil {
		t.Fatalf("queued submission error = %v", err)
	}
	if !res.Queued {
		t.Fatalf("over-limit submission = %+v, want it parked", res)
	}

	ids, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if len(ids) != 1 || ids[0] != holderSnap.ID {
		t.Fatalf("discovered %v, want only the holder %q: a parked execution's flush can only come back empty", ids, holderSnap.ID)
	}

	// The holder is drained (its intent delivered) so the last assertion is
	// about the promoted execution alone rather than about whoever else still
	// has an undeniable intent.
	if err := state.AckOutbox(ctx, holderSnap.ID, holderEntries[0].ID); err != nil {
		t.Fatalf("AckOutbox(holder) error = %v", err)
	}

	// Promotion is what makes the parked execution discoverable again: the hold
	// is a delay, not a removal, and discovery that never came back would strand
	// its work after the slot frees.
	if err := state.UpdateExecutionStatus(ctx, holderSnap.ID, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("release holder error = %v", err)
	}
	ids, err = state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() after promotion error = %v", err)
	}
	if len(ids) != 1 || ids[0] != queuedSnap.ID {
		t.Fatalf("discovered %v after promotion, want the promoted execution %q", ids, queuedSnap.ID)
	}
}
