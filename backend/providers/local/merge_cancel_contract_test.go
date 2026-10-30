package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestMemoryMergeCancelContract runs the shared merge-cancel contract against
// the in-memory backend: the fourth fenced System commit shape (canceled) and
// the MergeCancel intent written beside the merge node's terminal commit.
//
// The suite drives the store directly. The engine has no consumer for
// TaskTypeMergeCancel intents in this package's tests, so nothing here may let
// an engine flush the outbox.
func TestMemoryMergeCancelContract(t *testing.T) {
	statestoretest.RunMergeCancelContract(t, memoryCapabilityManifest(), New().State())
}

// TestMemoryCommitNodeAdvanceUnderCancelingProbe records what the existing
// advance block of commitNodeLocked does for an execution that is canceling.
//
// The folded-arrivals block is guarded against canceling and the new
// MergeCancel block is too; the older AdvanceTask block is not. This is a
// characterization of that block, not a requirement on it: it exists so a later
// change to the guard shows up as a deliberate edit to this assertion instead
// of a silent behavior change. The Redis script guards the advance slot, so the
// two backends currently differ here.
func TestMemoryCommitNodeAdvanceUnderCancelingProbe(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "advance-canceling-probe",
		Nodes: []types.NodeDef{
			{Name: "alpha", Type: "test.start"},
			{Name: "beta", Type: "test.finish"},
		},
		Connections: types.Connections{
			"alpha": {"main": types.PortConnections{Targets: []types.Connection{{Node: "beta", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	const id = types.ExecutionID("advance-canceling-probe")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	alphaIdx, _ := g.NodeIndex("alpha")
	lease := &engine.TaskLease{
		LeaseID: "probe-lease", LeaseToken: "probe-token", IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: "alpha", NodeIdx: alphaIdx, UnitIdx: g.UnitIndexForNode(alphaIdx),
			Type: engine.TaskTypeNodeExec, ActivationID: 1},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
	}
	if err := state.UpdateExecutionStatus(ctx, id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id, NodeName: "alpha", NodeIdx: alphaIdx, UnitIdx: g.UnitIndexForNode(alphaIdx),
		ActivationID: 1, LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: 1,
		Status: types.NodeStatusSuccess,
		AdvanceTask: &engine.Task{ExecutionID: id, NodeName: "alpha", NodeIdx: alphaIdx,
			UnitIdx: g.UnitIndexForNode(alphaIdx), Type: engine.TaskTypeNodeAdvance, ActivationID: 1},
	})
	if err != nil || res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitNode() = %+v, %v; want an accepted commit", res, err)
	}
	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	wrote := false
	for _, entry := range entries {
		if entry.Task.Type == engine.TaskTypeNodeAdvance {
			wrote = true
		}
	}
	// Observed: the local advance block writes the intent even though the
	// execution is canceling. Pinned here so changing the guard is a visible edit.
	if !wrote {
		t.Fatalf("no advance intent under canceling (outbox: %+v); the local advance block "+
			"changed. Update this probe and the merge-cancel notes together", entries)
	}
}
