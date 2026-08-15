package engine

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/types"
)

// recordingRenewer captures which node the engine asks the store to renew. The
// store contract itself is pinned in backend/internal/statestoretest; what this
// fake is for is the one thing only the engine decides — the name.
type recordingRenewer struct {
	*fakeState
	gotName string
	calls   int
}

func (r *recordingRenewer) RenewTaskLease(_ context.Context, _ types.ExecutionID, name string, _ LeaseToken, _ time.Time) (bool, error) {
	r.gotName = name
	r.calls++
	return true, nil
}

// A batch task names a synthetic node ("loop/_batch/0") that BuildSubgraphLease
// deliberately never writes to state, and the batch lease borrows the parent's
// LeaseID and token rather than minting its own. Renewing under the synthetic
// name therefore addresses a node that does not exist: the store answers
// Renewed=false, the runner reads that as "you lost the lease" and cancels a
// healthy batch, and meanwhile the parent — which is the lease actually ticking
// down while the batch runs — never gets extended.
//
// The parent is in Waiting for the whole fan-out, so this only works together
// with the store gate accepting Waiting.
func TestRenewTaskLeaseOnABatchExtendsTheParentNode(t *testing.T) {
	eng, state, queue, execID, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	batches := drainBatchTasks(t, eng, queue)

	lease, _, err := eng.BuildSubgraphLease(ctx, batches[0])
	if err != nil {
		t.Fatalf("BuildSubgraphLease() error = %v", err)
	}
	if lease.Task.NodeName == "loop" {
		t.Fatalf("test premise broken: the batch lease should carry the synthetic name, got %q",
			lease.Task.NodeName)
	}
	parent, err := state.GetNode(ctx, execID, "loop")
	if err != nil || parent == nil {
		t.Fatalf("GetNode(loop) = %v, %v", parent, err)
	}
	if parent.Status != types.NodeStatusWaiting {
		t.Fatalf("test premise broken: parent status = %v, want waiting while batches run",
			parent.Status)
	}

	rec := &recordingRenewer{fakeState: state}
	eng.state = rec
	renewed, err := eng.RenewTaskLease(ctx, lease, 5*time.Minute)
	if err != nil {
		t.Fatalf("RenewTaskLease() error = %v", err)
	}
	if !renewed {
		t.Fatal("RenewTaskLease() = false for a live batch")
	}
	if rec.calls != 1 {
		t.Fatalf("store renew calls = %d, want exactly 1", rec.calls)
	}
	if rec.gotName != "loop" {
		t.Fatalf("renewal targeted node %q, want the parent %q — the synthetic batch node "+
			"is never written to state, so renewing it refuses and the runner cancels a "+
			"healthy batch", rec.gotName, "loop")
	}
}

// A plain node lease must keep renewing itself. Resolving every lease through
// the expansion payload would break the ordinary path, which is the common case.
func TestRenewTaskLeaseOnAPlainNodeUsesItsOwnName(t *testing.T) {
	eng, state, queue, _, _ := batchLeaseWorkflow(t)
	ctx := context.Background()
	roots := queue.Drain()
	if len(roots) != 1 {
		t.Fatalf("root tasks = %d, want 1", len(roots))
	}
	lease, err := eng.BuildTaskLease(ctx, roots[0])
	if err != nil {
		t.Fatalf("BuildTaskLease() error = %v", err)
	}

	rec := &recordingRenewer{fakeState: state}
	eng.state = rec
	if _, err := eng.RenewTaskLease(ctx, lease, 5*time.Minute); err != nil {
		t.Fatalf("RenewTaskLease() error = %v", err)
	}
	if rec.gotName != "loop" {
		t.Fatalf("renewal targeted node %q, want the lease's own node %q", rec.gotName, "loop")
	}
}
