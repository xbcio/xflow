package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestMemoryActivityClockRefreshOnLeaseRenewAndCommit pins the local backend's
// write points for the execution activity clock: acquiring a lease, renewing it,
// and an accepted node commit each stamp it. It is the in-memory half of the
// shared orphan contract's recent-activity rule; the Redis half is
// TestEngineActivityClockRefreshOnLeaseRenewAndCommit in rstate, which reads the
// activity_at_ms key the Lua scripts write.
func TestMemoryActivityClockRefreshOnLeaseRenewAndCommit(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("memory-activity-clock")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: nodeStateGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	stale := func() {
		t.Helper()
		state.mu.Lock()
		defer state.mu.Unlock()
		state.activityAt[id] = time.UnixMilli(1)
	}
	activityAt := func() time.Time {
		t.Helper()
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.activityAt[id]
	}
	freshAfter := func(step string) {
		t.Helper()
		if got := activityAt(); !got.After(time.Now().Add(-time.Minute)) {
			t.Fatalf("%s did not refresh the activity clock: %v is not recent", step, got)
		}
	}

	lease := privateOutputLease(id, "stateful", "activity-lease", "activity-token", engine.TaskTypeNodeExec)
	stale()
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	freshAfter("lease acquisition")

	stale()
	if renewed, err := state.RenewTaskLease(ctx, id, "stateful", lease.LeaseToken, time.Now().Add(time.Minute)); err != nil || !renewed {
		t.Fatalf("RenewTaskLease() renewed=%v err=%v, want true/nil", renewed, err)
	}
	freshAfter("lease renewal")

	stale()
	committed, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "stateful",
		NodeIdx:     0,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     1,
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"visible": "downstream"},
		StoreOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !committed.Applied {
		t.Fatalf("CommitNode() = %+v, want an applied commit", committed)
	}
	freshAfter("accepted commit")
}
