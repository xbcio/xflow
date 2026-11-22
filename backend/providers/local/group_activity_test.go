package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// TestMemoryGroupActivityClockRefreshOnLeaseRenewAndCommit is the in-memory
// half of TestGroupActivityClockRefreshOnLeaseRenewAndCommit in rstate: group
// lease acquisition, renewal, and an accepted group commit each refresh
// activityAt, the clock the orphan sweep anchors on.
func TestMemoryGroupActivityClockRefreshOnLeaseRenewAndCommit(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("memory-group-activity-clock")
	g, err := graph.Compile(&types.WorkflowDef{
		Name:  "memory-group-activity",
		Nodes: []types.NodeDef{{Name: "start", Type: "test.echo"}, {Name: "end", Type: "test.echo"}},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "end", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile(group activity graph) error = %v", err)
	}
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
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

	lease := &engine.GroupLease{
		LeaseID: "activity-lease", LeaseToken: "activity-token", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: 0, GroupName: "group", EntryNodeIdx: 0,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	stale()
	acquired, err := state.AcquireGroupLease(ctx, lease)
	if err != nil || !acquired {
		t.Fatalf("AcquireGroupLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	freshAfter("group lease acquisition")

	stale()
	renewed, err := state.RenewGroupLease(ctx, id, 0, lease.LeaseToken, time.Now().UTC().Add(2*time.Minute))
	if err != nil || !renewed {
		t.Fatalf("RenewGroupLease() renewed=%v err=%v, want true/nil", renewed, err)
	}
	freshAfter("group lease renewal")

	stale()
	committed, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID:  id,
		GroupUnitIdx: 0,
		GroupID:      "group",
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Outcome:      engine.GroupOutcomeSuccess,
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	if !committed.Applied || committed.ExecutionDone {
		t.Fatalf("CommitGroup() = %+v, want an applied commit that leaves the execution running", committed)
	}
	freshAfter("accepted group commit")
}
