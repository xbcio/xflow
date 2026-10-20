package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// TestNodeTimingProjectionWritesOnce pins the SQL side of the node clock: the
// lease projection writes started_at and the terminal commit writes
// finished_at, and a later cyclic claim/commit must not move either. The row is
// the only timing record once the Redis keys expire, so a projection that
// restamps would rewrite history after the authoritative keys are gone.
func TestNodeTimingProjectionWritesOnce(t *testing.T) {
	state, db := newStateWithAuditStore(t)
	ctx := context.Background()
	// Two units: the first terminal commit must leave the execution running so
	// the cyclic re-entry's second commit is accepted (a terminal execution
	// refuses every later commit, which is a different contract).
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "node-timing-projection",
		Nodes: []types.NodeDef{
			{Name: "only", Type: "test.echo"},
			{Name: "next", Type: "test.echo"},
		},
		Connections: types.Connections{
			"only": {"main": {Targets: []types.Connection{{Node: "next", Input: "main"}}}},
		},
	})
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}
	id := types.ExecutionID("node-timing-projection-1")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	idx, _ := g.NodeIndex("only")
	unitIdx := g.UnitIndexForNode(idx)
	firstIssued := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)

	acquire := func(leaseID, token string, issued time.Time, activation int) {
		t.Helper()
		lease := &engine.TaskLease{
			LeaseID:    engine.LeaseID(leaseID),
			LeaseToken: engine.LeaseToken(token),
			Attempt:    1,
			IssuedAt:   issued,
			TTL:        time.Second,
			Task: engine.Task{ExecutionID: id, NodeName: "only", NodeIdx: idx,
				UnitIdx: unitIdx, Type: engine.TaskTypeNodeExec, ActivationID: activation},
		}
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease(%s, activation %d): acquired=%v err=%v",
				leaseID, activation, acquired, err)
		}
	}
	commit := func(leaseID, token string, activation int) {
		t.Helper()
		res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "only", NodeIdx: idx, UnitIdx: unitIdx,
			ActivationID: activation,
			LeaseID:      engine.LeaseID(leaseID), LeaseToken: engine.LeaseToken(token), Attempt: 1,
			Status: types.NodeStatusSuccess,
		})
		if err != nil || !res.Applied {
			t.Fatalf("CommitNode(%s, activation %d) = (%+v, %v), want applied",
				leaseID, activation, res, err)
		}
	}
	read := func() *store.NodeRecord {
		t.Helper()
		rec, err := db.GetNode(ctx, id, "only")
		if err != nil || rec == nil {
			t.Fatalf("GetNode() = (%v, %v), want a record", rec, err)
		}
		return rec
	}

	acquire("lease-1", "token-1", firstIssued, 1)
	if got := read(); got.StartedAt == nil || got.StartedAt.UnixMilli() != firstIssued.UnixMilli() {
		t.Fatalf("projected StartedAt = %v after the first lease, want %v", got.StartedAt, firstIssued)
	} else if got.FinishedAt != nil {
		t.Fatalf("projected FinishedAt = %v on a running node, want nil", got.FinishedAt)
	}

	time.Sleep(5 * time.Millisecond)
	commit("lease-1", "token-1", 1)
	first := read()
	if first.FinishedAt == nil {
		t.Fatalf("projected FinishedAt is nil after the terminal commit: the commit is " +
			"the only writer of the end line")
	}
	finished := *first.FinishedAt

	// A cyclic re-entry at a higher activation runs the node again; neither
	// line may move, in Redis or in the projection.
	acquire("lease-2", "token-2", time.Now().UTC().Add(time.Minute), 2)
	reentered := read()
	if reentered.StartedAt == nil || reentered.StartedAt.UnixMilli() != firstIssued.UnixMilli() {
		t.Fatalf("projected StartedAt = %v after reactivation, want %v",
			reentered.StartedAt, firstIssued)
	}
	if reentered.FinishedAt == nil || !reentered.FinishedAt.Equal(finished) {
		t.Fatalf("projected FinishedAt = %v after reactivation, want the first %v",
			reentered.FinishedAt, finished)
	}
	commit("lease-2", "token-2", 2)
	final := read()
	if final.FinishedAt == nil || !final.FinishedAt.Equal(finished) {
		t.Fatalf("projected FinishedAt = %v after the second terminal commit, want the first %v",
			final.FinishedAt, finished)
	}
}
