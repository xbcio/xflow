package rstate

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestCommitAgainstVanishedExecution drives the shape the shared statestore
// contract cannot express: a lease that is still live and still matches, against
// an execution whose exec:status key is gone.
//
// The contract suite can only say "no execution record" by never creating one,
// and that scenario is already caught by the lease fence — no lease exists, so
// both backends answer StaleToken and agree. It is the fence PASSING that is
// dangerous. Lua reads a missing key as false, which equals none of the terminal
// status strings, so both commit scripts used to fall straight through the
// execution check and mutate state for an execution Redis no longer has:
// boundary outputs written, the remaining/failed counters of a nonexistent
// execution decremented, and the commit reported to the runner as accepted.
//
// Deleting the key directly is not a synthetic setup. Both scripts re-EXPIRE the
// per-node and per-group keys on every commit while exec:status is refreshed
// only on the execution's own transitions, so a long-running execution outliving
// its own status key is the ordinary consequence of the TTLs, not a fault
// injection. In transient mode it is exactly the documented
// `transientTTL > max execution wall-clock` invariant, which nothing enforces.
func TestCommitAgainstVanishedExecution(t *testing.T) {
	ctx := context.Background()

	t.Run("node", func(t *testing.T) {
		state, _, rdb := newTestRedisState(t)
		g := testGraphOneNode()
		id := types.ExecutionID("vanished-node")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		idx, _ := g.NodeIndex("start")
		lease := &engine.TaskLease{
			LeaseID:    "lease-vanished",
			LeaseToken: "token-vanished",
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: idx,
				Type: engine.TaskTypeNodeExec, ActivationID: 1},
		}
		if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("AcquireTaskLease() acquired=%v err=%v", acquired, err)
		}

		ns := namespace.FromContext(ctx)
		if err := rdb.Del(ctx, execKey(ns, id, "status")).Err(); err != nil {
			t.Fatalf("delete exec status key: %v", err)
		}

		res, err := state.CommitLeasedNode(ctx, engine.CommitNodeRequest{
			ExecutionID:  id,
			NodeName:     "start",
			NodeIdx:      idx,
			ActivationID: 1,
			LeaseID:      lease.LeaseID,
			LeaseToken:   lease.LeaseToken,
			Attempt:      1,
			Status:       types.NodeStatusSuccess,
			StoreOutput:  true,
			Port:         "main",
			Output:       map[string]any{"k": "v"},
		})
		if err != nil {
			t.Fatalf("CommitLeasedNode() error = %v", err)
		}
		if res.Applied || res.Outcome != engine.CommitOutcomeExecutionInactive {
			t.Fatalf("CommitLeasedNode() = %+v, want an unapplied %q. The lease is still valid, "+
				"so the fence lets this through; only the execution check can stop it.",
				res, engine.CommitOutcomeExecutionInactive)
		}
		// The outcome alone would still pass if the script mutated state and then
		// reported inactive. This is the damage the outcome is standing in for.
		if n, err := rdb.Exists(ctx, outputKey(ns, id, "start")).Result(); err != nil || n != 0 {
			t.Errorf("output for %q/start exists after a refused commit (exists=%d err=%v)", id, n, err)
		}
		if got, err := rdb.Get(ctx, nodeStatusKey(ns, id, "start")).Result(); err == nil && got == string(types.NodeStatusSuccess) {
			t.Errorf("node %q/start was terminalized to %q for an execution that no longer exists", id, got)
		}
	})

	t.Run("group", func(t *testing.T) {
		state, _, rdb := newTestRedisState(t)
		g, err := graph.Compile(&types.WorkflowDef{
			Name: "vanished-group",
			Nodes: []types.NodeDef{
				{Name: "ingest", Kind: types.NodeKindTrigger},
				{Name: "analyze", Kind: types.NodeKindAction},
				{Name: "store", Kind: types.NodeKindAction},
			},
			Connections: types.Connections{
				"ingest":  {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}},
				"analyze": {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
			},
			Groups: []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
		})
		if err != nil {
			t.Fatalf("Compile() error = %v", err)
		}
		id := types.ExecutionID("vanished-group")
		if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("CreateExecution() error = %v", err)
		}
		gu := g.Groups()[0].UnitIdx
		ok, err := state.AcquireGroupLease(ctx, &engine.GroupLease{
			LeaseID: "L-vanished", LeaseToken: "T-vanished", Attempt: 1,
			ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
			IssuedAt: time.Now(), TTL: time.Minute,
		})
		if err != nil || !ok {
			t.Fatalf("AcquireGroupLease() ok=%v err=%v", ok, err)
		}

		ns := namespace.FromContext(ctx)
		if err := rdb.Del(ctx, execKey(ns, id, "status")).Err(); err != nil {
			t.Fatalf("delete exec status key: %v", err)
		}

		res, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
			ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
			LeaseID: "L-vanished", LeaseToken: "T-vanished", Attempt: 1,
			Outcome: engine.GroupOutcomeSuccess,
			Exits: []engine.GroupExitResult{{NodeName: "analyze", Port: "main",
				Data: map[string]any{"k": "v"}}},
		})
		if err != nil {
			t.Fatalf("CommitGroup() error = %v", err)
		}
		if res.Applied || res.Outcome != engine.CommitOutcomeExecutionInactive {
			t.Fatalf("CommitGroup() = %+v, want an unapplied %q. The group unit is still running "+
				"and the token still matches, so nothing downstream of the fence stops this.",
				res, engine.CommitOutcomeExecutionInactive)
		}
		if n, err := rdb.Exists(ctx, outputKey(ns, id, "analyze")).Result(); err != nil || n != 0 {
			t.Errorf("boundary output for %q/analyze exists after a refused group commit (exists=%d err=%v)", id, n, err)
		}
		if got, err := rdb.Get(ctx, groupUnitStatusKey(ns, id, gu)).Result(); err == nil && got == "done" {
			t.Errorf("group unit %d was terminalized for an execution that no longer exists", gu)
		}
	})
}
