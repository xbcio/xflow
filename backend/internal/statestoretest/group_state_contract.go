package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// GroupStore is a backend that implements both full state and group capabilities.
type GroupStore interface {
	engine.StateStore
	engine.GroupStateStore
	engine.GroupLeaseReader
	engine.GroupLeaseExpirer
}

// singleGroupGraph: ingest(trigger)->analyze, both in same group, no external nodes => UnitCount=1.
func singleGroupGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "single",
		Nodes: []types.NodeDef{
			{Name: "ingest", Kind: types.NodeKindTrigger},
			{Name: "analyze", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"ingest": {"main": {Targets: []types.Connection{{Node: "analyze", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "edge", Members: []string{"ingest", "analyze"}}},
	})
	if err != nil {
		t.Fatalf("compile single-group graph: %v", err)
	}
	return g
}

// twoUnitGraph: group {ingest,analyze} + external store => UnitCount=2.
func twoUnitGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "two",
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
		t.Fatalf("compile two-unit graph: %v", err)
	}
	return g
}

// RunGroupStateContract exercises the GroupStateStore contract (acquire, renew,
// commit, fence, downstream) against a concrete backend. Every backend
// implementing GroupStateStore should call this.
func RunGroupStateContract(t *testing.T, newStore func(*testing.T) GroupStore) {
	ctx := context.Background()
	seed := func(t *testing.T, g *graph.Graph) (GroupStore, types.ExecutionID, int) {
		s := newStore(t)
		id := types.ExecutionID("e-" + t.Name())
		if err := s.CreateExecution(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning}); err != nil {
			t.Fatalf("create execution: %v", err)
		}
		return s, id, g.Groups()[0].UnitIdx
	}
	lease := func(id types.ExecutionID, gu int, token engine.LeaseToken) *engine.GroupLease {
		return &engine.GroupLease{LeaseID: engine.LeaseID("L-" + string(token)), LeaseToken: token,
			Attempt: 1, ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
			IssuedAt: time.Now(), TTL: time.Minute}
	}
	commit := func(id types.ExecutionID, gu int, token engine.LeaseToken) engine.GroupCommitRequest {
		return engine.GroupCommitRequest{ExecutionID: id, GroupUnitIdx: gu, GroupID: "edge",
			LeaseID: engine.LeaseID("L-" + string(token)), LeaseToken: token, Attempt: 1,
			Outcome: engine.GroupOutcomeSuccess,
			Exits:   []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"k": "v"}}}}
	}

	t.Run("AcquireCommitHappyPath", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		if ok, err := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); err != nil || !ok {
			t.Fatalf("acquire: ok=%v err=%v", ok, err)
		}
		res, err := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if err != nil || !res.Applied {
			t.Fatalf("commit: %+v err=%v", res, err)
		}
		if res.Outcome != engine.CommitOutcomeAccepted {
			t.Fatalf("applied commit must report accepted outcome, got %q", res.Outcome)
		}
	})

	t.Run("DoubleAcquireRejected", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); !ok {
			t.Fatal("first acquire must succeed")
		}
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T2")); ok {
			t.Fatal("second acquire must be rejected while running")
		}
	})

	t.Run("RenewFencedByToken", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		if ok, _ := s.RenewGroupLease(ctx, id, gu, "WRONG", time.Now().Add(time.Minute)); ok {
			t.Fatal("renew with wrong token must fail")
		}
		if ok, _ := s.RenewGroupLease(ctx, id, gu, "T1", time.Now().Add(time.Minute)); !ok {
			t.Fatal("renew with owner token must succeed")
		}
	})

	t.Run("CommitStaleTokenRejected", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		res, _ := s.CommitGroup(ctx, commit(id, gu, "STALE"))
		if res.Applied {
			t.Fatal("commit with stale token must not apply")
		}
		if res.Outcome != engine.CommitOutcomeStaleToken {
			t.Fatalf("stale commit must report stale-token outcome, got %q", res.Outcome)
		}
	})

	// F2 regression: a commit attempt fenced out by a stale/wrong token must
	// not write boundary output at all — before the fix, Store.CommitGroup
	// wrote output via an unfenced SET before the fenced Lua transition ran,
	// so a stale attempt's output could still land even though the commit
	// itself was correctly rejected.
	t.Run("CommitStaleTokenDoesNotWriteOutput", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		staleReq := commit(id, gu, "STALE")
		staleReq.Exits = []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"k": "poisoned"}}}
		res, _ := s.CommitGroup(ctx, staleReq)
		if res.Applied {
			t.Fatal("commit with stale token must not apply")
		}
		out, err := s.GetOutput(ctx, id, "analyze")
		if err != nil {
			t.Fatalf("GetOutput: %v", err)
		}
		if out != nil {
			t.Fatalf("stale commit must not write output, got %+v", out)
		}
		// The legitimate T1 commit must still succeed afterward and write its
		// own (non-poisoned) output.
		res2, err := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if err != nil || !res2.Applied {
			t.Fatalf("legitimate commit after stale attempt: %+v err=%v", res2, err)
		}
		out2, err := s.GetOutput(ctx, id, "analyze")
		if err != nil {
			t.Fatalf("GetOutput after legitimate commit: %v", err)
		}
		if out2["k"] != "v" {
			t.Fatalf("output after legitimate commit = %+v, want k=v (not poisoned by the earlier stale attempt)", out2)
		}
	})

	t.Run("DuplicateCommitIdempotent", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		r1, _ := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if !r1.Applied {
			t.Fatal("first commit must apply")
		}
		r2, _ := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if r2.Applied {
			t.Fatal("duplicate commit must be idempotent (Applied=false)")
		}
		if r2.Outcome != engine.CommitOutcomeDuplicateTerminal {
			t.Fatalf("duplicate commit must report duplicate-terminal outcome, got %q", r2.Outcome)
		}
	})

	// F2 regression: a duplicate (already-terminal) commit attempt with
	// different exit data must not overwrite the output written by the
	// original, accepted commit.
	t.Run("DuplicateCommitDoesNotOverwriteOutput", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		r1, err := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if err != nil || !r1.Applied {
			t.Fatalf("first commit: %+v err=%v", r1, err)
		}
		dup := commit(id, gu, "T1")
		dup.Exits = []engine.GroupExitResult{{NodeName: "analyze", Port: "main", Data: map[string]any{"k": "poisoned"}}}
		r2, _ := s.CommitGroup(ctx, dup)
		if r2.Applied {
			t.Fatal("duplicate commit must be idempotent (Applied=false)")
		}
		out, err := s.GetOutput(ctx, id, "analyze")
		if err != nil {
			t.Fatalf("GetOutput: %v", err)
		}
		if out["k"] != "v" {
			t.Fatalf("output after duplicate commit = %+v, want unchanged k=v", out)
		}
	})

	// P0-2 regression: single group (UnitCount=1), one group commit must complete execution.
	// If remaining was seeded by NodeCount, execution would never complete.
	t.Run("SingleGroupCommitCompletesExecution", func(t *testing.T) {
		s, id, gu := seed(t, singleGroupGraph(t))
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); !ok {
			t.Fatal("acquire must succeed")
		}
		res, err := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("single-group execution must complete on group commit (P0-2): done=%v status=%v",
				res.ExecutionDone, res.ExecutionStatus)
		}
	})

	// A fatal group commit finalizes the execution as failed. The reason travels
	// on the commit request, not through UpdateExecutionStatus, so it is the
	// commit path's own readback that has to work: the group unit is terminalized
	// as a whole and no member node ever writes a node-level error, leaving the
	// execution-level reason as the only carrier.
	t.Run("FatalGroupCommitStoresExecutionError", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); !ok {
			t.Fatal("acquire must succeed")
		}
		const reason = "group member analyze failed fatally"
		req := commit(id, gu, "T1")
		req.Outcome = engine.GroupOutcomeFailed
		req.Fatal = true
		req.Error = reason
		res, err := s.CommitGroup(ctx, req)
		if err != nil {
			t.Fatalf("commit: %v", err)
		}
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusFailed {
			t.Fatalf("fatal commit must fail the execution: done=%v status=%v", res.ExecutionDone, res.ExecutionStatus)
		}
		snap, err := s.GetExecution(ctx, id)
		if err != nil || snap == nil {
			t.Fatalf("GetExecution: snap=%v err=%v", snap, err)
		}
		if snap.Error != reason {
			t.Fatalf("ExecutionSnapshot.Error = %q, want %q — the commit path's failure reason is not readable online", snap.Error, reason)
		}
	})

	// Negative half: a successful commit must leave no reason behind, or a
	// backend that echoes the request's Error field unconditionally would pass
	// the assertion above.
	t.Run("SuccessfulGroupCommitLeavesExecutionErrorEmpty", func(t *testing.T) {
		s, id, gu := seed(t, singleGroupGraph(t))
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); !ok {
			t.Fatal("acquire must succeed")
		}
		req := commit(id, gu, "T1")
		req.Error = "must not be recorded on a success"
		if _, err := s.CommitGroup(ctx, req); err != nil {
			t.Fatalf("commit: %v", err)
		}
		snap, err := s.GetExecution(ctx, id)
		if err != nil || snap == nil {
			t.Fatalf("GetExecution: snap=%v err=%v", snap, err)
		}
		if snap.Error != "" {
			t.Fatalf("ExecutionSnapshot.Error = %q on a successful commit, want empty", snap.Error)
		}
	})

	// Fan-in minimal coverage: group commit lights up downstream unit's wait_any first arrival.
	t.Run("CommitSchedulesWaitAnyDownstream", func(t *testing.T) {
		g := twoUnitGraph(t)
		s, id, gu := seed(t, g)
		if ok, _ := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); !ok {
			t.Fatal("acquire must succeed")
		}
		// store is the 3rd node in twoUnitGraph (NodeIdx=2), not a group member => its own unit.
		const storeNodeIdx = 2
		req := commit(id, gu, "T1")
		req.Downstream = []engine.DownstreamArrival{{
			NodeName:     "store",
			NodeIdx:      storeNodeIdx,
			UnitIdx:      g.UnitIndexForNode(storeNodeIdx),
			ArrivalCount: 1,
			ActiveCount:  1,
			MergeMode:    "wait_any",
		}}
		res, err := s.CommitGroup(ctx, req)
		if err != nil || !res.Applied {
			t.Fatalf("commit: %+v err=%v", res, err)
		}
		if len(res.OutboxIDs) == 0 {
			t.Fatal("wait_any first arrival must schedule the downstream unit (execute outbox intent)")
		}
	})

	t.Run("GetGroupLeaseReturnsActiveCheckpoint", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		l := lease(id, gu, "T1")
		if ok, _ := s.AcquireGroupLease(ctx, l); !ok {
			t.Fatal("acquire must succeed")
		}
		got, err := s.GetGroupLease(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupLease: %v", err)
		}
		if got == nil {
			t.Fatal("GetGroupLease returned nil for active lease")
		}
		if got.LeaseToken != "T1" {
			t.Errorf("token = %q, want T1", got.LeaseToken)
		}
		if got.Attempt < 1 {
			t.Errorf("attempt = %d, want >= 1", got.Attempt)
		}
	})

	t.Run("GetGroupLeaseReturnsNilWhenInactive", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		got, err := s.GetGroupLease(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupLease: %v", err)
		}
		if got != nil {
			t.Fatalf("GetGroupLease must be nil for unacquired unit, got %+v", got)
		}
	})

	t.Run("ExpireGroupLeaseTransitionsToPending", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		l := lease(id, gu, "T1")
		s.AcquireGroupLease(ctx, l)

		expired, err := s.ExpireGroupLease(ctx, id, gu, "T1")
		if err != nil {
			t.Fatalf("ExpireGroupLease: %v", err)
		}
		if !expired {
			t.Fatal("ExpireGroupLease returned false for active lease with matching token")
		}

		// After expiry, GetGroupLease should return nil (no longer running).
		got, _ := s.GetGroupLease(ctx, id, gu)
		if got != nil {
			t.Fatal("GetGroupLease should be nil after expiry")
		}
	})

	t.Run("ExpireGroupLeaseFencedByToken", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))

		expired, err := s.ExpireGroupLease(ctx, id, gu, "WRONG")
		if err != nil {
			t.Fatalf("ExpireGroupLease: %v", err)
		}
		if expired {
			t.Fatal("ExpireGroupLease with wrong token must return false")
		}
	})

	t.Run("ReacquireAfterExpiryIncrementsAttempt", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		l1 := lease(id, gu, "T1")
		s.AcquireGroupLease(ctx, l1)
		firstAttempt := l1.Attempt

		s.ExpireGroupLease(ctx, id, gu, "T1")

		l2 := lease(id, gu, "T2")
		ok, err := s.AcquireGroupLease(ctx, l2)
		if err != nil || !ok {
			t.Fatalf("re-acquire after expiry: ok=%v err=%v", ok, err)
		}
		if l2.Attempt <= firstAttempt {
			t.Errorf("attempt after re-acquire = %d, want > %d", l2.Attempt, firstAttempt)
		}
	})

	// A group lease whose deadline has passed must be discoverable by the lease
	// sweeper, exactly like a node lease. Without this, a runner that dies while
	// holding a group lease strands its unit in "running" forever: nothing
	// expires the lease, AcquireGroupLease refuses to re-acquire a running unit,
	// and the execution never completes.
	//
	// The Redis store additionally used to PRUNE the entry: group and node
	// leases share one expiry ZSET, but group members are encoded as
	// "<execID>|group:<idx>" while the scan split them as "<execID>|<nodeName>"
	// and looked up a node key that never exists. Reading redis.Nil there means
	// "terminal node, stale index entry" — so the scan deleted the only record
	// that the group lease had ever been leased.
	t.Run("ExpiredGroupLeaseIsVisibleToSweeper", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		l := lease(id, gu, "T1")
		l.IssuedAt = time.Now().Add(-2 * time.Minute)
		l.TTL = time.Minute
		if ok, err := s.AcquireGroupLease(ctx, l); err != nil || !ok {
			t.Fatalf("acquire: ok=%v err=%v", ok, err)
		}

		expired, err := s.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("ListExpiredLeases: %v", err)
		}
		var found *engine.ExpiredLease
		for i := range expired {
			if expired[i].ExecutionID == id && expired[i].UnitIdx == gu {
				found = &expired[i]
				break
			}
		}
		if found == nil {
			t.Fatalf("expired group lease %q/#%d not returned by ListExpiredLeases (got %+v) — "+
				"a runner that dies holding this lease strands the unit forever", id, gu, expired)
		}
		if found.LeaseToken != "T1" {
			t.Errorf("token = %q, want T1", found.LeaseToken)
		}
		if found.TaskType != engine.TaskTypeGroupExec {
			t.Errorf("task type = %v, want TaskTypeGroupExec — reclaim needs it to rebuild the group task",
				found.TaskType)
		}

		// The lease must still be leased after the scan: a scan that silently
		// dropped it would make this the last sweep that could ever see it.
		still, err := s.GetGroupLease(ctx, id, gu)
		if err != nil {
			t.Fatalf("GetGroupLease after scan: %v", err)
		}
		if still == nil {
			t.Fatal("group lease disappeared during the expiry scan")
		}

		// A second scan must still see it — proves the first scan did not
		// consume the only index entry.
		again, err := s.ListExpiredLeases(ctx, time.Now())
		if err != nil {
			t.Fatalf("second ListExpiredLeases: %v", err)
		}
		for i := range again {
			if again[i].ExecutionID == id && again[i].UnitIdx == gu {
				return
			}
		}
		t.Fatalf("expired group lease vanished after one scan (got %+v) — the sweeper pruned its own work item", again)
	})

	// A group unit whose lease is revoked must be redelivered in the SAME
	// transition. Revoking alone would leave it pending, unleased and unqueued —
	// a state ListExpiredLeases cannot see (it only reports leased units), so a
	// crash in the gap would strand the unit permanently.
	t.Run("RevokeGroupLeaseWithOutboxIsAtomic", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		reclaimer, ok := s.(engine.GroupLeaseReclaimer)
		if !ok {
			t.Skip("backend does not implement GroupLeaseReclaimer")
		}
		atomic, ok := s.(engine.AtomicStateStore)
		if !ok {
			t.Skip("backend does not implement AtomicStateStore")
		}
		if ok, err := s.AcquireGroupLease(ctx, lease(id, gu, "T1")); err != nil || !ok {
			t.Fatalf("acquire: ok=%v err=%v", ok, err)
		}

		entry := engine.OutboxEntry{
			ID: "requeue-group/" + string(id),
			Task: engine.Task{ExecutionID: id, NodeName: "edge", UnitIdx: gu,
				Type: engine.TaskTypeGroupExec},
		}
		revoked, err := reclaimer.RevokeGroupLeaseWithOutbox(ctx, id, gu, "T1", entry)
		if err != nil {
			t.Fatalf("RevokeGroupLeaseWithOutbox: %v", err)
		}
		if !revoked {
			t.Fatal("revoke returned false for the live token")
		}

		entries, err := atomic.ListOutbox(ctx, id, time.Now().Add(time.Minute), 16)
		if err != nil {
			t.Fatalf("ListOutbox: %v", err)
		}
		var got *engine.OutboxEntry
		for i := range entries {
			if entries[i].ID == entry.ID {
				got = &entries[i]
				break
			}
		}
		if got == nil {
			t.Fatalf("redelivery intent %q missing from outbox (got %+v) — the unit is pending "+
				"with nothing queued and no lease, so no sweep can ever find it again", entry.ID, entries)
		}
		if got.Task.Type != engine.TaskTypeGroupExec || got.Task.UnitIdx != gu {
			t.Errorf("queued task = {type:%v unit:%d}, want {%v %d}",
				got.Task.Type, got.Task.UnitIdx, engine.TaskTypeGroupExec, gu)
		}

		// The unit must be re-acquirable: that is the whole point of the revoke.
		if ok, err := s.AcquireGroupLease(ctx, lease(id, gu, "T2")); err != nil || !ok {
			t.Fatalf("re-acquire after revoke: ok=%v err=%v", ok, err)
		}

		// Fence: the stale token must not revoke the new owner's lease.
		stale, err := reclaimer.RevokeGroupLeaseWithOutbox(ctx, id, gu, "T1",
			engine.OutboxEntry{ID: "requeue-group/stale/" + string(id), Task: entry.Task})
		if err != nil {
			t.Fatalf("stale RevokeGroupLeaseWithOutbox: %v", err)
		}
		if stale {
			t.Error("stale token revoked the new owner's lease")
		}
	})

	t.Run("CommitAfterExpiryStaleTokenRejected", func(t *testing.T) {
		s, id, gu := seed(t, twoUnitGraph(t))
		s.AcquireGroupLease(ctx, lease(id, gu, "T1"))
		s.ExpireGroupLease(ctx, id, gu, "T1")

		// Re-acquire with new token.
		l2 := lease(id, gu, "T2")
		s.AcquireGroupLease(ctx, l2)

		// Old token commit must be rejected.
		res, _ := s.CommitGroup(ctx, commit(id, gu, "T1"))
		if res.Applied {
			t.Fatal("commit with expired/stale token must not apply")
		}
		if res.Outcome != engine.CommitOutcomeStaleToken {
			t.Errorf("outcome = %q, want stale-token", res.Outcome)
		}

		// New token commit succeeds.
		c := commit(id, gu, "T2")
		c.Attempt = l2.Attempt
		res2, err := s.CommitGroup(ctx, c)
		if err != nil || !res2.Applied {
			t.Fatalf("commit with current token: %+v err=%v", res2, err)
		}
	})
}
