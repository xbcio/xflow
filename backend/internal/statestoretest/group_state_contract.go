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
		Connections: types.Connections{"ingest": {"main": {{Node: "analyze", Input: "main"}}}},
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
			"ingest":  {"main": {{Node: "analyze", Input: "main"}}},
			"analyze": {"main": {{Node: "store", Input: "main"}}},
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
}
