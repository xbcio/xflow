package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RunArrivalFoldingContract exercises the backend half of downstream-arrival
// folding: AtomicStateStore.CommitNode applying CommitNodeRequest.Arrivals in
// the same fenced transition as the terminal node write, and reporting the
// units it resolved as skip in CommitNodeResult.Skipped.
//
// It drives the public store API only. Every case creates its own execution, so
// the schedule markers and counters never leak between cases, and every
// assertion reads the durable outbox rather than a returned count wherever the
// two could disagree.
//
// The contract is what proves CapabilityAtomicArrivalFolding. The capability's
// interface assertion cannot: engine.ArrivalFoldingCommitter reuses CommitNode's
// signature, so every AtomicStateStore satisfies it structurally. A backend that
// silently ignored the Arrivals field would pass the manifest check and fail
// every case below.
func RunArrivalFoldingContract(t *testing.T, manifest *Manifest, state engine.StateStore) {
	t.Helper()
	manifest.Report(t)
	RequiredCapability[engine.ArrivalFoldingCommitter](t, manifest, CapabilityAtomicArrivalFolding, state)
	atomic := RequiredCapability[engine.AtomicStateStore](t, manifest, CapabilityAtomicStateStore, state)

	h := &arrivalFoldingHarness{state: state, atomic: atomic, g: arrivalFoldingGraph()}

	t.Run("execute when the in-degree reaches zero with an active input", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-execute")
		h.createExecution(t, id)

		// One of the two inbound edges is consumed: the in-degree is 1, not 0,
		// so nothing may be scheduled yet. This half is what distinguishes a
		// real counter from "schedule on every arrival".
		left := h.lease(t, id, "left")
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 1)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(left) = %+v, want an applied accepted commit", res)
		}
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(left).Skipped = %v, want none: the in-degree had not reached zero", res.Skipped)
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("join's second inbound edge was not yet consumed, but %d outbox entries were "+
				"scheduled: %+v", len(entries), entries)
		}

		right := h.lease(t, id, "right")
		req = h.request(right)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 1)}
		res = h.commit(t, req)
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(right).Skipped = %v, want none: an active input arrived", res.Skipped)
		}
		intent := h.onlyIntent(t, id, "join", engine.TaskTypeNodeExec)
		if got, want := intent.Task.UnitIdx, h.unit(t, "join"); got != want {
			t.Errorf("execute intent UnitIdx = %d, want %d (the unit's counter key)", got, want)
		}
		if got, want := intent.Task.NodeIdx, h.nodeIdx(t, "join"); got != want {
			t.Errorf("execute intent NodeIdx = %d, want %d (the representative node index)", got, want)
		}
		if !containsString(res.OutboxIDs, intent.ID) {
			t.Errorf("OutboxIDs = %v, want the written execute intent %q among them: the field "+
				"is a candidate hint, so it may name unwritten intents too, but the written "+
				"one must be in it", res.OutboxIDs, intent.ID)
		}
	})

	t.Run("skip when the in-degree reaches zero with no active input", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-skip")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "tail", 1, 0)}
		res := h.commit(t, req)
		if len(res.Skipped) != 1 || res.Skipped[0] != (engine.SkippedUnit{NodeName: "tail", Count: 1}) {
			t.Fatalf("CommitNode(left).Skipped = %v, want [{tail 1}]: a zero in-degree with no "+
				"active input resolves the unit as skip", res.Skipped)
		}
		intent := h.onlyIntent(t, id, "tail", engine.TaskTypeNodeSkip)
		if got, want := intent.Task.UnitIdx, h.unit(t, "tail"); got != want {
			t.Errorf("skip intent UnitIdx = %d, want %d", got, want)
		}
		if got, want := intent.Task.NodeIdx, h.nodeIdx(t, "tail"); got != want {
			t.Errorf("skip intent NodeIdx = %d, want %d", got, want)
		}
		if !containsString(res.OutboxIDs, intent.ID) {
			t.Errorf("OutboxIDs = %v, want the written skip intent %q among them: the field is "+
				"a candidate hint, so it may name unwritten intents too (the execute candidate "+
				"of the same arrival), but the written one must be in it", res.OutboxIDs, intent.ID)
		}
		if execs := h.intents(t, id, "tail", engine.TaskTypeNodeExec); len(execs) != 0 {
			t.Errorf("skip resolution also wrote %d execute intents for tail: %+v", len(execs), execs)
		}
	})

	t.Run("wait_any fires on the first active arrival", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-wait-any")
		h.createExecution(t, id)

		any1 := h.lease(t, id, "any1")
		req := h.request(any1)
		req.Arrivals = []engine.DownstreamArrival{h.arrivalWithMode(t, "anyjoin", 1, 1, "wait_any")}
		res := h.commit(t, req)
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(any1).Skipped = %v, want none", res.Skipped)
		}
		intent := h.onlyIntent(t, id, "anyjoin", engine.TaskTypeNodeExec)
		if got, want := intent.Task.UnitIdx, h.unit(t, "anyjoin"); got != want {
			t.Errorf("wait_any execute intent UnitIdx = %d, want %d", got, want)
		}

		// A second active arrival must not schedule the unit twice: the schedule
		// marker is written once and is what makes the transition idempotent.
		any2 := h.lease(t, id, "any2")
		req = h.request(any2)
		req.Arrivals = []engine.DownstreamArrival{h.arrivalWithMode(t, "anyjoin", 1, 1, "wait_any")}
		res = h.commit(t, req)
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(any2).Skipped = %v, want none", res.Skipped)
		}
		execs := h.intents(t, id, "anyjoin", engine.TaskTypeNodeExec)
		if len(execs) != 1 {
			t.Fatalf("wait_any destination has %d execute intents, want 1: an already scheduled "+
				"unit must not be scheduled again (%+v)", len(execs), execs)
		}
	})

	t.Run("a repeated commit does not decrement twice", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-duplicate")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		req := h.request(left)
		// Half of join's in-degree. If the duplicate below decremented again the
		// in-degree would reach zero and schedule a skip intent, which is what
		// the empty-outbox assertion catches.
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 0)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("first CommitNode = %+v, want an applied accepted commit", res)
		}

		dup, err := h.atomic.CommitNode(context.Background(), req)
		if err != nil {
			t.Fatalf("duplicate CommitNode error = %v", err)
		}
		if dup.Outcome != engine.CommitOutcomeDuplicateTerminal || dup.Applied {
			t.Fatalf("duplicate CommitNode = %+v, want a non-applied duplicate-terminal", dup)
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("a duplicate commit decremented the counters again and scheduled %d outbox "+
				"entries: %+v", len(entries), entries)
		}
	})

	t.Run("a stale token does not touch the counters", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-stale")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		stale := h.request(left)
		stale.LeaseToken = "not-the-issued-token"
		stale.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 0)}
		res, err := h.atomic.CommitNode(context.Background(), stale)
		if err != nil {
			t.Fatalf("CommitNode(stale token) error = %v", err)
		}
		if res.Outcome != engine.CommitOutcomeStaleToken || res.Applied {
			t.Fatalf("CommitNode(stale token) = %+v, want a non-applied stale-token refusal", res)
		}

		// The valid commit consumes exactly one of join's two inbound edges. If
		// the refused call had decremented, the in-degree would now be zero and
		// this commit would schedule a skip for an inactive destination.
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 0)}
		res = h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(valid token after stale) = %+v, want an applied accepted commit", res)
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("a stale-token commit mutated the scheduling counters: %d outbox entries "+
				"were scheduled: %+v", len(entries), entries)
		}
	})

	t.Run("canceling commits write no intents", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-canceling")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		if err := h.state.UpdateExecutionStatus(context.Background(), id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
		}
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 2, 2)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(canceling) = %+v, want the in-flight commit accepted: cancel "+
				"does not invalidate work already leased", res)
		}
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(canceling).Skipped = %v, want none", res.Skipped)
		}
		// OutboxIDs is a candidate hint, not a write log (see
		// engine.CommitNodeResult.OutboxIDs): both backends may report the
		// execute/skip candidates of an arrival whose folding the canceling
		// guard suppressed, so it is deliberately not asserted empty here. The
		// durable outbox below is the authority on what was written.
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("a commit under canceling scheduled %d outbox entries: %+v; no new "+
				"downstream work may be scheduled while a Cancel is in flight", len(entries), entries)
		}
	})

	t.Run("fatal and cyclic commits reject arrivals", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-rejected")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		cases := []struct {
			name   string
			mutate func(*engine.CommitNodeRequest)
		}{
			{
				name: "fatal",
				mutate: func(req *engine.CommitNodeRequest) {
					req.Fatal = true
					req.Status = types.NodeStatusFailed
				},
			},
			{
				name: "cyclic",
				mutate: func(req *engine.CommitNodeRequest) {
					req.AllowCycles = true
				},
			},
		}
		for _, tc := range cases {
			req := h.request(left)
			tc.mutate(&req)
			req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 1)}
			if _, err := h.atomic.CommitNode(context.Background(), req); err == nil {
				t.Fatalf("CommitNode(%s with arrivals) was accepted: Validate must reject a request "+
					"whose completion protocol contradicts its payload", tc.name)
			}
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("rejected commits wrote %d outbox entries: %+v", len(entries), entries)
		}

		// Positive control: neither rejection consumed the lease or the counters,
		// so the same lease still folds its arrivals on a legal acyclic commit.
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 2, 2)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(after rejections) = %+v, want an applied accepted commit", res)
		}
		h.onlyIntent(t, id, "join", engine.TaskTypeNodeExec)
	})

	t.Run("a commit that finalizes the execution writes no intents", func(t *testing.T) {
		// A two-node acyclic graph: the second commit drives the remaining-unit
		// counter to zero, so the execution finalizes inside the same
		// transition. Folded arrivals must be suppressed there — downstream
		// work for a finished execution could never be consumed.
		h2 := &arrivalFoldingHarness{state: state, atomic: atomic, g: twoNodeArrivalGraph()}
		id := types.ExecutionID("arrival-folding-done")
		h2.createExecution(t, id)

		alpha := h2.lease(t, id, "alpha")
		res := h2.commit(t, h2.request(alpha))
		if res.ExecutionDone {
			t.Fatalf("first commit finished the execution (%+v); the contract needs the second "+
				"commit to be the one that finalizes it", res)
		}

		beta := h2.lease(t, id, "beta")
		req := h2.request(beta)
		req.Arrivals = []engine.DownstreamArrival{h2.arrival(t, "alpha", 1, 1)}
		res = h2.commit(t, req)
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("final CommitNode = %+v, want the execution finalized as success", res)
		}
		if len(res.Skipped) != 0 {
			t.Fatalf("final CommitNode.Skipped = %v, want none", res.Skipped)
		}
		if entries := h2.outbox(t, id); len(entries) != 0 {
			t.Fatalf("a commit that finalized the execution still scheduled %d outbox entries: "+
				"%+v", len(entries), entries)
		}
	})

	t.Run("parallel edges aggregate into one arrival", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-merged-edges")
		h.createExecution(t, id)

		// left contributes two parallel edges to wide (ArrivalCount 2 of its
		// in-degree 3): the counter must consume both at once and still not
		// schedule, because right has not arrived.
		left := h.lease(t, id, "left")
		req := h.request(left)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "wide", 2, 2)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(left) = %+v, want an applied accepted commit", res)
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("aggregated arrivals scheduled work before the in-degree reached zero: %+v", entries)
		}

		right := h.lease(t, id, "right")
		req = h.request(right)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "wide", 1, 1)}
		res = h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(right) = %+v, want an applied accepted commit", res)
		}
		intent := h.onlyIntent(t, id, "wide", engine.TaskTypeNodeExec)
		if got, want := intent.Task.UnitIdx, h.unit(t, "wide"); got != want {
			t.Errorf("execute intent UnitIdx = %d, want %d", got, want)
		}
		if got, want := intent.Task.NodeIdx, h.nodeIdx(t, "wide"); got != want {
			t.Errorf("execute intent NodeIdx = %d, want %d", got, want)
		}
	})

	t.Run("skip cascade resolves the scheduling marker by unit index", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-cascade")
		h.createExecution(t, id)

		left := h.lease(t, id, "left")
		req := h.request(left)
		// Both of join's inbound edges are carried by this one transition, and
		// neither is active: the unit resolves as skip.
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 2, 0)}
		res := h.commit(t, req)
		if len(res.Skipped) != 1 || res.Skipped[0].NodeName != "join" {
			t.Fatalf("CommitNode(left).Skipped = %v, want join", res.Skipped)
		}
		joinSkip := h.onlyIntent(t, id, "join", engine.TaskTypeNodeSkip)
		// The contract graph declares a supply node before join, so join's node
		// index is one past its unit index. A backend that resolved the cascade
		// marker by NodeIdx would address tail's counters instead.
		if got, want := joinSkip.Task.UnitIdx, h.unit(t, "join"); got != want {
			t.Fatalf("join skip intent UnitIdx = %d, want %d", got, want)
		}
		if got, want := joinSkip.Task.NodeIdx, h.nodeIdx(t, "join"); got != want {
			t.Fatalf("join skip intent NodeIdx = %d, want %d", got, want)
		}

		// The cascade's second hop: the engine commits the skipped unit as a
		// system transition, which resolves its "skip" marker by UnitIdx, and
		// folds that unit's own downstream arrivals into the same commit.
		cascade := engine.CommitNodeRequest{
			ExecutionID:  id,
			NodeName:     "join",
			NodeIdx:      h.nodeIdx(t, "join"),
			UnitIdx:      h.unit(t, "join"),
			ActivationID: 1,
			Status:       types.NodeStatusSkipped,
			System:       true,
			Arrivals:     []engine.DownstreamArrival{h.arrival(t, "tail", 1, 0)},
		}
		res = h.commit(t, cascade)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(cascade join) = %+v, want an applied accepted commit: the system "+
				"fence must resolve the skip marker written under join's UnitIdx", res)
		}
		if len(res.Skipped) != 1 || res.Skipped[0] != (engine.SkippedUnit{NodeName: "tail", Count: 1}) {
			t.Fatalf("CommitNode(cascade join).Skipped = %v, want [{tail 1}]", res.Skipped)
		}
		tailSkip := h.onlyIntent(t, id, "tail", engine.TaskTypeNodeSkip)
		if got, want := tailSkip.Task.UnitIdx, h.unit(t, "tail"); got != want {
			t.Errorf("tail skip intent UnitIdx = %d, want %d", got, want)
		}
		if got, want := tailSkip.Task.NodeIdx, h.nodeIdx(t, "tail"); got != want {
			t.Errorf("tail skip intent NodeIdx = %d, want %d", got, want)
		}
	})

	t.Run("output reclaim and arrivals apply in one commit", func(t *testing.T) {
		id := types.ExecutionID("arrival-folding-reclaim")
		h.createExecution(t, id)
		if err := h.state.PutOutput(context.Background(), id, "reclaimed-output", map[string]any{"v": 1}); err != nil {
			t.Fatalf("PutOutput() error = %v", err)
		}

		// The destination's in-degree is 2 and this transition carries only one
		// active edge, so nothing may be scheduled yet. That half is what makes
		// this case sensitive to the arrival key base: the counter triplet of
		// the first arrival sits after the reclaim keys, and a backend whose
		// base forgot the reclaim count would decrement some other key and read
		// its in-degree as exhausted, scheduling a unit from a branch that had
		// not arrived.
		left := h.lease(t, id, "left")
		req := h.request(left)
		req.ReclaimOutputNames = []string{"reclaimed-output"}
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 1)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(reclaim+arrivals) = %+v, want an applied accepted commit", res)
		}
		if len(res.Skipped) != 0 {
			t.Fatalf("CommitNode(reclaim+arrivals).Skipped = %v, want none: join's in-degree "+
				"had not reached zero", res.Skipped)
		}
		out, err := h.state.GetOutput(context.Background(), id, "reclaimed-output")
		if err != nil {
			t.Fatalf("GetOutput(reclaimed-output) error = %v", err)
		}
		if out != nil {
			t.Fatalf("GetOutput(reclaimed-output) = %#v, want nil: the reclaim must be applied by "+
				"the same transition that accepted the commit", out)
		}
		if entries := h.outbox(t, id); len(entries) != 0 {
			t.Fatalf("one of join's two inbound edges was consumed, but %d outbox entries were "+
				"scheduled: %+v; an arrival key base that ignored the reclaim keys would count "+
				"the arrival against some other destination", len(entries), entries)
		}

		// The second inbound edge completes join's in-degree. If the folded
		// arrival above had decremented a different key (the failure the first
		// half is looking for), join's in-degree would still be 2 here and no
		// intent would be written.
		right := h.lease(t, id, "right")
		req = h.request(right)
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "join", 1, 1)}
		res = h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(right after reclaim) = %+v, want an applied accepted commit", res)
		}
		h.onlyIntent(t, id, "join", engine.TaskTypeNodeExec)
	})
}

// arrivalFoldingHarness bundles one backend's store with the graph and atomic
// view the contract drives.
type arrivalFoldingHarness struct {
	state  engine.StateStore
	atomic engine.AtomicStateStore
	g      *graph.Graph
}

// arrivalFoldingGraph returns the graph every folding case but the finalization
// case drives.
//
// Node indices diverge from unit indices on purpose: the declaration-only
// supply node "rules" occupies a node index but no unit, so every node after it
// sits one past its unit. A backend that confused the two would still pass a
// graph without a supply node.
//
// In-degrees: join 2 (left, right), tail 1 (join), leaf 1 (tail), anyjoin 2
// (any1, any2, merged as wait_any), wide 3 (two parallel edges from left plus
// one from right).
func arrivalFoldingGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "arrival-folding",
		Nodes: []types.NodeDef{
			{Name: "left", Type: "test.start"},
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "right", Type: "test.start"},
			{Name: "join", Type: "test.join"},
			{Name: "tail", Type: "test.tail"},
			{Name: "leaf", Type: "test.leaf"},
			{Name: "any1", Type: "test.start"},
			{Name: "any2", Type: "test.start"},
			{Name: "anyjoin", Type: "xflow.merge", Parameters: map[string]any{"mode": "wait_any"}},
			{Name: "wide", Type: "test.join"},
		},
		Connections: types.Connections{
			"left": {"main": types.PortConnections{Targets: []types.Connection{
				{Node: "join", Input: "main"},
				{Node: "wide", Input: "main"},
				{Node: "wide", Input: "main"},
			}}},
			"right": {"main": types.PortConnections{Targets: []types.Connection{
				{Node: "join", Input: "main"},
				{Node: "wide", Input: "main"},
			}}},
			"join": {"main": types.PortConnections{Targets: []types.Connection{{Node: "tail", Input: "main"}}}},
			"tail": {"main": types.PortConnections{Targets: []types.Connection{{Node: "leaf", Input: "main"}}}},
			"any1": {"main": types.PortConnections{Targets: []types.Connection{{Node: "anyjoin", Input: "main"}}}},
			"any2": {"main": types.PortConnections{Targets: []types.Connection{{Node: "anyjoin", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "join", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}

// twoNodeArrivalGraph is the smallest acyclic graph whose second commit drives
// the remaining-unit counter to zero.
func twoNodeArrivalGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "arrival-folding-done",
		Nodes: []types.NodeDef{
			{Name: "alpha", Type: "test.start"},
			{Name: "beta", Type: "test.finish"},
		},
		Connections: types.Connections{
			"alpha": {"main": types.PortConnections{Targets: []types.Connection{{Node: "beta", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}

func (h *arrivalFoldingHarness) createExecution(t *testing.T, id types.ExecutionID) {
	t.Helper()
	if err := h.state.CreateExecution(context.Background(), &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  h.g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
}

// lease claims a first-generation lease for node, the shape an ordinary runner
// commit carries: the lease fields and the attempt are what CommitNode fences
// on. The attempt is read back from the stored node because it is the store,
// not the caller, that assigns it.
func (h *arrivalFoldingHarness) lease(t *testing.T, id types.ExecutionID, node string) *engine.TaskLease {
	t.Helper()
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease/" + string(id) + "/" + node),
		LeaseToken: engine.LeaseToken("token/" + string(id) + "/" + node),
		IssuedAt:   time.Now().UTC().Truncate(time.Millisecond),
		TTL:        time.Minute,
		Task: engine.Task{
			ExecutionID:  id,
			NodeName:     node,
			NodeIdx:      h.nodeIdx(t, node),
			UnitIdx:      h.unit(t, node),
			Type:         engine.TaskTypeNodeExec,
			ActivationID: 1,
		},
	}
	if _, acquired, err := h.state.AcquireTaskLease(context.Background(), lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(%q/%q) acquired=%v err=%v", id, node, acquired, err)
	}
	snap, err := h.state.GetNode(context.Background(), id, node)
	if err != nil || snap == nil {
		t.Fatalf("GetNode(%q/%q) = %+v, %v; want the acquired node", id, node, snap, err)
	}
	lease.Attempt = snap.Attempt
	return lease
}

func (h *arrivalFoldingHarness) request(lease *engine.TaskLease) engine.CommitNodeRequest {
	return engine.CommitNodeRequest{
		ExecutionID:  lease.Task.ExecutionID,
		NodeName:     lease.Task.NodeName,
		NodeIdx:      lease.Task.NodeIdx,
		UnitIdx:      lease.Task.UnitIdx,
		ActivationID: lease.Task.ActivationID,
		LeaseID:      lease.LeaseID,
		LeaseToken:   lease.LeaseToken,
		Attempt:      lease.Attempt,
		Status:       types.NodeStatusSuccess,
	}
}

func (h *arrivalFoldingHarness) commit(t *testing.T, req engine.CommitNodeRequest) engine.CommitNodeResult {
	t.Helper()
	res, err := h.atomic.CommitNode(context.Background(), req)
	if err != nil {
		t.Fatalf("CommitNode(%q/%q) error = %v", req.ExecutionID, req.NodeName, err)
	}
	return res
}

func (h *arrivalFoldingHarness) nodeIdx(t *testing.T, node string) int {
	t.Helper()
	idx, ok := h.g.NodeIndex(node)
	if !ok {
		t.Fatalf("arrival contract graph has no node %q", node)
	}
	return idx
}

func (h *arrivalFoldingHarness) unit(t *testing.T, node string) int {
	t.Helper()
	unit := h.g.UnitIndexForNode(h.nodeIdx(t, node))
	if unit < 0 {
		t.Fatalf("node %q is declaration-only and has no scheduling unit", node)
	}
	return unit
}

func (h *arrivalFoldingHarness) arrival(t *testing.T, node string, arrivalCount, activeCount int) engine.DownstreamArrival {
	t.Helper()
	return h.arrivalWithMode(t, node, arrivalCount, activeCount, "")
}

func (h *arrivalFoldingHarness) arrivalWithMode(t *testing.T, node string, arrivalCount, activeCount int, mergeMode string) engine.DownstreamArrival {
	t.Helper()
	return engine.DownstreamArrival{
		NodeName:     node,
		NodeIdx:      h.nodeIdx(t, node),
		UnitIdx:      h.unit(t, node),
		ArrivalCount: arrivalCount,
		ActiveCount:  activeCount,
		MergeMode:    mergeMode,
	}
}

func (h *arrivalFoldingHarness) outbox(t *testing.T, id types.ExecutionID) []engine.OutboxEntry {
	t.Helper()
	entries, err := h.atomic.ListOutbox(context.Background(), id, time.Now().Add(time.Hour), 256)
	if err != nil {
		t.Fatalf("ListOutbox(%q) error = %v", id, err)
	}
	return entries
}

func (h *arrivalFoldingHarness) intents(t *testing.T, id types.ExecutionID, node string, taskType engine.TaskType) []engine.OutboxEntry {
	t.Helper()
	var matching []engine.OutboxEntry
	for _, entry := range h.outbox(t, id) {
		if entry.Task.NodeName == node && entry.Task.Type == taskType {
			matching = append(matching, entry)
		}
	}
	return matching
}

func (h *arrivalFoldingHarness) onlyIntent(t *testing.T, id types.ExecutionID, node string, taskType engine.TaskType) engine.OutboxEntry {
	t.Helper()
	matching := h.intents(t, id, node, taskType)
	if len(matching) != 1 {
		t.Fatalf("outbox has %d intents of task type %d for %q, want exactly 1: %+v",
			len(matching), int(taskType), node, matching)
	}
	return matching[0]
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
