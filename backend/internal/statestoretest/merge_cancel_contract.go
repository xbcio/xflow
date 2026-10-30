package statestoretest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// mcReason is the error text every cancel commit in this contract carries; a
// backend must store it on the node row as given.
const mcReason = "canceled by merge"

// RunMergeCancelContract exercises the backend half of xflow.merge
// on_others: cancel: the fourth fenced System commit shape (a canceled node)
// that retires a losing branch, and the MergeCancel intent a successful merge
// commit writes in the same transition.
//
// It drives the public store API only, so the Redis backend runs the same
// cases. Every case creates its own execution; the scheduling markers and the
// remaining-unit counter therefore never leak between cases.
//
// The capability marker (engine.MergeCancelCommitter) is only a declaration. A
// backend that declares it and still answers StaleToken to every canceled
// commit, or drops the MergeCancel field, passes the manifest check and fails
// the cases below.
func RunMergeCancelContract(t *testing.T, manifest *Manifest, state engine.StateStore) {
	t.Helper()
	manifest.Report(t)
	RequiredCapability[engine.MergeCancelCommitter](t, manifest, CapabilityMergeCancel, state)
	atomic := RequiredCapability[engine.AtomicStateStore](t, manifest, CapabilityAtomicStateStore, state)

	pair := &arrivalFoldingHarness{state: state, atomic: atomic, g: mergeCancelPairGraph()}
	fan := &arrivalFoldingHarness{state: state, atomic: atomic, g: mergeCancelFanGraph()}

	mcFenceAccepts(t, pair)
	mcFenceRefuses(t, pair, manifest)
	mcRemainingCounter(t, pair)
	mcFoldedArrivals(t, fan)
	mcLateSignal(t, pair, manifest)
	mcLeaseRetired(t, pair, manifest)
	mcIntent(t, pair)
	mcRace(t, pair)
}

// mergeCancelPairGraph is the smallest acyclic graph whose second terminal
// commit drives the remaining-unit counter to zero: alpha feeds beta.
//
// The declaration-only supply node "rules" comes first on purpose. It occupies
// a node index but no unit, so every node after it sits one past its unit, and
// a fence that read the scheduling marker by NodeIdx would address a
// neighbour's counters. Two units remain, so the execution finalizes exactly
// when the second one reaches a terminal status: that is how the cases below
// observe the remaining counter through the public API.
func mergeCancelPairGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "merge-cancel-pair",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "alpha", Type: "test.start"},
			{Name: "beta", Type: "test.finish"},
		},
		Connections: types.Connections{
			"alpha": {"main": types.PortConnections{Targets: []types.Connection{{Node: "beta", Input: "main"}}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "beta", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}

// mergeCancelFanGraph gives a canceled node somewhere to fold its arrivals:
// loser has one inbound edge (from up), so one arrival schedules it, and tail
// and alt are loser's successors, each with in-degree 1. Like the pair graph it
// puts a supply node first so unit and node indices diverge.
func mergeCancelFanGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "merge-cancel-fan",
		Nodes: []types.NodeDef{
			{Name: "rules", Type: "xflow.supply.static", Kind: types.NodeKindSupply},
			{Name: "up", Type: "test.start"},
			{Name: "loser", Type: "test.approval"},
			{Name: "tail", Type: "test.tail"},
			{Name: "alt", Type: "test.tail"},
		},
		Connections: types.Connections{
			"up": {"main": types.PortConnections{Targets: []types.Connection{{Node: "loser", Input: "main"}}}},
			"loser": {"main": types.PortConnections{Targets: []types.Connection{
				{Node: "tail", Input: "main"}, {Node: "alt", Input: "main"},
			}}},
		},
		DependencyEdges: []types.DependencyEdge{{Node: "loser", Supply: "rules"}},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}

// mcCancel builds the fourth System commit shape: a canceled node. It carries
// no lease, because the merge handler that issues it never held the loser's.
func mcCancel(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string) engine.CommitNodeRequest {
	t.Helper()
	return engine.CommitNodeRequest{
		ExecutionID:  id,
		NodeName:     node,
		NodeIdx:      h.nodeIdx(t, node),
		UnitIdx:      h.unit(t, node),
		ActivationID: 1,
		Status:       types.NodeStatusCanceled,
		System:       true,
		Error:        mcReason,
	}
}

// mcClaim moves a Running lease to Committing, the step an engine takes
// before it commits or suspends.
func mcClaim(t *testing.T, h *arrivalFoldingHarness, lease *engine.TaskLease) {
	t.Helper()
	if _, claimed, err := h.state.ClaimTaskLease(context.Background(), lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease(%q) claimed=%v err=%v, want a claimed lease", lease.Task.NodeName, claimed, err)
	}
}

// mcSuspend parks a leased node on a signal wait and returns it Suspended.
func mcSuspend(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string, spec *types.SuspendSpec) *engine.TaskLease {
	t.Helper()
	suspender, ok := h.state.(engine.DurableLeaseSuspender)
	if !ok {
		t.Fatalf("%T does not implement engine.DurableLeaseSuspender", h.state)
	}
	lease := h.lease(t, id, node)
	mcClaim(t, h, lease)
	committed, err := suspender.SuspendTaskLeaseWithOutbox(context.Background(), lease, nil, false, false, spec, "")
	if err != nil || !committed {
		t.Fatalf("SuspendTaskLeaseWithOutbox(%q) committed=%v err=%v, want true/nil", node, committed, err)
	}
	return lease
}

// mcPlant schedules a unit the way an upstream commit does: committing from
// as a success whose single folded arrival into to is active (execute marker)
// or inactive (skip marker). The target is left with no node row.
func mcPlant(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, from, to string, active int) {
	t.Helper()
	req := h.request(h.lease(t, id, from))
	req.Arrivals = []engine.DownstreamArrival{h.arrival(t, to, 1, active)}
	if res := h.commit(t, req); res.Outcome != engine.CommitOutcomeAccepted {
		t.Fatalf("CommitNode(%q) = %+v, want an accepted commit to plant the %q marker", from, res, to)
	}
}

// mcPending writes the Pending row a scheduled-but-unleased node can have.
func mcPending(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string) {
	t.Helper()
	if err := h.state.UpsertNode(context.Background(), &engine.NodeSnapshot{
		ExecutionID: id, Name: node, NodeIdx: h.nodeIdx(t, node), UnitIdx: h.unit(t, node),
		Status: types.NodeStatusPending, ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(%q pending) error = %v", node, err)
	}
}

// mcFreeze fingerprints everything a refused commit must leave alone: the
// node row, the execution status, and the durable outbox.
func mcFreeze(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string) string {
	t.Helper()
	ctx := context.Background()
	snap, err := h.state.GetNode(ctx, id, node)
	if err != nil {
		t.Fatalf("GetNode(%q) error = %v", node, err)
	}
	row := "<no row>"
	if snap != nil {
		row = fmt.Sprintf("%s/act=%d/attempt=%d/port=%q/err=%q/lease=%q/finished=%v",
			snap.Status, snap.ActivationID, snap.Attempt, snap.Port, snap.Error, snap.LeaseToken, !snap.FinishedAt.IsZero())
	}
	ids := make([]string, 0)
	for _, entry := range h.outbox(t, id) {
		ids = append(ids, entry.ID)
	}
	sort.Strings(ids)
	return fmt.Sprintf("node{%s} execution=%s outbox=%v", row, mcStatus(t, h, id), ids)
}

func mcStatus(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID) types.ExecutionStatus {
	t.Helper()
	snap, err := h.state.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = %+v, %v; want the execution", id, snap, err)
	}
	return snap.Status
}

// mcRequireCanceled pins what the accepted fourth shape writes to the node
// row: Canceled with no port, the caller's error text, a finish stamp, and no
// lease left behind.
func mcRequireCanceled(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string) {
	t.Helper()
	snap, err := h.state.GetNode(context.Background(), id, node)
	if err != nil || snap == nil {
		t.Fatalf("GetNode(%q) = %+v, %v; want the canceled node", node, snap, err)
	}
	if snap.Status != types.NodeStatusCanceled || snap.Port != "" || snap.Error != mcReason {
		t.Fatalf("node %q = status %q port %q error %q, want canceled with no port and error %q",
			node, snap.Status, snap.Port, snap.Error, mcReason)
	}
	if snap.FinishedAt.IsZero() {
		t.Errorf("node %q has no FinishedAt: the first terminal commit stamps it", node)
	}
	if snap.LeaseID != "" || snap.LeaseToken != "" {
		t.Errorf("node %q kept lease %q/%q after the cancel: the rebuilt row must not carry the lease",
			node, snap.LeaseID, snap.LeaseToken)
	}
}

// mcExec creates the execution one case runs in. The ID is fixed per case, like
// the other contracts', so a failing case names the same execution every run.
func mcExec(t *testing.T, h *arrivalFoldingHarness, name string) types.ExecutionID {
	t.Helper()
	id := types.ExecutionID("merge-cancel-" + name)
	h.createExecution(t, id)
	return id
}

// mcFenceAccepts pins the legal pre-states of the canceled shape: a node that
// is parked or leased (Suspended, Running, Committing), and a unit that was
// scheduled to execute but never leased (execute marker, with or without a
// Pending row).
func mcFenceAccepts(t *testing.T, h *arrivalFoldingHarness) {
	accept := func(t *testing.T, id types.ExecutionID, node string) {
		t.Helper()
		res := h.commit(t, mcCancel(t, h, id, node))
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(canceled %q) = %+v, want an applied accepted commit", node, res)
		}
		mcRequireCanceled(t, h, id, node)
	}

	t.Run("canceled commit is accepted for a suspended node", func(t *testing.T) {
		id := mcExec(t, h, "accept-suspended")
		mcSuspend(t, h, id, "alpha", &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}})
		accept(t, id, "alpha")
	})

	t.Run("canceled commit is accepted for a running node", func(t *testing.T) {
		id := mcExec(t, h, "accept-running")
		h.lease(t, id, "alpha")
		accept(t, id, "alpha")
	})

	t.Run("canceled commit is accepted for a committing node", func(t *testing.T) {
		id := mcExec(t, h, "accept-committing")
		mcClaim(t, h, h.lease(t, id, "alpha"))
		accept(t, id, "alpha")
	})

	t.Run("canceled commit is accepted for a pending node with an execute marker", func(t *testing.T) {
		id := mcExec(t, h, "accept-pending")
		mcPlant(t, h, id, "alpha", "beta", 1)
		mcPending(t, h, id, "beta")
		accept(t, id, "beta")
	})

	t.Run("canceled commit is accepted for a scheduled unit with no node row", func(t *testing.T) {
		id := mcExec(t, h, "accept-no-row")
		mcPlant(t, h, id, "alpha", "beta", 1)
		if snap, err := h.state.GetNode(context.Background(), id, "beta"); err != nil || snap != nil {
			t.Fatalf("GetNode(beta) = %+v, %v; the case needs a scheduled unit with no row", snap, err)
		}
		accept(t, id, "beta")
	})
}

// mcRefuse commits a canceled system request and requires the verdict, an
// untouched node row, execution status and outbox, and Applied=false.
func mcRefuse(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, node string, want engine.CommitOutcome) {
	t.Helper()
	before := mcFreeze(t, h, id, node)
	res := h.commit(t, mcCancel(t, h, id, node))
	if res.Outcome != want || res.Applied || res.ExecutionDone {
		t.Fatalf("CommitNode(canceled %q) = %+v, want outcome %v, not applied", node, res, want)
	}
	if len(res.OutboxIDs) != 0 || len(res.Skipped) != 0 {
		t.Fatalf("refused CommitNode(canceled %q) reported work: %+v", node, res)
	}
	if after := mcFreeze(t, h, id, node); after != before {
		t.Fatalf("refused CommitNode(canceled %q) changed state:\n before %s\n after  %s", node, before, after)
	}
}

// mcFenceRefuses pins the pre-states the canceled shape must refuse. A refusal
// is reported as handled to the engine, so a wrong accept here would cancel a
// unit another path owns; every case therefore also requires zero state change.
func mcFenceRefuses(t *testing.T, h *arrivalFoldingHarness, manifest *Manifest) {
	t.Run("canceled commit is refused for an unscheduled unit", func(t *testing.T) {
		id := mcExec(t, h, "refuse-unscheduled")
		mcRefuse(t, h, id, "beta", engine.CommitOutcomeStaleToken)

		// The refusal did not consume the unit's share of the remaining counter:
		// alpha's commit leaves beta outstanding, and only beta's finalizes.
		res := h.commit(t, h.request(h.lease(t, id, "alpha")))
		if res.ExecutionDone {
			t.Fatalf("CommitNode(alpha) finalized the execution (%+v): the refused cancel of "+
				"beta decremented the remaining counter", res)
		}
		res = h.commit(t, h.request(h.lease(t, id, "beta")))
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("CommitNode(beta) = %+v, want the execution finalized as success", res)
		}
	})

	t.Run("canceled commit is refused for a unit a skip cascade owns", func(t *testing.T) {
		id := mcExec(t, h, "refuse-skip")
		mcPlant(t, h, id, "alpha", "beta", 0)
		mcRefuse(t, h, id, "beta", engine.CommitOutcomeStaleToken)

		// The cascade still resolves the unit: the skip marker survived.
		cascade := engine.CommitNodeRequest{
			ExecutionID: id, NodeName: "beta", NodeIdx: h.nodeIdx(t, "beta"), UnitIdx: h.unit(t, "beta"),
			ActivationID: 1, Status: types.NodeStatusSkipped, System: true,
		}
		res := h.commit(t, cascade)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.ExecutionDone {
			t.Fatalf("CommitNode(skipped beta) = %+v, want the cascade accepted and the execution finalized", res)
		}
	})

	t.Run("canceled commit is refused for a unit a seed resolved", func(t *testing.T) {
		seeder, ok := OptionalCapability[engine.ExecutionSeeder](t, manifest, CapabilityExecutionSeeder, h.state)
		if !ok {
			return
		}
		id := types.ExecutionID("merge-cancel-refuse-seed")
		unit := engine.SeedUnit{
			UnitIdx: h.unit(t, "beta"), NodeName: "beta", NodeIdx: h.nodeIdx(t, "beta"),
			Status: types.NodeStatusSuccess, Port: "main", Output: map[string]any{"seeded": true},
		}
		if _, err := seeder.SeedExecution(context.Background(), engine.SeedExecutionRequest{
			Execution: &engine.ExecutionSnapshot{ID: id, Graph: h.g, Status: types.ExecutionStatusRunning},
			Units:     []engine.SeedUnit{unit},
		}); err != nil {
			t.Fatalf("SeedExecution() error = %v", err)
		}
		// A seeded unit is resolved: its row is terminal and its marker is "seed".
		// The refusal comes from whichever of the two the backend reads first,
		// and both must agree.
		mcRefuse(t, h, id, "beta", engine.CommitOutcomeStaleToken)
	})

	t.Run("canceled commit is refused for a waiting node", func(t *testing.T) {
		expander, ok := h.state.(engine.LeaseExpander)
		if !ok {
			t.Fatalf("%T does not implement engine.LeaseExpander", h.state)
		}
		id := mcExec(t, h, "refuse-waiting")
		lease := h.lease(t, id, "alpha")
		mcClaim(t, h, lease)
		if started, err := expander.BeginTaskExpansion(context.Background(), lease); err != nil || !started {
			t.Fatalf("BeginTaskExpansion(alpha) started=%v err=%v, want a waiting parent", started, err)
		}
		mcRefuse(t, h, id, "alpha", engine.CommitOutcomeStaleToken)

		// The parent still commits under its own lease, and that commit is the
		// one that decrements: a second decrement would finalize it here.
		res := h.commit(t, h.request(lease))
		if res.Outcome != engine.CommitOutcomeAccepted || res.ExecutionDone {
			t.Fatalf("CommitNode(waiting alpha) = %+v, want an accepted commit that leaves beta outstanding", res)
		}
	})

	t.Run("canceled commit is refused for a terminal node", func(t *testing.T) {
		id := mcExec(t, h, "refuse-terminal")
		h.commit(t, h.request(h.lease(t, id, "alpha")))
		mcRefuse(t, h, id, "alpha", engine.CommitOutcomeStaleToken)
	})
}

// mcRemainingCounter pins the completion counter across the fourth shape:
// the cancel retires its unit exactly once, a Canceled unit is not a failure,
// and a repeated cancel is a duplicate that counts nothing.
func mcRemainingCounter(t *testing.T, h *arrivalFoldingHarness) {
	t.Run("canceled commit decrements the remaining counter exactly once", func(t *testing.T) {
		id := mcExec(t, h, "counter-once")
		h.lease(t, id, "alpha")
		res := h.commit(t, mcCancel(t, h, id, "alpha"))
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(canceled alpha) = %+v, want an applied accepted commit", res)
		}
		if res.ExecutionDone {
			t.Fatalf("CommitNode(canceled alpha) finalized the execution (%+v) while beta is "+
				"outstanding: the remaining counter was decremented more than once", res)
		}

		// A repeat of the same cancel (an intent redelivered after a crash) is a
		// duplicate of the terminal row it wrote, and counts nothing.
		before := mcFreeze(t, h, id, "alpha")
		dup := h.commit(t, mcCancel(t, h, id, "alpha"))
		if dup.Outcome != engine.CommitOutcomeDuplicateTerminal || dup.Applied || dup.ExecutionDone {
			t.Fatalf("CommitNode(canceled alpha again) = %+v, want a non-applied duplicate-terminal verdict", dup)
		}
		if after := mcFreeze(t, h, id, "alpha"); after != before {
			t.Fatalf("a duplicate cancel changed state:\n before %s\n after  %s", before, after)
		}

		// Beta is the one outstanding unit. It finalizes the execution, and as a
		// success: a Canceled sibling is not a failed one.
		res = h.commit(t, h.request(h.lease(t, id, "beta")))
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("CommitNode(beta) = %+v, want the execution finalized as success: a "+
				"canceled node must not count as failed", res)
		}
	})

	t.Run("canceling the last outstanding unit finalizes the execution", func(t *testing.T) {
		id := mcExec(t, h, "counter-last")
		mcPlant(t, h, id, "alpha", "beta", 1)
		res := h.commit(t, mcCancel(t, h, id, "beta"))
		if res.Outcome != engine.CommitOutcomeAccepted || !res.ExecutionDone ||
			res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("CommitNode(canceled beta) = %+v, want the execution finalized as success", res)
		}
		if got := mcStatus(t, h, id); got != types.ExecutionStatusSuccess {
			t.Fatalf("execution status = %q, want success", got)
		}
	})
}

// mcSkipShape reports what a commit of the given status wrote for tail and alt
// (the task shape, not the per-execution ID) and what it resolved as skipped,
// so a canceled commit can be compared with the skipped commit of the same shape.
func mcSkipShape(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID, res engine.CommitNodeResult) string {
	t.Helper()
	var intents []string
	for _, node := range []string{"tail", "alt"} {
		for _, entry := range h.outbox(t, id) {
			if entry.Task.NodeName == node {
				intents = append(intents, fmt.Sprintf("%s:type=%d,node=%d,unit=%d",
					node, int(entry.Task.Type), entry.Task.NodeIdx, entry.Task.UnitIdx))
			}
		}
	}
	sort.Strings(intents)
	skipped := make([]string, 0, len(res.Skipped))
	for _, unit := range res.Skipped {
		skipped = append(skipped, fmt.Sprintf("%s*%d", unit.NodeName, unit.Count))
	}
	sort.Strings(skipped)
	return fmt.Sprintf("skipped=%v intents=%v", skipped, intents)
}

// mcFoldedArrivals pins that the canceled shape shares the terminal tail of the
// other shapes: its folded arrivals schedule downstream exactly as the skipped
// commit of a skip-marked unit does.
func mcFoldedArrivals(t *testing.T, h *arrivalFoldingHarness) {
	t.Run("canceled commit folds its arrivals like a skipped commit", func(t *testing.T) {
		arrivals := func() []engine.DownstreamArrival {
			return []engine.DownstreamArrival{h.arrival(t, "tail", 1, 0), h.arrival(t, "alt", 1, 0)}
		}

		idCancel := mcExec(t, h, "fold-canceled")
		mcPlant(t, h, idCancel, "up", "loser", 1)
		req := mcCancel(t, h, idCancel, "loser")
		req.Arrivals = arrivals()
		canceled := h.commit(t, req)
		if canceled.Outcome != engine.CommitOutcomeAccepted || !canceled.Applied {
			t.Fatalf("CommitNode(canceled loser) = %+v, want an applied accepted commit", canceled)
		}

		idSkip := mcExec(t, h, "fold-skipped")
		mcPlant(t, h, idSkip, "up", "loser", 0)
		ref := mcCancel(t, h, idSkip, "loser")
		ref.Status = types.NodeStatusSkipped
		ref.Error = ""
		ref.Arrivals = arrivals()
		skipped := h.commit(t, ref)
		if skipped.Outcome != engine.CommitOutcomeAccepted || !skipped.Applied {
			t.Fatalf("CommitNode(skipped loser) = %+v, want an applied accepted commit", skipped)
		}

		got, want := mcSkipShape(t, h, idCancel, canceled), mcSkipShape(t, h, idSkip, skipped)
		if got != want {
			t.Fatalf("canceled commit scheduled %s, want the skipped commit's %s", got, want)
		}
		if !strings.Contains(got, "tail:type=") || !strings.Contains(got, "alt:type=") {
			t.Fatalf("canceled commit scheduled %s, want a skip intent for both successors", got)
		}
	})
}

// mcResumeEntries lists the resume intents in the outbox.
func mcResumeEntries(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID) []engine.OutboxEntry {
	t.Helper()
	return h.intents(t, id, "alpha", engine.TaskTypeNodeResume)
}

// mcLateSignal pins the Suspended cleanup: after the cancel commit the node
// has no waiter, so a signal that arrives late cannot wake it. A leftover
// waiter would write a resume intent for a node that is already terminal.
func mcLateSignal(t *testing.T, h *arrivalFoldingHarness, manifest *Manifest) {
	t.Run("canceled node ignores late signal", func(t *testing.T) {
		ctx := context.Background()
		id := mcExec(t, h, "late-signal")
		spec := &types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"approve"}}
		mcSuspend(t, h, id, "alpha", spec)

		waiting, err := h.state.ListSuspendedNodes(ctx, id)
		if err != nil || !containsString(waiting, "alpha") {
			t.Fatalf("ListSuspendedNodes() = %v, %v; want alpha parked before the cancel", waiting, err)
		}
		if res := h.commit(t, mcCancel(t, h, id, "alpha")); res.Outcome != engine.CommitOutcomeAccepted {
			t.Fatalf("CommitNode(canceled alpha) = %+v, want an accepted commit", res)
		}

		waiting, err = h.state.ListSuspendedNodes(ctx, id)
		if err != nil || containsString(waiting, "alpha") {
			t.Fatalf("ListSuspendedNodes() = %v, %v; want alpha gone once it is canceled", waiting, err)
		}
		node, payload, err := h.state.DeliverSignal(ctx, id, "approve", map[string]any{"by": "late"})
		if err != nil || node != "" || payload != nil {
			t.Fatalf("DeliverSignal() = %q, %+v, %v; want no resumed node: the waiter was "+
				"retired with the cancel", node, payload, err)
		}
		if manifest.Declares(CapabilityDurableSignalDeliverer) {
			durable := RequiredCapability[engine.DurableSignalDeliverer](t, manifest, CapabilityDurableSignalDeliverer, h.state)
			if target, err := durable.PeekResumeTarget(ctx, id, "approve"); err != nil || target != "" {
				t.Fatalf("PeekResumeTarget() = %q, %v; want no waiter for a canceled node", target, err)
			}
			resumed, _, committed, err := durable.DeliverSignalWithOutbox(ctx, id, "approve",
				map[string]any{"by": "late"}, engine.ResumeIntent{
					NodeName: "alpha", NodeIdx: h.nodeIdx(t, "alpha"), UnitIdx: h.unit(t, "alpha"), ActivationID: 1,
				})
			if err != nil || resumed != "" || committed {
				t.Fatalf("DeliverSignalWithOutbox() = %q committed=%v err=%v; want the signal "+
					"stored with no resume committed", resumed, committed, err)
			}
		}
		if entries := mcResumeEntries(t, h, id); len(entries) != 0 {
			t.Fatalf("a late signal wrote %d resume intents for a canceled node: %+v", len(entries), entries)
		}
		mcRequireCanceled(t, h, id, "alpha")
	})
}

// mcLeaseRetired pins the Running case: cancelling a node under a live handler
// retires its lease, so the handler is told to stop on its next renewal and
// its own late commit cannot overwrite the cancel.
func mcLeaseRetired(t *testing.T, h *arrivalFoldingHarness, manifest *Manifest) {
	t.Run("canceled running node loses its lease", func(t *testing.T) {
		ctx := context.Background()
		renewer := RequiredCapability[engine.NodeLeaseRenewer](t, manifest, CapabilityNodeLeaseRenewer, h.state)
		id := mcExec(t, h, "lease")
		lease := h.lease(t, id, "alpha")
		deadline := time.Now().Add(time.Minute)
		if renewed, err := renewer.RenewTaskLease(ctx, id, "alpha", lease.LeaseToken, deadline); err != nil || !renewed {
			t.Fatalf("RenewTaskLease() = %v, %v before the cancel; want the live lease renewed", renewed, err)
		}
		if res := h.commit(t, mcCancel(t, h, id, "alpha")); res.Outcome != engine.CommitOutcomeAccepted {
			t.Fatalf("CommitNode(canceled alpha) = %+v, want an accepted commit", res)
		}
		if renewed, err := renewer.RenewTaskLease(ctx, id, "alpha", lease.LeaseToken, deadline); err != nil || renewed {
			t.Fatalf("RenewTaskLease() = %v, %v after the cancel; want false so the handler stops", renewed, err)
		}
		expired, err := h.state.ListExpiredLeases(ctx, time.Now().Add(time.Hour))
		if err != nil {
			t.Fatalf("ListExpiredLeases() error = %v", err)
		}
		for _, listed := range expired {
			if listed.ExecutionID == id && listed.NodeName == "alpha" {
				t.Fatalf("ListExpiredLeases() still lists the canceled node: %+v", listed)
			}
		}

		late := h.commit(t, h.request(lease))
		if late.Outcome != engine.CommitOutcomeStaleToken || late.Applied {
			t.Fatalf("late handler CommitNode() = %+v, want a non-applied stale-token refusal", late)
		}
		mcRequireCanceled(t, h, id, "alpha")
	})
}

// mcIntents lists the merge-cancel intents in the durable outbox.
func mcIntents(t *testing.T, h *arrivalFoldingHarness, id types.ExecutionID) []engine.OutboxEntry {
	t.Helper()
	var matching []engine.OutboxEntry
	for _, entry := range h.outbox(t, id) {
		if entry.Task.Type == engine.TaskTypeMergeCancel {
			matching = append(matching, entry)
		}
	}
	return matching
}

// mcMergeCommit is the merge node's successful commit carrying the intent, as
// the engine builds it: alpha stands in for the merge, beta for its successor.
func mcMergeCommit(t *testing.T, h *arrivalFoldingHarness, lease *engine.TaskLease) engine.CommitNodeRequest {
	t.Helper()
	req := h.request(lease)
	req.MergeCancel = &engine.Task{
		ExecutionID:  lease.Task.ExecutionID,
		NodeName:     lease.Task.NodeName,
		NodeIdx:      lease.Task.NodeIdx,
		UnitIdx:      lease.Task.UnitIdx,
		Type:         engine.TaskTypeMergeCancel,
		ActivationID: lease.Task.ActivationID,
	}
	return req
}

// mcIntent pins the write side of the capability: the intent is persisted by
// the same transition that accepts the merge's terminal commit, exactly once,
// and only when there is still an execution to cancel branches in.
func mcIntent(t *testing.T, h *arrivalFoldingHarness) {
	t.Run("an accepted merge commit writes exactly one merge-cancel intent", func(t *testing.T) {
		id := mcExec(t, h, "intent-written")
		lease := h.lease(t, id, "alpha")
		req := mcMergeCommit(t, h, lease)
		// MergeCancel rides alongside Arrivals: it is not a scheduling carrier.
		req.Arrivals = []engine.DownstreamArrival{h.arrival(t, "beta", 1, 1)}
		res := h.commit(t, req)
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied || res.ExecutionDone {
			t.Fatalf("CommitNode(merge) = %+v, want an applied accepted commit that leaves beta outstanding", res)
		}
		intent := mcIntents(t, h, id)
		if len(intent) != 1 {
			t.Fatalf("outbox has %d merge-cancel intents, want exactly 1: %+v", len(intent), intent)
		}
		if !strings.HasPrefix(intent[0].ID, "mergecancel/") {
			t.Errorf("merge-cancel intent ID = %q, want the mergecancel/ prefix: the dead-letter "+
				"replay derives the intent name from it", intent[0].ID)
		}
		if intent[0].Task.NodeName != "alpha" {
			t.Errorf("merge-cancel intent names node %q, want the merge node alpha", intent[0].Task.NodeName)
		}
		if !containsString(res.OutboxIDs, intent[0].ID) {
			t.Errorf("OutboxIDs = %v, want the written intent %q among them", res.OutboxIDs, intent[0].ID)
		}
		h.onlyIntent(t, id, "beta", engine.TaskTypeNodeExec)

		// A repeated commit is a duplicate terminal and writes nothing, even
		// once the first intent has been delivered and acknowledged.
		if err := h.atomic.AckOutbox(context.Background(), id, intent[0].ID); err != nil {
			t.Fatalf("AckOutbox(%q) error = %v", intent[0].ID, err)
		}
		dup := h.commit(t, req)
		if dup.Outcome != engine.CommitOutcomeDuplicateTerminal || dup.Applied {
			t.Fatalf("CommitNode(merge again) = %+v, want a non-applied duplicate-terminal verdict", dup)
		}
		if again := mcIntents(t, h, id); len(again) != 0 {
			t.Fatalf("a duplicate merge commit wrote %d merge-cancel intents: %+v", len(again), again)
		}
	})

	t.Run("a merge commit that finalizes the execution writes no merge-cancel intent", func(t *testing.T) {
		id := mcExec(t, h, "intent-done")
		h.commit(t, h.request(h.lease(t, id, "beta")))
		res := h.commit(t, mcMergeCommit(t, h, h.lease(t, id, "alpha")))
		if !res.ExecutionDone || res.ExecutionStatus != types.ExecutionStatusSuccess {
			t.Fatalf("CommitNode(merge) = %+v, want the execution finalized as success", res)
		}
		if got := mcIntents(t, h, id); len(got) != 0 {
			t.Fatalf("a finalizing merge commit wrote %d merge-cancel intents: %+v; no branch is left "+
				"to cancel", len(got), got)
		}
	})

	t.Run("a merge commit under canceling writes no merge-cancel intent", func(t *testing.T) {
		id := mcExec(t, h, "intent-canceling")
		lease := h.lease(t, id, "alpha")
		if err := h.state.UpdateExecutionStatus(context.Background(), id, types.ExecutionStatusCanceling, ""); err != nil {
			t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
		}
		res := h.commit(t, mcMergeCommit(t, h, lease))
		if res.Outcome != engine.CommitOutcomeAccepted || !res.Applied {
			t.Fatalf("CommitNode(merge under canceling) = %+v, want the in-flight commit accepted", res)
		}
		if got := mcIntents(t, h, id); len(got) != 0 {
			t.Fatalf("a merge commit under canceling wrote %d merge-cancel intents: %+v; the "+
				"execution-wide cancel retires the suspended branches itself", len(got), got)
		}
	})

	t.Run("a fatal merge commit writes no merge-cancel intent", func(t *testing.T) {
		id := mcExec(t, h, "intent-fatal")
		req := mcMergeCommit(t, h, h.lease(t, id, "alpha"))
		req.Fatal = true
		// Validate rejects the combination before any state is touched; a backend
		// that accepted it anyway must still not have written the intent.
		_, _ = h.atomic.CommitNode(context.Background(), req)
		if got := mcIntents(t, h, id); len(got) != 0 {
			t.Fatalf("a fatal merge commit wrote %d merge-cancel intents: %+v", len(got), got)
		}
	})
}

// mcRace pins the single-winner fence: a node's own commit and the merge's
// cancel commit may land together, and exactly one of them takes the unit. The
// loser is told so (StaleToken or DuplicateTerminal), writes nothing, and the
// remaining counter is decremented once.
func mcRace(t *testing.T, h *arrivalFoldingHarness) {
	t.Run("concurrent self commit and cancel commit have one winner", func(t *testing.T) {
		const rounds = 25
		for round := 0; round < rounds; round++ {
			id := mcExec(t, h, fmt.Sprintf("race-%d", round))
			lease := h.lease(t, id, "alpha")
			self, cancel := h.request(lease), mcCancel(t, h, id, "alpha")

			var wg sync.WaitGroup
			start := make(chan struct{})
			results := make([]engine.CommitNodeResult, 2)
			errs := make([]error, 2)
			for i, req := range []engine.CommitNodeRequest{self, cancel} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					results[i], errs[i] = h.atomic.CommitNode(context.Background(), req)
				}()
			}
			close(start)
			wg.Wait()

			accepted := 0
			for i, res := range results {
				if errs[i] != nil {
					t.Fatalf("round %d: CommitNode(%d) error = %v", round, i, errs[i])
				}
				switch res.Outcome {
				case engine.CommitOutcomeAccepted:
					accepted++
					if !res.Applied || res.ExecutionDone {
						t.Fatalf("round %d: accepted commit %d = %+v, want applied with beta outstanding", round, i, res)
					}
				case engine.CommitOutcomeStaleToken, engine.CommitOutcomeDuplicateTerminal:
					if res.Applied {
						t.Fatalf("round %d: refused commit %d = %+v, want not applied", round, i, res)
					}
				default:
					t.Fatalf("round %d: commit %d outcome = %v, want accepted, stale or duplicate", round, i, res.Outcome)
				}
			}
			if accepted != 1 {
				t.Fatalf("round %d: %d commits were accepted (%+v), want exactly 1", round, accepted, results)
			}
			// One decrement: beta's commit is the one that finalizes the execution.
			final := h.commit(t, h.request(h.lease(t, id, "beta")))
			if !final.ExecutionDone || final.ExecutionStatus != types.ExecutionStatusSuccess {
				t.Fatalf("round %d: CommitNode(beta) = %+v, want the execution finalized as success", round, final)
			}
		}
	})
}
