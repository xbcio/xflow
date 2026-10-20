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

// This file pins the CommitNode half of the readiness-index contract: the
// registration is written only for a commit that carries outbox intent.
//
// state_outbox_index.go states the contract as "called after every atomic
// transition that APPENDS an outbox entry". After the S4b fold a commit appends
// an entry only when it schedules something downstream — an advance intent, a
// cyclic intent, or a folded execute/skip arrival — while a commit that
// finalizes the execution, a fatal commit, and a leaf commit with no outgoing
// edge write nothing inside commitNodeLua. Registering those was a ZADD that
// could never name work; the gate is CommitNodeResult.OutboxIDs, the candidate
// hint that is populated exactly for the transitions that can append.

// outboxIndexZAdds counts ZADDs addressed to one namespace's readiness index,
// issued on its own or inside a pipeline. The key identifies the write:
// matching on the command name alone would also count the execution's own ready
// ZSET, whose commands carry the same name.
func outboxIndexZAdds(hook *commandCapture, ns namespace.Namespace) int {
	want := "zadd " + outboxReadyIndexKey(ns)
	pipelines, singles := hook.snapshot()
	count := 0
	for _, command := range singles {
		if command == want {
			count++
		}
	}
	for _, batch := range pipelines {
		for _, command := range batch {
			if command == want {
				count++
			}
		}
	}
	return count
}

// commitIndexFixture is one running execution with a command capture attached
// to the store's client, so a test can count the index writes a single commit
// issues rather than inferring them from the index's eventual contents (a
// re-registration and no registration are indistinguishable there once a member
// already exists).
type commitIndexFixture struct {
	state *Store
	hook  *commandCapture
	ctx   context.Context
	id    types.ExecutionID
	g     *graph.Graph
}

func newCommitIndexFixture(t *testing.T, id types.ExecutionID, g *graph.Graph) *commitIndexFixture {
	t.Helper()
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  g,
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(%q) error = %v", id, err)
	}
	hook := &commandCapture{}
	rdb.AddHook(hook)
	return &commitIndexFixture{state: state, hook: hook, ctx: ctx, id: id, g: g}
}

// lease claims a first-generation lease for node and reads back the attempt the
// store assigned, which is what CommitNode fences on.
func (f *commitIndexFixture) lease(t *testing.T, node string) *engine.TaskLease {
	t.Helper()
	idx := mustNodeIdx(t, f.g, node)
	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease/" + string(f.id) + "/" + node),
		LeaseToken: engine.LeaseToken("token/" + string(f.id) + "/" + node),
		Task: engine.Task{
			ExecutionID: f.id,
			NodeName:    node,
			NodeIdx:     idx,
			UnitIdx:     idx,
			Type:        engine.TaskTypeNodeExec,
		},
	}
	if _, acquired, err := f.state.AcquireTaskLease(f.ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(%q/%q) acquired=%v err=%v", f.id, node, acquired, err)
	}
	snap, err := f.state.GetNode(f.ctx, f.id, node)
	if err != nil || snap == nil {
		t.Fatalf("GetNode(%q/%q) = %+v, %v; want the acquired node", f.id, node, snap, err)
	}
	lease.Attempt = snap.Attempt
	return lease
}

func (f *commitIndexFixture) request(lease *engine.TaskLease) engine.CommitNodeRequest {
	return engine.CommitNodeRequest{
		ExecutionID: lease.Task.ExecutionID,
		NodeName:    lease.Task.NodeName,
		NodeIdx:     lease.Task.NodeIdx,
		UnitIdx:     lease.Task.UnitIdx,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     lease.Attempt,
		Status:      types.NodeStatusSuccess,
	}
}

func (f *commitIndexFixture) commit(t *testing.T, req engine.CommitNodeRequest) engine.CommitNodeResult {
	t.Helper()
	res, err := f.state.CommitNode(f.ctx, req)
	if err != nil {
		t.Fatalf("CommitNode(%q/%q) error = %v", f.id, req.NodeName, err)
	}
	return res
}

func (f *commitIndexFixture) outboxEntries(t *testing.T) []engine.OutboxEntry {
	t.Helper()
	entries, err := f.state.ListOutbox(f.ctx, f.id, time.Now().Add(time.Minute), 64)
	if err != nil {
		t.Fatalf("ListOutbox(%q) error = %v", f.id, err)
	}
	return entries
}

// compileForkGraph builds start -> {left, right}. It gives the no-intent gate a
// leaf commit that does NOT finalize the execution, which separates "nothing to
// schedule" from "the execution is done": the gate must skip both, and a test
// that only covered the finalizing leaf could not tell the two apart.
func compileForkGraph(tb testing.TB) *graph.Graph {
	tb.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "rstate-fork",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.echo"},
			{Name: "left", Type: "test.echo"},
			{Name: "right", Type: "test.echo"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{
				{Node: "left", Input: "main"},
				{Node: "right", Input: "main"},
			}}},
		},
	})
	if err != nil {
		tb.Fatalf("Compile() error = %v", err)
	}
	return g
}

// TestCommitNodeReadyIndexSkipsCommitsWithoutOutboxIntent pins the gate on the
// commits that append nothing. Each of them used to end in an unconditional
// ZADD; after the gate the command stream carries none.
func TestCommitNodeReadyIndexSkipsCommitsWithoutOutboxIntent(t *testing.T) {
	t.Run("leaf commit that finalizes the execution", func(t *testing.T) {
		f := newCommitIndexFixture(t, "commit-index-leaf-done", testGraphOneNode())
		lease := f.lease(t, "start")

		res := f.commit(t, f.request(lease))
		if !res.Applied || !res.ExecutionDone {
			t.Fatalf("CommitNode(leaf) = %+v, want an applied commit that finalizes the execution", res)
		}
		if len(res.OutboxIDs) != 0 {
			t.Fatalf("OutboxIDs = %v, want none: this leaf has no outgoing edge and the commit "+
				"finalized the execution, so nothing can have been appended", res.OutboxIDs)
		}
		if entries := f.outboxEntries(t); len(entries) != 0 {
			t.Fatalf("finalizing commit scheduled %d outbox entries: %+v", len(entries), entries)
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 0 {
			t.Fatalf("ready-index ZADDs = %d, want 0: a commit that appended no outbox entry must "+
				"not register the execution", got)
		}
	})

	t.Run("fatal commit", func(t *testing.T) {
		f := newCommitIndexFixture(t, "commit-index-fatal", testGraphTwoNode())
		lease := f.lease(t, "start")

		req := f.request(lease)
		req.Status = types.NodeStatusFailed
		req.Error = "permanent failure"
		req.Fatal = true
		res := f.commit(t, req)
		if !res.Applied || !res.ExecutionDone {
			t.Fatalf("CommitNode(fatal) = %+v, want an applied commit that finalizes the execution", res)
		}
		if res.ExecutionStatus != types.ExecutionStatusFailed {
			t.Fatalf("CommitNode(fatal).ExecutionStatus = %q, want failed", res.ExecutionStatus)
		}
		if len(res.OutboxIDs) != 0 {
			t.Fatalf("OutboxIDs = %v, want none: commitNodeLua suppresses every outbox write on "+
				"the fatal path", res.OutboxIDs)
		}
		if entries := f.outboxEntries(t); len(entries) != 0 {
			t.Fatalf("fatal commit scheduled %d outbox entries: %+v", len(entries), entries)
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 0 {
			t.Fatalf("ready-index ZADDs = %d, want 0: a fatal commit appends nothing and must not "+
				"register the execution", got)
		}
	})

	t.Run("leaf commit that does not finalize the execution", func(t *testing.T) {
		// A fold-capable engine sends a leaf's commit with no arrivals and no
		// advance task. Here another branch is still outstanding, so the
		// execution stays running — the gate is about the missing intent, not
		// about ExecutionDone.
		f := newCommitIndexFixture(t, "commit-index-leaf-running", compileForkGraph(t))
		lease := f.lease(t, "left")

		res := f.commit(t, f.request(lease))
		if !res.Applied || res.ExecutionDone {
			t.Fatalf("CommitNode(leaf) = %+v, want an applied commit that leaves the execution "+
				"running", res)
		}
		if len(res.OutboxIDs) != 0 {
			t.Fatalf("OutboxIDs = %v, want none: a leaf has no downstream scheduling", res.OutboxIDs)
		}
		if entries := f.outboxEntries(t); len(entries) != 0 {
			t.Fatalf("leaf commit scheduled %d outbox entries: %+v", len(entries), entries)
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 0 {
			t.Fatalf("ready-index ZADDs = %d, want 0: the commit appended nothing", got)
		}
	})
}

// TestCommitNodeReadyIndexMarksAppendingCommits is the positive control: every
// shape that does append an intent marks exactly once, so the gate above cannot
// be satisfied by a CommitNode that simply stopped marking.
func TestCommitNodeReadyIndexMarksAppendingCommits(t *testing.T) {
	nextArrival := func(t *testing.T, g *graph.Graph, activeCount int) engine.DownstreamArrival {
		t.Helper()
		idx := mustNodeIdx(t, g, "next")
		return engine.DownstreamArrival{
			NodeName:     "next",
			NodeIdx:      idx,
			UnitIdx:      idx,
			ArrivalCount: 1,
			ActiveCount:  activeCount,
			MergeMode:    "wait_all",
		}
	}

	t.Run("folded arrival resolved as execute", func(t *testing.T) {
		g := twoNodeLinearGraph(t)
		f := newCommitIndexFixture(t, "commit-index-fold-execute", g)
		lease := f.lease(t, "start")

		req := f.request(lease)
		req.Arrivals = []engine.DownstreamArrival{nextArrival(t, g, 1)}
		res := f.commit(t, req)
		if !res.Applied || res.ExecutionDone {
			t.Fatalf("CommitNode(fold execute) = %+v, want an applied non-finalizing commit", res)
		}
		if len(res.OutboxIDs) == 0 {
			t.Fatalf("OutboxIDs = none, want the arrival's candidates: the hint is what the gate " +
				"reads")
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 1 {
			t.Fatalf("ready-index ZADDs = %d, want exactly 1: the commit appended an execute intent",
				got)
		}
		entries := f.outboxEntries(t)
		if len(entries) != 1 || entries[0].Task.NodeName != "next" || entries[0].Task.Type != engine.TaskTypeNodeExec {
			t.Fatalf("outbox = %+v, want one execute intent for next", entries)
		}
	})

	t.Run("folded arrival resolved as skip", func(t *testing.T) {
		g := twoNodeLinearGraph(t)
		f := newCommitIndexFixture(t, "commit-index-fold-skip", g)
		lease := f.lease(t, "start")

		req := f.request(lease)
		req.Arrivals = []engine.DownstreamArrival{nextArrival(t, g, 0)}
		res := f.commit(t, req)
		if !res.Applied || res.ExecutionDone {
			t.Fatalf("CommitNode(fold skip) = %+v, want an applied non-finalizing commit", res)
		}
		if len(res.OutboxIDs) == 0 {
			t.Fatalf("OutboxIDs = none, want the arrival's candidates")
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 1 {
			t.Fatalf("ready-index ZADDs = %d, want exactly 1: the commit appended a skip intent", got)
		}
		entries := f.outboxEntries(t)
		if len(entries) != 1 || entries[0].Task.NodeName != "next" || entries[0].Task.Type != engine.TaskTypeNodeSkip {
			t.Fatalf("outbox = %+v, want one skip intent for next", entries)
		}
	})

	t.Run("advance intent", func(t *testing.T) {
		g := twoNodeLinearGraph(t)
		f := newCommitIndexFixture(t, "commit-index-advance", g)
		lease := f.lease(t, "start")

		req := f.request(lease)
		req.AdvanceTask = &engine.Task{
			ExecutionID: f.id,
			NodeName:    "start",
			NodeIdx:     lease.Task.NodeIdx,
			Type:        engine.TaskTypeNodeAdvance,
		}
		res := f.commit(t, req)
		if !res.Applied || res.ExecutionDone {
			t.Fatalf("CommitNode(advance) = %+v, want an applied non-finalizing commit", res)
		}
		if len(res.OutboxIDs) == 0 {
			t.Fatalf("OutboxIDs = none, want the advance intent candidate")
		}
		if got := outboxIndexZAdds(f.hook, namespace.Default); got != 1 {
			t.Fatalf("ready-index ZADDs = %d, want exactly 1: the commit appended an advance intent",
				got)
		}
		entries := f.outboxEntries(t)
		if len(entries) != 1 || entries[0].Task.Type != engine.TaskTypeNodeAdvance {
			t.Fatalf("outbox = %+v, want one advance intent", entries)
		}
	})
}

// TestCommitNodeReadyIndexMarksCandidateThatWasNotWritten records the one
// direction the gate permits: a candidate the transition did not write still
// registers.
//
// A commit under a canceling execution reports both arrival candidates (see the
// OutboxIDs contract) while commitNodeLua suppresses the fold, so the member it
// writes names an execution whose ready set stays empty. That is deliberately
// accepted: the extra mark costs one no-op claim, which re-arms or prunes the
// member, and it is the cost of never missing a registration. This test asserts
// the durable outbox (the authority) is empty and that the harmless mark
// happened, so a future change that starts suppressing candidates on this path
// is a deliberate decision rather than a silent drift.
func TestCommitNodeReadyIndexMarksCandidateThatWasNotWritten(t *testing.T) {
	g := twoNodeLinearGraph(t)
	f := newCommitIndexFixture(t, "commit-index-canceling", g)
	lease := f.lease(t, "start")
	if err := f.state.UpdateExecutionStatus(f.ctx, f.id, types.ExecutionStatusCanceling, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(canceling) error = %v", err)
	}
	f.hook.reset() // the status transition's own commands are not the commit's

	nextIdx := mustNodeIdx(t, g, "next")
	req := f.request(lease)
	req.Arrivals = []engine.DownstreamArrival{{
		NodeName:     "next",
		NodeIdx:      nextIdx,
		UnitIdx:      nextIdx,
		ArrivalCount: 1,
		ActiveCount:  1,
		MergeMode:    "wait_all",
	}}
	res := f.commit(t, req)
	if !res.Applied || res.ExecutionDone {
		t.Fatalf("CommitNode(canceling) = %+v, want the in-flight commit accepted", res)
	}
	if len(res.OutboxIDs) == 0 {
		t.Fatalf("OutboxIDs = none, want the candidates: the hint is reported even when the " +
			"canceling guard suppresses the fold")
	}
	if entries := f.outboxEntries(t); len(entries) != 0 {
		t.Fatalf("a commit under canceling scheduled %d outbox entries: %+v; no downstream work "+
			"may be scheduled while a Cancel is in flight", len(entries), entries)
	}
	if got := outboxIndexZAdds(f.hook, namespace.Default); got != 1 {
		t.Fatalf("ready-index ZADDs = %d, want 1: the candidate hint is conservative, so this "+
			"commit is allowed (not required) to register; a missing registration here is the "+
			"direction that would lose work", got)
	}
}

// TestCommitNodeWithoutIntentKeepsPendingBacklogDiscoverable is the correctness
// half of the gate: a commit that does not mark must not make work that was
// already pending invisible.
//
// It seeds a ready outbox entry the way the production create path does — which
// also registers the execution — and then runs a finalizing leaf commit, the
// exact transition that no longer marks. The backlog must stay discoverable
// through the index (the member the seed wrote is untouched), through the
// keyspace sweep when the index is switched off (the accelerator is not the
// authority), and through ListOutbox/LeaseOutbox (discovery is not delivery).
//
// This is what separates "the gate saves a wasted ZADD" from "the gate hides a
// backlog": ListOutboxExecutions is the API OutboxDispatcher discovers with, and
// it still returns the execution after the unmarked commit.
func TestCommitNodeWithoutIntentKeepsPendingBacklogDiscoverable(t *testing.T) {
	state, _, rdb := newTestRedisState(t)
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)
	const id = types.ExecutionID("commit-index-backlog")
	entryID := "root/" + string(id) + "/start/0"

	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID:     id,
		Graph:  testGraphOneNode(),
		Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{{
		ID:          entryID,
		Task:        engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
		AvailableAt: time.Now().Add(-time.Second),
	}}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox(%q) error = %v", id, err)
	}
	if _, present := indexScore(t, ctx, rdb, namespace.Default, id); !present {
		t.Fatal("the seeded create did not register the execution in the readiness index; the " +
			"backlog below would not be discoverable to begin with")
	}
	hook := &commandCapture{}
	rdb.AddHook(hook)

	lease := &engine.TaskLease{
		LeaseID:    engine.LeaseID("lease/" + string(id) + "/start"),
		LeaseToken: engine.LeaseToken("token/" + string(id) + "/start"),
		Task:       engine.Task{ExecutionID: id, NodeName: "start", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(%q/start) acquired=%v err=%v", id, acquired, err)
	}
	snap, err := state.GetNode(ctx, id, "start")
	if err != nil || snap == nil {
		t.Fatalf("GetNode(%q/start) = %+v, %v", id, snap, err)
	}
	res, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID: id,
		NodeName:    "start",
		NodeIdx:     0,
		UnitIdx:     0,
		LeaseID:     lease.LeaseID,
		LeaseToken:  lease.LeaseToken,
		Attempt:     snap.Attempt,
		Status:      types.NodeStatusSuccess,
	})
	if err != nil {
		t.Fatalf("CommitNode(%q/start) error = %v", id, err)
	}
	if !res.Applied || !res.ExecutionDone {
		t.Fatalf("CommitNode(%q/start) = %+v, want the finalizing leaf commit the gate is about", id, res)
	}
	if len(res.OutboxIDs) != 0 {
		t.Fatalf("OutboxIDs = %v, want none for this finalizing leaf commit", res.OutboxIDs)
	}
	if got := outboxIndexZAdds(hook, namespace.Default); got != 0 {
		t.Fatalf("ready-index ZADDs = %d, want 0: the unmarked commit is what the visibility "+
			"assertions below are about", got)
	}

	// The seed's member is what the index still answers with: the unmarked
	// commit neither removed it nor needs to re-register it.
	if _, present := indexScore(t, ctx, rdb, namespace.Default, id); !present {
		t.Fatal("the unmarked commit removed the execution from the readiness index, so a pending " +
			"backlog would wait for a sweep to be rediscovered")
	}
	discovered, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions() error = %v", err)
	}
	if !containsExecution(discovered, id) {
		t.Fatalf("discovered %v, want %q: a commit that appended nothing must not hide the work an "+
			"earlier transition registered", discovered, id)
	}

	// The index is an accelerator, not the authority: with it switched off the
	// same backlog is found by the keyspace sweep, which is the recovery path
	// for any registration that is ever missed.
	state.ConfigureOutboxReadyIndex(false)
	t.Cleanup(func() { state.ConfigureOutboxReadyIndex(true) })
	swept, err := state.ListOutboxExecutions(ctx, 16)
	if err != nil {
		t.Fatalf("ListOutboxExecutions(index off) error = %v", err)
	}
	if !containsExecution(swept, id) {
		t.Fatalf("keyspace sweep discovered %v, want %q: with the index off the pending backlog "+
			"must still be found", swept, id)
	}

	entries, err := state.ListOutbox(ctx, id, time.Now().Add(time.Second), 8)
	if err != nil {
		t.Fatalf("ListOutbox(%q) error = %v", id, err)
	}
	if len(entries) != 1 || entries[0].ID != entryID {
		t.Fatalf("ListOutbox(%q) = %+v, want the pending entry %q", id, entries, entryID)
	}
	leased, err := state.LeaseOutbox(ctx, id, time.Now().UTC(), 8)
	if err != nil {
		t.Fatalf("LeaseOutbox(%q) error = %v", id, err)
	}
	if len(leased) != 1 || leased[0].ID != entryID {
		t.Fatalf("LeaseOutbox(%q) = %+v, want the pending entry %q to remain claimable", id, leased, entryID)
	}
}
