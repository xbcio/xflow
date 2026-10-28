package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// EntryAdmissionTestStore is a backend implementing both state and entry
// admission capabilities.
//
// AtomicStateStore is part of the required surface, not an accident of the two
// in-tree backends: the contract reads the outbox and drives a CommitNode to
// prove an admission schedules (and only schedules) what it should. Those
// calls used to be unchecked type assertions, which made a backend without the
// atomic interface panic instead of failing with a readable reason.
type EntryAdmissionTestStore interface {
	engine.StateStore
	engine.AtomicStateStore
	engine.EntryAdmissionStore
}

// triggerGroupOnlyGraph: group "tg" with members [entry, body], no external
// nodes => UnitCount=1. Trigger group is the only unit; admission should
// complete the execution.
func triggerGroupOnlyGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "tg-only",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}}},
		Groups:      []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("compile triggerGroupOnlyGraph: %v", err)
	}
	return g
}

// triggerGroupWithDownstreamGraph: group "tg" with members [entry, body], plus
// external downstream "store" (body->store) => UnitCount=2. Admission should
// schedule downstream but NOT complete the execution.
func triggerGroupWithDownstreamGraph(t *testing.T) *graph.Graph {
	t.Helper()
	return triggerGroupWithDownstreamGraphSettings(t, nil)
}

// triggerGroupWithDownstreamGraphSettings is the same graph under the given
// workflow settings, so a case can give it a settings.timeout.
func triggerGroupWithDownstreamGraphSettings(t *testing.T, settings *types.WorkflowSettings) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "tg-downstream",
		Settings: settings,
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
			{Name: "store", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}},
			"body":  {"main": {Targets: []types.Connection{{Node: "store", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("compile triggerGroupWithDownstreamGraph: %v", err)
	}
	return g
}

// triggerGroupWithSiblingRootGraph: trigger group "tg" [entry, body] plus a
// second, ungrouped trigger root "hook" -> "hook_end". UnitCount=3. Seeding tg
// must resolve hook as skipped.
func triggerGroupWithSiblingRootGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "tg-sibling-root",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
			{Name: "hook", Kind: types.NodeKindTrigger},
			{Name: "hook_end", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "body", Input: "main"}}}},
			"hook":  {"main": {Targets: []types.Connection{{Node: "hook_end", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("compile triggerGroupWithSiblingRootGraph: %v", err)
	}
	return g
}

func buildAdmissionRequest(t *testing.T, g *graph.Graph, key engine.AdmissionKey, outcome engine.GroupOutcome, exits []engine.BoundaryExit, downstream []engine.DownstreamArrival) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	groups := g.Groups()
	if len(groups) == 0 {
		t.Fatal("graph has no groups")
	}
	gm := groups[0]
	hash := engine.ComputeResultHash(outcome, exits)
	return engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    key,
		Namespace:       namespace.Default,
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		EntryUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         outcome,
		Exits:           exits,
		ResultHash:      hash,
		Downstream:      downstream,
	}
}

// RunEntryAdmissionContract exercises the EntryAdmissionStore contract
// against a concrete backend. Both local and distributed implementations must
// pass identically.
func RunEntryAdmissionContract(t *testing.T, newStore func(*testing.T) EntryAdmissionTestStore) {
	ctx := context.Background()

	t.Run("HappyPath_AbsentToAccepted", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req := buildAdmissionRequest(t, g, "k1", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}},
			nil)
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("SeedExecutionFromEntry: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}
		if resp.Duplicate {
			t.Fatal("first admission must not be duplicate")
		}
		if resp.ExecutionID == "" {
			t.Fatal("execution ID must be non-empty")
		}
		// Verify deterministic ID.
		expected := engine.DeterministicExecutionID("k1")
		if resp.ExecutionID != expected {
			t.Fatalf("execution ID = %q, want deterministic %q", resp.ExecutionID, expected)
		}
		// The admission creates the execution inside its seed transition; it
		// must stamp the clock there too, or a trigger-created execution reads
		// back from Inspect with no start line at all.
		seeded, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil || seeded == nil {
			t.Fatalf("GetExecution() after admission = (%v, %v), want a snapshot", seeded, err)
		}
		if seeded.CreatedAt.IsZero() || seeded.UpdatedAt.IsZero() {
			t.Fatalf("admission-created execution timing = (%v, %v), want both non-zero",
				seeded.CreatedAt, seeded.UpdatedAt)
		}
	})

	t.Run("RecordsTheAdmissionNamespace", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		// The admission's namespace is the registration namespace; dispatch runs
		// under a queue ctx that names the deployment instead. A backend that
		// does not persist the admission's value sends a non-default-namespace
		// execution's sub-workflow lookups back to the ctx fallback, which misses.
		nsCtx := namespace.WithNamespace(ctx, namespace.Namespace("tenant-x"))
		req := buildAdmissionRequest(t, g, "k-namespace", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}},
			nil)
		req.Namespace = "tenant-x"
		resp, err := s.SeedExecutionFromEntry(nsCtx, req)
		if err != nil {
			t.Fatalf("SeedExecutionFromEntry: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}
		snap, err := s.GetExecution(nsCtx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution(%q): %v", resp.ExecutionID, err)
		}
		if snap == nil {
			t.Fatalf("GetExecution(%q) = nil, want the seeded execution", resp.ExecutionID)
		}
		if snap.Namespace != "tenant-x" {
			t.Fatalf("Namespace = %q, want %q", snap.Namespace, "tenant-x")
		}
	})

	t.Run("DuplicateAccepted_SameKeyAndHash", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req := buildAdmissionRequest(t, g, "k2", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 2}}},
			nil)
		r1, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		r2, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if r2.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted (duplicate)", r2.State)
		}
		if !r2.Duplicate {
			t.Fatal("second admission must be duplicate")
		}
		if r2.ExecutionID != r1.ExecutionID {
			t.Fatalf("duplicate must return same execution ID: got %q vs %q", r2.ExecutionID, r1.ExecutionID)
		}
	})

	t.Run("Conflict_SameKeyDifferentHash", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req1 := buildAdmissionRequest(t, g, "k3", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}},
			nil)
		_, err := s.SeedExecutionFromEntry(ctx, req1)
		if err != nil {
			t.Fatalf("first: %v", err)
		}
		// Same key but different exits => different hash.
		req2 := buildAdmissionRequest(t, g, "k3", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 99}}},
			nil)
		r2, err := s.SeedExecutionFromEntry(ctx, req2)
		if err != nil {
			t.Fatalf("second: %v", err)
		}
		if r2.State != engine.AdmissionStateConflict {
			t.Fatalf("state = %q, want conflict", r2.State)
		}
		if r2.Duplicate {
			t.Fatal("conflict must not be flagged as duplicate")
		}
	})

	t.Run("AcceptedCreatesExecution", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req := buildAdmissionRequest(t, g, "k4", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"v": true}}},
			nil)
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		snap, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if snap == nil {
			t.Fatal("execution must exist after admission")
		}
		// The admission request names the workflow the entry unit belongs to.
		// The created execution must carry it: Inspect/GET /v1/executions/{id}
		// reads the identity off the snapshot, so dropping it here leaves every
		// trigger-created execution unattributed while its SQL row is not.
		if snap.WorkflowID != req.WorkflowID || snap.WorkflowVersion != req.WorkflowVersion {
			t.Fatalf("execution identity = (%q, %q), want the admitted request's (%q, %q)",
				snap.WorkflowID, snap.WorkflowVersion, req.WorkflowID, req.WorkflowVersion)
		}
	})

	t.Run("AcceptedWritesBoundaryOutputs", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req := buildAdmissionRequest(t, g, "k5", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"k": "v"}}},
			nil)
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		out, err := s.GetOutput(ctx, resp.ExecutionID, "body")
		if err != nil {
			t.Fatalf("GetOutput: %v", err)
		}
		if out == nil || out["k"] != "v" {
			t.Fatalf("boundary output = %v, want {k:v}", out)
		}
	})

	t.Run("SingleUnitCompletesExecution", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupOnlyGraph(t)
		req := buildAdmissionRequest(t, g, "k6", engine.GroupOutcomeSuccess,
			[]engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"done": true}}},
			nil)
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		snap, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if snap == nil {
			t.Fatal("execution must exist")
		}
		// Single unit graph: after group admitted, execution should be terminal.
		if !types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("status = %s, want terminal (single unit)", snap.Status)
		}
		if snap.Status != types.ExecutionStatusSuccess {
			t.Fatalf("status = %s, want success", snap.Status)
		}
	})

	t.Run("MultiUnitSchedulesDownstream", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupWithDownstreamGraph(t)
		groups := g.Groups()
		gm := groups[0]

		// Build downstream arrival for "store" node (external to the group).
		storeIdx, ok := g.NodeIndex("store")
		if !ok {
			t.Fatal("store node not found")
		}
		storeUnit := g.UnitIndexForNode(storeIdx)
		downstream := []engine.DownstreamArrival{{
			NodeName:     "store",
			NodeIdx:      storeIdx,
			UnitIdx:      storeUnit,
			ArrivalCount: 1,
			ActiveCount:  1,
			MergeMode:    "wait_all",
			ExecTaskType: engine.TaskTypeNodeExec,
		}}

		req := engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    "k7",
			Namespace:       namespace.Default,
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         engine.GroupOutcomeSuccess,
			Exits:           []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}},
			ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}}),
			Downstream:      downstream,
		}

		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}

		// Execution should NOT be complete (2 units, only 1 done).
		snap, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if snap == nil {
			t.Fatal("execution must exist")
		}
		if types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("multi-unit execution must not be terminal after single group admission, got %s", snap.Status)
		}

		// Outbox should have a downstream entry.
		entries, err := s.(engine.AtomicStateStore).ListOutbox(ctx, resp.ExecutionID, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListOutbox: %v", err)
		}
		if len(entries) == 0 {
			t.Fatal("downstream outbox entry must exist after multi-unit admission")
		}
		found := false
		for _, e := range entries {
			if e.Task.NodeName == "store" {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("outbox entries %+v do not contain a task for 'store'", entries)
		}
	})

	// A workflow may declare several entries; an admission seeds exactly one.
	// Every other root must be resolved as skipped in the same transition, or
	// it holds the completion counter open forever (see
	// engine.UnselectedRootSkips). The skip intent is only useful with its
	// scheduling marker, so the proof is that the system skip commit the
	// intent drives is accepted rather than refused as stale.
	t.Run("UnselectedRootIsResolvedAsSkip", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupWithSiblingRootGraph(t)
		gm := g.Groups()[0]
		exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}}
		req := engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    "k-sibling",
			Namespace:       namespace.Default,
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         engine.GroupOutcomeSuccess,
			Exits:           exits,
			ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
		}
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}

		entries, err := s.(engine.AtomicStateStore).ListOutbox(ctx, resp.ExecutionID, time.Now().Add(time.Hour), 10)
		if err != nil {
			t.Fatalf("ListOutbox: %v", err)
		}
		var skip *engine.Task
		for i := range entries {
			if entries[i].Task.NodeName == "hook" {
				skip = &entries[i].Task
			}
		}
		if skip == nil || skip.Type != engine.TaskTypeNodeSkip {
			t.Fatalf("outbox %+v carries no skip intent for the unselected root \"hook\"", entries)
		}
		hookIdx, _ := g.NodeIndex("hook")
		if skip.NodeIdx != hookIdx || skip.UnitIdx != g.UnitIndexForNode(hookIdx) {
			t.Fatalf("skip intent = %+v, want node %d unit %d", *skip, hookIdx, g.UnitIndexForNode(hookIdx))
		}

		res, err := s.(engine.AtomicStateStore).CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: resp.ExecutionID,
			NodeName:    skip.NodeName,
			NodeIdx:     skip.NodeIdx,
			UnitIdx:     skip.UnitIdx,
			Status:      types.NodeStatusSkipped,
			System:      true,
		})
		if err != nil {
			t.Fatalf("CommitNode(skip hook): %v", err)
		}
		if res.Outcome != engine.CommitOutcomeAccepted {
			t.Fatalf("skip commit outcome = %q, want accepted (no skip marker was written)", res.Outcome)
		}

		// A duplicate retry applied nothing, so it must not add a second intent.
		if _, err := s.SeedExecutionFromEntry(ctx, req); err != nil {
			t.Fatalf("duplicate admission: %v", err)
		}
	})

	t.Run("FailedOutcomeFailsTheExecution", func(t *testing.T) {
		s := newStore(t)
		// A single-unit trigger group: the entry unit IS the whole execution, so
		// the failure has nowhere to hide and both backends must agree.
		//
		// Note on the fixture: the earlier shape here was the two-unit graph with
		// Downstream explicitly nil. That combination is not reachable —
		// deriveEntrySeedTopology derives arrivals from UnitOutEdges regardless of
		// outcome, so an entry unit with out-edges always carries Downstream — and
		// the two backends genuinely disagree on it (local short-circuits on a
		// `fatal` flag the Lua has no equivalent of, which leaves the Redis
		// execution Running forever). Pinning that disagreement would pin a shape
		// production cannot produce.
		g := triggerGroupOnlyGraph(t)
		groups := g.Groups()
		gm := groups[0]
		req := engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    "k8",
			Namespace:       namespace.Default,
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         engine.GroupOutcomeFailed,
			Exits:           nil,
			Error:           "processing error",
			ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeFailed, nil),
			Downstream:      nil,
		}
		resp, err := s.SeedExecutionFromEntry(ctx, req)
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}
		// This used to assert nothing beyond "accepted" -- the comment in its place
		// said the status was implementation-defined, which let a Success verdict
		// on a failed group pass on either backend.
		snap, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if snap == nil {
			t.Fatal("execution must exist")
		}
		if snap.Status != types.ExecutionStatusFailed {
			t.Fatalf("status = %q, want %q: the only unit failed, so Success here is a "+
				"wrong verdict rather than a missing metric", snap.Status, types.ExecutionStatusFailed)
		}
	})
	// The single-unit subtest above cannot see the failed counter on the local
	// backend: there `fatal ||` decides the verdict before the counter is read.
	// The counter is the sole decider when the failed group DID schedule
	// downstream -- the execution stays alive, a later unit succeeds, and the
	// entry's failure survives only in that counter. Dropping the increment turns
	// a failed workflow into a reported Success, the worst direction for this
	// defect to point.
	t.Run("FailedEntryIsRememberedWhenDownstreamStillRuns", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupWithDownstreamGraph(t)
		groups := g.Groups()
		gm := groups[0]
		storeIdx, ok := g.NodeIndex("store")
		if !ok {
			t.Fatal("graph has no 'store' node")
		}
		resp, err := s.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
			AdmissionKey:    "k9",
			Namespace:       namespace.Default,
			WorkflowID:      "wf-test",
			WorkflowVersion: "v1",
			EntryUnitID:     gm.Name,
			EntryUnitIdx:    gm.UnitIdx,
			Graph:           g,
			Outcome:         engine.GroupOutcomeFailed,
			Exits:           nil,
			Error:           "processing error",
			ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeFailed, nil),
			Downstream: []engine.DownstreamArrival{{
				NodeName:     "store",
				NodeIdx:      storeIdx,
				UnitIdx:      g.UnitIndexForNode(storeIdx),
				ArrivalCount: 1,
				ActiveCount:  1,
				MergeMode:    "wait_all",
			}},
		})
		if err != nil {
			t.Fatalf("admission: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}
		snap, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if types.IsTerminalExecutionStatus(snap.Status) {
			t.Fatalf("status = %q: a failed group that scheduled downstream must leave "+
				"the execution running so the downstream unit can execute", snap.Status)
		}

		// Run the downstream unit to a clean success, the way a runner would.
		lease := &engine.TaskLease{
			LeaseID:    "L-store",
			LeaseToken: "T-store",
			IssuedAt:   time.Now().UTC(),
			TTL:        time.Minute,
			Task: engine.Task{
				ExecutionID: resp.ExecutionID,
				NodeName:    "store",
				NodeIdx:     storeIdx,
				Type:        engine.TaskTypeNodeExec,
			},
		}
		if _, acquired, err := s.AcquireTaskLease(ctx, lease); err != nil || !acquired {
			t.Fatalf("acquire lease on downstream node: acquired=%v err=%v", acquired, err)
		}
		res, err := s.(engine.AtomicStateStore).CommitNode(ctx, engine.CommitNodeRequest{
			ExecutionID: resp.ExecutionID,
			NodeName:    "store",
			NodeIdx:     storeIdx,
			LeaseID:     lease.LeaseID,
			LeaseToken:  lease.LeaseToken,
			Attempt:     1,
			Status:      types.NodeStatusSuccess,
		})
		if err != nil {
			t.Fatalf("CommitNode(store): %v", err)
		}
		if res.Outcome != engine.CommitOutcomeAccepted {
			t.Fatalf("CommitNode(store).Outcome = %q, want accepted", res.Outcome)
		}
		if !res.ExecutionDone {
			t.Fatalf("committing the last unit did not finish the execution (outcome %q)", res.Outcome)
		}
		// The last unit succeeded, so the ONLY thing that can make this Failed is
		// the entry group's failure having been counted at admission time.
		if res.ExecutionStatus != types.ExecutionStatusFailed {
			t.Errorf("final status from CommitNode = %q, want %q: the entry group "+
				"failed, and a workflow that reports Success after a failed unit is a "+
				"silently wrong verdict, not a missing metric",
				res.ExecutionStatus, types.ExecutionStatusFailed)
		}
		final, err := s.GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution after completion: %v", err)
		}
		if final.Status != types.ExecutionStatusFailed {
			t.Errorf("persisted status = %q, want %q", final.Status, types.ExecutionStatusFailed)
		}
	})

	// An engine submission stamps the execution budget before the store sees the
	// snapshot, but an entry seed never passes through the engine's start path
	// and hands the store only the graph. The store has to do the stamping, or a
	// trigger-created execution of a workflow with settings.timeout reads back
	// with no budget and is never expired.
	t.Run("StampsTheExecutionDeadline", func(t *testing.T) {
		s := newStore(t)
		const timeout = 90 * time.Second
		g := triggerGroupWithDownstreamGraphSettings(t, &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout})
		resp, err := s.SeedExecutionFromEntry(ctx, entrySeedRequest(t, g, "k-deadline"))
		if err != nil {
			t.Fatalf("SeedExecutionFromEntry: %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted", resp.State)
		}
		requireSeedDeadlineStamped(t, s, ctx, resp.ExecutionID, timeout)
	})

	t.Run("StampsNoDeadlineWithoutATimeout", func(t *testing.T) {
		s := newStore(t)
		g := triggerGroupWithDownstreamGraph(t)
		resp, err := s.SeedExecutionFromEntry(ctx, entrySeedRequest(t, g, "k-no-deadline"))
		if err != nil {
			t.Fatalf("SeedExecutionFromEntry: %v", err)
		}
		requireSeedDeadlineStamped(t, s, ctx, resp.ExecutionID, 0)
	})
}

// requireSeedDeadlineStamped reads back an entry-seeded execution and checks the
// budget the store stamped on it. timeout 0 means the graph declares none and
// the Deadline must stay the zero time.
//
// The deadline is compared with the execution's own CreatedAt, within a second,
// and not for equality: the Redis backend stamps created_at_ms from its own
// clock reading, which is not the engine's and may differ by a few
// milliseconds. What the contract fixes is the size of the budget, not the
// exact instant it started.
func requireSeedDeadlineStamped(t *testing.T, state engine.StateStore, ctx context.Context, id types.ExecutionID, timeout time.Duration) {
	t.Helper()
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = (%v, %v), want the seeded execution", id, snap, err)
	}
	listed := false
	if state, ok := state.(engine.ExecutionDeadlineExpirer); ok {
		due, err := state.ListDeadlineExceededExecutions(context.Background(), deadlineContractBase.AddDate(100, 0, 0), 1024)
		if err != nil {
			t.Fatalf("ListDeadlineExceededExecutions() error = %v", err)
		}
		for _, item := range due {
			listed = listed || item.ExecutionID == id
		}
	}
	if timeout <= 0 {
		if !snap.Deadline.IsZero() {
			t.Fatalf("Deadline = %v for a graph without settings.timeout, want the zero time", snap.Deadline)
		}
		if listed {
			t.Fatalf("execution %q is in the deadline index though its graph declares no settings.timeout", id)
		}
		return
	}
	if snap.Deadline.IsZero() {
		t.Fatalf("Deadline is the zero time for a graph with settings.timeout %v: the seed "+
			"must stamp the budget, or the execution can never be expired", timeout)
	}
	if snap.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt is the zero time, so the budget %v has no start to be measured from", timeout)
	}
	budget := snap.Deadline.Sub(snap.CreatedAt)
	if diff := budget - timeout; diff < -time.Second || diff > time.Second {
		t.Fatalf("Deadline - CreatedAt = %v, want %v within one second (Deadline %v, CreatedAt %v)",
			budget, timeout, snap.Deadline, snap.CreatedAt)
	}
	// Registering the deadline is half of stamping it: the stored Deadline is
	// what a lease reads, the index entry is what the sweep finds. A seed that
	// wrote only the first would leave an execution nothing could expire once its
	// runner was gone. The listing is asked at an instant far past any budget, so
	// no case here waits for the deadline to arrive.
	if _, ok := state.(engine.ExecutionDeadlineExpirer); ok && !listed {
		t.Fatalf("execution %q has Deadline %v but is not in the deadline index: the sweep can never find it",
			id, snap.Deadline)
	}
}
