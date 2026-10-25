package local

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// entryDeadlineGraph compiles a trigger group with one downstream node, so a
// seeded execution is still running after the seed. timeout is the workflow's
// settings.timeout (0 leaves it unbounded).
func entryDeadlineGraph(t *testing.T, timeout time.Duration) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     "entry-deadline",
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
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
		t.Fatalf("Compile() error = %v", err)
	}
	return g
}

// entryDeadlineRequest builds the seed one entry-unit runner submits: the
// trigger group's result plus its arrival at the downstream node.
func entryDeadlineRequest(t *testing.T, g *graph.Graph, key engine.AdmissionKey) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	group := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"r": 1}}}
	idx, ok := g.NodeIndex("store")
	if !ok {
		t.Fatal("graph has no store node")
	}
	return engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    key,
		Namespace:       namespace.Default,
		WorkflowID:      "wf-deadline",
		WorkflowVersion: "v1",
		EntryUnitID:     group.Name,
		EntryUnitIdx:    group.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
		Downstream: []engine.DownstreamArrival{{
			NodeName:     "store",
			NodeIdx:      idx,
			UnitIdx:      g.UnitIndexForNode(idx),
			ArrivalCount: 1,
			ActiveCount:  1,
			MergeMode:    "wait_all",
			ExecTaskType: engine.TaskTypeNodeExec,
		}},
	}
}

// requireEntryDeadline asserts the seeded execution's budget: Deadline is
// CreatedAt plus the graph's timeout, and zero when the graph has none.
func requireEntryDeadline(t *testing.T, state *memoryState, id types.ExecutionID, timeout time.Duration) {
	t.Helper()
	snap, err := state.GetExecution(context.Background(), id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = %+v, %v", id, snap, err)
	}
	if timeout <= 0 {
		if !snap.Deadline.IsZero() {
			t.Fatalf("Deadline = %v, want zero for a graph without settings.timeout", snap.Deadline)
		}
		return
	}
	if snap.Deadline.IsZero() {
		t.Fatal("Deadline is zero, want it stamped from settings.timeout")
	}
	if snap.CreatedAt.IsZero() {
		t.Fatal("CreatedAt is zero")
	}
	if got := snap.Deadline.Sub(snap.CreatedAt); got != timeout {
		t.Fatalf("Deadline - CreatedAt = %v, want %v", got, timeout)
	}
}

func TestMemorySeedExecutionFromEntryStampsTheDeadline(t *testing.T) {
	ctx := context.Background()
	const timeout = 90 * time.Second

	t.Run("a timeout is stamped from creation", func(t *testing.T) {
		state := newMemoryState()
		resp, err := state.SeedExecutionFromEntry(ctx, entryDeadlineRequest(t, entryDeadlineGraph(t, timeout), "k-deadline"))
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntry() = %+v, %v", resp, err)
		}
		requireEntryDeadline(t, state, resp.ExecutionID, timeout)
		// The stamp is what registers the execution with the deadline index.
		requireDeadlineIDs(t, listedDeadlines(t, state, time.Now().UTC().Add(time.Hour), 8), resp.ExecutionID)
	})

	t.Run("no timeout leaves the deadline zero", func(t *testing.T) {
		state := newMemoryState()
		resp, err := state.SeedExecutionFromEntry(ctx, entryDeadlineRequest(t, entryDeadlineGraph(t, 0), "k-no-deadline"))
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntry() = %+v, %v", resp, err)
		}
		requireEntryDeadline(t, state, resp.ExecutionID, 0)
		requireDeadlineIDs(t, listedDeadlines(t, state, time.Now().UTC().AddDate(100, 0, 0), 8))
	})
}

// TestMemorySeedExecutionFromEntryWithConcurrencyStampsTheDeadline covers the
// limited path for an admission that starts running and for one that parks:
// the queued wait is spent out of the budget, so the parked seed carries the
// deadline too.
func TestMemorySeedExecutionFromEntryWithConcurrencyStampsTheDeadline(t *testing.T) {
	ctx := context.Background()
	const timeout = 90 * time.Second
	g := entryDeadlineGraph(t, timeout)
	admission := engine.ConcurrencyAdmission{
		Namespace: "default", Workflow: g.Name(), Limit: 1,
		Mode: types.ConcurrencyModeQueue,
	}
	state := newMemoryState()

	running, err := state.SeedExecutionFromEntryWithConcurrency(ctx, entryDeadlineRequest(t, g, "k-limited-running"), admission)
	if err != nil || running.State != engine.AdmissionStateAccepted || running.Queued {
		t.Fatalf("first limited seed = %+v, %v, want it running", running, err)
	}
	requireEntryDeadline(t, state, running.ExecutionID, timeout)

	parked, err := state.SeedExecutionFromEntryWithConcurrency(ctx, entryDeadlineRequest(t, g, "k-limited-queued"), admission)
	if err != nil || parked.State != engine.AdmissionStateAccepted || !parked.Queued {
		t.Fatalf("second limited seed = %+v, %v, want it parked", parked, err)
	}
	requireEntryDeadline(t, state, parked.ExecutionID, timeout)
}

// TestMemorySeedExecutionKeepsTheCallersDeadline pins that the rerun seed
// writes the budget the engine stamped on its snapshot: the store neither
// drops it nor restamps it, and the deadline index registers it.
func TestMemorySeedExecutionKeepsTheCallersDeadline(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	g := entryDeadlineGraph(t, time.Minute)
	created := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	id := engine.NewExecutionID()

	if _, err := state.SeedExecution(ctx, engine.SeedExecutionRequest{
		Execution: &engine.ExecutionSnapshot{
			ID: id, Graph: g, Status: types.ExecutionStatusRunning,
			CreatedAt: created, Deadline: created.Add(time.Minute),
		},
	}); err != nil {
		t.Fatalf("SeedExecution() error = %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution() = %+v, %v", snap, err)
	}
	if !snap.Deadline.Equal(created.Add(time.Minute)) {
		t.Fatalf("Deadline = %v, want %v", snap.Deadline, created.Add(time.Minute))
	}
	requireDeadlineIDs(t, listedDeadlines(t, state, created.Add(time.Minute), 8), id)
}
