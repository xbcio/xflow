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

// timeoutSeedRequest is seedRequestFor with a whole-execution time budget on
// the graph. A zero timeout leaves the graph unbounded.
func timeoutSeedRequest(t *testing.T, name, admissionKey string, timeout time.Duration) engine.SeedExecutionFromEntryRequest {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name:     name,
		Settings: &types.WorkflowSettings{Timezone: "UTC", Timeout: timeout},
		Nodes: []types.NodeDef{
			{Name: "entry", Type: "test.echo"},
			{Name: "body", Type: "test.echo"},
		},
		Connections: types.Connections{
			"entry": {"main": types.PortConnections{Targets: []types.Connection{{Node: "body", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	gm := g.Groups()[0]
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	return engine.SeedExecutionFromEntryRequest{
		AdmissionKey: engine.AdmissionKey(admissionKey),
		WorkflowID:   types.WorkflowID("wf"), WorkflowVersion: "v1",
		EntryUnitID: gm.Name, EntryUnitIdx: gm.UnitIdx, Graph: g,
		Outcome:    engine.GroupOutcomeSuccess,
		Exits:      exits,
		ResultHash: engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	}
}

// requireEntrySeedDeadline reads the seeded execution back and checks that its
// deadline is the graph's budget counted from the execution's own created_at.
func requireEntrySeedDeadline(t *testing.T, state *Store, ctx context.Context, id types.ExecutionID, timeout time.Duration) {
	t.Helper()
	snap, err := state.GetExecution(ctx, id)
	if err != nil || snap == nil {
		t.Fatalf("GetExecution(%q) = (%+v, %v), want the seeded execution", id, snap, err)
	}
	if snap.Deadline.IsZero() {
		t.Fatalf("Deadline of the entry-seeded %q read back zero: a lost deadline is a silently disabled timeout", id)
	}
	if snap.CreatedAt.IsZero() {
		t.Fatalf("CreatedAt of %q read back zero", id)
	}
	got := snap.Deadline.Sub(snap.CreatedAt)
	if diff := got - timeout; diff < -time.Second || diff > time.Second {
		t.Fatalf("Deadline-CreatedAt = %v, want %v within 1s", got, timeout)
	}
}

func TestEntrySeedStampsTheDeadline(t *testing.T) {
	const timeout = 90 * time.Second

	t.Run("SeedExecutionFromEntry", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		ctx := context.Background()
		req := timeoutSeedRequest(t, "entry-deadline", "entry-deadline-key", timeout)
		resp, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
		}
		requireEntrySeedDeadline(t, state, ctx, resp.ExecutionID, timeout)
	})

	t.Run("SeedExecutionFromEntryWithConcurrency", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		ctx := context.Background()
		req := timeoutSeedRequest(t, "entry-deadline-conc", "entry-deadline-conc-key", timeout)
		resp, err := state.SeedExecutionFromEntryWithConcurrency(ctx, req, engine.ConcurrencyAdmission{
			Namespace: "default", Workflow: req.Graph.Name(), Limit: 1, Mode: types.ConcurrencyModeReject,
		})
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntryWithConcurrency = (%+v, %v), want accepted", resp, err)
		}
		requireEntrySeedDeadline(t, state, ctx, resp.ExecutionID, timeout)
	})

	t.Run("a graph without a budget writes no deadline", func(t *testing.T) {
		srv, _, state := newMiniredisStore(t)
		ctx := context.Background()
		req := timeoutSeedRequest(t, "entry-no-deadline", "entry-no-deadline-key", 0)
		resp, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil || resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
		}
		if srv.Exists(execKey(namespace.Default, resp.ExecutionID, "deadline_ms")) {
			t.Fatal("an entry-seeded execution of an unbounded graph wrote deadline_ms")
		}
		requireDeadline(t, state, resp.ExecutionID, time.Time{})
	})

	t.Run("a duplicate admission keeps the original deadline", func(t *testing.T) {
		_, _, state := newMiniredisStore(t)
		ctx := context.Background()
		req := timeoutSeedRequest(t, "entry-deadline-dup", "entry-deadline-dup-key", timeout)
		first, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil || first.State != engine.AdmissionStateAccepted {
			t.Fatalf("first SeedExecutionFromEntry = (%+v, %v), want accepted", first, err)
		}
		before, err := state.GetExecution(ctx, first.ExecutionID)
		if err != nil || before == nil {
			t.Fatalf("GetExecution = (%+v, %v)", before, err)
		}
		time.Sleep(5 * time.Millisecond)
		second, err := state.SeedExecutionFromEntry(ctx, req)
		if err != nil || !second.Duplicate {
			t.Fatalf("second SeedExecutionFromEntry = (%+v, %v), want a duplicate", second, err)
		}
		after, err := state.GetExecution(ctx, first.ExecutionID)
		if err != nil || after == nil {
			t.Fatalf("GetExecution = (%+v, %v)", after, err)
		}
		if !after.Deadline.Equal(before.Deadline) {
			t.Fatalf("a duplicate admission moved the deadline from %v to %v", before.Deadline, after.Deadline)
		}
	})
}

// TestEntrySeedRegistersTheNamespace pins that the entry-admission path puts a
// non-default namespace in the discovery registry. Every maintenance loop lists
// namespaces from it, so a namespace only this path ever creates executions in
// would otherwise be invisible to them.
func TestEntrySeedRegistersTheNamespace(t *testing.T) {
	_, rdb, state := newMiniredisStore(t)
	ns := namespace.Namespace("tenant-entry-only")
	ctx := namespace.WithNamespace(context.Background(), ns)

	req := timeoutSeedRequest(t, "entry-namespace", "entry-namespace-key", time.Minute)
	resp, err := state.SeedExecutionFromEntry(ctx, req)
	if err != nil || resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry = (%+v, %v), want accepted", resp, err)
	}
	registered, err := rdb.SIsMember(ctx, namespaceSetKey, string(ns)).Result()
	if err != nil {
		t.Fatalf("SISMEMBER %s: %v", namespaceSetKey, err)
	}
	if !registered {
		t.Fatalf("namespace %q is missing from %s after an entry-only admission", ns, namespaceSetKey)
	}
	listed, err := state.scopedNamespaces(ctx)
	if err != nil {
		t.Fatalf("scopedNamespaces: %v", err)
	}
	found := false
	for _, n := range listed {
		if n == ns {
			found = true
		}
	}
	if !found {
		t.Fatalf("scopedNamespaces = %v, want it to include %q", listed, ns)
	}
}
