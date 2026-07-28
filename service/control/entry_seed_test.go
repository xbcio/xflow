package control

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

func entrySeedTestGraph(t *testing.T) *graph.Graph {
	t.Helper()
	g, err := graph.Compile(&types.WorkflowDef{
		Name: "tg-only",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"entry": {"main": {{Node: "body", Input: "main"}}}},
		Groups:      []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	})
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	return g
}

func TestCoreEntrySeedAcceptedThenDuplicate(t *testing.T) {
	backend := local.New()
	eng := engine.New(backend.State(), backend.Queue())
	core := &Core{engine: eng}

	g := entrySeedTestGraph(t)
	gm := g.Groups()[0]
	outcome := engine.GroupOutcomeSuccess
	exits := []engine.BoundaryExit{{NodeName: "body", Port: "main", Data: map[string]any{"x": 1}}}
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "k1",
		WorkflowID:      "wf-test",
		WorkflowVersion: "v1",
		EntryUnitID:     gm.Name,
		EntryUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         outcome,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(outcome, exits),
	}

	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	resp, err := core.SeedExecutionFromEntry(ctx, req)
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
	if want := engine.DeterministicExecutionID("k1"); resp.ExecutionID != want {
		t.Fatalf("execution ID = %q, want deterministic %q", resp.ExecutionID, want)
	}

	resp2, err := core.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatalf("second SeedExecutionFromEntry: %v", err)
	}
	if !resp2.Duplicate {
		t.Fatal("second admission with same key+hash must be duplicate")
	}
	if resp2.ExecutionID != resp.ExecutionID {
		t.Fatalf("duplicate execution ID = %q, want %q", resp2.ExecutionID, resp.ExecutionID)
	}
}
