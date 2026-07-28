package control

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend"
	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// entrySeedResolveDef is a two-node workflow: standalone trigger "trig" feeding
// a downstream action "down". Seeding "trig" must fan out to "down".
func entrySeedResolveDef() *types.WorkflowDef {
	return &types.WorkflowDef{
		Name:    "trig-down",
		Version: "v1",
		Nodes: []types.NodeDef{
			{Name: "trig", Kind: types.NodeKindTrigger},
			{Name: "down", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{"trig": {"main": {{Node: "down", Input: "main"}}}},
	}
}

// TestCoreEntrySeedResolvesDownstream verifies that when a workflow registry is
// wired, a remote seed carrying only the entry-unit ID + workflow ID/version is
// resolved server-side to the compiled graph + downstream arrivals, so the
// admitted seed actually schedules the downstream node (spec §11.5).
func TestCoreEntrySeedResolvesDownstream(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	be := local.New()
	eng := engine.New(be.State(), be.Queue())

	def := entrySeedResolveDef()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	reg := be.WorkflowRegistry()
	rec, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:             "wf-resolve",
		Key:            "default/trig-down@v1",
		Namespace:      string(namespace.Default),
		Name:           "trig-down",
		Version:        "v1",
		DefinitionHash: "sha256:test",
		Definition:     def,
		Graph:          g,
	})
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	core := &Core{engine: eng, workflowRegistry: reg}

	outcome := engine.GroupOutcomeSuccess
	exits := []engine.BoundaryExit{{NodeName: "trig", Port: "main", Data: map[string]any{"x": 1}}}
	// The remote runner carries ONLY the identity + entry-unit ID; it does NOT
	// set Graph / EntryUnitIdx / Downstream — those are resolved server-side.
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "seed-resolve-1",
		WorkflowID:      rec.ID,
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
		Outcome:         outcome,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(outcome, exits),
	}

	resp, err := core.SeedExecutionFromEntry(ctx, req)
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted || resp.Duplicate {
		t.Fatalf("state=%q duplicate=%v, want accepted+not-duplicate", resp.State, resp.Duplicate)
	}

	// Server-side resolution must have produced a downstream execute intent for
	// "down". Without it, the execution is admitted with the entry unit done but
	// "down" is never scheduled (no fan-out). The intent lands in the durable
	// outbox; node status only materializes once a runner executes it.
	atomic, ok := be.State().(engine.AtomicStateStore)
	if !ok {
		t.Fatal("local state must implement AtomicStateStore")
	}
	entries, err := atomic.ListOutbox(ctx, resp.ExecutionID, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox: %v", err)
	}
	found := false
	for _, e := range entries {
		if e.Task.NodeName == "down" && e.Task.Type == engine.TaskTypeNodeExec {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("outbox %+v has no execute intent for downstream node 'down' (server-side fan-out did not happen)", entries)
	}
}

// TestCoreEntrySeedUnregisteredWorkflowRejected verifies fail-closed behavior:
// a seed for a workflow that is not in the registry is rejected with
// ErrEntrySeedWorkflowUnknown and creates no execution.
func TestCoreEntrySeedUnregisteredWorkflowRejected(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	be := local.New()
	eng := engine.New(be.State(), be.Queue())
	core := &Core{engine: eng, workflowRegistry: be.WorkflowRegistry()}

	outcome := engine.GroupOutcomeSuccess
	exits := []engine.BoundaryExit{{NodeName: "trig", Port: "main", Data: map[string]any{"x": 1}}}
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "seed-unregistered-1",
		WorkflowID:      "wf-does-not-exist",
		WorkflowVersion: "v1",
		EntryUnitID:     "trig",
		Outcome:         outcome,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(outcome, exits),
	}

	if _, err := core.SeedExecutionFromEntry(ctx, req); !errors.Is(err, ErrEntrySeedWorkflowUnknown) {
		t.Fatalf("err = %v, want ErrEntrySeedWorkflowUnknown", err)
	}
	if _, err := eng.Inspect(ctx, engine.DeterministicExecutionID("seed-unregistered-1")); !errors.Is(err, engine.ErrExecutionNotFound) {
		t.Fatalf("unregistered seed must not create an execution, inspect err = %v", err)
	}
}

// TestCoreEntrySeedUnknownEntryUnitRejected verifies fail-closed behavior when
// the workflow is registered but the entry-unit ID does not resolve to a unit
// in the compiled graph.
func TestCoreEntrySeedUnknownEntryUnitRejected(t *testing.T) {
	ctx := namespace.WithNamespace(context.Background(), namespace.Default)

	be := local.New()
	eng := engine.New(be.State(), be.Queue())

	def := entrySeedResolveDef()
	g, err := graph.Compile(def)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	reg := be.WorkflowRegistry()
	rec, err := reg.AddWorkflow(ctx, backend.WorkflowRecord{
		ID:      "wf-unknown-unit",
		Key:     "default/trig-down@v1",
		Version: "v1",
		Graph:   g,
	})
	if err != nil {
		t.Fatalf("AddWorkflow: %v", err)
	}

	core := &Core{engine: eng, workflowRegistry: reg}

	outcome := engine.GroupOutcomeSuccess
	req := engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "seed-unknown-unit-1",
		WorkflowID:      rec.ID,
		WorkflowVersion: "v1",
		EntryUnitID:     "no-such-unit",
		Outcome:         outcome,
		ResultHash:      engine.ComputeResultHash(outcome, nil),
	}

	if _, err := core.SeedExecutionFromEntry(ctx, req); !errors.Is(err, ErrEntrySeedWorkflowUnknown) {
		t.Fatalf("err = %v, want ErrEntrySeedWorkflowUnknown", err)
	}
	if _, err := eng.Inspect(ctx, engine.DeterministicExecutionID("seed-unknown-unit-1")); !errors.Is(err, engine.ErrExecutionNotFound) {
		t.Fatalf("unknown-unit seed must not create an execution, inspect err = %v", err)
	}
}
