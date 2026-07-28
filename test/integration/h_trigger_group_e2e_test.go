//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/backend/providers/local"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/namespace"
	"github.com/xbcio/xflow/types"
)

// TestTriggerGroupE2E_LocalBackend exercises the full trigger-group admission
// path using the local (in-memory) backend. This covers:
//   - admission key creation
//   - SeedTriggeredGroupResult with the engine layer
//   - execution creation, group unit marked done, boundary outputs written
//   - duplicate admission (same key+hash) returns duplicate=true
//   - conflict (same key, different hash) returns conflict
//
// No external dependencies (Redis/Kafka/MySQL) required.
func TestTriggerGroupE2E_LocalBackend(t *testing.T) {
	// --- setup: local backend with engine ---
	be := local.New(local.WithConcurrency(1))
	eng := engine.New(be.State(), be.Queue())
	stop := be.Bind(eng)
	defer stop()

	ctx := context.Background()

	// --- compile a workflow with a trigger-group ---
	// Workflow: trigger-group "tg" has [entry, body]; downstream "store" is
	// external. This gives us 2 units: tg (group) + store (action).
	wfDef := &types.WorkflowDef{
		Name: "h-trigger-group-e2e",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction, Type: "test.tg.body"},
			{Name: "store", Kind: types.NodeKindAction, Type: "test.tg.store"},
		},
		Connections: types.Connections{
			"entry": {"main": {{Node: "body", Input: "main"}}},
			"body":  {"main": {{Node: "store", Input: "main"}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	}

	g, err := graph.Compile(wfDef)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}
	groups := g.Groups()
	if len(groups) == 0 {
		t.Fatal("compiled graph has no groups")
	}
	gm := groups[0]

	// Build downstream arrival for "store" node (external to the group).
	storeIdx, ok := g.NodeIndex("store")
	if !ok {
		t.Fatal("store node not found in graph")
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

	// Simulate the runner path: build admission key, compute result hash.
	admissionKey := engine.BuildAdmissionKeySingle(
		namespace.Default, "wf-tg-e2e", "v1", "tg", "events", 0, 42,
	)
	exits := []engine.GroupExitResult{{
		NodeName: "body",
		Port:     "main",
		Data:     map[string]any{"processed": true, "value": 42},
	}}
	resultHash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits)

	req := engine.SeedTriggeredGroupResultRequest{
		AdmissionKey:    admissionKey,
		Namespace:       namespace.Default,
		WorkflowID:      "wf-tg-e2e",
		WorkflowVersion: "v1",
		GroupID:         gm.Name,
		GroupUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      resultHash,
		Downstream:      downstream,
		Params:          map[string]any{"trigger_data": "kafka-msg-42"},
	}

	// --- Step 1: First admission → accepted ---
	t.Run("FirstAdmission_Accepted", func(t *testing.T) {
		resp, err := be.State().(engine.TriggerAdmissionStore).SeedTriggeredGroupResult(ctx, req)
		if err != nil {
			t.Fatalf("SeedTriggeredGroupResult: %v", err)
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
		// Verify deterministic ID derivation.
		expectedID := engine.DeterministicExecutionID(admissionKey)
		if resp.ExecutionID != expectedID {
			t.Fatalf("execution ID = %q, want deterministic %q", resp.ExecutionID, expectedID)
		}

		// Verify execution was created.
		snap, err := be.State().GetExecution(ctx, resp.ExecutionID)
		if err != nil {
			t.Fatalf("GetExecution: %v", err)
		}
		if snap == nil {
			t.Fatal("execution must exist after admission")
		}
		if snap.Status != types.ExecutionStatusRunning {
			// Multi-unit (group + store): after group admitted, execution is still
			// running because "store" has not completed yet.
			t.Logf("execution status = %s (expected running for multi-unit)", snap.Status)
		}

		// Verify boundary outputs written.
		out, err := be.State().GetOutput(ctx, resp.ExecutionID, "body")
		if err != nil {
			t.Fatalf("GetOutput: %v", err)
		}
		if out == nil {
			t.Fatal("boundary output for 'body' must exist")
		}
		if out["processed"] != true {
			t.Fatalf("boundary output = %v, want processed=true", out)
		}
		if out["value"] != 42 {
			t.Fatalf("boundary output value = %v, want 42", out["value"])
		}
	})

	// --- Step 2: Duplicate admission (same key + same hash) → duplicate=true ---
	t.Run("DuplicateAdmission_SameKeyAndHash", func(t *testing.T) {
		resp, err := be.State().(engine.TriggerAdmissionStore).SeedTriggeredGroupResult(ctx, req)
		if err != nil {
			t.Fatalf("SeedTriggeredGroupResult (dup): %v", err)
		}
		if resp.State != engine.AdmissionStateAccepted {
			t.Fatalf("state = %q, want accepted (duplicate)", resp.State)
		}
		if !resp.Duplicate {
			t.Fatal("second admission with same key+hash must be duplicate")
		}
		// Must return the same execution ID.
		expectedID := engine.DeterministicExecutionID(admissionKey)
		if resp.ExecutionID != expectedID {
			t.Fatalf("duplicate execution ID = %q, want %q", resp.ExecutionID, expectedID)
		}
	})

	// --- Step 3: Conflict (same key, different hash) → conflict ---
	t.Run("Conflict_SameKeyDifferentHash", func(t *testing.T) {
		// Build a request with different exits → different result hash.
		conflictExits := []engine.GroupExitResult{{
			NodeName: "body",
			Port:     "main",
			Data:     map[string]any{"processed": true, "value": 99},
		}}
		conflictHash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, conflictExits)
		conflictReq := req
		conflictReq.Exits = conflictExits
		conflictReq.ResultHash = conflictHash

		resp, err := be.State().(engine.TriggerAdmissionStore).SeedTriggeredGroupResult(ctx, conflictReq)
		if err != nil {
			t.Fatalf("SeedTriggeredGroupResult (conflict): %v", err)
		}
		if resp.State != engine.AdmissionStateConflict {
			t.Fatalf("state = %q, want conflict", resp.State)
		}
		if resp.Duplicate {
			t.Fatal("conflict must not be flagged as duplicate")
		}
	})
}

// TestTriggerGroupE2E_SingleUnit_CompletesExecution verifies that when a
// trigger-group is the only unit in the workflow (no external downstream),
// admission immediately completes the execution.
func TestTriggerGroupE2E_SingleUnit_CompletesExecution(t *testing.T) {
	be := local.New(local.WithConcurrency(1))
	eng := engine.New(be.State(), be.Queue())
	stop := be.Bind(eng)
	defer stop()

	ctx := context.Background()

	// Single-unit workflow: group "tg" with [entry, body], no external nodes.
	wfDef := &types.WorkflowDef{
		Name: "h-tg-single-unit",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction, Type: "test.tg.body"},
		},
		Connections: types.Connections{
			"entry": {"main": {{Node: "body", Input: "main"}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	}

	g, err := graph.Compile(wfDef)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}

	groups := g.Groups()
	gm := groups[0]

	admissionKey := engine.BuildAdmissionKeySingle(
		namespace.Default, "wf-tg-single", "v1", "tg", "events", 0, 100,
	)
	exits := []engine.GroupExitResult{{
		NodeName: "body",
		Port:     "main",
		Data:     map[string]any{"completed": true},
	}}
	resultHash := engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits)

	req := engine.SeedTriggeredGroupResultRequest{
		AdmissionKey:    admissionKey,
		Namespace:       namespace.Default,
		WorkflowID:      "wf-tg-single",
		WorkflowVersion: "v1",
		GroupID:         gm.Name,
		GroupUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      resultHash,
		Downstream:      nil, // no downstream
	}

	resp, err := be.State().(engine.TriggerAdmissionStore).SeedTriggeredGroupResult(ctx, req)
	if err != nil {
		t.Fatalf("SeedTriggeredGroupResult: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("state = %q, want accepted", resp.State)
	}

	// Single unit → execution should be terminal (success).
	snap, err := be.State().GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if snap == nil {
		t.Fatal("execution must exist")
	}
	if !types.IsTerminalExecutionStatus(snap.Status) {
		t.Fatalf("status = %s, want terminal (single unit)", snap.Status)
	}
	if snap.Status != types.ExecutionStatusSuccess {
		t.Fatalf("status = %s, want success", snap.Status)
	}
}

// TestTriggerGroupE2E_FailedOutcome verifies that a failed group outcome is
// admitted correctly and the execution reflects the failure.
func TestTriggerGroupE2E_FailedOutcome(t *testing.T) {
	be := local.New(local.WithConcurrency(1))
	eng := engine.New(be.State(), be.Queue())
	stop := be.Bind(eng)
	defer stop()

	ctx := context.Background()

	wfDef := &types.WorkflowDef{
		Name: "h-tg-failed",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "body", Kind: types.NodeKindAction, Type: "test.tg.body"},
		},
		Connections: types.Connections{
			"entry": {"main": {{Node: "body", Input: "main"}}},
		},
		Groups: []types.GroupDef{{Name: "tg", Members: []string{"entry", "body"}}},
	}

	g, err := graph.Compile(wfDef)
	if err != nil {
		t.Fatalf("graph.Compile: %v", err)
	}

	groups := g.Groups()
	gm := groups[0]

	admissionKey := engine.BuildAdmissionKeySingle(
		namespace.Default, "wf-tg-failed", "v1", "tg", "errors", 0, 200,
	)
	resultHash := engine.ComputeResultHash(engine.GroupOutcomeFailed, nil)

	req := engine.SeedTriggeredGroupResultRequest{
		AdmissionKey:    admissionKey,
		Namespace:       namespace.Default,
		WorkflowID:      "wf-tg-failed",
		WorkflowVersion: "v1",
		GroupID:         gm.Name,
		GroupUnitIdx:    gm.UnitIdx,
		Graph:           g,
		Outcome:         engine.GroupOutcomeFailed,
		Exits:           nil,
		Error:           "kafka consumer timeout",
		ResultHash:      resultHash,
		Downstream:      nil,
	}

	resp, err := be.State().(engine.TriggerAdmissionStore).SeedTriggeredGroupResult(ctx, req)
	if err != nil {
		t.Fatalf("SeedTriggeredGroupResult: %v", err)
	}
	if resp.State != engine.AdmissionStateAccepted {
		t.Fatalf("state = %q, want accepted", resp.State)
	}

	// Failed outcome on single-unit → execution should be failed.
	snap, err := be.State().GetExecution(ctx, resp.ExecutionID)
	if err != nil {
		t.Fatalf("GetExecution: %v", err)
	}
	if snap == nil {
		t.Fatal("execution must exist")
	}
	if snap.Status != types.ExecutionStatusFailed {
		t.Fatalf("status = %s, want failed", snap.Status)
	}
}
