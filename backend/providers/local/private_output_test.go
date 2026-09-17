package local

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestMemoryPrivateNodeOutputStaysRuntimeOnly(t *testing.T) {
	ctx := context.Background()
	backend := New()
	state := backend.state
	id := types.ExecutionID("private-node-output")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: privateOutputNodeGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	lease := privateOutputLease(id, "secret-node", "lease-1", "token-1", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	secret := map[string]any{"token": "do-not-publish"}
	committed, err := state.CommitNode(ctx, engine.CommitNodeRequest{
		ExecutionID:   id,
		NodeName:      "secret-node",
		NodeIdx:       0,
		LeaseID:       lease.LeaseID,
		LeaseToken:    lease.LeaseToken,
		Attempt:       1,
		Status:        types.NodeStatusSuccess,
		Output:        secret,
		StoreOutput:   true,
		PrivateOutput: true,
	})
	if err != nil {
		t.Fatalf("CommitNode() error = %v", err)
	}
	if !committed.ExecutionDone {
		t.Fatal("CommitNode() did not complete the one-unit execution")
	}

	snap, err := state.GetNode(ctx, id, "secret-node")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if snap == nil || !snap.PrivateOutput {
		t.Fatalf("node private marker = %+v, want true", snap)
	}
	if snap.Output != nil {
		t.Fatalf("public NodeSnapshot.Output = %#v, want nil", snap.Output)
	}
	assertRuntimeOutput(t, state, ctx, id, "secret-node", secret)
	assertOutputAbsent(t, state.GetAllOutputs(id), "secret-node")

	result, err := backend.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("WaitDone() status = %q, want success", result.Status)
	}
	if _, found := result.Output["secret-node"]; found {
		t.Fatalf("WaitDone() leaked private node output: %#v", result.Output["secret-node"])
	}
}

func TestMemoryPrivateOutputMarkerSurvivesZeroUpdatesAndLeaseTransitions(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	id := types.ExecutionID("private-marker-transitions")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: privateOutputNodeGraph(t), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:   id,
		Name:          "waiter",
		NodeIdx:       0,
		Status:        types.NodeStatusPending,
		Output:        map[string]any{"must": "not appear"},
		PrivateOutput: true,
	}); err != nil {
		t.Fatalf("UpsertNode(private) error = %v", err)
	}
	// A normal zero-value update cannot declassify the policy established above.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "waiter",
		NodeIdx:     0,
		Status:      types.NodeStatusPending,
		Output:      map[string]any{"must": "still not appear"},
	}); err != nil {
		t.Fatalf("UpsertNode(zero privacy) error = %v", err)
	}
	assertPrivateSnapshot(t, state, ctx, id, "waiter")

	lease := privateOutputLease(id, "waiter", "lease-1", "token-1", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	lease.Attempt = 1
	assertPrivateSnapshot(t, state, ctx, id, "waiter")
	if _, claimed, err := state.ClaimTaskLease(ctx, lease); err != nil || !claimed {
		t.Fatalf("ClaimTaskLease() claimed=%v err=%v, want true/nil", claimed, err)
	}

	secret := map[string]any{"approval": "sensitive"}
	if _, committed, err := state.SuspendTaskLease(ctx, lease, secret, true, false,
		&types.SuspendSpec{Mode: types.ModeSignal, Signals: []string{"go"}}, ""); err != nil || !committed {
		t.Fatalf("SuspendTaskLease() committed=%v err=%v, want true/nil", committed, err)
	}
	assertPrivateSnapshot(t, state, ctx, id, "waiter")
	assertRuntimeOutput(t, state, ctx, id, "waiter", secret)
	assertOutputAbsent(t, state.GetAllOutputs(id), "waiter")

	if _, _, delivered, err := state.DeliverSignalWithOutbox(ctx, id, "go", map[string]any{"ok": true}, engine.ResumeIntent{
		NodeName: "waiter", NodeIdx: 0, UnitIdx: 0,
	}); err != nil || !delivered {
		t.Fatalf("DeliverSignalWithOutbox() delivered=%v err=%v, want true/nil", delivered, err)
	}
	resumeLease := privateOutputLease(id, "waiter", "lease-2", "token-2", engine.TaskTypeNodeResume)
	if _, acquired, err := state.AcquireTaskLease(ctx, resumeLease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(resume) acquired=%v err=%v, want true/nil", acquired, err)
	}
	assertPrivateSnapshot(t, state, ctx, id, "waiter")

	retry := engine.OutboxEntry{
		ID:   "retry/private-marker-transitions/waiter/2",
		Task: engine.Task{ExecutionID: id, NodeName: "waiter", NodeIdx: 0, Type: engine.TaskTypeNodeExec},
	}
	if reset, err := state.ResetNodeForRetryWithOutbox(ctx, id, "waiter", resumeLease.LeaseToken, retry); err != nil || !reset {
		t.Fatalf("ResetNodeForRetryWithOutbox() reset=%v err=%v, want true/nil", reset, err)
	}
	assertPrivateSnapshot(t, state, ctx, id, "waiter")

	retryLease := privateOutputLease(id, "waiter", "lease-3", "token-3", engine.TaskTypeNodeExec)
	if _, acquired, err := state.AcquireTaskLease(ctx, retryLease); err != nil || !acquired {
		t.Fatalf("AcquireTaskLease(retry) acquired=%v err=%v, want true/nil", acquired, err)
	}
	assertPrivateSnapshot(t, state, ctx, id, "waiter")
}

func TestMemoryPrivateGroupExitOutputStaysRuntimeOnly(t *testing.T) {
	ctx := context.Background()
	backend := New()
	state := backend.state
	graph := privateOutputGroupGraph(t)
	id := types.ExecutionID("private-group-output")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: graph, Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	group := graph.Groups()[0]
	lease := &engine.GroupLease{
		LeaseID: "group-lease-1", LeaseToken: "group-token-1", Attempt: 1,
		ExecutionID: id, GroupUnitIdx: group.UnitIdx, GroupID: group.Name,
		IssuedAt: time.Now().UTC(), TTL: time.Minute,
	}
	if acquired, err := state.AcquireGroupLease(ctx, lease); err != nil || !acquired {
		t.Fatalf("AcquireGroupLease() acquired=%v err=%v, want true/nil", acquired, err)
	}
	privateData := map[string]any{"secret": "group-only"}
	publicData := map[string]any{"visible": "group-public"}
	committed, err := state.CommitGroup(ctx, engine.GroupCommitRequest{
		ExecutionID: id, GroupUnitIdx: group.UnitIdx, GroupID: group.Name,
		LeaseID: lease.LeaseID, LeaseToken: lease.LeaseToken, Attempt: lease.Attempt,
		Outcome: engine.GroupOutcomeSuccess,
		Exits: []engine.GroupExitResult{
			{NodeName: "private-exit", Port: "main", Data: privateData, PrivateOutput: true},
			{NodeName: "public-exit", Port: "main", Data: publicData},
		},
	})
	if err != nil {
		t.Fatalf("CommitGroup() error = %v", err)
	}
	if !committed.ExecutionDone {
		t.Fatal("CommitGroup() did not complete the one-unit execution")
	}
	assertRuntimeOutput(t, state, ctx, id, "private-exit", privateData)
	assertRuntimeOutput(t, state, ctx, id, "public-exit", publicData)
	all := state.GetAllOutputs(id)
	assertOutputAbsent(t, all, "private-exit")
	assertPublicOutput(t, all, "public-exit", publicData)

	result, err := backend.WaitDone(ctx, id)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	assertOutputAbsent(t, result.Output, "private-exit")
	assertPublicOutput(t, result.Output, "public-exit", publicData)
}

func TestMemoryPrivateEntryExitOutputStaysRuntimeOnly(t *testing.T) {
	ctx := context.Background()
	backend := New()
	state := backend.state
	graph := privateOutputGroupGraph(t)
	group := graph.Groups()[0]
	privateData := map[string]any{"secret": "entry-only"}
	publicData := map[string]any{"visible": "entry-public"}
	exits := []engine.BoundaryExit{
		{NodeName: "private-entry-exit", Port: "main", Data: privateData, PrivateOutput: true},
		{NodeName: "public-entry-exit", Port: "main", Data: publicData},
	}
	response, err := state.SeedExecutionFromEntry(ctx, engine.SeedExecutionFromEntryRequest{
		AdmissionKey:    "private-entry-output",
		WorkflowID:      "private-output-workflow",
		WorkflowVersion: "v1",
		EntryUnitID:     group.Name,
		EntryUnitIdx:    group.UnitIdx,
		Graph:           graph,
		Outcome:         engine.GroupOutcomeSuccess,
		Exits:           exits,
		ResultHash:      engine.ComputeResultHash(engine.GroupOutcomeSuccess, exits),
	})
	if err != nil {
		t.Fatalf("SeedExecutionFromEntry() error = %v", err)
	}
	if response.State != engine.AdmissionStateAccepted {
		t.Fatalf("SeedExecutionFromEntry() state = %q, want accepted", response.State)
	}
	assertRuntimeOutput(t, state, ctx, response.ExecutionID, "private-entry-exit", privateData)
	assertRuntimeOutput(t, state, ctx, response.ExecutionID, "public-entry-exit", publicData)
	all := state.GetAllOutputs(response.ExecutionID)
	assertOutputAbsent(t, all, "private-entry-exit")
	assertPublicOutput(t, all, "public-entry-exit", publicData)

	result, err := backend.WaitDone(ctx, response.ExecutionID)
	if err != nil {
		t.Fatalf("WaitDone() error = %v", err)
	}
	if result.Status != types.ExecutionStatusSuccess {
		t.Fatalf("WaitDone() status = %q, want success", result.Status)
	}
	assertOutputAbsent(t, result.Output, "private-entry-exit")
	assertPublicOutput(t, result.Output, "public-entry-exit", publicData)
}

func privateOutputNodeGraph(t *testing.T) *graph.Graph {
	t.Helper()
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name:  "private-output-node",
		Nodes: []types.NodeDef{{Name: "secret-node", Type: "test.secret"}},
	})
	if err != nil {
		t.Fatalf("Compile(node graph) error = %v", err)
	}
	return compiled
}

func privateOutputGroupGraph(t *testing.T) *graph.Graph {
	t.Helper()
	compiled, err := graph.Compile(&types.WorkflowDef{
		Name: "private-output-group",
		Nodes: []types.NodeDef{
			{Name: "entry", Kind: types.NodeKindTrigger},
			{Name: "worker", Kind: types.NodeKindAction},
		},
		Connections: types.Connections{
			"entry": {"main": {Targets: []types.Connection{{Node: "worker", Input: "main"}}}},
		},
		Groups: []types.GroupDef{{Name: "entry-group", Members: []string{"entry", "worker"}}},
	})
	if err != nil {
		t.Fatalf("Compile(group graph) error = %v", err)
	}
	return compiled
}

func privateOutputLease(id types.ExecutionID, name string, leaseID engine.LeaseID, token engine.LeaseToken, taskType engine.TaskType) *engine.TaskLease {
	return &engine.TaskLease{
		LeaseID: leaseID, LeaseToken: token, IssuedAt: time.Now().UTC(), TTL: time.Minute,
		Task: engine.Task{ExecutionID: id, NodeName: name, NodeIdx: 0, UnitIdx: 0, Type: taskType},
	}
}

func assertPrivateSnapshot(t *testing.T, state *memoryState, ctx context.Context, id types.ExecutionID, name string) {
	t.Helper()
	snap, err := state.GetNode(ctx, id, name)
	if err != nil {
		t.Fatalf("GetNode(%q) error = %v", name, err)
	}
	if snap == nil || !snap.PrivateOutput {
		t.Fatalf("GetNode(%q) private marker = %+v, want true", name, snap)
	}
	if snap.Output != nil {
		t.Fatalf("GetNode(%q) public output = %#v, want nil", name, snap.Output)
	}
}

func assertRuntimeOutput(t *testing.T, state *memoryState, ctx context.Context, id types.ExecutionID, name string, want map[string]any) {
	t.Helper()
	got, err := state.GetOutput(ctx, id, name)
	if err != nil {
		t.Fatalf("GetOutput(%q) error = %v", name, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetOutput(%q) = %#v, want %#v", name, got, want)
	}
}

func assertOutputAbsent(t *testing.T, outputs map[string]any, name string) {
	t.Helper()
	if got, found := outputs[name]; found {
		t.Fatalf("public outputs leaked %q: %#v", name, got)
	}
}

func assertPublicOutput(t *testing.T, outputs map[string]any, name string, want map[string]any) {
	t.Helper()
	got, found := outputs[name]
	if !found {
		t.Fatalf("public outputs missing %q: %#v", name, outputs)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("public output %q = %#v, want %#v", name, got, want)
	}
}
