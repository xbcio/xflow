package contract

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/backend/memory"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

func TestMemoryStateStoreContract(t *testing.T) {
	state := memory.New().State()
	runStateStoreContract(t, state)
}

func runStateStoreContract(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract")
	g := contractGraph()

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:      id,
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		Params:  map[string]any{"claim_id": "c-1"},
		Runtime: &types.Runtime{Vars: map[string]any{"tenant_id": "tenant-a"}},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap.Runtime == nil || snap.Runtime.Vars["tenant_id"] != "tenant-a" {
		t.Fatalf("Runtime = %#v, want tenant_id tenant-a", snap.Runtime)
	}

	loaded, err := state.LoadGraph(ctx, id)
	if err != nil {
		t.Fatalf("LoadGraph() error = %v", err)
	}
	if loaded == nil || loaded.Name != "contract" {
		t.Fatalf("LoadGraph() = %+v, want graph named contract", loaded)
	}

	started := &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusRunning,
	}
	if err := state.UpsertNode(ctx, started); err != nil {
		t.Fatalf("UpsertNode(running) error = %v", err)
	}

	done := &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "start",
		NodeIdx:     0,
		Status:      types.NodeStatusSuccess,
		Output:      map[string]any{"ok": true},
		Port:        "main",
	}
	if err := state.UpsertNode(ctx, done); err != nil {
		t.Fatalf("UpsertNode(success) error = %v", err)
	}
	if err := state.UpsertNode(ctx, started); err != nil {
		t.Fatalf("UpsertNode(running after terminal) error = %v", err)
	}
	ns, err := state.GetNode(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetNode() error = %v", err)
	}
	if ns == nil || ns.Status != types.NodeStatusSuccess {
		t.Fatalf("terminal node overwritten: %+v", ns)
	}

	lease := &engine.TaskLease{
		LeaseToken: engine.LeaseToken("token-1"),
		Task: engine.Task{
			ExecutionID: id,
			NodeName:    "finish",
			NodeIdx:     1,
		},
	}
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: id,
		Name:        "finish",
		NodeIdx:     1,
		Status:      types.NodeStatusRunning,
		LeaseToken:  lease.LeaseToken,
	}); err != nil {
		t.Fatalf("UpsertNode(leased running) error = %v", err)
	}
	claimed, valid, err := state.ClaimTaskLease(ctx, lease)
	if err != nil || !valid {
		t.Fatalf("ClaimTaskLease(valid) = (%+v, %v, %v), want valid claim", claimed, valid, err)
	}
	if claimed.Status != types.NodeStatusCommitting {
		t.Fatalf("claimed status = %q, want committing", claimed.Status)
	}
	if claimed.LeaseToken != lease.LeaseToken {
		t.Fatalf("claimed lease token = %q, want retained token %q for recovery fencing", claimed.LeaseToken, lease.LeaseToken)
	}
	_, valid, err = state.ClaimTaskLease(ctx, lease)
	if err != nil || valid {
		t.Fatalf("ClaimTaskLease(duplicate) valid = %v, err = %v; want false, nil", valid, err)
	}

	if err := state.PutOutput(ctx, id, "start", map[string]any{"ok": true}); err != nil {
		t.Fatalf("PutOutput() error = %v", err)
	}
	out, err := state.GetOutput(ctx, id, "start")
	if err != nil {
		t.Fatalf("GetOutput() error = %v", err)
	}
	if out["ok"] != true {
		t.Fatalf("GetOutput()[ok] = %v, want true", out["ok"])
	}

	remaining, active, err := state.DecrementInDegree(ctx, id, 1, true)
	if err != nil {
		t.Fatalf("DecrementInDegree() error = %v", err)
	}
	if remaining != 0 || active != 1 {
		t.Fatalf("DecrementInDegree() = (%d, %d), want (0, 1)", remaining, active)
	}

	resume, payload, err := state.DeliverSignal(ctx, id, "approval", map[string]any{"by": "lead"})
	if err != nil {
		t.Fatalf("DeliverSignal(pre) error = %v", err)
	}
	if resume != "" || payload != nil {
		t.Fatalf("pre-delivered signal = (%q, %+v), want stored", resume, payload)
	}
	payload, err = state.SuspendOrConsume(ctx, id, "approve", &types.SuspendSpec{Signals: []string{"approval"}})
	if err != nil {
		t.Fatalf("SuspendOrConsume() error = %v", err)
	}
	if payload == nil || payload.Name != "approval" || payload.Data["by"] != "lead" {
		t.Fatalf("SuspendOrConsume() payload = %+v", payload)
	}

	acquired, err := state.AcquireResumeLock(ctx, id, "approve")
	if err != nil || !acquired {
		t.Fatalf("AcquireResumeLock(first) = (%v, %v), want true, nil", acquired, err)
	}
	acquired, err = state.AcquireResumeLock(ctx, id, "approve")
	if err != nil || acquired {
		t.Fatalf("AcquireResumeLock(second) = (%v, %v), want false, nil", acquired, err)
	}

	events, err := state.WatchExecution(ctx, id)
	if err != nil {
		t.Fatalf("WatchExecution() error = %v", err)
	}
	wantEvent := engine.ExecutionEvent{ExecutionID: id, Status: types.ExecutionStatusSuccess}
	if err := state.PublishExecutionEvent(ctx, wantEvent); err != nil {
		t.Fatalf("PublishExecutionEvent() error = %v", err)
	}
	select {
	case got := <-events:
		if got.ExecutionID != wantEvent.ExecutionID || got.Status != wantEvent.Status {
			t.Fatalf("event = %+v, want %+v", got, wantEvent)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for execution event")
	}
}

func contractGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "contract",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "finish", Type: "test.finish"},
		},
		Connections: types.Connections{
			"start": {"main": []types.Connection{{Node: "finish", Input: "main"}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}
