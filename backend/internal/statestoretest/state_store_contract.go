// Package statestoretest is a test-only support package providing shared
// StateStore contract and concurrency test suites so every backend
// implementation (memory, distributed, ...) can be validated against the same
// assertions, catching semantic drift between backends. It is deliberately
// placed under internal/ and named with the conventional xxxtest suffix: the
// non-_test.go files here import "testing" on purpose, and internal/ prevents
// accidental use from production code.
package statestoretest

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/engine/graph"
	"github.com/xbcio/xflow/types"
)

// RunStateStoreContract exercises the StateStore contract (create → load graph
// → node terminal protection → lease claim → output → in-degree → signal
// suspend/consume → resume lock → pub/sub) against a concrete backend. Every
// backend implementation should run this so semantic drift between e.g. the
// in-memory and Redis/Lua backends is caught by a shared assertion.
func RunStateStoreContract(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract")
	g := ContractGraph()

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:      id,
		Graph:   g,
		Status:  types.ExecutionStatusRunning,
		Params:  map[string]any{"claim_id": "c-1"},
		Runtime: &types.Runtime{Vars: map[string]any{"namespace_id": "namespace-a"}},
		Scope:   map[string]any{"$index": float64(3)},
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}
	snap, err := state.GetExecution(ctx, id)
	if err != nil {
		t.Fatalf("GetExecution() error = %v", err)
	}
	if snap.Runtime == nil || snap.Runtime.Vars["namespace_id"] != "namespace-a" {
		t.Fatalf("Runtime = %#v, want namespace_id namespace-a", snap.Runtime)
	}
	// Scope holds the execution-wide expression roots (a map body's
	// $item/$index/$items). buildInput merges them into EVERY node's Data, so a
	// backend that drops them on the round trip silently un-fixes the
	// non-entry-member defect. float64 is the value asserted because a JSON
	// backend decodes any number as one; asserting an int would pass on the
	// memory backend and fail on Redis for a reason unrelated to the contract.
	if snap.Scope["$index"] != float64(3) {
		t.Fatalf("Scope = %#v, want $index 3", snap.Scope)
	}

	loaded, err := state.LoadGraph(ctx, id)
	if err != nil {
		t.Fatalf("LoadGraph() error = %v", err)
	}
	if loaded == nil || loaded.Name() != "contract" {
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
	// The active lease-acquisition path (AcquireTaskLease) writes the token
	// into the node meta hash atomically; ClaimTaskLease then retains it for
	// crash recovery fencing. Here the lease was set via UpsertNode, which is
	// the snapshot/recovery path, not the active acquisition path — backends
	// differ on whether UpsertNode persists the token into meta. Assert only
	// that the claim is valid and transitions to committing; token-retention
	// through AcquireTaskLease is covered by the concurrency suite.
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

	runExecutionErrorRoundTrip(t, state)
	runOutboxDeliveryLease(t, state)
}

// runOutboxDeliveryLease pins the ListOutbox delivery lease on both backends.
//
// Without it every worker that flushes the same execution lists the same
// un-acked entries and delivers them again: a 800-item map at concurrency 4 ran
// its body 826–1110 times. The lease is what makes ordinary concurrency stop
// producing duplicates, while leaving the at-least-once contract intact for the
// cases it exists for — a deliverer that dies still has its entries relisted
// once the lease lapses.
//
// It drives the store directly because an end-to-end fan-out cannot distinguish
// "leased, then redelivered on expiry" from "never leased at all".
func runOutboxDeliveryLease(t *testing.T, state engine.StateStore) {
	t.Helper()
	atomic, ok := state.(engine.AtomicStateStore)
	if !ok {
		t.Fatal("state store does not implement AtomicStateStore")
	}
	ctx := context.Background()

	id := types.ExecutionID("exec-contract-outbox-lease")
	g := ContractGraph()
	idx, _ := g.NodeIndex("start")
	entry := engine.OutboxEntry{
		ID: "root/exec-contract-outbox-lease/start/1",
		Task: engine.Task{
			ExecutionID: id, NodeName: "start", NodeIdx: idx,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}
	if err := atomic.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: g, Status: types.ExecutionStatusRunning,
	}, []engine.OutboxEntry{entry}); err != nil {
		t.Fatalf("CreateExecutionWithOutbox() error = %v", err)
	}

	now := time.Now().UTC()
	first, err := atomic.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox() error = %v", err)
	}
	if len(first) != 1 || first[0].ID != entry.ID {
		t.Fatalf("ListOutbox() = %+v, want the single seeded entry", first)
	}

	// A second flush of the same execution must not see the in-flight entry.
	leased, err := atomic.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox(leased) error = %v", err)
	}
	if len(leased) != 0 {
		t.Fatalf("ListOutbox(leased) returned %d entries, want 0 — a concurrent "+
			"flush would redeliver work already in flight", len(leased))
	}

	// Releasing hands it straight back: backpressure and a failed handoff are
	// both retry-now conditions and must not wait out the visibility timeout.
	releaser, ok := atomic.(engine.OutboxReleaser)
	if !ok {
		t.Fatal("a store that leases must implement engine.OutboxReleaser")
	}
	if err := releaser.ReleaseOutbox(ctx, id, first[0]); err != nil {
		t.Fatalf("ReleaseOutbox() error = %v", err)
	}
	released, err := atomic.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox(released) error = %v", err)
	}
	if len(released) != 1 {
		t.Fatalf("ListOutbox(released) returned %d entries, want 1 — a released "+
			"entry must be deliverable immediately", len(released))
	}

	// And the lease lapses on its own, so a deliverer that dies mid-flight
	// does not strand its entries.
	expired, err := atomic.ListOutbox(ctx, id, now.Add(engine.OutboxDeliveryLeaseTTL+time.Second), 16)
	if err != nil {
		t.Fatalf("ListOutbox(expired) error = %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("ListOutbox(expired) returned %d entries, want 1 — a lapsed "+
			"lease must make the entry listable again", len(expired))
	}

	// Ack is still the only removal path.
	if err := atomic.AckOutbox(ctx, id, entry.ID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}
	acked, err := atomic.ListOutbox(ctx, id, now.Add(2*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("ListOutbox(acked) error = %v", err)
	}
	if len(acked) != 0 {
		t.Fatalf("ListOutbox(acked) returned %d entries, want 0", len(acked))
	}
	// Releasing an already-acked entry must not make it deliverable again.
	// This is the idempotence half only: a store may also self-heal a ready
	// entry whose body is gone, which would hide a missing guard from here.
	// TestRedisReleaseOutboxDoesNotResurrectAnAckedEntry inspects the index
	// directly for that.
	if err := releaser.ReleaseOutbox(ctx, id, first[0]); err != nil {
		t.Fatalf("ReleaseOutbox(acked) error = %v", err)
	}
	ghost, err := atomic.ListOutbox(ctx, id, now.Add(2*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("ListOutbox(ghost) error = %v", err)
	}
	if len(ghost) != 0 {
		t.Fatalf("ListOutbox(ghost) returned %d entries, want 0 — releasing an "+
			"acked entry redelivered it", len(ghost))
	}
}

// runExecutionErrorRoundTrip pins the execution-level failure reason on the
// snapshot round trip. It uses a fresh execution because the contract's main
// one is already driven through a success event above.
//
// This is the only carrier when no node holds the reason: a cyclic execution
// that trips MaxAutoDepth fails with every node at success, because the engine
// rejects the downstream activation rather than failing the node that tripped
// it. A backend that persists the reason but never loads it back leaves the SQL
// audit row as the sole readback, invisible to callers of the inspect API.
func runExecutionErrorRoundTrip(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()

	const reason = "max auto execution depth exceeded"
	failed := types.ExecutionID("exec-contract-error")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     failed,
		Graph:  ContractGraph(),
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(error round trip) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, failed, types.ExecutionStatusFailed, reason); err != nil {
		t.Fatalf("UpdateExecutionStatus(failed) error = %v", err)
	}
	snap, err := state.GetExecution(ctx, failed)
	if err != nil {
		t.Fatalf("GetExecution(failed) error = %v", err)
	}
	if snap == nil {
		t.Fatal("GetExecution(failed) = nil")
	}
	if snap.Error != reason {
		t.Fatalf("ExecutionSnapshot.Error = %q, want %q — the stored failure reason is not readable online", snap.Error, reason)
	}

	// Negative half: a success must carry no reason, or a backend that
	// unconditionally echoes the argument would pass the assertion above.
	ok := types.ExecutionID("exec-contract-error-ok")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID:     ok,
		Graph:  ContractGraph(),
		Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution(success round trip) error = %v", err)
	}
	if err := state.UpdateExecutionStatus(ctx, ok, types.ExecutionStatusSuccess, ""); err != nil {
		t.Fatalf("UpdateExecutionStatus(success) error = %v", err)
	}
	snap, err = state.GetExecution(ctx, ok)
	if err != nil {
		t.Fatalf("GetExecution(success) error = %v", err)
	}
	if snap == nil {
		t.Fatal("GetExecution(success) = nil")
	}
	if snap.Error != "" {
		t.Fatalf("ExecutionSnapshot.Error = %q on a successful execution, want empty", snap.Error)
	}
}

// ContractGraph returns the two-node graph used by RunStateStoreContract.
func ContractGraph() *graph.Graph {
	def := &types.WorkflowDef{
		Name: "contract",
		Nodes: []types.NodeDef{
			{Name: "start", Type: "test.start"},
			{Name: "finish", Type: "test.finish"},
		},
		Connections: types.Connections{
			"start": {"main": types.PortConnections{Targets: []types.Connection{{Node: "finish", Input: "main"}}}},
		},
	}
	g, err := graph.Compile(def)
	if err != nil {
		panic(err)
	}
	return g
}
