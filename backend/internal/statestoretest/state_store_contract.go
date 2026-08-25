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
	"slices"
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
	runDurableSignalTOCTOU(t, state)
	runCancelSuspendedNode(t, state)
	runExecutionStatusAgreesWithSnapshot(t, state)
}

// runExecutionStatusAgreesWithSnapshot pins that the narrow status read and the
// full snapshot read never disagree.
//
// engine.executionActive prefers ExecutionStatusReader and falls back to
// GetExecution, so these two are interchangeable answers to one question: is
// this execution still active? If a backend ever answered differently through
// the two methods, whether a lease may be issued against a finished execution
// would depend on which method the engine happened to call — and since the
// preference is silent (a type assertion), that would not look like a bug at the
// call site.
//
// The terminal arm is the one that matters. A narrow read that fetched the
// wrong key, or a cached one, would still agree while the execution is Running;
// only the transition exposes it. The missing arm pins the other encoding:
// GetExecution says "no such execution" with a nil snapshot, and the narrow read
// must say it with found=false rather than an empty status that
// IsTerminalExecutionStatus would read as active.
func runExecutionStatusAgreesWithSnapshot(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()

	reader, ok := state.(engine.ExecutionStatusReader)
	if !ok {
		// Not a skip: both backends implement it, and a store that stopped
		// would silently fall back to GetExecution — correct, but eight Redis
		// round trips per activeness check instead of one, with nothing failing
		// to say so.
		t.Fatalf("%T does not implement engine.ExecutionStatusReader", state)
	}

	// Missing execution, checked first so it cannot be contaminated by the
	// writes below.
	if status, found, err := reader.GetExecutionStatus(ctx, "exec-contract-status-absent"); err != nil {
		t.Fatalf("GetExecutionStatus(absent) error = %v", err)
	} else if found {
		t.Errorf("GetExecutionStatus(absent) = (%q, true), want found=false. An "+
			"empty status with found=true is not terminal, so executionActive "+
			"would treat a nonexistent execution as live.", status)
	}
	if snap, err := state.GetExecution(ctx, "exec-contract-status-absent"); err != nil {
		t.Fatalf("GetExecution(absent) error = %v", err)
	} else if snap != nil {
		t.Errorf("GetExecution(absent) = %+v, want nil: the two methods must "+
			"encode absence the same way", snap)
	}

	id := types.ExecutionID("exec-contract-status-agreement")
	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// Running, then every terminal status the engine can leave behind. Cancel
	// and completion take different write paths, so agreement under one does
	// not imply agreement under the others.
	for _, want := range []types.ExecutionStatus{
		types.ExecutionStatusRunning,
		types.ExecutionStatusCanceled,
	} {
		if want != types.ExecutionStatusRunning {
			if err := state.UpdateExecutionStatus(ctx, id, want, ""); err != nil {
				t.Fatalf("UpdateExecutionStatus(%q) error = %v", want, err)
			}
		}
		status, found, err := reader.GetExecutionStatus(ctx, id)
		if err != nil {
			t.Fatalf("GetExecutionStatus(%q) error = %v", want, err)
		}
		snap, err := state.GetExecution(ctx, id)
		if err != nil {
			t.Fatalf("GetExecution(%q) error = %v", want, err)
		}
		if snap == nil {
			t.Fatalf("GetExecution returned nil for a created execution in %q", want)
		}
		if !found || status != snap.Status {
			t.Errorf("GetExecutionStatus = (%q, %v), GetExecution().Status = %q. "+
				"engine.executionActive uses whichever the backend implements, so "+
				"these must be the same answer.", status, found, snap.Status)
		}
		if status != want {
			t.Errorf("status = %q, want %q", status, want)
		}
	}
}

// runCancelSuspendedNode pins that a fenced cancel retires the waiter, not just
// the node status.
//
// engine.Cancel reads ListSuspendedNodes and cancels each name it returns, so
// the two calls are halves of one operation: whatever CancelSuspendedNode
// reports canceled must stop being reported as suspended. The memory backend
// flipped the node snapshot to Canceled and left the suspend registration in
// place, which meant ListSuspendedNodes kept naming a Canceled node and — since
// that same map is the index DeliverSignal and PeekResumeTarget scan — a later
// signal still found a waiter there and consumed itself against a node that can
// never run. The distributed backend SREMs the suspended set inside the same
// Lua transition.
//
// This was invisible to the memory backend's own test because that test drove
// the node into Suspended with UpsertNode alone. UpsertNode writes the node
// snapshot and nothing else, so s.suspended was empty the whole time and there
// was no stale entry left to find. Production parks a waiter with two writes;
// so does this test.
//
// Scope note: neither backend clears the per-signal waiter registration here.
// The distributed one defers that to cleanupOnCancel when the execution reaches
// Canceled, and engine.Cancel always gets there. That asymmetry is real but is
// not what this contract pins, because only one backend can honor it today.
func runCancelSuspendedNode(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract-cancel-suspended")

	canceler, ok := state.(engine.SuspendedNodeCanceler)
	if !ok {
		// Not a skip, for the same reason runDurableSignalTOCTOU does not skip:
		// both backends implement it, and a store that stopped would silently
		// drop this contract and fall back to engine.Cancel's unfenced path.
		t.Fatalf("%T does not implement engine.SuspendedNodeCanceler", state)
	}

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	const waiter = "finish"
	const signal = "cancel-me"
	idx, ok := ContractGraph().NodeIndex(waiter)
	if !ok {
		t.Fatalf("ContractGraph has no node %q", waiter)
	}
	// Both writes, in the order the engine makes them: the node commit records
	// Suspended, then the waiter is registered. Either one alone leaves a
	// backend with nothing for CancelSuspendedNode to act on.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID:  id,
		Name:         waiter,
		NodeIdx:      idx,
		Status:       types.NodeStatusSuspended,
		ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(suspended) error = %v", err)
	}
	if _, err := state.SuspendOrConsume(ctx, id, waiter,
		&types.SuspendSpec{Signals: []string{signal}}); err != nil {
		t.Fatalf("SuspendOrConsume() error = %v", err)
	}

	// Positive control. Without it a backend whose SuspendOrConsume registered
	// nothing satisfies the post-condition below for the wrong reason.
	before, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes(before) error = %v", err)
	}
	if !slices.Contains(before, waiter) {
		t.Fatalf("ListSuspendedNodes(before) = %v, want it to contain %q. The "+
			"waiter was never registered, so the assertion below would pass "+
			"against a cancel that removes nothing.", before, waiter)
	}

	canceled, err := canceler.CancelSuspendedNode(ctx, id, waiter)
	if err != nil {
		t.Fatalf("CancelSuspendedNode() error = %v", err)
	}
	if !canceled {
		t.Fatalf("CancelSuspendedNode(%q) = false, want true: the node is "+
			"Suspended and registered as a waiter", waiter)
	}

	after, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes(after) error = %v", err)
	}
	if slices.Contains(after, waiter) {
		t.Errorf("ListSuspendedNodes(after cancel) = %v, still contains %q. "+
			"CancelSuspendedNode reported the node canceled but left it "+
			"registered as suspended, so engine.Cancel's own source of truth "+
			"disagrees with the transition it just performed — and on a backend "+
			"where that registration is also the signal waiter index, a later "+
			"signal matches a node that can never run.", after, waiter)
	}
}

// runOutboxDeliveryLease pins the delivery lease on both backends.
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

	leaser, ok := atomic.(engine.OutboxLeaser)
	if !ok {
		t.Fatal("state store does not implement engine.OutboxLeaser")
	}

	now := time.Now().UTC()

	// Reading must not claim. Every probe, metric, and admin query goes through
	// ListOutbox; if reading took a lease, any of them would hide the entry from
	// the flush that has to deliver it.
	for i := 0; i < 3; i++ {
		read, err := atomic.ListOutbox(ctx, id, now, 16)
		if err != nil {
			t.Fatalf("ListOutbox(read %d) error = %v", i, err)
		}
		if len(read) != 1 || read[0].ID != entry.ID {
			t.Fatalf("ListOutbox(read %d) = %+v, want the seeded entry every time — "+
				"a read that claims starves the flush path", i, read)
		}
	}

	first, err := leaser.LeaseOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox() error = %v", err)
	}
	if len(first) != 1 || first[0].ID != entry.ID {
		t.Fatalf("LeaseOutbox() = %+v, want the single seeded entry", first)
	}

	// A second flush of the same execution must not see the in-flight entry.
	leased, err := leaser.LeaseOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(leased) error = %v", err)
	}
	if len(leased) != 0 {
		t.Fatalf("LeaseOutbox(leased) returned %d entries, want 0 — a concurrent "+
			"flush would redeliver work already in flight", len(leased))
	}

	// A leased entry is not ready, so the read path must not report it either.
	readLeased, err := atomic.ListOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("ListOutbox(leased) error = %v", err)
	}
	if len(readLeased) != 0 {
		t.Fatalf("ListOutbox(leased) returned %d entries, want 0 — an entry in "+
			"flight is not ready", len(readLeased))
	}

	// Renewal keeps a live deliverer's claim alive past the TTL, which is what
	// lets the TTL be short enough to recover from a crash quickly.
	renewAt := now.Add(engine.OutboxDeliveryLeaseTTL - time.Second)
	renewed, err := leaser.RenewOutbox(ctx, id, first, renewAt)
	if err != nil {
		t.Fatalf("RenewOutbox() error = %v", err)
	}
	if len(renewed) != 1 {
		t.Fatalf("RenewOutbox() granted %d entries, want 1 — a live deliverer "+
			"could not extend its own lease", len(renewed))
	}
	if renewed[0].LeaseDeadlineMs <= first[0].LeaseDeadlineMs {
		t.Fatalf("RenewOutbox() deadline = %d, want later than %d — without a "+
			"fresh deadline the next renewal presents a stale token and is refused",
			renewed[0].LeaseDeadlineMs, first[0].LeaseDeadlineMs)
	}
	stillHeld, err := leaser.LeaseOutbox(ctx, id, now.Add(engine.OutboxDeliveryLeaseTTL+time.Second), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(renewed) error = %v", err)
	}
	if len(stillHeld) != 0 {
		t.Fatalf("LeaseOutbox(renewed) returned %d entries, want 0 — a renewed "+
			"lease was stolen from a live deliverer, which redelivers its work",
			len(stillHeld))
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
	released, err := leaser.LeaseOutbox(ctx, id, now, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(released) error = %v", err)
	}
	if len(released) != 1 {
		t.Fatalf("LeaseOutbox(released) returned %d entries, want 1 — a released "+
			"entry must be deliverable immediately", len(released))
	}

	// Renewing a lapsed lease must NOT reclaim it. Once the lease lapses another
	// deliverer may legitimately claim the entry, and pulling it back would
	// deliver it twice — the duplication the lease exists to prevent. This is
	// the case the lease exists for: a deliverer that stalls, then comes back.
	lapsed := now.Add(2 * engine.OutboxDeliveryLeaseTTL)
	takenOver, err := leaser.LeaseOutbox(ctx, id, lapsed, 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(takeover) error = %v", err)
	}
	if len(takenOver) != 1 {
		t.Fatalf("LeaseOutbox(takeover) returned %d entries, want 1 — a lapsed "+
			"lease must be claimable by the next deliverer", len(takenOver))
	}
	staleRenew, err := leaser.RenewOutbox(ctx, id, released, now)
	if err != nil {
		t.Fatalf("RenewOutbox(lapsed) error = %v", err)
	}
	if len(staleRenew) != 0 {
		t.Fatalf("RenewOutbox(lapsed) granted %d entries, want 0 — the previous "+
			"holder was told it still owns an entry another deliverer took over",
			len(staleRenew))
	}
	afterStaleRenew, err := leaser.LeaseOutbox(ctx, id, lapsed.Add(time.Second), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(after stale renew) error = %v", err)
	}
	if len(afterStaleRenew) != 0 {
		t.Fatalf("LeaseOutbox(after stale renew) returned %d entries, want 0 — a "+
			"renewal from the previous holder pulled back an entry the current "+
			"deliverer owns", len(afterStaleRenew))
	}

	// And the lease lapses on its own, so a deliverer that dies mid-flight
	// does not strand its entries.
	expired, err := leaser.LeaseOutbox(ctx, id, lapsed.Add(2*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(expired) error = %v", err)
	}
	if len(expired) != 1 {
		t.Fatalf("LeaseOutbox(expired) returned %d entries, want 1 — a lapsed "+
			"lease must make the entry claimable again", len(expired))
	}

	// Ack is still the only removal path.
	if err := atomic.AckOutbox(ctx, id, entry.ID); err != nil {
		t.Fatalf("AckOutbox() error = %v", err)
	}
	acked, err := leaser.LeaseOutbox(ctx, id, lapsed.Add(4*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(acked) error = %v", err)
	}
	if len(acked) != 0 {
		t.Fatalf("LeaseOutbox(acked) returned %d entries, want 0", len(acked))
	}
	// Releasing an already-acked entry must not make it deliverable again.
	// This is the idempotence half only: a store may also self-heal a ready
	// entry whose body is gone, which would hide a missing guard from here.
	// TestRedisReleaseOutboxDoesNotResurrectAnAckedEntry inspects the index
	// directly for that.
	if err := releaser.ReleaseOutbox(ctx, id, first[0]); err != nil {
		t.Fatalf("ReleaseOutbox(acked) error = %v", err)
	}
	ghost, err := leaser.LeaseOutbox(ctx, id, lapsed.Add(4*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(ghost) error = %v", err)
	}
	if len(ghost) != 0 {
		t.Fatalf("LeaseOutbox(ghost) returned %d entries, want 0 — releasing an "+
			"acked entry redelivered it", len(ghost))
	}
	// Renewing an acked entry must not resurrect it either.
	if _, err := leaser.RenewOutbox(ctx, id, first, lapsed.Add(4*engine.OutboxDeliveryLeaseTTL)); err != nil {
		t.Fatalf("RenewOutbox(acked) error = %v", err)
	}
	renewGhost, err := leaser.LeaseOutbox(ctx, id, lapsed.Add(8*engine.OutboxDeliveryLeaseTTL), 16)
	if err != nil {
		t.Fatalf("LeaseOutbox(renew ghost) error = %v", err)
	}
	if len(renewGhost) != 0 {
		t.Fatalf("LeaseOutbox(renew ghost) returned %d entries, want 0 — renewing "+
			"an acked entry resurrected it", len(renewGhost))
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

// durableSignalOutboxLister is the slice of the atomic store this contract
// needs: DeliverSignalWithOutbox's whole point is that the resume intent lands
// in the outbox, so asserting on the outbox is the only way to tell "stored the
// signal" from "committed a resume".
type durableSignalOutboxLister interface {
	ListOutbox(ctx context.Context, id types.ExecutionID, before time.Time, limit int) ([]engine.OutboxEntry, error)
}

// runDurableSignalTOCTOU pins the guard for the window between the engine's
// PeekResumeTarget and its DeliverSignalWithOutbox.
//
// engine/signal.go peeks for a waiter, and when it finds none it passes a ZERO
// ResumeIntent to DeliverSignalWithOutbox. Another goroutine can suspend a node
// on the same signal in between, so the backend can be handed a live waiter and
// an intent that describes nothing. NodeIdx and UnitIdx are then 0 — not
// absent, but pointing at whatever unit 0 of the graph happens to be.
//
// A backend must not commit a resume it cannot construct. Storing the signal
// and leaving the waiter intact is recoverable: the next delivery peeks
// successfully, and the suspend timeout is still armed. Consuming the waiter
// and enqueueing a task with zero indices is not — the waiter is gone, so no
// later signal can wake it, and the task that replaced it names one node while
// its indices address another.
//
// The distributed backend guards this explicitly and calls it out in the Lua
// ("Guard FIRST, before tearing down any waiter state"), with a named
// regression test behind it. The memory backend had no equivalent and no test
// touching this method at all, which is the shape this contract exists to
// catch: a fix verified on one implementation of an interface and invisible on
// the other.
func runDurableSignalTOCTOU(t *testing.T, state engine.StateStore) {
	t.Helper()
	ctx := context.Background()
	id := types.ExecutionID("exec-contract-toctou")

	durable, ok := state.(engine.DurableSignalDeliverer)
	if !ok {
		// Not a skip. Both backends implement this interface; a store that
		// stopped doing so would silently drop this contract, which is exactly
		// how the gap being closed here opened.
		t.Fatalf("%T does not implement engine.DurableSignalDeliverer", state)
	}
	lister, ok := state.(durableSignalOutboxLister)
	if !ok {
		t.Fatalf("%T does not implement ListOutbox", state)
	}

	if err := state.CreateExecution(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
	}); err != nil {
		t.Fatalf("CreateExecution() error = %v", err)
	}

	// "finish" is deliberately not unit 0. A zero ResumeIntent addresses unit 0,
	// so suspending unit 0 would make the wrong indices coincidentally right and
	// the assertion below unable to tell the two backends apart.
	const waiter = "finish"
	const signal = "toctou"
	if _, err := state.SuspendOrConsume(ctx, id, waiter,
		&types.SuspendSpec{Signals: []string{signal}}); err != nil {
		t.Fatalf("SuspendOrConsume() error = %v", err)
	}
	before, err := lister.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox(before) error = %v", err)
	}

	// The racing delivery: a live waiter, and the empty intent the engine builds
	// when its peek ran a moment too early.
	node, _, committed, err := durable.DeliverSignalWithOutbox(ctx, id, signal,
		map[string]any{"by": "racer"}, engine.ResumeIntent{})
	if err != nil {
		t.Fatalf("DeliverSignalWithOutbox(empty intent) error = %v", err)
	}
	if committed || node != "" {
		t.Errorf("DeliverSignalWithOutbox(empty intent) = (%q, committed=%v), want "+
			"(\"\", false): the backend committed a resume from an intent that "+
			"carries no node, so the outbox entry's NodeIdx/UnitIdx are 0 while its "+
			"NodeName is %q", node, committed, waiter)
	}

	after, err := lister.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox(after) error = %v", err)
	}
	if len(after) != len(before) {
		t.Errorf("outbox grew from %d to %d entries on a delivery that could not "+
			"build a valid resume; entries: %+v", len(before), len(after), after)
	}

	suspended, err := state.ListSuspendedNodes(ctx, id)
	if err != nil {
		t.Fatalf("ListSuspendedNodes() error = %v", err)
	}
	var stillWaiting bool
	for _, n := range suspended {
		if n == waiter {
			stillWaiting = true
		}
	}
	if !stillWaiting {
		t.Fatalf("%q is no longer suspended after a delivery that committed nothing "+
			"(suspended = %v). The waiter was torn down without a resume replacing "+
			"it, so no later signal can wake it and only the execution TTL clears it.",
			waiter, suspended)
	}

	// Positive control. Without it every assertion above is satisfied by a
	// backend whose DeliverSignalWithOutbox does nothing at all.
	idx, ok := ContractGraph().NodeIndex(waiter)
	if !ok {
		t.Fatalf("ContractGraph has no node %q", waiter)
	}
	unit := ContractGraph().UnitIndexForNode(idx)
	// The returned payload is deliberately NOT asserted. The interface reads as
	// if it were part of the result, but the distributed backend always returns
	// nil (state_suspend.go ends `return nodeName, nil, true, nil`) and puts the
	// payload only inside the outbox entry, while the memory backend returns it.
	// The sole production caller, engine/signal.go, discards the value. Requiring
	// either shape here would pin one backend's incidental behaviour as contract
	// for something nothing reads.
	node, _, committed, err = durable.DeliverSignalWithOutbox(ctx, id, signal,
		map[string]any{"by": "lead"},
		engine.ResumeIntent{NodeName: waiter, NodeIdx: idx, UnitIdx: unit})
	if err != nil {
		t.Fatalf("DeliverSignalWithOutbox(valid intent) error = %v", err)
	}
	if !committed || node != waiter {
		t.Fatalf("DeliverSignalWithOutbox(valid intent) = (%q, committed=%v), want "+
			"(%q, true) -- the waiter did not survive the racing delivery in a "+
			"usable state, so the assertions above prove nothing",
			node, committed, waiter)
	}
	final, err := lister.ListOutbox(ctx, id, time.Now().Add(time.Hour), 16)
	if err != nil {
		t.Fatalf("ListOutbox(final) error = %v", err)
	}
	if len(final) != len(before)+1 {
		t.Fatalf("outbox entries after the valid delivery = %d, want %d: the resume "+
			"the contract says was committed is not in the outbox", len(final), len(before)+1)
	}
}
