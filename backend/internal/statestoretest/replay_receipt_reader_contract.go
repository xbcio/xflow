package statestoretest

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// ReplayReceiptReaderStore is a backend that stores authoritative replay
// receipts (through the durable-outbox replay path) and can scan them through
// the optional engine.ReplayReceiptReader capability.
type ReplayReceiptReaderStore interface {
	engine.StateStore
	engine.AtomicStateStore
	engine.OutboxFailureRecorder
	engine.DeadLetterStore
	engine.ReplayReceiptReader
}

// ReplayReceiptReaderBackend is one fresh backend under test. WantNamespace is
// the Namespace every scanned receipt must carry.
//
// The two backends legitimately differ here: the Redis backend names the
// execution's namespace (its keys are namespace-prefixed), while the local
// backend has no namespace dimension at all — its ReplayDeadLetterRequest
// carries none — and must report the empty string. A fabricated name would be
// projected into the durable SQL audit row as if the operator's replay had been
// made under that tenant.
type ReplayReceiptReaderBackend struct {
	Store         ReplayReceiptReaderStore
	WantNamespace string
}

// RunReplayReceiptReaderContract pins the engine.ReplayReceiptReader contract
// every backend must share: a full scan hands the caller every receipt the
// backend stored — exactly once, in namespace-then-execution (sorted-key)
// order, carrying the correlation fields the SQL projector needs — and an fn
// error aborts the scan unchanged.
//
// The contract drives the receipts through the PUBLIC replay path rather than
// seeding backend internals, so what the scan returns is exactly what a real
// operator's replays left behind.
//
// A decidable rejection is driven on purpose: the replay-writer contract
// requires every decidable rejection to leave a receipt too — both backends
// write one — so the scan must hand it back exactly like a successful
// replay's. An operator's audit trail has to show what was refused, not only
// what was done.
func RunReplayReceiptReaderContract(t *testing.T, newBackend func(*testing.T) ReplayReceiptReaderBackend) {
	t.Helper()
	ctx := context.Background()

	b := newBackend(t)
	if b.Store == nil {
		t.Fatal("newBackend returned a nil store")
	}

	entry := func(id types.ExecutionID, node string) engine.OutboxEntry {
		return engine.OutboxEntry{
			ID: "execute/" + string(id) + "/" + node + "/1",
			Task: engine.Task{
				ExecutionID: id, NodeName: node, NodeIdx: 1,
				Type: engine.TaskTypeNodeExec, ActivationID: 1,
			},
		}
	}
	// seed creates a running execution with two outbox entries ("execute"
	// intents at nodes finish/verify, both running at activation 1) and drives
	// each delivery to failure until it is dead-lettered, so both entries are
	// replayable.
	seed := func(t *testing.T, id types.ExecutionID) (finish, verify engine.OutboxEntry) {
		t.Helper()
		finish, verify = entry(id, "finish"), entry(id, "verify")
		if err := b.Store.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
			ID: id, Graph: ContractGraph(), Status: types.ExecutionStatusRunning,
		}, []engine.OutboxEntry{finish, verify}); err != nil {
			t.Fatalf("CreateExecutionWithOutbox(%s): %v", id, err)
		}
		for _, name := range []string{"finish", "verify"} {
			if err := b.Store.UpsertNode(ctx, &engine.NodeSnapshot{
				ExecutionID: id, Name: name, NodeIdx: 1,
				Status: types.NodeStatusRunning, ActivationID: 1,
			}); err != nil {
				t.Fatalf("UpsertNode(%s/%s): %v", id, name, err)
			}
		}
		for _, e := range []engine.OutboxEntry{finish, verify} {
			for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
				if _, err := b.Store.RecordOutboxFailure(ctx, id, e, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
					t.Fatalf("RecordOutboxFailure(%s)[%d]: %v", e.ID, i, err)
				}
			}
		}
		dead, err := b.Store.ListDeadLetters(ctx, id, engine.DeadLetterPage{Limit: 10})
		if err != nil {
			t.Fatalf("ListDeadLetters(%s): %v", id, err)
		}
		if len(dead.Entries) != 2 {
			t.Fatalf("dead letters for %s = %+v, want 2", id, dead.Entries)
		}
		return finish, verify
	}
	replay := func(t *testing.T, id types.ExecutionID, entryID, requestID string) engine.ReplayDeadLetterResult {
		t.Helper()
		res, err := b.Store.ReplayDeadLetter(ctx, engine.ReplayDeadLetterRequest{
			ExecutionID: id, EntryID: entryID, RequestID: requestID,
			Operator: "contract:tester", Reason: "contract replay",
		})
		if err != nil {
			t.Fatalf("ReplayDeadLetter(%s/%s): %v", id, requestID, err)
		}
		return res
	}
	scan := func(t *testing.T) []engine.ReplayReceipt {
		t.Helper()
		var got []engine.ReplayReceipt
		if err := b.Store.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
			got = append(got, r)
			return nil
		}); err != nil {
			t.Fatalf("ScanReplayReceipts: %v", err)
		}
		return got
	}

	execA := types.ExecutionID("receipt-contract-a")
	execB := types.ExecutionID("receipt-contract-b")
	finishA, verifyA := seed(t, execA)
	finishB, verifyB := seed(t, execB)

	// Two receipts in one execution and one in a second execution pin the
	// execution-then-request ordering; a decidable rejection (terminal node)
	// pins that rejections leave receipts too.
	replayedA1 := replay(t, execA, finishA.ID, "req-01")
	replayedA2 := replay(t, execA, verifyA.ID, "req-02")
	replayedB1 := replay(t, execB, finishB.ID, "req-01")
	for _, res := range []engine.ReplayDeadLetterResult{replayedA1, replayedA2, replayedB1} {
		if res.Outcome != engine.ReplayReplayed {
			t.Fatalf("replay outcome = %q, want replayed (all three seeds must land)", res.Outcome)
		}
		if res.AuditID == "" {
			t.Fatalf("replayed result %+v carries no AuditID; a successful replay must write a receipt", res)
		}
	}
	if err := b.Store.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: execB, Name: "verify", NodeIdx: 1,
		Status: types.NodeStatusSuccess, ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(terminal verify): %v", err)
	}
	rejectedB2 := replay(t, execB, verifyB.ID, "req-02")
	if rejectedB2.Outcome != engine.ReplayRejectedNodeTerminal {
		t.Fatalf("terminal-node replay outcome = %q, want rejected_node_terminal", rejectedB2.Outcome)
	}
	if rejectedB2.AuditID == "" {
		t.Fatalf("rejected result %+v carries no AuditID; a decidable rejection must leave a receipt too", rejectedB2)
	}

	type wantReceipt struct {
		exec       string
		requestID  string
		auditID    string
		node       string
		activation string
		entryID    string
		outcome    engine.DeadLetterReplayOutcome
	}
	wants := []wantReceipt{
		{string(execA), "req-01", replayedA1.AuditID, "finish", "1", finishA.ID, engine.ReplayReplayed},
		{string(execA), "req-02", replayedA2.AuditID, "verify", "1", verifyA.ID, engine.ReplayReplayed},
		{string(execB), "req-01", replayedB1.AuditID, "finish", "1", finishB.ID, engine.ReplayReplayed},
		{string(execB), "req-02", rejectedB2.AuditID, "verify", "1", verifyB.ID, engine.ReplayRejectedNodeTerminal},
	}

	got := scan(t)
	// A second scan must reproduce the first: map-iteration order would pass a
	// single scan by luck and reorder the projector's work on the next run.
	again := scan(t)
	if len(got) != len(again) {
		t.Fatalf("two scans returned %d and %d receipts; the scan must be deterministic", len(got), len(again))
	}
	for i := range got {
		if got[i] != again[i] {
			t.Fatalf("scan[%d] differs between runs: %+v vs %+v", i, got[i], again[i])
		}
	}

	// Every replay that decided — success or decidable rejection — must appear,
	// each once, in (execution, request) order.
	var wantOrder []string
	seen := make(map[string]engine.ReplayReceipt, len(got))
	var gotOrder []string
	for _, r := range got {
		key := r.ExecutionID + "/" + r.RequestID
		if _, dup := seen[key]; dup {
			t.Fatalf("receipt %s was emitted twice", key)
		}
		seen[key] = r
		gotOrder = append(gotOrder, key)
	}
	for _, w := range wants {
		wantOrder = append(wantOrder, w.exec+"/"+w.requestID)
	}
	if !slices.Equal(gotOrder, wantOrder) {
		t.Fatalf("scan order = %v, want %v\n(interface: namespace-then-execution / sorted-key order)", gotOrder, wantOrder)
	}

	for _, w := range wants {
		key := w.exec + "/" + w.requestID
		r, ok := seen[key]
		if !ok {
			t.Fatalf("receipt %s missing from the scan (its replay carried audit id %q)", key, w.auditID)
		}
		if r.Namespace != b.WantNamespace {
			t.Fatalf("%s: Namespace = %q, want %q", key, r.Namespace, b.WantNamespace)
		}
		if r.AuditID != w.auditID {
			t.Fatalf("%s: AuditID = %q, want %q (the replay result's id — the SQL idempotency key)", key, r.AuditID, w.auditID)
		}
		if r.NodeID != w.node || r.ActivationID != w.activation {
			t.Fatalf("%s: node/activation = %q/%q, want %q/%q", key, r.NodeID, r.ActivationID, w.node, w.activation)
		}
		if r.Outcome != w.outcome {
			t.Fatalf("%s: Outcome = %q, want %q", key, r.Outcome, w.outcome)
		}
		if r.EntryID != w.entryID {
			t.Fatalf("%s: EntryID = %q, want %q", key, r.EntryID, w.entryID)
		}
		if r.Operator != "contract:tester" || r.Reason != "contract replay" {
			t.Fatalf("%s: operator/reason = %q/%q, want contract:tester/contract replay", key, r.Operator, r.Reason)
		}
		if r.TimestampMs <= 0 {
			t.Fatalf("%s: TimestampMs = %d, want the replay wall-clock time: a zero makes the "+
				"projector stamp the durable row with reconcile time instead", key, r.TimestampMs)
		}
	}

	// fn's error aborts the scan unchanged, so a projector can stop at the
	// first receipt it cannot project.
	sentinel := errors.New("receipt scan aborted by test")
	calls := 0
	if err := b.Store.ScanReplayReceipts(ctx, func(engine.ReplayReceipt) error {
		calls++
		return sentinel
	}); err != sentinel {
		t.Fatalf("scan error = %v, want the fn error propagated unchanged", err)
	}
	if calls != 1 {
		t.Fatalf("fn calls before abort = %d, want 1", calls)
	}
}
