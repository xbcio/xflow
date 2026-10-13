package local

import (
	"context"
	"errors"
	"testing"

	"github.com/xbcio/xflow/backend/internal/statestoretest"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// seedReceiptReaderExecution creates a running execution with two outbox
// entries, both driven to failure until they are dead-lettered, shaped exactly
// like the cross-backend receipt contract seeds them: "execute" intent entries
// at nodes "finish" and "verify" activation 1, with both nodes running, so the
// same fixture is replayable on the Redis backend's stricter guards too.
// Returns (finishEntryID, verifyEntryID).
func seedReceiptReaderExecution(t *testing.T, state *memoryState, id types.ExecutionID) (string, string) {
	t.Helper()
	ctx := context.Background()
	entries := []engine.OutboxEntry{
		receiptReaderEntry(id, "finish"),
		receiptReaderEntry(id, "verify"),
	}
	if err := state.CreateExecutionWithOutbox(ctx, &engine.ExecutionSnapshot{
		ID: id, Graph: statestoretest.ContractGraph(), Status: types.ExecutionStatusRunning,
	}, entries); err != nil {
		t.Fatalf("CreateExecutionWithOutbox: %v", err)
	}
	for _, name := range []string{"finish", "verify"} {
		if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
			ExecutionID: id, Name: name, NodeIdx: 1,
			Status: types.NodeStatusRunning, ActivationID: 1,
		}); err != nil {
			t.Fatalf("UpsertNode(%s): %v", name, err)
		}
	}
	for _, e := range entries {
		deadLetterReceiptEntry(t, state, e)
	}
	return entries[0].ID, entries[1].ID
}

func receiptReaderEntry(id types.ExecutionID, node string) engine.OutboxEntry {
	return engine.OutboxEntry{
		ID: "execute/" + string(id) + "/" + node + "/1",
		Task: engine.Task{
			ExecutionID: id, NodeName: node, NodeIdx: 1,
			Type: engine.TaskTypeNodeExec, ActivationID: 1,
		},
	}
}

// deadLetterReceiptEntry drives the entry's delivery to failure until it lands
// in dead-letter storage.
func deadLetterReceiptEntry(t *testing.T, state *memoryState, entry engine.OutboxEntry) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < engine.DefaultOutboxMaxDeliveryAttempts; i++ {
		if _, err := state.RecordOutboxFailure(ctx, entry.Task.ExecutionID, entry, engine.DefaultOutboxMaxDeliveryAttempts); err != nil {
			t.Fatalf("RecordOutboxFailure[%d]: %v", i, err)
		}
	}
	dead, err := state.ListDeadLetters(ctx, entry.Task.ExecutionID, engine.DeadLetterPage{Limit: 10})
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	found := false
	for _, d := range dead.Entries {
		found = found || d.ID == entry.ID
	}
	if !found {
		t.Fatalf("dead letters = %+v, want %q among them", dead.Entries, entry.ID)
	}
}

// replayReceiptEntry replays one entry and requires a replayed outcome,
// returning the result whose AuditID is the receipt's idempotency key.
func replayReceiptEntry(t *testing.T, state *memoryState, id types.ExecutionID, entryID, requestID string) engine.ReplayDeadLetterResult {
	t.Helper()
	res, err := state.ReplayDeadLetter(context.Background(), engine.ReplayDeadLetterRequest{
		ExecutionID: id, EntryID: entryID, RequestID: requestID,
		Operator: "cli:tester", Reason: "operator replay after root-cause",
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetter(%s): %v", requestID, err)
	}
	if res.Outcome != engine.ReplayReplayed {
		t.Fatalf("ReplayDeadLetter(%s) outcome = %q, want replayed", requestID, res.Outcome)
	}
	return res
}

// TestMemoryScanReplayReceiptsCarriesEveryReceiptInOrder is the local
// implementation of the engine.ReplayReceiptReader contract: every receipt the
// backend stored — a successful replay's and a decidable rejection's alike,
// since every decided replay leaves one — is handed to fn exactly once,
// carrying the correlation fields the SQL projector needs, in
// execution-then-request order (the local mirror of the Redis reader's
// sorted-key order), and with the empty namespace the local backend's absent
// namespace dimension must produce.
func TestMemoryScanReplayReceiptsCarriesEveryReceiptInOrder(t *testing.T) {
	ctx := context.Background()
	state := newMemoryState()
	execA := types.ExecutionID("receipt-scan-a")
	execB := types.ExecutionID("receipt-scan-b")
	finishA, verifyA := seedReceiptReaderExecution(t, state, execA)
	finishB, verifyB := seedReceiptReaderExecution(t, state, execB)

	first := replayReceiptEntry(t, state, execA, finishA, "req-01")
	second := replayReceiptEntry(t, state, execA, verifyA, "req-02")
	other := replayReceiptEntry(t, state, execB, finishB, "req-01")
	// audit_id is "<requestID>:<millis>" on every backend, so two executions
	// replayed in the same millisecond legitimately share one; only receipts
	// within one execution must be told apart by it.
	if first.AuditID == second.AuditID {
		t.Fatalf("two replays in one execution share audit id %q; receipts must be per-replay", first.AuditID)
	}

	// Terminalize execB's verify node, then replay that entry: a decidable
	// rejection must leave a receipt too, so the scan must carry it exactly
	// like a success — an operator's audit trail has to show what was refused.
	if err := state.UpsertNode(ctx, &engine.NodeSnapshot{
		ExecutionID: execB, Name: "verify", NodeIdx: 1,
		Status: types.NodeStatusSuccess, ActivationID: 1,
	}); err != nil {
		t.Fatalf("UpsertNode(verify terminal): %v", err)
	}
	rejected, err := state.ReplayDeadLetter(ctx, engine.ReplayDeadLetterRequest{
		ExecutionID: execB, EntryID: verifyB, RequestID: "req-02",
		Operator: "cli:tester", Reason: "operator replay after root-cause",
	})
	if err != nil {
		t.Fatalf("ReplayDeadLetter(rejected): %v", err)
	}
	if rejected.Outcome != engine.ReplayRejectedNodeTerminal {
		t.Fatalf("terminal-node replay outcome = %q, want rejected_node_terminal", rejected.Outcome)
	}
	if rejected.AuditID == "" {
		t.Fatalf("rejected result %+v carries no AuditID; a decidable rejection must leave a receipt too", rejected)
	}

	var got []engine.ReplayReceipt
	if err := state.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		got = append(got, r)
		return nil
	}); err != nil {
		t.Fatalf("ScanReplayReceipts: %v", err)
	}
	order := []struct{ exec, req string }{
		{string(execA), "req-01"},
		{string(execA), "req-02"},
		{string(execB), "req-01"},
		{string(execB), "req-02"},
	}
	if len(got) != len(order) {
		t.Fatalf("scanned %d receipts, want %d: %+v", len(got), len(order), got)
	}
	for i, want := range order {
		if got[i].ExecutionID != want.exec || got[i].RequestID != want.req {
			t.Fatalf("receipt[%d] = %s/%s, want %s/%s (execution-then-request order)",
				i, got[i].ExecutionID, got[i].RequestID, want.exec, want.req)
		}
	}

	r := got[0]
	if r.Namespace != "" {
		t.Fatalf("Namespace = %q, want the empty string: the local backend has no "+
			"namespace dimension, and a fabricated name would be projected into the "+
			"durable audit row as if the replay had been made under it", r.Namespace)
	}
	if r.AuditID != first.AuditID {
		t.Fatalf("AuditID = %q, want %q (the replay result's id — the SQL idempotency key)", r.AuditID, first.AuditID)
	}
	if r.NodeID != "finish" || r.ActivationID != "1" {
		t.Fatalf("node/activation = %q/%q, want finish/1", r.NodeID, r.ActivationID)
	}
	if r.Outcome != engine.ReplayReplayed {
		t.Fatalf("Outcome = %q, want replayed", r.Outcome)
	}
	if r.EntryID != finishA {
		t.Fatalf("EntryID = %q, want %q", r.EntryID, finishA)
	}
	if r.Operator != "cli:tester" || r.Reason != "operator replay after root-cause" {
		t.Fatalf("operator/reason = %q/%q, want the request's values", r.Operator, r.Reason)
	}
	if r.TimestampMs <= 0 {
		t.Fatalf("TimestampMs = %d, want the replay wall-clock time: a zero makes "+
			"the projector stamp the audit row with reconcile time instead", r.TimestampMs)
	}
	if got[1].AuditID != second.AuditID || got[2].AuditID != other.AuditID {
		t.Fatalf("receipt audit ids = %q/%q, want %q/%q",
			got[1].AuditID, got[2].AuditID, second.AuditID, other.AuditID)
	}
	if got[1].NodeID != "verify" {
		t.Fatalf("receipt[1].NodeID = %q, want verify (the second entry's node)", got[1].NodeID)
	}

	last := got[3]
	if last.AuditID != rejected.AuditID {
		t.Fatalf("rejected receipt AuditID = %q, want %q (the rejection result's id — the SQL idempotency key)", last.AuditID, rejected.AuditID)
	}
	if last.Outcome != engine.ReplayRejectedNodeTerminal {
		t.Fatalf("rejected receipt Outcome = %q, want rejected_node_terminal: a decidable "+
			"rejection must appear in the audit trail, not only the successes", last.Outcome)
	}
	if last.NodeID != "verify" || last.EntryID != verifyB {
		t.Fatalf("rejected receipt node/entry = %q/%q, want verify/%q", last.NodeID, last.EntryID, verifyB)
	}
	if last.Operator != "cli:tester" || last.Reason != "operator replay after root-cause" {
		t.Fatalf("rejected receipt operator/reason = %q/%q, want the request's values", last.Operator, last.Reason)
	}
	if last.TimestampMs <= 0 {
		t.Fatalf("rejected receipt TimestampMs = %d, want the replay wall-clock time", last.TimestampMs)
	}
}

// TestMemoryScanReplayReceiptsAbortsOnFnError pins the interface's abort
// contract: fn's error stops the scan and propagates unchanged, so the
// projector can fail the reconcile on the first projection it cannot write.
func TestMemoryScanReplayReceiptsAbortsOnFnError(t *testing.T) {
	state := newMemoryState()
	execA := types.ExecutionID("receipt-abort-a")
	finishA, verifyA := seedReceiptReaderExecution(t, state, execA)
	replayReceiptEntry(t, state, execA, finishA, "req-01")
	replayReceiptEntry(t, state, execA, verifyA, "req-02")

	sentinel := errors.New("projection down")
	emitted := 0
	err := state.ScanReplayReceipts(context.Background(), func(engine.ReplayReceipt) error {
		emitted++
		return sentinel
	})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want the fn error propagated unchanged", err)
	}
	if emitted != 1 {
		t.Fatalf("fn calls = %d, want 1 (the scan aborts on the first error)", emitted)
	}
}

// TestMemoryScanReplayReceiptsEmptyState proves an execution-less store scans
// cleanly with no receipts rather than erroring.
func TestMemoryScanReplayReceiptsEmptyState(t *testing.T) {
	state := newMemoryState()
	calls := 0
	if err := state.ScanReplayReceipts(context.Background(), func(engine.ReplayReceipt) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("ScanReplayReceipts on an empty store: %v", err)
	}
	if calls != 0 {
		t.Fatalf("fn calls = %d, want 0", calls)
	}
}
