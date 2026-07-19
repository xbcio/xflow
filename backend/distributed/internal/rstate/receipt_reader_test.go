package rstate

import (
	"context"
	"sort"
	"testing"

	"github.com/xbcio/xflow/backend/tenant"
	"github.com/xbcio/xflow/engine"
	"github.com/xbcio/xflow/types"
)

// TestScanReplayReceiptsProjectsAllReceipts proves the receipt reader scans
// every authoritative Redis replay receipt across tenants and decodes the
// fields the SQL projector needs (audit_id, node, activation, outcome,
// entry_id, ts). It is the diff-scan source for the reconcile command.
func TestScanReplayReceiptsProjectsAllReceipts(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	defer mr.Close()
	ctx := context.Background()

	// Seed two receipts in tenant-a and one in tenant-b.
	seedReceipt(t, state, "tenant-a", "exec-a-1", "req-a-1", "audit-a-1", "review", "1", "replayed")
	seedReceipt(t, state, "tenant-a", "exec-a-2", "req-a-2", "audit-a-2", "approve", "2", "rejected_terminal")
	seedReceipt(t, state, "tenant-b", "exec-b-1", "req-b-1", "audit-b-1", "review", "1", "replayed")

	var got []engine.ReplayReceipt
	err := state.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanReplayReceipts: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("scanned %d receipts, want 3: %+v", len(got), got)
	}
	// Stable sort for assertion.
	sort.Slice(got, func(i, j int) bool { return got[i].AuditID < got[j].AuditID })
	if got[0].AuditID != "audit-a-1" || got[0].TenantID != "tenant-a" || got[0].ExecutionID != "exec-a-1" {
		t.Fatalf("receipt[0] = %+v", got[0])
	}
	if got[0].NodeID != "review" || got[0].ActivationID != "1" || got[0].Outcome != engine.ReplayReplayed {
		t.Fatalf("receipt[0] correlation = %+v", got[0])
	}
	if got[0].EntryID != "entry-exec-a-1" {
		t.Fatalf("receipt[0].EntryID = %q, want entry-exec-a-1", got[0].EntryID)
	}
	if got[2].TenantID != "tenant-b" {
		t.Fatalf("receipt[2].TenantID = %q, want tenant-b", got[2].TenantID)
	}
}

// TestScanReplayReceiptsSkipsEmptyAuditID proves a malformed/legacy receipt
// hash without an audit_id is skipped (no idempotency key → no projectable
// row) rather than aborting the scan.
func TestScanReplayReceiptsSkipsEmptyAuditID(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	defer mr.Close()
	ctx := context.Background()

	// One healthy receipt and one malformed (no audit_id).
	seedReceipt(t, state, "tenant-a", "exec-good", "req-good", "audit-good", "n", "1", "replayed")
	seedMalformedReceipt(t, state, "tenant-a", "exec-bad", "req-bad")

	var got []engine.ReplayReceipt
	err := state.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatalf("ScanReplayReceipts: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("scanned %d receipts, want 1 (malformed skipped): %+v", len(got), got)
	}
	if got[0].AuditID != "audit-good" {
		t.Fatalf("scanned receipt = %+v, want the healthy one", got[0])
	}
}

// TestScanReplayReceiptsAbortOnError proves fn returning an error aborts the
// scan and propagates the error.
func TestScanReplayReceiptsAbortOnError(t *testing.T) {
	state, mr, _ := newTestRedisState(t)
	defer mr.Close()
	ctx := context.Background()

	seedReceipt(t, state, "tenant-a", "exec-abort", "req-abort", "audit-abort", "n", "1", "replayed")
	boom := errScanAbort{}
	err := state.ScanReplayReceipts(ctx, func(r engine.ReplayReceipt) error {
		return boom
	})
	if err != boom {
		t.Fatalf("ScanReplayReceipts err = %v, want boom (abort propagation)", err)
	}
}

type errScanAbort struct{}

func (errScanAbort) Error() string { return "scan abort" }

func seedReceipt(t *testing.T, state *Store, tenantName, execID, requestID, auditID, node, activation, outcome string) {
	t.Helper()
	ctx := context.Background()
	// Register the tenant so listTenants (the scan fan-out source) includes it.
	if err := state.registerTenant(ctx, tenant.TenantID(tenantName)); err != nil {
		t.Fatalf("register tenant: %v", err)
	}
	key := outboxReplayReceiptKey(tenant.TenantID(tenantName), types.ExecutionID(execID), requestID)
	if err := state.rdb.HSet(ctx, key,
		"audit_id", auditID,
		"node", node,
		"activation", activation,
		"outcome", outcome,
		"operator", "cli:tester",
		"reason", "operator rationale",
		"entry_id", "entry-"+execID,
		"ts_ms", "1700000000000",
	).Err(); err != nil {
		t.Fatalf("HSet receipt: %v", err)
	}
}

func seedMalformedReceipt(t *testing.T, state *Store, tenantName, execID, requestID string) {
	t.Helper()
	ctx := context.Background()
	if err := state.registerTenant(ctx, tenant.TenantID(tenantName)); err != nil {
		t.Fatalf("register tenant: %v", err)
	}
	key := outboxReplayReceiptKey(tenant.TenantID(tenantName), types.ExecutionID(execID), requestID)
	// No audit_id field → the projector has no idempotency key and must skip.
	if err := state.rdb.HSet(ctx, key, "node", "n", "outcome", "replayed").Err(); err != nil {
		t.Fatalf("HSet malformed receipt: %v", err)
	}
}

// TestParseReplayReceiptKey covers the key parser directly, including
// requestIDs that contain colons (opaque to Redis).
func TestParseReplayReceiptKey(t *testing.T) {
	cases := []struct {
		key       string
		execID    string
		requestID string
		ok        bool
	}{
		{"xflow:ttenant-a:exec:{exec-1}:replay:receipt:req-1", "exec-1", "req-1", true},
		{"xflow:ttenant-a:exec:{exec-1}:replay:receipt:req:with:colons", "exec-1", "req:with:colons", true},
		{"xflow:ttenant-a:exec:{exec-1}:outbox:dead", "", "", false},
		{"xflow:ttenant-a:exec:{exec-1}:replay:receipt:", "", "", false},
		{"legacy:exec:{exec-1}:replay:receipt:req-1", "", "", false},
	}
	for _, c := range cases {
		execID, requestID, ok := parseReplayReceiptKey(c.key)
		if ok != c.ok {
			t.Errorf("parseReplayReceiptKey(%q): ok=%v, want %v", c.key, ok, c.ok)
			continue
		}
		if ok && (string(execID) != c.execID || requestID != c.requestID) {
			t.Errorf("parseReplayReceiptKey(%q): execID=%q requestID=%q, want %q/%q", c.key, execID, requestID, c.execID, c.requestID)
		}
	}
}
