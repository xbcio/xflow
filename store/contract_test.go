package store

import "testing"

// UpsertNodeUpdateFields lists the NodeRecord fields that an UpsertNode on an
// existing row must refresh. Both the memstore and sqlstore implementations
// must update exactly this set so a node's lifecycle projection stays
// consistent across backends. created_at is preserved; updated_at is refreshed
// separately by each backend.
//
// Keep this list in sync with:
//   - store/memstore/memstore.go UpsertNode
//   - store/sqlstore/node.go UpsertNode OnConflict DoUpdates
var UpsertNodeUpdateFields = []string{
	"NodeType",
	"Status",
	"LeaseID",
	"LeaseToken",
	"Attempt",
	"Output",
	"Port",
	"SignalName",
	"SignalConfig",
	"Timeout",
}

// TestUpsertNodeUpdateFields_Contract is the cross-backend field-set contract.
// The memstore half is exercised end-to-end by
// store/memstore/memstore_test.go:TestUpsertNode_FullFieldUpdate. The sqlstore
// half (OnConflict DoUpdates) is kept in sync manually against this constant
// until a test-grade MySQL fixture enables a cross-backend integration test.
func TestUpsertNodeUpdateFields_Contract(t *testing.T) {
	if len(UpsertNodeUpdateFields) == 0 {
		t.Fatal("UpsertNodeUpdateFields must not be empty")
	}
	seen := make(map[string]bool, len(UpsertNodeUpdateFields))
	for _, f := range UpsertNodeUpdateFields {
		if f == "" {
			t.Fatal("UpsertNodeUpdateFields contains an empty field name")
		}
		if seen[f] {
			t.Fatalf("duplicate field in UpsertNodeUpdateFields: %q", f)
		}
		seen[f] = true
	}
	// Sanity: the contract must cover the lease/attempt/signal columns that
	// are load-bearing for node lifecycle correctness.
	for _, required := range []string{"Status", "LeaseID", "LeaseToken", "Attempt", "SignalName", "Timeout"} {
		if !seen[required] {
			t.Errorf("UpsertNodeUpdateFields missing required field %q", required)
		}
	}
}
