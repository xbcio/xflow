package store

import (
	"reflect"
	"testing"
)

// TestUpsertNodeUpdateFields_NameRealFields is the half of the contract that
// can be checked without an implementation: every entry must name a field that
// actually exists on NodeRecord.
//
// The list is consumed by reflection on both sides (memstore's copy check and
// sqlstore's column derivation), and a reflect lookup for a name that does not
// exist yields the zero Value rather than an error. Without this test a typo or
// a field rename would turn a covered field into a silently skipped one — the
// contract would shrink and every downstream test would stay green.
func TestUpsertNodeUpdateFields_NameRealFields(t *testing.T) {
	ty := reflect.TypeFor[NodeRecord]()
	for _, name := range UpsertNodeUpdateFields {
		if _, ok := ty.FieldByName(name); !ok {
			t.Errorf("UpsertNodeUpdateFields names %q, which is not a field of NodeRecord; "+
				"reflection-driven consumers would silently skip it", name)
		}
	}
}

// TestUpsertNodeUpdateFields_Wellformed guards the list's own shape. A
// duplicate or empty entry would not fail the check above but would make the
// derived column list wrong.
func TestUpsertNodeUpdateFields_Wellformed(t *testing.T) {
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
	// The lease/attempt/signal columns are load-bearing for node lifecycle
	// correctness, so dropping one from the contract has to be deliberate.
	for _, required := range []string{"Status", "LeaseID", "LeaseToken", "Attempt", "SignalName", "Timeout"} {
		if !seen[required] {
			t.Errorf("UpsertNodeUpdateFields missing required field %q", required)
		}
	}
	// Identity and the timestamps are excluded by design: execution_id/node_name
	// are the conflict key, created_at is preserved, and updated_at is refreshed
	// by each backend outside this set. Listing one would make the derived
	// sqlstore column list assert the wrong thing.
	for _, excluded := range []string{"ID", "ExecutionID", "NodeName", "CreatedAt", "UpdatedAt"} {
		if seen[excluded] {
			t.Errorf("UpsertNodeUpdateFields must not list %q: it is either the conflict "+
				"key or maintained outside the ON CONFLICT update set", excluded)
		}
	}
}
