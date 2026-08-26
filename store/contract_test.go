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
	// The contract's own content, pinned. The previous version of this check
	// listed six "load-bearing" names, which left NodeType, Output, Port and
	// SignalConfig unguarded: deleting "Output" from UpsertNodeUpdateFields
	// passed every test in this file (the remaining nine names are still real
	// NodeRecord fields, still unique, still non-empty, still not excluded) and
	// every test downstream, because store/sqlstore and store/memstore verify
	// their implementations *against this list* -- shrink the list and their
	// coverage shrinks with it, in lockstep and undetectably. The consequence
	// of losing "Output" specifically: on a node retry (same execution_id +
	// node_name conflict key) status, attempt and the lease columns refresh
	// while the persisted output stays at the first attempt's value, on both
	// backends, with no error.
	//
	// This is an exact set comparison, so adding a field to NodeRecord's update
	// set requires an edit here too. That is the intent: a new field that no
	// one adds here is a field neither backend is checked on.
	want := map[string]bool{
		"NodeType":     true,
		"Status":       true,
		"LeaseID":      true,
		"LeaseToken":   true,
		"Attempt":      true,
		"Output":       true,
		"Port":         true,
		"SignalName":   true,
		"SignalConfig": true,
		"Timeout":      true,
	}
	for name := range want {
		if !seen[name] {
			t.Errorf("UpsertNodeUpdateFields is missing %q: both backends derive "+
				"what they refresh from this list, so a name dropped here is a "+
				"column neither backend updates and neither backend's test notices",
				name)
		}
	}
	for name := range seen {
		if !want[name] {
			t.Errorf("UpsertNodeUpdateFields gained %q, which this contract has not "+
				"been reviewed for; if it belongs in the ON CONFLICT update set, "+
				"add it to the expected set here as well", name)
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
