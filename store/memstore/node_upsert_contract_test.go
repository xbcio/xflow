package memstore

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xbcio/xflow/store"
	"github.com/xbcio/xflow/types"
)

// setDistinct writes a value derived from seed into v, so two records built
// with different seeds differ in every contract field.
//
// An unhandled kind is a hard failure rather than a skip. A field added to
// NodeRecord with a type this does not know would otherwise be "checked" by
// comparing two zero values, which succeeds whether or not UpsertNode copies
// it — the exact silence this test exists to remove.
func setDistinct(t *testing.T, v reflect.Value, name string, seed int) {
	t.Helper()
	switch {
	case v.Kind() == reflect.String:
		v.SetString(fmt.Sprintf("%s-v%d", name, seed))
	case v.Kind() == reflect.Int || v.Kind() == reflect.Int64:
		v.SetInt(int64(seed))
	case v.Kind() == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8:
		v.SetBytes(fmt.Appendf(nil, `{"v":%d}`, seed))
	case v.Kind() == reflect.Ptr && v.Type().Elem() == reflect.TypeOf(time.Time{}):
		ts := time.Unix(1700000000, 0).UTC().Add(time.Duration(seed) * time.Minute)
		v.Set(reflect.ValueOf(&ts))
	default:
		t.Fatalf("setDistinct has no case for NodeRecord.%s of type %s; add one rather "+
			"than letting the contract check compare two zero values", name, v.Type())
	}
}

// TestUpsertNode_RefreshesEveryContractField is the memstore half of the
// cross-backend UpsertNode field set, driven by store.UpsertNodeUpdateFields
// rather than by a hand-written list of comparisons.
//
// The hand-written twin (TestUpsertNode_FullFieldUpdate) does have teeth for
// the fields it names, but it only catches a field that stops being copied —
// not a field that was never added to it. Adding a column to NodeRecord and to
// both the contract and the sqlstore update set, while forgetting the one line
// in memstore's UpsertNode, leaves that test green because it never mentions
// the new field. Reflection over the contract covers a field the day it is
// listed.
func TestUpsertNode_RefreshesEveryContractField(t *testing.T) {
	ctx := context.Background()
	s := New()
	execID := types.ExecutionID("exec-contract")

	build := func(seed int) *store.NodeRecord {
		rec := &store.NodeRecord{ExecutionID: execID, NodeName: "n1"}
		rv := reflect.ValueOf(rec).Elem()
		for _, name := range store.UpsertNodeUpdateFields {
			f := rv.FieldByName(name)
			if !f.IsValid() {
				t.Fatalf("NodeRecord has no field %q named by the contract", name)
			}
			setDistinct(t, f, name, seed)
		}
		return rec
	}

	if err := s.UpsertNode(ctx, build(1)); err != nil {
		t.Fatalf("initial upsert: %v", err)
	}
	updated := build(2)
	if err := s.UpsertNode(ctx, updated); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	got, err := s.GetNode(ctx, execID, "n1")
	if err != nil {
		t.Fatalf("get node: %v", err)
	}
	gotV := reflect.ValueOf(got).Elem()
	wantV := reflect.ValueOf(updated).Elem()
	for _, name := range store.UpsertNodeUpdateFields {
		g, w := gotV.FieldByName(name).Interface(), wantV.FieldByName(name).Interface()
		if !reflect.DeepEqual(g, w) {
			t.Errorf("after upsert on an existing row, NodeRecord.%s = %v, want %v; "+
				"the contract lists it but memstore's UpsertNode does not refresh it",
				name, g, w)
		}
	}

	// CreatedAt is preserved, not refreshed — the other half of the contract,
	// and the one an over-eager "copy everything" fix would break.
	if got.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero after an upsert on an existing row; it must be preserved")
	}
}
