package transform_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

func TestRename_RenamesMappedFields(t *testing.T) {
	b := node.Rename(map[string]string{"first_name": "firstName", "last_name": "lastName"})

	h, ok := registry.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	out, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"first_name": "Ada", "last_name": "Lovelace", "age": 36},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Data["firstName"] != "Ada" || out.Data["lastName"] != "Lovelace" || out.Data["age"] != 36 {
		t.Fatalf("data = %#v, want renamed names and preserved age", out.Data)
	}
	if _, ok := out.Data["first_name"]; ok {
		t.Fatalf("first_name should have been removed: %#v", out.Data)
	}
}

// TestRename_ChainedMappingIsOrderIndependent covers the case the mapping above
// cannot reach. {"first_name":"firstName","last_name":"lastName"} has disjoint
// sources and targets, so it produces the same result under any iteration
// order — including under the in-place delete+set form that rename.go:54-59
// documents as the bug it replaced. That comment is the only record in the repo
// that chained renames were ever broken; nothing executed the chain.
//
// {"a":"b","b":"c"} is the chain: "b" is both a rename target and a rename
// source. Reading every source out of a separate map before writing any target
// is what makes the answer independent of Go's randomized map iteration.
// In-place, a→b first clobbers the original b, and b→c then carries a's value
// into c — silently, with no error on any path, so a workflow that renames a
// field onto a name another rename reads from loses data on roughly half its
// executions.
func TestRename_ChainedMappingIsOrderIndependent(t *testing.T) {
	b := node.Rename(map[string]string{"a": "b", "b": "c"})

	h, ok := registry.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	// Map iteration order is randomized per range statement, so a single call
	// would only catch an order-dependent implementation about half the time.
	// Repeating pins the property the fix actually claims: the same input always
	// yields the same output.
	const runs = 40
	for i := range runs {
		out, err := h.Execute(context.Background(), &types.Input{
			Params: b.RawParams().(map[string]any),
			Data:   map[string]any{"a": 1, "b": 2},
		})
		if err != nil {
			t.Fatalf("run %d: Execute() error = %v", i, err)
		}
		// a's value lands on b; b's ORIGINAL value lands on c. Both renames read
		// the pre-rename snapshot.
		if out.Data["b"] != 1 || out.Data["c"] != 2 {
			t.Fatalf("run %d: data = %#v, want map[b:1 c:2] — a chained rename "+
				"read a value that an earlier rename had already overwritten",
				i, out.Data)
		}
		if _, ok := out.Data["a"]; ok {
			t.Fatalf("run %d: a should have been consumed by the rename: %#v", i, out.Data)
		}
	}
}

// TestRename_RejectsEmptyMappingName covers the guard at rename.go:49-53.
// Every existing mapping fixture uses non-empty old and new names on both
// sides, so nothing exercises the branch that rejects an empty name. Without
// the guard, a mapping entry with an empty new name silently writes the
// source field's value onto data[""] instead of erroring, corrupting the
// output with no error on any path.
func TestRename_RejectsEmptyMappingName(t *testing.T) {
	b := node.Rename(map[string]string{"old_name": ""})

	h, ok := registry.Lookup("xflow.transform.rename")
	if !ok {
		t.Fatal("rename handler not registered")
	}
	_, err := h.Execute(context.Background(), &types.Input{
		Params: b.RawParams().(map[string]any),
		Data:   map[string]any{"old_name": "value"},
	})
	if err == nil {
		t.Fatal("Execute() error = nil, want a rejection for a mapping with an empty new name")
	}
	// Pin the specific rejection, not merely "some error". Execute rejects a
	// missing or unparsable mapping a few lines earlier with a different
	// message; asserting only err != nil would keep this test green if the
	// empty-name guard were deleted and the entry happened to be rejected by
	// that earlier check instead -- which would make the test claim coverage
	// of a branch it no longer reaches.
	const want = "mapping names must not be empty"
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("Execute() error = %q, want substring %q", err.Error(), want)
	}
}
