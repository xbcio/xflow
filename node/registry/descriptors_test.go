package registry

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/types"
)

// The tests below run against an isolated newRegistry() rather than the global
// one: the registry has no unregister, so test-local registrations in the
// global table would leak into every other test in the binary.

type fakeDesc struct {
	typ     string
	kind    types.NodeKind
	version int
	label   string
	shared  *types.Descriptor // when set, Descriptor() returns this value (aliased slices)
}

func (f fakeDesc) Descriptor() types.Descriptor {
	if f.shared != nil {
		return *f.shared
	}
	return types.Descriptor{
		Type:        f.typ,
		Kind:        f.kind,
		DisplayName: f.label,
		Params:      []types.ParamSpec{{Name: "p", Type: types.ParamString}},
	}
}

func (f fakeDesc) NodeVersion() int { return f.version }

type fakeAction struct{ fakeDesc }

func (fakeAction) Execute(context.Context, *types.Input) (*types.Output, error) { return nil, nil }

type fakeTrigger struct{ fakeDesc }

func (fakeTrigger) Activate(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return nil, nil
}

type fakeBoth struct{ fakeDesc }

func (fakeBoth) Execute(context.Context, *types.Input) (*types.Output, error) { return nil, nil }
func (fakeBoth) Activate(context.Context, *types.TriggerActivateInput) (types.TriggerSubscription, error) {
	return nil, nil
}

type entry struct {
	typ     string
	version int
	label   string
}

func summarize(rds []RegisteredDescriptor) []entry {
	out := make([]entry, len(rds))
	for i, rd := range rds {
		out[i] = entry{rd.Type, rd.Version, rd.Descriptor.DisplayName}
	}
	return out
}

func TestDescriptorsMergesTablesDedupsAndSorts(t *testing.T) {
	r := newRegistry()
	// Registration order is deliberately not sorted order.
	r.register(fakeAction{fakeDesc{typ: "t.multi", kind: types.NodeKindAction, version: 2, label: "multi v2"}})
	r.registerTrigger(fakeTrigger{fakeDesc{typ: "t.trigger-only", kind: types.NodeKindTrigger, version: 1, label: "trigger only"}})
	r.register(fakeAction{fakeDesc{typ: "t.multi", kind: types.NodeKindAction, version: 1, label: "multi v1"}})
	r.register(fakeBoth{fakeDesc{typ: "t.both", kind: types.NodeKindTrigger, version: 1, label: "both"}})
	r.registerTrigger(fakeBoth{fakeDesc{typ: "t.both-via-trigger", kind: types.NodeKindTrigger, version: 3, label: "both via trigger"}})
	r.register(fakeAction{fakeDesc{typ: "t.multi", kind: types.NodeKindAction, version: 3, label: "multi v3"}})

	// Precondition: the dual-interface handlers really are in both tables,
	// so the single entry below is dedup, not a missing table.
	for _, k := range []struct {
		typ string
		v   int
	}{{"t.both", 1}, {"t.both-via-trigger", 3}} {
		if r.versioned[k.typ][k.v] == nil || r.triggerVer[k.typ][k.v] == nil {
			t.Fatalf("%s@%d not written into both tables", k.typ, k.v)
		}
	}
	if _, inActions := r.versioned["t.trigger-only"]; inActions {
		t.Fatal("trigger-only handler leaked into the action table")
	}

	got := summarize(r.descriptors())
	want := []entry{
		{"t.both", 1, "both"},
		{"t.both-via-trigger", 3, "both via trigger"},
		{"t.multi", 1, "multi v1"},
		{"t.multi", 2, "multi v2"},
		{"t.multi", 3, "multi v3"},
		{"t.trigger-only", 1, "trigger only"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("descriptors() =\n  %v\nwant\n  %v", got, want)
	}
}

func TestDescriptorsActionTableWinsOnClash(t *testing.T) {
	r := newRegistry()
	r.registerTrigger(fakeTrigger{fakeDesc{typ: "t.clash", version: 1, label: "from trigger"}})
	r.register(fakeAction{fakeDesc{typ: "t.clash", version: 1, label: "from action"}})

	got := summarize(r.descriptors())
	want := []entry{{"t.clash", 1, "from action"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("descriptors() = %v, want %v", got, want)
	}
}

func TestDescriptorsReturnsClones(t *testing.T) {
	shared := &types.Descriptor{
		Type:   "t.shared",
		Params: []types.ParamSpec{{Name: "p", Type: types.ParamString}},
	}
	r := newRegistry()
	r.register(fakeAction{fakeDesc{typ: "t.shared", version: 1, shared: shared}})

	rds := r.descriptors()
	if len(rds) != 1 {
		t.Fatalf("got %d descriptors, want 1", len(rds))
	}
	rds[0].Descriptor.Params[0].Name = "mutated"
	if shared.Params[0].Name != "p" {
		t.Fatal("mutating a returned Descriptor reached the handler's shared value: Descriptors() must Clone")
	}
}

func TestDescriptorsEmptyRegistry(t *testing.T) {
	if got := newRegistry().descriptors(); len(got) != 0 {
		t.Fatalf("empty registry returned %d descriptors", len(got))
	}
}
