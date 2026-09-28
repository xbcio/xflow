package types

import (
	"reflect"
	"testing"
)

func sampleDescriptor() Descriptor {
	return Descriptor{
		Type:        "x.sample",
		Kind:        NodeKindAction,
		DisplayName: "Sample",
		Credentials: []string{"db"},
		Params: []ParamSpec{
			{Name: "opts", Type: ParamObject, Default: map[string]any{
				"nested": map[string]any{"k": "v"},
				"list":   []any{map[string]any{"a": 1.0}},
			}},
			{Name: "tags", Type: ParamArray, Default: []string{"a"}},
		},
		Inputs:       []PortSpec{{Name: "main"}},
		Outputs:      []PortSpec{{Name: "main"}, {Name: "error"}},
		Capabilities: []string{"cap"},
	}
}

func TestDescriptorCloneIsEqual(t *testing.T) {
	src := sampleDescriptor()
	if got := src.Clone(); !reflect.DeepEqual(got, src) {
		t.Fatalf("Clone() = %#v, want deep-equal to source %#v", got, src)
	}
}

// TestDescriptorCloneIsolatesCallerWrites pins that writing through a clone --
// top-level slices, a ParamSpec element, and containers nested inside Default
// -- never reaches the source.
func TestDescriptorCloneIsolatesCallerWrites(t *testing.T) {
	src := sampleDescriptor()
	c := src.Clone()

	c.Credentials[0] = "mutated"
	c.Inputs[0].Name = "mutated"
	c.Outputs[1].Name = "mutated"
	c.Capabilities[0] = "mutated"
	c.Params[0].Name = "mutated"
	opts := c.Params[0].Default.(map[string]any)
	opts["nested"].(map[string]any)["k"] = "mutated"
	opts["list"].([]any)[0].(map[string]any)["a"] = 2.0
	opts["added"] = true
	c.Params[1].Default.([]string)[0] = "mutated"

	if !reflect.DeepEqual(src, sampleDescriptor()) {
		t.Fatalf("writes through Clone() reached the source: %#v", src)
	}
}

// TestDescriptorCloneIsolatesLaterSourceAppends pins the other direction: a
// source that keeps appending after handing out a clone must not write into
// the clone's backing arrays, even when the source slices have spare capacity.
func TestDescriptorCloneIsolatesLaterSourceAppends(t *testing.T) {
	src := sampleDescriptor()
	src.Params = append(make([]ParamSpec, 0, 8), src.Params...)
	src.Credentials = append(make([]string, 0, 8), src.Credentials...)

	c := src.Clone()
	want := c.Clone()

	src.Params = append(src.Params, ParamSpec{Name: "late"})
	src.Credentials = append(src.Credentials, "late")
	src.Params[0].Name = "mutated"

	if !reflect.DeepEqual(c, want) {
		t.Fatalf("source writes after Clone() reached the clone: %#v", c)
	}
}

func TestDescriptorClonePreservesNilSlices(t *testing.T) {
	c := (Descriptor{Type: "x.empty"}).Clone()
	if c.Credentials != nil || c.Params != nil || c.Inputs != nil || c.Outputs != nil || c.Capabilities != nil {
		t.Fatalf("Clone() turned nil slices into empty ones: %#v", c)
	}
}

// TestDescriptorCloneCoversEveryField fails when Descriptor or ParamSpec gains
// a field, so the author must decide how Clone copies it. Update the counts
// only after extending Clone (and cloneParamValue for any `any`-typed field).
func TestDescriptorCloneCoversEveryField(t *testing.T) {
	if n := reflect.TypeOf(Descriptor{}).NumField(); n != 8 {
		t.Fatalf("Descriptor has %d fields, Clone was written for 8: extend Descriptor.Clone", n)
	}
	if n := reflect.TypeOf(ParamSpec{}).NumField(); n != 6 {
		t.Fatalf("ParamSpec has %d fields, clone was written for 6: extend ParamSpec.clone", n)
	}
	if n := reflect.TypeOf(PortSpec{}).NumField(); n != 2 {
		t.Fatalf("PortSpec has %d fields, clonePorts copies by value: re-check it", n)
	}
}
