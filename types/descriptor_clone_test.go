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

// cloneFieldCounts lists every struct type reachable from Descriptor with the
// field count its clone function was written for. Update an entry only after
// extending the matching clone (and routing any any-typed field through
// cloneParamValue).
var cloneFieldCounts = map[reflect.Type]int{
	reflect.TypeOf(Descriptor{}):      12,
	reflect.TypeOf(ParamSpec{}):       18,
	reflect.TypeOf(PortSpec{}):        2,
	reflect.TypeOf(EnumOption{}):      3,
	reflect.TypeOf(ConditionalEnum{}): 2,
	reflect.TypeOf(Constraints{}):     9,
	reflect.TypeOf(Condition{}):       7,
	reflect.TypeOf(OneOfGroup{}):      2,
	reflect.TypeOf(GroupSpec{}):       4,
}

// reachableStructs collects every struct type reachable from t through
// pointers, slices, arrays, maps, and struct fields.
func reachableStructs(t reflect.Type, seen map[reflect.Type]bool) {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		reachableStructs(t.Elem(), seen)
	case reflect.Map:
		reachableStructs(t.Key(), seen)
		reachableStructs(t.Elem(), seen)
	case reflect.Struct:
		if seen[t] {
			return
		}
		seen[t] = true
		for i := 0; i < t.NumField(); i++ {
			reachableStructs(t.Field(i).Type, seen)
		}
	}
}

// TestDescriptorCloneCoversEveryField fails when any struct reachable from
// Descriptor gains a field, or a new struct type becomes reachable, so the
// author must decide how Clone copies it.
func TestDescriptorCloneCoversEveryField(t *testing.T) {
	seen := map[reflect.Type]bool{}
	reachableStructs(reflect.TypeOf(Descriptor{}), seen)
	for typ := range seen {
		want, ok := cloneFieldCounts[typ]
		if !ok {
			t.Errorf("%v is reachable from Descriptor but not in cloneFieldCounts: extend Clone, then the table", typ)
			continue
		}
		if n := typ.NumField(); n != want {
			t.Errorf("%v has %d fields, Clone was written for %d: extend its clone, then the table", typ, n, want)
		}
	}
	for typ := range cloneFieldCounts {
		if !seen[typ] {
			t.Errorf("%v is in cloneFieldCounts but no longer reachable from Descriptor", typ)
		}
	}
}

type fillMode int

const (
	fillFull        fillMode = iota // every slice has two filled elements
	fillEmptySlices                 // every slice is empty but non-nil
	fillNilSlices                   // every slice is nil
)

// fillAnyValue is the value planted in every any-typed field: one of each
// container shape cloneParamValue deep-copies, nested.
func fillAnyValue() any {
	return map[string]any{
		"scalar": "v",
		"map":    map[string]any{"k": "v", "deep": map[string]any{"k": 1.0}},
		"list":   []any{map[string]any{"a": 1.0}, []any{"x"}, "s"},
		"strs":   []string{"a", "b"},
		"maps":   []map[string]any{{"k": "v"}, {"n": map[string]any{"k": "v"}}},
	}
}

// fill sets every field reachable from v to a non-zero value (subject to
// mode for slices). Each struct type may appear at most twice on a path,
// which bounds the recursive types (ParamSpec.Item/Fields, Condition.Not/AllOf/AnyOf).
func fill(v reflect.Value, mode fillMode, depth map[reflect.Type]int) {
	switch v.Kind() {
	case reflect.String:
		v.SetString("s")
	case reflect.Bool:
		v.SetBool(true)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(1)
	case reflect.Interface:
		v.Set(reflect.ValueOf(fillAnyValue()))
	case reflect.Pointer:
		if et := v.Type().Elem(); et.Kind() == reflect.Struct && depth[et] >= 2 {
			return
		}
		p := reflect.New(v.Type().Elem())
		fill(p.Elem(), mode, depth)
		v.Set(p)
	case reflect.Slice:
		switch mode {
		case fillNilSlices:
			return
		case fillEmptySlices:
			v.Set(reflect.MakeSlice(v.Type(), 0, 0))
			return
		}
		if et := v.Type().Elem(); et.Kind() == reflect.Struct && depth[et] >= 2 {
			return
		}
		s := reflect.MakeSlice(v.Type(), 2, 2)
		for i := 0; i < s.Len(); i++ {
			fill(s.Index(i), mode, depth)
		}
		v.Set(s)
	case reflect.Struct:
		depth[v.Type()]++
		for i := 0; i < v.NumField(); i++ {
			fill(v.Field(i), mode, depth)
		}
		depth[v.Type()]--
	default:
		panic("fill: unhandled kind " + v.Kind().String())
	}
}

func filledDescriptor(mode fillMode) Descriptor {
	var d Descriptor
	fill(reflect.ValueOf(&d).Elem(), mode, map[reflect.Type]int{})
	return d
}

// mutate changes every scalar reachable from v and adds a key to every map,
// writing through slices, maps, pointers, and any-typed values.
func mutate(v reflect.Value) {
	switch v.Kind() {
	case reflect.String:
		v.SetString(v.String() + "!")
	case reflect.Bool:
		v.SetBool(!v.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		v.SetInt(v.Int() + 1)
	case reflect.Float32, reflect.Float64:
		v.SetFloat(v.Float() + 1)
	case reflect.Pointer:
		if !v.IsNil() {
			mutate(v.Elem())
		}
	case reflect.Slice:
		for i := 0; i < v.Len(); i++ {
			mutate(v.Index(i))
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			mutate(v.Field(i))
		}
	case reflect.Map:
		if v.IsNil() {
			return
		}
		for _, k := range v.MapKeys() {
			e := reflect.New(v.Type().Elem()).Elem()
			e.Set(v.MapIndex(k))
			mutate(e)
			v.SetMapIndex(k, e)
		}
		v.SetMapIndex(reflect.ValueOf("added"), reflect.ValueOf(any("added")).Convert(v.Type().Elem()))
	case reflect.Interface:
		if v.IsNil() {
			return
		}
		switch inner := v.Elem(); inner.Kind() {
		case reflect.Map, reflect.Slice, reflect.Pointer:
			mutate(inner) // shares storage with v; writes land in place
		default:
			if v.CanSet() {
				v.Set(reflect.ValueOf("mutated"))
			}
		}
	default:
		panic("mutate: unhandled kind " + v.Kind().String())
	}
}

// TestDescriptorCloneHasNoAliasing fills every reachable field, clones, and
// writes to every reachable mutable location of the clone: none may reach the
// source.
func TestDescriptorCloneHasNoAliasing(t *testing.T) {
	src := filledDescriptor(fillFull)
	c := src.Clone()
	if !reflect.DeepEqual(c, src) {
		t.Fatalf("Clone() is not deep-equal to its source")
	}

	mutate(reflect.ValueOf(&c).Elem())
	if reflect.DeepEqual(c, src) {
		t.Fatalf("mutate changed nothing; the aliasing check below would be vacuous")
	}
	if !reflect.DeepEqual(src, filledDescriptor(fillFull)) {
		t.Fatalf("a write through Clone() reached the source")
	}
}

// TestDescriptorClonePreservesSliceNilness pins, at every nesting level, that
// nil slices stay nil and empty non-nil slices stay empty non-nil
// (reflect.DeepEqual distinguishes the two).
func TestDescriptorClonePreservesSliceNilness(t *testing.T) {
	for name, mode := range map[string]fillMode{"empty": fillEmptySlices, "nil": fillNilSlices} {
		src := filledDescriptor(mode)
		if got := src.Clone(); !reflect.DeepEqual(got, src) {
			t.Errorf("%s slices: Clone() changed slice nilness: %#v", name, got)
		}
	}
}

// TestDescriptorClonePreservesTypedNilContainers pins that a typed-nil
// container in an any-typed field stays a typed nil rather than becoming an
// untyped nil or an empty container.
func TestDescriptorClonePreservesTypedNilContainers(t *testing.T) {
	src := Descriptor{Params: []ParamSpec{{
		Name:    "p",
		Default: map[string]any(nil),
		Enum:    []EnumOption{{Value: []any(nil)}},
	}}}
	c := src.Clone()
	if m, ok := c.Params[0].Default.(map[string]any); !ok || m != nil {
		t.Fatalf("Default = %#v, want typed-nil map[string]any", c.Params[0].Default)
	}
	if s, ok := c.Params[0].Enum[0].Value.([]any); !ok || s != nil {
		t.Fatalf("Enum[0].Value = %#v, want typed-nil []any", c.Params[0].Enum[0].Value)
	}
}
