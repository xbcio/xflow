package apiserver

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// updateNodeForm rewrites the node-form golden files and the cross-language
// fixture: go test ./service/apiserver -run 'NodeForm' -update
var updateNodeForm = flag.Bool("update", false, "rewrite node-form golden files and the web fixture")

// builtinDescriptors is the registry filtered to builtin types. The test
// binary's global registry also holds test-registered types (test.e2e).
func builtinDescriptors() []registry.RegisteredDescriptor {
	var out []registry.RegisteredDescriptor
	for _, rd := range registry.Descriptors() {
		if strings.HasPrefix(rd.Type, "xflow.") {
			out = append(out, rd)
		}
	}
	return out
}

func projectBuiltin(t *testing.T, typ string) nodeFormSchema {
	t.Helper()
	for _, rd := range builtinDescriptors() {
		if rd.Type == typ {
			return projectNodeForm(rd, builtinNodeFormFallbacks())
		}
	}
	t.Fatalf("builtin %s is not registered", typ)
	return nodeFormSchema{}
}

func findField(t *testing.T, fields []nodeFormField, path ...string) nodeFormField {
	t.Helper()
	for _, f := range fields {
		if f.Name != path[0] {
			continue
		}
		if len(path) == 1 {
			return f
		}
		if f.Item != nil && len(f.Item.Fields) > 0 {
			return findField(t, f.Item.Fields, path[1:]...)
		}
		return findField(t, f.Fields, path[1:]...)
	}
	t.Fatalf("field %v not found", path)
	return nodeFormField{}
}

func marshalString(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// TestProjectEveryBuiltinDescriptor: every registered builtin projects to a
// well-formed schema whose field tree mirrors the ParamSpec tree.
func TestProjectEveryBuiltinDescriptor(t *testing.T) {
	descs := builtinDescriptors()
	if len(descs) < 25 {
		t.Fatalf("only %d builtin descriptors registered", len(descs))
	}
	fieldTypes := map[string]bool{"string": true, "number": true, "boolean": true, "array": true, "object": true}
	modes := map[string]bool{"none": true, "pure": true, "template": true, "literal": true}
	kinds := map[types.NodeKind]bool{types.NodeKindAction: true, types.NodeKindTrigger: true, types.NodeKindSupply: true}

	var walk func(t *testing.T, where string, spec types.ParamSpec, it nodeFormItem)
	walk = func(t *testing.T, where string, spec types.ParamSpec, it nodeFormItem) {
		if !fieldTypes[it.Type] {
			t.Errorf("%s: type %q", where, it.Type)
		}
		if it.Expression == nil || !modes[string(it.Expression.Mode)] {
			t.Errorf("%s: expression %+v", where, it.Expression)
		}
		if (spec.Item == nil) != (it.Item == nil) {
			t.Errorf("%s: item presence differs", where)
		}
		if spec.Item != nil && it.Item != nil {
			walk(t, where+"[]", *spec.Item, *it.Item)
		}
		if len(spec.Fields) != len(it.Fields) {
			t.Fatalf("%s: %d fields, want %d", where, len(it.Fields), len(spec.Fields))
		}
		for i := range spec.Fields {
			if it.Fields[i].Name != spec.Fields[i].Name || it.Fields[i].Path == "" {
				t.Errorf("%s: field %d = %q (%q)", where, i, it.Fields[i].Name, it.Fields[i].Path)
			}
			walk(t, where+"."+spec.Fields[i].Name, spec.Fields[i], it.Fields[i].nodeFormItem)
		}
	}

	for _, rd := range descs {
		t.Run(rd.Type, func(t *testing.T) {
			s := projectNodeForm(rd, builtinNodeFormFallbacks())
			if s.Spec != "node-form/v1" || s.NodeType != rd.Type || s.NodeVersion != rd.Version || s.NodeVersion < 1 {
				t.Fatalf("header = %s %s@%d", s.Spec, s.NodeType, s.NodeVersion)
			}
			if !kinds[s.Kind] {
				t.Errorf("kind %q", s.Kind)
			}
			if s.Fields == nil || len(s.Fields) != len(rd.Descriptor.Params) {
				t.Fatalf("fields = %d, want %d", len(s.Fields), len(rd.Descriptor.Params))
			}
			for i, f := range s.Fields {
				if f.Path != "/parameters/"+f.Name {
					t.Errorf("field %q path %q", f.Name, f.Path)
				}
				if f.Label == "" {
					t.Errorf("field %q has no label", f.Name)
				}
				walk(t, f.Name, rd.Descriptor.Params[i], f.nodeFormItem)
			}
			raw := marshalString(t, s)
			if strings.Contains(raw, `"eq":null`) {
				t.Errorf("eq:null written: %s", raw)
			}
		})
	}
}

// TestNodeFormGolden snapshots the wire JSON of representative types.
func TestNodeFormGolden(t *testing.T) {
	for _, typ := range []string{"xflow.wait", "xflow.switch", "xflow.script", "xflow.http", "xflow.trigger.kafka", "xflow.map"} {
		t.Run(typ, func(t *testing.T) {
			got, err := json.MarshalIndent(projectBuiltin(t, typ), "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join("testdata", "node_form", typ+".json")
			if *updateNodeForm {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("%v (run with -update to create)", err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s is stale; rerun with -update and review the diff\ngot:\n%s", path, got)
			}
		})
	}
}

func TestNodeFormConditionWire(t *testing.T) {
	yes := true
	cases := []struct {
		name string
		cond types.Condition
		want string
	}{
		{"nil Eq is omitted, nil in In kept", types.Condition{Param: "mode", In: []any{"signal", nil}}, `{"param":"mode","in":["signal",null]}`},
		{"Eq written", types.Condition{Param: "mode", Eq: "timer"}, `{"param":"mode","eq":"timer"}`},
		{"Eq false written", types.Condition{Param: "on", Eq: false}, `{"param":"on","eq":false}`},
		{"truthy", types.Condition{Param: "signals", Truthy: &yes}, `{"param":"signals","truthy":true}`},
		{"empty non-nil In kept", types.Condition{Param: "x", In: []any{}}, `{"param":"x","in":[]}`},
		{"empty non-nil AnyOf kept", types.Condition{AnyOf: []types.Condition{}}, `{"any_of":[]}`},
		{"never", types.Condition{Not: &types.Condition{}}, `{"not":{}}`},
		{"nested", types.Condition{AllOf: []types.Condition{{Param: "a", Eq: 1}}, AnyOf: []types.Condition{{Param: "b", Eq: "x"}}},
			`{"all_of":[{"param":"a","eq":1}],"any_of":[{"param":"b","eq":"x"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := marshalString(t, projectCondition(&tc.cond)); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestNodeFormHiddenPlaceholderProjectsNever(t *testing.T) {
	f := findField(t, projectBuiltin(t, "xflow.script").Fields, "__artifact_file_path")
	if got := marshalString(t, f.VisibleWhen); got != `{"not":{}}` {
		t.Fatalf("visible_when = %s, want {\"not\":{}}", got)
	}
}

func TestNodeFormExpressionModes(t *testing.T) {
	cases := []struct {
		typ  string
		path []string
		want string
	}{
		{"xflow.map", []string{"body"}, "none"},
		{"xflow.map", []string{"items"}, "pure"},
		{"xflow.script", []string{"code"}, "literal"},
		{"xflow.if", []string{"condition"}, "pure"},
		{"xflow.switch", []string{"rules", "condition"}, "pure"},
		{"xflow.switch", []string{"rules", "output"}, "template"},
		{"xflow.trigger.kafka", []string{"topic"}, "template"},
		{"xflow.trigger.kafka", []string{"tuning", "max_wait"}, "template"},
		{"xflow.transform.set", []string{"expressions"}, "pure"},
	}
	for _, tc := range cases {
		f := findField(t, projectBuiltin(t, tc.typ).Fields, tc.path...)
		if f.Expression == nil || string(f.Expression.Mode) != tc.want {
			t.Errorf("%s %v: mode = %+v, want %s", tc.typ, tc.path, f.Expression, tc.want)
		}
	}
}

func TestNodeFormPaths(t *testing.T) {
	kafka := projectBuiltin(t, "xflow.trigger.kafka")
	if got := findField(t, kafka.Fields, "tuning", "max_wait").Path; got != "/parameters/tuning/max_wait" {
		t.Errorf("object sub-field path = %q", got)
	}
	// Array element fields are element-relative: compileNodeForm binds them
	// by name inside the repeat row and ignores path.
	sw := projectBuiltin(t, "xflow.switch")
	if got := findField(t, sw.Fields, "rules", "condition").Path; got != "condition" {
		t.Errorf("item field path = %q", got)
	}
}

// TestNodeFormFallbacks: every constant fallback surfaces on the field it
// names; derived ones and params with a Default carry none; and no entry
// names a field the projection does not produce (it would vanish silently).
func TestNodeFormFallbacks(t *testing.T) {
	byType := map[string]nodeFormSchema{}
	for _, s := range projectNodeTypes(builtinDescriptors(), builtinNodeFormFallbacks()) {
		byType[s.NodeType] = s
	}
	fallbacks := node.BuiltinFallbacks()
	if len(fallbacks) == 0 {
		t.Fatal("no builtin fallbacks")
	}
	for _, fb := range fallbacks {
		s, ok := byType[fb.Type]
		if !ok || s.NodeVersion != fb.Version {
			t.Errorf("%s@%d: type not projected", fb.Type, fb.Version)
			continue
		}
		f := findField(t, s.Fields, strings.Split(fb.Param, ".")...)
		if fb.Derived != "" {
			if f.Fallback != nil {
				t.Errorf("%s %s: derived fallback projected as %v", fb.Type, fb.Param, f.Fallback)
			}
			continue
		}
		if marshalString(t, f.Fallback) != marshalString(t, fb.Value) {
			t.Errorf("%s %s: fallback = %v, want %v", fb.Type, fb.Param, f.Fallback, fb.Value)
		}
	}
	if got := findField(t, byType["xflow.switch"].Fields, "default_output").Fallback; got != "default" {
		t.Errorf("switch.default_output fallback = %v", got)
	}
}

func TestNodeFormFieldMapping(t *testing.T) {
	mp := projectBuiltin(t, "xflow.map")
	coe := findField(t, mp.Fields, "continue_on_error")
	if coe.Type != "boolean" {
		t.Errorf("bool type = %q, want boolean", coe.Type)
	}
	if got := marshalString(t, coe.Default); got != "false" {
		t.Errorf("false default = %s, want written", got)
	}
	http := projectBuiltin(t, "xflow.http")
	url := findField(t, http.Fields, "url")
	if got := marshalString(t, url.Rules); got != `[{"type":"format","format":"url","advisory":true}]` {
		t.Errorf("url rules = %s", got)
	}
	if http.Kind != types.NodeKindAction || len(http.Credentials) == 0 {
		t.Errorf("http kind/credentials = %q %v", http.Kind, http.Credentials)
	}
	if kind := projectBuiltin(t, "xflow.trigger.kafka").Kind; kind != types.NodeKindTrigger {
		t.Errorf("kafka kind = %q", kind)
	}
}

func TestNodeFormSyntheticDescriptor(t *testing.T) {
	zero, one := 0.0, 5.0
	two, three := 2, 3
	rd := registry.RegisteredDescriptor{Type: "custom.x", Version: 3, Descriptor: types.Descriptor{
		Type:    "custom.x",
		Inputs:  []types.PortSpec{{Name: "main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Groups:  []types.GroupSpec{{Key: "adv", DisplayName: "Advanced", Collapsed: true}},
		OneOf:   []types.OneOfGroup{{Params: []string{"a", "b"}, Mode: types.OneOfAtLeast}},
		Docs:    "docs",
		Params: []types.ParamSpec{
			{Name: "n", Type: types.ParamNumber, Constraints: &types.Constraints{Min: &zero, Max: &one}, Group: "adv", Order: 2, Secret: true, Deprecated: "use m"},
			{Name: "s", Type: types.ParamString, Constraints: &types.Constraints{MinLength: &two, MaxLength: &three, Pattern: "^a", Format: "host-port"}},
			{Name: "l", Type: types.ParamArray, Item: &types.ParamSpec{Type: types.ParamString, Enum: []types.EnumOption{{Value: "x", DisplayName: "X"}}},
				Constraints: &types.Constraints{MinItems: &two, MaxItems: &three, UniqueItems: true}},
			{Name: "sel", Type: types.ParamString, Enum: []types.EnumOption{{Value: 1}},
				EnumWhen: []types.ConditionalEnum{{When: types.Condition{Param: "s", Eq: "a"}, Enum: []types.EnumOption{{Value: 2, Description: "two"}}}}},
		},
	}}
	s := projectNodeForm(rd, nil)
	if s.Kind != types.NodeKindAction {
		t.Errorf("empty kind = %q, want action", s.Kind)
	}
	want := `{"spec":"node-form/v1","node_type":"custom.x","node_version":3,"kind":"action","docs":"docs",` +
		`"ports":{"inputs":[{"name":"main"}],"outputs":[{"name":"main","display_name":"Main"}]},` +
		`"groups":[{"key":"adv","display_name":"Advanced","collapsed":true}],"one_of":[{"params":["a","b"],"mode":"at_least"}],"fields":[` +
		`{"name":"n","path":"/parameters/n","label":"n","type":"number","group":"adv","order":2,"secret":true,"deprecated":"use m","expression":{"mode":"template"},"rules":[{"type":"min","value":0},{"type":"max","value":5}]},` +
		`{"name":"s","path":"/parameters/s","label":"s","type":"string","expression":{"mode":"template"},"rules":[{"type":"min_length","value":2},{"type":"max_length","value":3},{"type":"pattern","pattern":"^a"},{"type":"format","format":"host-port","advisory":true}]},` +
		`{"name":"l","path":"/parameters/l","label":"l","type":"array","expression":{"mode":"template"},"rules":[{"type":"min_items","value":2},{"type":"max_items","value":3},{"type":"unique_items"}],"item":{"type":"string","expression":{"mode":"template"},"options":[{"value":"x","label":"X"}]}},` +
		`{"name":"sel","path":"/parameters/sel","label":"sel","type":"string","expression":{"mode":"template"},"options":[{"value":1}],"options_when":[{"when":{"param":"s","eq":"a"},"options":[{"value":2,"description":"two"}]}]}]}`
	if got := marshalString(t, s); got != want {
		t.Errorf("got\n%s\nwant\n%s", got, want)
	}
}
