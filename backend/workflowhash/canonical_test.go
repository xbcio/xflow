package workflowhash

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/types"
)

// fakeSpecs stands in for node.BuiltinParamSpecs with a few Defaults in the
// builtins' own shapes (an int, a string, a bool, one per kind).
func fakeSpecs(kind types.NodeKind, nodeType string, version int) ([]types.ParamSpec, bool) {
	if kind == "" {
		kind = types.NodeKindAction
	}
	if version != 0 && version != 1 {
		return nil, false
	}
	switch {
	case kind == types.NodeKindAction && nodeType == "xflow.wait":
		return []types.ParamSpec{{Name: "mode", Default: "signal"}, {Name: "signal_name"}}, true
	case kind == types.NodeKindAction && nodeType == "xflow.approval":
		return []types.ParamSpec{{Name: "mode", Default: "any"}}, true
	case kind == types.NodeKindAction && nodeType == "xflow.map":
		return []types.ParamSpec{{Name: "batch_size", Default: 1}, {Name: "body"}}, true
	case kind == types.NodeKindAction && nodeType == "xflow.http":
		return []types.ParamSpec{{Name: "method", Default: "GET"}, {Name: "body"}}, true
	case kind == types.NodeKindAction && nodeType == "xflow.required":
		return []types.ParamSpec{{Name: "url", Required: true}}, true
	case kind == types.NodeKindTrigger && nodeType == "xflow.trigger.cron":
		return []types.ParamSpec{{Name: "timezone", Default: "UTC"}}, true
	case kind == types.NodeKindSupply && nodeType == "xflow.supply.static":
		return []types.ParamSpec{{Name: "require_ready", Default: true}}, true
	}
	return nil, false
}

func oneNode(n types.NodeDef) *types.WorkflowDef {
	return &types.WorkflowDef{Namespace: "default", Name: "wf", Version: "v1", Nodes: []types.NodeDef{n}}
}

// subgraph returns a body param in the generic shape the SDK builder and the
// JSON decoder both produce.
func subgraph(members ...map[string]any) map[string]any {
	nodes := make([]any, len(members))
	for i, m := range members {
		nodes[i] = m
	}
	return map[string]any{
		"type":       types.SubgraphNodeType,
		"parameters": map[string]any{"nodes": nodes, "connections": map[string]any{}},
	}
}

func TestCanonicalFillsDefaults(t *testing.T) {
	tests := []struct {
		name string
		node types.NodeDef
		want map[string]any
	}{
		{"missing param is filled",
			types.NodeDef{Name: "w", Type: "xflow.wait", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"signal_name": "go"}},
			map[string]any{"signal_name": "go", "mode": "signal"}},
		{"nil params are filled",
			types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1},
			map[string]any{"mode": "signal"}},
		{"explicit nil is filled",
			types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1, Parameters: map[string]any{"mode": nil}},
			map[string]any{"mode": "signal"}},
		{"empty string is kept",
			types.NodeDef{Name: "a", Type: "xflow.approval", Version: 1, Parameters: map[string]any{"mode": ""}},
			map[string]any{"mode": ""}},
		{"set value is kept",
			types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1, Parameters: map[string]any{"mode": "timer"}},
			map[string]any{"mode": "timer"}},
		{"disabled node is filled",
			types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1, Disabled: true},
			map[string]any{"mode": "signal"}},
		{"version 0 resolves latest",
			types.NodeDef{Name: "w", Type: "xflow.wait"},
			map[string]any{"mode": "signal"}},
		{"trigger kind",
			types.NodeDef{Name: "c", Type: "xflow.trigger.cron", Kind: types.NodeKindTrigger, Version: 1},
			map[string]any{"timezone": "UTC"}},
		{"supply kind",
			types.NodeDef{Name: "s", Type: "xflow.supply.static", Kind: types.NodeKindSupply, Version: 1},
			map[string]any{"require_ready": true}},
		{"payload body is not a subgraph",
			types.NodeDef{Name: "h", Type: "xflow.http", Version: 1, Parameters: map[string]any{"body": map[string]any{"type": "json"}}},
			map[string]any{"body": map[string]any{"type": "json"}, "method": "GET"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Canonical(oneNode(tt.node), fakeSpecs).Nodes[0].Parameters
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Canonical(%s) params = %#v, want %#v", tt.node.Type, got, tt.want)
			}
		})
	}
}

func TestCanonicalLeavesUnknownNodesAlone(t *testing.T) {
	tests := []struct {
		name string
		node types.NodeDef
	}{
		{"custom type", types.NodeDef{Name: "x", Type: "custom.node", Version: 1}},
		{"direct handler", types.NodeDef{Name: "x", Type: "__direct__/x"}},
		{"kind mismatch", types.NodeDef{Name: "x", Type: "xflow.wait", Kind: types.NodeKindTrigger, Version: 1}},
		{"unknown version", types.NodeDef{Name: "x", Type: "xflow.wait", Version: 7}},
		{"required without default", types.NodeDef{Name: "x", Type: "xflow.required", Version: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def := oneNode(tt.node)
			if got := Canonical(def, fakeSpecs); got != def {
				t.Fatalf("Canonical(%s) returned a copy, want def itself", tt.node.Type)
			}
		})
	}
}

func TestCanonicalBodies(t *testing.T) {
	member := func(params map[string]any) map[string]any {
		m := map[string]any{"name": "w", "type": "xflow.wait", "kind": "action", "version": float64(1)}
		if params != nil {
			m["parameters"] = params
		}
		return m
	}

	t.Run("body-bearing parent is skipped and members are filled", func(t *testing.T) {
		def := oneNode(types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{
			"items": "{{ $input }}",
			"body":  subgraph(member(nil), member(map[string]any{"mode": "timer"})),
		}})
		got := Canonical(def, fakeSpecs).Nodes[0].Parameters
		if _, filled := got["batch_size"]; filled {
			t.Fatalf("body-bearing parent was filled: %#v", got)
		}
		want := subgraph(member(map[string]any{"mode": "signal"}), member(map[string]any{"mode": "timer"}))
		if !reflect.DeepEqual(got["body"], want) {
			t.Fatalf("body = %#v, want %#v", got["body"], want)
		}
	})

	t.Run("nested body members are filled", func(t *testing.T) {
		inner := map[string]any{"name": "inner", "type": "xflow.map", "version": float64(1),
			"parameters": map[string]any{"body": subgraph(member(nil))}}
		def := oneNode(types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": subgraph(inner)}})
		got := Canonical(def, fakeSpecs).Nodes[0].Parameters["body"]
		wantInner := map[string]any{"name": "inner", "type": "xflow.map", "version": float64(1),
			"parameters": map[string]any{"body": subgraph(member(map[string]any{"mode": "signal"}))}}
		if want := subgraph(wantInner); !reflect.DeepEqual(got, want) {
			t.Fatalf("body = %#v, want %#v", got, want)
		}
	})

	t.Run("non-generic body is normalized only when filled", func(t *testing.T) {
		typed := map[string]any{
			"type": types.SubgraphNodeType,
			"parameters": map[string]any{
				"nodes": []types.NodeDef{{Name: "w", Type: "xflow.wait", Kind: types.NodeKindAction, Version: 1}},
			},
		}
		def := oneNode(types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": typed}})
		got := Canonical(def, fakeSpecs)
		data, err := json.Marshal(got.Nodes[0].Parameters["body"])
		if err != nil {
			t.Fatalf("marshal body: %v", err)
		}
		var decoded struct {
			Parameters struct {
				Nodes []types.NodeDef `json:"nodes"`
			} `json:"parameters"`
		}
		if err := json.Unmarshal(data, &decoded); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if len(decoded.Parameters.Nodes) != 1 || decoded.Parameters.Nodes[0].Parameters["mode"] != "signal" {
			t.Fatalf("typed body member not filled: %s", data)
		}

		filled := map[string]any{
			"type": types.SubgraphNodeType,
			"parameters": map[string]any{
				"nodes": []types.NodeDef{{Name: "w", Type: "xflow.wait", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"mode": "signal"}}},
			},
		}
		same := oneNode(types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": filled}})
		if Canonical(same, fakeSpecs) != same {
			t.Fatal("a typed body with nothing to fill was rewritten")
		}
	})
}

func TestCanonicalDoesNotMutateInput(t *testing.T) {
	member := map[string]any{"name": "w", "type": "xflow.wait", "version": float64(1)}
	def := &types.WorkflowDef{Name: "wf", Nodes: []types.NodeDef{
		{Name: "w", Type: "xflow.wait", Version: 1, Parameters: map[string]any{"signal_name": "go"}},
		{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": subgraph(member)}},
	}}
	before, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal before: %v", err)
	}
	if got := Canonical(def, fakeSpecs); got == def {
		t.Fatal("Canonical returned def itself although it filled params")
	}
	after, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal after: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("Canonical mutated its input:\nbefore %s\nafter  %s", before, after)
	}
}

func TestCanonicalNilInputs(t *testing.T) {
	if Canonical(nil, fakeSpecs) != nil {
		t.Fatal("Canonical(nil) != nil")
	}
	def := oneNode(types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1})
	if Canonical(def, nil) != def {
		t.Fatal("Canonical(def, nil) returned a copy")
	}
}

// TestRuntimeOmittedEqualsDefault is the cross-path identity: a definition
// that omits a builtin param hashes like one that spells out its Default,
// including an int Default against the float64 a JSON decode yields.
func TestRuntimeOmittedEqualsDefault(t *testing.T) {
	hash := func(t *testing.T, def *types.WorkflowDef) string {
		t.Helper()
		h, err := Runtime(def, fakeSpecs)
		if err != nil {
			t.Fatalf("Runtime: %v", err)
		}
		return h
	}
	tests := []struct {
		name            string
		omitted, filled types.NodeDef
	}{
		{"string default",
			types.NodeDef{Name: "w", Type: "xflow.wait", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"signal_name": "go"}},
			types.NodeDef{Name: "w", Type: "xflow.wait", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"signal_name": "go", "mode": "signal"}}},
		{"int default against json float64",
			types.NodeDef{Name: "m", Type: "xflow.map", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"expression": "x"}},
			types.NodeDef{Name: "m", Type: "xflow.map", Kind: types.NodeKindAction, Version: 1, Parameters: map[string]any{"expression": "x", "batch_size": float64(1)}}},
		{"body member",
			types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": subgraph(
				map[string]any{"name": "w", "type": "xflow.wait", "version": float64(1)})}},
			types.NodeDef{Name: "m", Type: "xflow.map", Version: 1, Parameters: map[string]any{"body": subgraph(
				map[string]any{"name": "w", "type": "xflow.wait", "version": float64(1), "parameters": map[string]any{"mode": "signal"}})}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if a, b := hash(t, oneNode(tt.omitted)), hash(t, oneNode(tt.filled)); a != b {
				t.Fatalf("omitted %q != filled %q", a, b)
			}
		})
	}

	t.Run("a real param change still differs", func(t *testing.T) {
		a := hash(t, oneNode(types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1}))
		b := hash(t, oneNode(types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1, Parameters: map[string]any{"mode": "timer"}}))
		if a == b {
			t.Fatal("a non-default mode hashed like the default")
		}
	})

	t.Run("reconcile upgrades a legacy omitted record", func(t *testing.T) {
		omitted := oneNode(types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1})
		filled := oneNode(types.NodeDef{Name: "w", Type: "xflow.wait", Version: 1, Parameters: map[string]any{"mode": "signal"}})
		got, upgrade, err := Reconcile("sha256:legacy", omitted, fakeSpecs)
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if want := hash(t, filled); got != want || !upgrade {
			t.Fatalf("Reconcile = (%q, %v), want (%q, true)", got, upgrade, want)
		}
	})
}
