package graph_test

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/engine/graph"
	_ "github.com/xbcio/xflow/node" // register the builtin node types the cases name
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// TestExpressionMode pins every branch of ExpressionMode against real builtin
// node types. Each case also asserts the (type, path) it uses is declared in
// that type's registered Descriptor, so a renamed parameter fails here instead
// of the case silently testing a path nothing reads.
func TestExpressionMode(t *testing.T) {
	cases := []struct {
		name string
		kind types.NodeKind
		typ  string
		path string
		want graph.ExpressionModeValue
	}{
		// Triggers are rendered at activation (EvaluateActivationParams), so a
		// template in one is honoured -- they are not "none".
		{"trigger param", types.NodeKindTrigger, "xflow.trigger.kafka", "topic", graph.ExpressionModeTemplate},
		// cron's "expression" is a cron spec, not an expr: keyed by type.
		{"trigger cron expression", types.NodeKindTrigger, "xflow.trigger.cron", "expression", graph.ExpressionModeTemplate},
		{"map body", types.NodeKindAction, "xflow.map", "body", graph.ExpressionModeNone},
		{"map body nested path", types.NodeKindAction, "xflow.map", "body/nodes", graph.ExpressionModeNone},
		{"script code", types.NodeKindAction, "xflow.script", "code", graph.ExpressionModeLiteral},
		{"function code is expr, not host source", types.NodeKindAction, "xflow.function", "code", graph.ExpressionModePure},
		{"if condition", types.NodeKindAction, "xflow.if", "condition", graph.ExpressionModePure},
		{"map expression", types.NodeKindAction, "xflow.map", "expression", graph.ExpressionModePure},
		{"switch rules condition", types.NodeKindAction, "xflow.switch", "rules/condition", graph.ExpressionModePure},
		{"switch rules output", types.NodeKindAction, "xflow.switch", "rules/output", graph.ExpressionModeTemplate},
		{"switch rules as a whole", types.NodeKindAction, "xflow.switch", "rules", graph.ExpressionModeTemplate},
		{"http url", types.NodeKindAction, "xflow.http", "url", graph.ExpressionModeTemplate},
		// xflow.http's "body" is a request payload, not a sub-graph body.
		{"http body", types.NodeKindAction, "xflow.http", "body", graph.ExpressionModeTemplate},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			desc, ok := registeredDescriptor(tc.typ)
			if !ok {
				t.Fatalf("node type %q is not registered", tc.typ)
			}
			// Most builtin action Descriptors leave Kind empty; empty means action.
			kind := desc.Kind
			if kind == "" {
				kind = types.NodeKindAction
			}
			if kind != tc.kind {
				t.Fatalf("node type %q has kind %q, case says %q", tc.typ, kind, tc.kind)
			}
			if !declaresPath(desc.Params, tc.path) {
				t.Fatalf("node type %q does not declare parameter path %q", tc.typ, tc.path)
			}
			if got := graph.ExpressionMode(tc.kind, tc.typ, tc.path); got != tc.want {
				t.Errorf("ExpressionMode(%q, %q, %q) = %q, want %q", tc.kind, tc.typ, tc.path, got, tc.want)
			}
		})
	}
}

// TestExpressionModeEdgePaths covers paths no Descriptor declares: they must
// fall through to template rather than panic or inherit a neighbour's mode.
func TestExpressionModeEdgePaths(t *testing.T) {
	cases := []struct {
		typ, path string
		want      graph.ExpressionModeValue
	}{
		{"xflow.if", "", graph.ExpressionModeTemplate},
		{"xflow.if", "/condition", graph.ExpressionModeTemplate}, // leading slash is not the format
		{"xflow.switch", "rules/condition/deeper", graph.ExpressionModePure},
		{"xflow.transform.set", "expressions/total", graph.ExpressionModePure},
		{"xflow.unknown", "anything", graph.ExpressionModeTemplate},
	}
	for _, tc := range cases {
		if got := graph.ExpressionMode(types.NodeKindAction, tc.typ, tc.path); got != tc.want {
			t.Errorf("ExpressionMode(action, %q, %q) = %q, want %q", tc.typ, tc.path, got, tc.want)
		}
	}
}

func TestExpressionPredicates(t *testing.T) {
	if !graph.IsEvaluableParam("xflow.if", "condition") {
		t.Error(`IsEvaluableParam("xflow.if", "condition") = false`)
	}
	if graph.IsEvaluableParam("xflow.trigger.cron", "expression") {
		t.Error(`IsEvaluableParam("xflow.trigger.cron", "expression") = true: keyed by type, not name`)
	}
	if graph.IsEvaluableParam("xflow.switch", "rules") {
		t.Error(`IsEvaluableParam("xflow.switch", "rules") = true: only its condition sub-field is`)
	}
	if !graph.IsEvaluableSubField("xflow.switch", "rules", "condition") {
		t.Error(`IsEvaluableSubField("xflow.switch", "rules", "condition") = false`)
	}
	if graph.IsEvaluableSubField("xflow.switch", "rules", "output") {
		t.Error(`IsEvaluableSubField("xflow.switch", "rules", "output") = true`)
	}
	if !graph.HasEvaluableSubFields("xflow.switch", "rules") || graph.HasEvaluableSubFields("xflow.switch", "expression") {
		t.Error("HasEvaluableSubFields misclassifies xflow.switch")
	}
	if !graph.IsHostSourceParam("xflow.script", "code") {
		t.Error(`IsHostSourceParam("xflow.script", "code") = false`)
	}
	if graph.IsHostSourceParam("xflow.function", "code") {
		t.Error(`IsHostSourceParam("xflow.function", "code") = true: function code is an expr`)
	}
}

func registeredDescriptor(typ string) (types.Descriptor, bool) {
	for _, rd := range registry.Descriptors() {
		if rd.Type == typ {
			return rd.Descriptor, true
		}
	}
	return types.Descriptor{}, false
}

// declaresPath walks "param/sub/..." through Params, Fields and Item.Fields.
// A path below an object parameter with no declared Fields (a free map, or a
// sub-graph body) is accepted once its first segment is declared.
func declaresPath(params []types.ParamSpec, path string) bool {
	head, rest, _ := strings.Cut(path, "/")
	for _, p := range params {
		if p.Name != head {
			continue
		}
		if rest == "" {
			return true
		}
		fields := p.Fields
		if p.Item != nil {
			fields = p.Item.Fields
		}
		if len(fields) == 0 {
			return true
		}
		return declaresPath(fields, rest)
	}
	return false
}
