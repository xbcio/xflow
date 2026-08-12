package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// 正对照:一个恒 true 的拒绝判据会让上面那条绿而这条红。
func TestCompileAcceptsATemplateInAnEvaluatedParam(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.start"},
			{Name: "B", Type: "xflow.if", Parameters: map[string]any{
				"condition": "${{ $params.amount > 1000 }}",
			}},
		},
		Connections: types.Connections{"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}}},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("xflow.if condition is evaluated; compile must accept it: %v", err)
	}
}

// TestCronExpressionIsNotAnEvaluableParam pins the (nodeType, paramName) keying.
// xflow.trigger.cron has a parameter called "expression" holding a cron spec
// ("0 */5 * * *"), not an expr. If the table were keyed by parameter name alone
// it would be reclassified as handler-evaluated, and the boundary would then
// skip it -- see execution/params.go's exemption derivation.
func TestCronExpressionIsNotAnEvaluableParam(t *testing.T) {
	if EvaluableParams()["xflow.trigger.cron"]["expression"] {
		t.Error("xflow.trigger.cron's \"expression\" is a cron spec, not an expr; " +
			"marking it handler-evaluated makes the boundary skip it")
	}
	if !EvaluableParams()["xflow.map"]["expression"] {
		t.Error("xflow.map's \"expression\" IS handler-evaluated (per-item env); " +
			"positive control for the assertion above")
	}
}

// TestCompileAcceptsATemplateInAnHTTPHeader is the positive control for the
// reachability gate's removal. Before Task 1's boundary evaluation layer
// existed, a template here shipped verbatim to the remote endpoint with HTTP
// 200 and node success -- which is why Task 0 rejected it at compile time.
// Now execution/params.go evaluates every non-exempt parameter at the handler
// boundary, so this form is the SUPPORTED one: DSL-SPECIFICATION.md's own
// examples (:354, :427, :447) author templates in exactly these parameters.
// Rejecting it would make the spec's documented form undeployable.
func TestCompileAcceptsATemplateInAnHTTPHeader(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "http-tmpl",
		Nodes: []types.NodeDef{
			{Name: "n1", Type: "xflow.http", Version: 1, Parameters: map[string]any{
				"url": "https://example.com",
				"headers": map[string]any{
					"X-Order": "{{ $params.order_id }}",
				},
				"order_id": "A-1",
			}},
		},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("a template in xflow.http headers must compile -- the boundary "+
			"evaluation layer evaluates it at runtime: %v", err)
	}
	for _, w := range g.Warnings() {
		if strings.Contains(w, "template") {
			t.Errorf("unexpected template warning: %q", w)
		}
	}
}

// spec §4.1:${{ }} 必须包裹整个值,前后有文本是编译错误。
// 这一条也管豁免参数——RenderTemplate 的规则 1 建立在它已生效之上。
func TestMalformedTemplateIsRejectedEvenInAnEvaluableParam(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.start"},
			{Name: "B", Type: "xflow.if", Parameters: map[string]any{
				"condition": "x ${{ $params.amount > 1000 }} y",
			}},
		},
		Connections: types.Connections{"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}}},
	}
	if _, err := Compile(def); err == nil {
		t.Fatal("expected rejection: text before and after ${{ }}")
	}
}
