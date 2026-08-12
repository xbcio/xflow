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

// TestMalformedTemplateGateSkipsHostSourceParams pins that the §4.1 form rule
// does NOT apply to a parameter holding host-language source. "${{" is legal JS:
// inside a template literal it interpolates an object literal, as in
// `${{a:1}.a}`. The form rule exists for values the template layer renders; JS
// source is handed to a script engine verbatim (script.go:150-163 passes it
// straight through, base64-decoding only for wasm), so no template layer ever
// sees it and no rule about wrapping applies.
//
// Without the exemption the compiler rejects a legal script at deploy time --
// the same class of false rejection as the reachability gate this package just
// removed, and with no workaround available to the author: they cannot rewrite
// their JS to avoid a construct the language defines.
func TestMalformedTemplateGateSkipsHostSourceParams(t *testing.T) {
	// Legal JS that trips every clause of the form rule: text before and after,
	// and a second "{{" from the nested object literal.
	const js = "const o = `${{a:1}.a}`; return o + `${{b:2}.b}`;"
	def := &types.WorkflowDef{
		Name: "js-src",
		Nodes: []types.NodeDef{
			{Name: "n1", Type: "xflow.script", Version: 1, Parameters: map[string]any{
				"language": "js",
				"runtime":  "js->goja",
				"code":     js,
			}},
		},
	}
	if _, err := Compile(def); err != nil {
		t.Fatalf("xflow.script's \"code\" is host-language source, not a template; "+
			"the §4.1 form rule must not apply to it: %v", err)
	}
}

// TestMalformedTemplateGateStillAppliesToFunctionCode is the positive control
// for the exemption above: it must be keyed on (nodeType, paramName), not on the
// parameter name "code" alone. xflow.function's "code" is an expr -- function.go
// hands it to EvalExpr via executeExpr -- so a malformed template there is a
// real error the author can fix. A name-only exemption would silently drop the
// rule for it too.
func TestMalformedTemplateGateStillAppliesToFunctionCode(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "fn-src",
		Nodes: []types.NodeDef{
			{Name: "n1", Type: "xflow.function", Version: 1, Parameters: map[string]any{
				"code": "x ${{ $params.amount }} y",
			}},
		},
	}
	if _, err := Compile(def); err == nil {
		t.Fatal("xflow.function's \"code\" is an expr, not host source; " +
			"a malformed template in it must still be rejected")
	}
}

// TestHostSourceParamsAreExemptAtTheBoundary pins the subset relation that makes
// skipping the compile-time form check safe. Every hostSourceParams entry must
// also be exempt in evaluableParams, because execution/params.go skips exempt
// parameters entirely -- so RenderTemplate never sees these values and its
// rule-1 precondition ("the malformed form was already excluded at compile
// time") is not weakened by the skip.
//
// If someone adds a host-source parameter that the boundary DOES evaluate, this
// fails. Without it, that combination silently reaches RenderTemplate in
// interpolation mode and the leading "$" becomes part of the output string --
// the exact failure the compile-time gate exists to prevent.
func TestHostSourceParamsAreExemptAtTheBoundary(t *testing.T) {
	for nodeType, params := range hostSourceParams {
		evaluable, known := EvaluableParams()[nodeType]
		if !known {
			t.Errorf("hostSourceParams[%q] has no EvaluableParams entry: the boundary "+
				"would evaluate its parameters and RenderTemplate would see a value "+
				"the compile-time form check just skipped", nodeType)
			continue
		}
		for param := range params {
			if !evaluable[param] {
				t.Errorf("hostSourceParams[%q][%q] is not exempt in EvaluableParams: "+
					"the boundary evaluates it, so skipping the compile-time form "+
					"check lets a malformed value reach RenderTemplate", nodeType, param)
			}
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
