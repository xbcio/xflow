package graph

import (
	"strings"
	"testing"

	"github.com/xbcio/xflow/types"
)

// xflow.http 的 headers 是本仓库最严重的静默形态:模板照发,对端收到字面量,
// HTTP 200,节点成功,零诊断。编译期必须拒绝。
func TestCompileRejectsATemplateInANonEvaluatedParam(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.start"},
			{Name: "B", Type: "xflow.http", Parameters: map[string]any{
				"url": "https://example.com/x",
				"headers": map[string]any{
					"X-Order": "${{ $params.order_id }}",
				},
			}},
		},
		Connections: types.Connections{"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}}},
	}
	_, err := Compile(def)
	if err == nil {
		t.Fatal("expected compile to reject a template in xflow.http headers, got nil")
	}
	for _, want := range []string{"B", "headers"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error must name the node and the parameter; got %q", err.Error())
		}
	}
	// 安全:错误信息不得回显参数值。值可能是 "Bearer {{ ... }}" 展开后的凭证。
	if strings.Contains(err.Error(), "order_id") {
		t.Errorf("error must not echo the parameter value; got %q", err.Error())
	}
}

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

// xflow.trigger.cron 的 expression 是 cron 规格串,不是表达式。
// 只按参数名建白名单会把它误放进可求值集合,这条测试钉住 (type, param) 键。
func TestCronExpressionIsNotAnEvaluableParam(t *testing.T) {
	if evaluableParams["xflow.trigger.cron"]["expression"] {
		t.Error("xflow.trigger.cron expression is a cron spec, not an expr")
	}
	if !evaluableParams["xflow.switch"]["expression"] {
		t.Error("xflow.switch expression IS evaluated")
	}
}

func TestContainsTemplateDetectsBothForms(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want bool
	}{
		{"${{ $params.x }}", true},
		{"prefix {{ $params.x }} suffix", true},
		{`{"a":1}`, false}, // http 的 JSON 请求体:有花括号但无 {{
		{"", false},
		{"plain text", false},
	} {
		if got := containsTemplate(tc.in); got != tc.want {
			t.Errorf("containsTemplate(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// xflow.switch 只求值 rules[].condition;rules[].output 被 cast.ToString 当端口名。
// 整树豁免会让 output 里的模板既不被拒也不被求值,字面量当端口名 → 静默走 default。
func TestSwitchRuleOutputIsNotExempt(t *testing.T) {
	mk := func(rule map[string]any) *types.WorkflowDef {
		return &types.WorkflowDef{
			Name: "wf",
			Nodes: []types.NodeDef{
				{Name: "A", Type: "xflow.start"},
				{Name: "B", Type: "xflow.switch", Parameters: map[string]any{
					"rules": []any{rule},
				}},
			},
			Connections: types.Connections{"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}}},
		}
	}
	// condition 里的模板合法。
	if _, err := Compile(mk(map[string]any{
		"condition": "${{ $params.amount > 1000 }}", "output": "big",
	})); err != nil {
		t.Fatalf("rules[].condition is evaluated; must be accepted: %v", err)
	}
	// output 里的模板必须被拒。
	_, err := Compile(mk(map[string]any{
		"condition": "true", "output": "${{ $params.port }}",
	}))
	if err == nil {
		t.Fatal("expected rejection: rules[].output is used verbatim as a port name")
	}
	if !strings.Contains(err.Error(), "output") {
		t.Errorf("error must name the offending field; got %q", err.Error())
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

// 未登记的节点类型只警告,不硬拒——它可能在自己 handler 里求值。
// 表的完备性缺口不该变成对第三方节点的误拒。
func TestUnknownNodeTypeWarnsInsteadOfRejecting(t *testing.T) {
	def := &types.WorkflowDef{
		Name: "wf",
		Nodes: []types.NodeDef{
			{Name: "A", Type: "xflow.start"},
			{Name: "B", Type: "vendor.custom", Parameters: map[string]any{
				"whatever": "${{ $params.x }}",
			}},
		},
		Connections: types.Connections{"A": {"main": {Targets: []types.Connection{{Node: "B", Input: "main"}}}}},
	}
	g, err := Compile(def)
	if err != nil {
		t.Fatalf("an unregistered node type must not be rejected: %v", err)
	}
	warnings := g.Warnings()
	if len(warnings) == 0 {
		t.Fatal("expected a warning for an unregistered type carrying a template")
	}
	for _, w := range warnings {
		if strings.Contains(w, "$params.x") {
			t.Errorf("warning must not echo the value; got %q", w)
		}
	}
}
