package node

import (
	"context"
	"fmt"

	"github.com/expr-lang/expr"
)

// SwitchRule defines a single routing rule for SwitchNode.
type SwitchRule struct {
	Condition string
	Output    string
}

// SwitchNode implements xflow.switch — multi-branch routing node.
type SwitchNode struct {
	BaseNode
	Mode          string // "rules" or "expression"
	Rules         []SwitchRule
	Expression    string
	DefaultOutput string
}

// Switch creates a multi-branch routing node in rules mode.
//
//	node.Switch([]node.SwitchRule{
//	    {Condition: "status == \"active\"", Output: "active"},
//	}, "default")
func Switch(rules []SwitchRule, defaultOutput string) *SwitchNode {
	return &SwitchNode{Mode: "rules", Rules: rules, DefaultOutput: defaultOutput}
}

// SwitchExpr creates a multi-branch routing node in expression mode.
//
//	node.SwitchExpr("category", "other")
func SwitchExpr(expression string, defaultOutput string) *SwitchNode {
	return &SwitchNode{Mode: "expression", Expression: expression, DefaultOutput: defaultOutput}
}

func (n *SwitchNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.switch",
		DisplayName: "Switch",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: true, Description: "Routing mode: \"rules\" or \"expression\""},
			{Name: "outputs", DisplayName: "Outputs", Type: ParamArray, Required: true, Description: "List of output port names (dynamic)"},
			{Name: "rules", DisplayName: "Rules", Type: ParamArray, Required: false, Description: "Rule list for rules mode"},
			{Name: "expression", DisplayName: "Expression", Type: ParamString, Required: false, Description: "Expression for expression mode"},
			{Name: "default_output", DisplayName: "Default Output", Type: ParamString, Required: false, Description: "Port name used when no rule matches"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{},
	}
}

func (n *SwitchNode) NodeType() string { return "xflow.switch" }
func (n *SwitchNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *SwitchNode) RawParams() any {
	params := map[string]any{
		"mode":           n.Mode,
		"default_output": n.DefaultOutput,
	}
	if n.Mode == "rules" {
		rawRules := make([]any, len(n.Rules))
		for i, r := range n.Rules {
			rawRules[i] = map[string]any{"condition": r.Condition, "output": r.Output}
		}
		params["rules"] = rawRules
	} else {
		params["expression"] = n.Expression
	}
	return params
}

func (n *SwitchNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	mode, _ := input.Params["mode"].(string)
	if mode == "" {
		mode = "rules"
	}

	switch mode {
	case "rules":
		return n.executeRules(input)
	case "expression":
		return n.executeExpression(input)
	default:
		return nil, fmt.Errorf("xflow.switch: unknown mode %q", mode)
	}
}

func (n *SwitchNode) executeRules(input *Input) (*Output, error) {
	rules, _ := input.Params["rules"].([]any)
	env := buildExprEnv(input)

	for _, r := range rules {
		rule, ok := r.(map[string]any)
		if !ok {
			continue
		}
		condStr, _ := rule["condition"].(string)
		if condStr == "" {
			continue
		}
		output, _ := rule["output"].(string)
		if output == "" {
			continue
		}

		program, err := expr.Compile(condStr, expr.Env(env), expr.AsBool())
		if err != nil {
			return nil, fmt.Errorf("xflow.switch: compile rule condition %q: %w", condStr, err)
		}
		result, err := expr.Run(program, env)
		if err != nil {
			return nil, fmt.Errorf("xflow.switch: evaluate rule condition %q: %w", condStr, err)
		}
		if matched, _ := result.(bool); matched {
			return &Output{Data: input.Data, Port: output}, nil
		}
	}

	defaultOutput, _ := input.Params["default_output"].(string)
	if defaultOutput == "" {
		defaultOutput = "default"
	}
	return &Output{Data: input.Data, Port: defaultOutput}, nil
}

func (n *SwitchNode) executeExpression(input *Input) (*Output, error) {
	exprStr, _ := input.Params["expression"].(string)
	if exprStr == "" {
		return nil, fmt.Errorf("xflow.switch: expression parameter is required in expression mode")
	}

	env := buildExprEnv(input)
	program, err := expr.Compile(exprStr, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("xflow.switch: compile expression: %w", err)
	}
	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("xflow.switch: evaluate expression: %w", err)
	}

	port := fmt.Sprintf("%v", result)
	if port == "" {
		defaultOutput, _ := input.Params["default_output"].(string)
		if defaultOutput == "" {
			defaultOutput = "default"
		}
		port = defaultOutput
	}
	return &Output{Data: input.Data, Port: port}, nil
}

func init() { Register(&SwitchNode{}) }
