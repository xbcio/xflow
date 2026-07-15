package flow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/utils/exprx"
	"github.com/xbcio/xflow/node/registry"
	"github.com/spf13/cast"
)

// SwitchRule defines a single routing rule for SwitchNode.
type SwitchRule struct {
	Condition string
	Output    string
}

// SwitchNode implements xflow.switch — multi-branch routing node.
type SwitchNode struct {
	nodeinternal.BaseNode
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

func (n *SwitchNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.switch",
		DisplayName: "Switch",
		Params: []types.ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: true, Description: "Routing mode: \"rules\" or \"expression\""},
			{Name: "outputs", DisplayName: "Outputs", Type: types.ParamArray, Required: true, Description: "List of output port names (dynamic)"},
			{Name: "rules", DisplayName: "Rules", Type: types.ParamArray, Required: false, Description: "Rule list for rules mode"},
			{Name: "expression", DisplayName: "Expression", Type: types.ParamString, Required: false, Description: "Expression for expression mode"},
			{Name: "default_output", DisplayName: "Default Output", Type: types.ParamString, Required: false, Description: "Port name used when no rule matches"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{},
	}
}

func (n *SwitchNode) NodeType() string { return "xflow.switch" }
func (n *SwitchNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
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

func (n *SwitchNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	mode := cast.ToString(input.Params["mode"])
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

func (n *SwitchNode) executeRules(input *types.Input) (*types.Output, error) {
	rules, _ := input.Params["rules"].([]any)
	env := exprx.BuildExprEnv(input, nil)

	for _, r := range rules {
		rule, ok := r.(map[string]any)
		if !ok {
			continue
		}
		condStr := cast.ToString(rule["condition"])
		if condStr == "" {
			continue
		}
		output := cast.ToString(rule["output"])
		if output == "" {
			continue
		}

		result, err := exprx.EvalExpr(condStr, env, true)
		if err != nil {
			return nil, fmt.Errorf("xflow.switch: rule condition %q: %w", condStr, err)
		}
		if matched := cast.ToBool(result); matched {
			return &types.Output{Data: input.Data, Port: output}, nil
		}
	}

	defaultOutput := cast.ToString(input.Params["default_output"])
	if defaultOutput == "" {
		defaultOutput = "default"
	}
	return &types.Output{Data: input.Data, Port: defaultOutput}, nil
}

func (n *SwitchNode) executeExpression(input *types.Input) (*types.Output, error) {
	exprStr := cast.ToString(input.Params["expression"])
	if exprStr == "" {
		return nil, fmt.Errorf("xflow.switch: expression parameter is required in expression mode")
	}

	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(exprStr, env, false)
	if err != nil {
		return nil, fmt.Errorf("xflow.switch: %w", err)
	}

	port := cast.ToString(result)
	if port == "" {
		defaultOutput := cast.ToString(input.Params["default_output"])
		if defaultOutput == "" {
			defaultOutput = "default"
		}
		port = defaultOutput
	}
	return &types.Output{Data: input.Data, Port: port}, nil
}

func init() { registry.Register(&SwitchNode{}) }
