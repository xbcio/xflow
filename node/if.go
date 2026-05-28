package node

import (
	"context"
	"fmt"

	"github.com/expr-lang/expr"
)

// IfNode implements xflow.if — conditional branch node.
type IfNode struct {
	BaseNode
	Condition string
}

// IF creates a conditional branch node.
//
//	node.IF("age > 18")
func IF(condition string) *IfNode {
	return &IfNode{Condition: condition}
}

func (n *IfNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.if",
		DisplayName: "IF Condition",
		Params: []ParamSpec{
			{Name: "condition", DisplayName: "Condition", Type: ParamString, Required: true, Description: "Boolean expression to evaluate"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "true", DisplayName: "True"}, {Name: "false", DisplayName: "False"}},
	}
}

func (n *IfNode) NodeType() string { return "xflow.if" }
func (n *IfNode) RawParams() any   { return map[string]any{"condition": n.Condition} }
func (n *IfNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *IfNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	condStr, _ := input.Params["condition"].(string)
	if condStr == "" {
		return nil, fmt.Errorf("xflow.if: condition parameter is required")
	}

	env := buildExprEnv(input)
	program, err := expr.Compile(condStr, expr.Env(env), expr.AsBool())
	if err != nil {
		return nil, fmt.Errorf("xflow.if: compile condition: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("xflow.if: evaluate condition: %w", err)
	}

	boolResult, ok := result.(bool)
	if !ok {
		return nil, fmt.Errorf("xflow.if: condition did not return bool, got %T", result)
	}

	if boolResult {
		return &Output{Data: input.Data, Port: "true"}, nil
	}
	return &Output{Data: input.Data, Port: "false"}, nil
}

func init() { Register(&IfNode{}) }
