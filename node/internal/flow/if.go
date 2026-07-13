package flow

import "github.com/xbcio/xflow/types"

import (
	"context"
	"fmt"

	. "github.com/xbcio/xflow/node/internal"

	"github.com/xbcio/xflow/node/internal/utils/exprx"
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

func (n *IfNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.if",
		DisplayName: "IF Condition",
		Params: []types.ParamSpec{
			{Name: "condition", DisplayName: "Condition", Type: types.ParamString, Required: true, Description: "Boolean expression to evaluate"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "true", DisplayName: "True"}, {Name: "false", DisplayName: "False"}},
	}
}

func (n *IfNode) NodeType() string { return "xflow.if" }
func (n *IfNode) RawParams() any   { return map[string]any{"condition": n.Condition} }
func (n *IfNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *IfNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	condStr, _ := input.Params["condition"].(string)
	if condStr == "" {
		return nil, fmt.Errorf("xflow.if: condition parameter is required")
	}

	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(condStr, env, true)
	if err != nil {
		return nil, fmt.Errorf("xflow.if: %w", err)
	}

	boolResult, ok := result.(bool)
	if !ok {
		return nil, fmt.Errorf("xflow.if: condition did not return bool, got %T", result)
	}

	if boolResult {
		return &types.Output{Data: input.Data, Port: "true"}, nil
	}
	return &types.Output{Data: input.Data, Port: "false"}, nil
}

func init() { Register(&IfNode{}) }
