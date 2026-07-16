package flow

import (
	"context"
	"fmt"

	"github.com/spf13/cast"

	"github.com/xbcio/xflow/types"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/utils/exprx"
	"github.com/xbcio/xflow/node/registry"
)

// IfNode implements xflow.if — conditional branch node.
type IfNode struct {
	nodeinternal.BaseNode
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

	// Use cast.ToBool for parity with xflow.switch rules mode. AsBool=true at
	// compile time already forces expr to return a bool, so this is a robustness
	// measure: if the expression engine ever yields a truthy non-bool (e.g. an
	// integer 1), both flow nodes coerce consistently instead of one erroring.
	boolResult := cast.ToBool(result)

	if boolResult {
		return &types.Output{Data: input.Data, Port: "true"}, nil
	}
	return &types.Output{Data: input.Data, Port: "false"}, nil
}

func init() { registry.Register(&IfNode{}) }
