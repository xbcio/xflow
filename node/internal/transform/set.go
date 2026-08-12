package transform

import (
	"context"
	"fmt"
	"sort"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/exprx"
	"github.com/xbcio/xflow/types"
)

// SetNode implements xflow.transform.set — assigns literal and expression
// fields onto the current input data.
type SetNode struct {
	nodeinternal.BaseNode
	Fields      map[string]any
	Expressions map[string]string
}

func Set(fields map[string]any) *SetNode {
	return &SetNode{Fields: fields}
}

func (n *SetNode) SetExpr(expressions map[string]string) *SetNode {
	n.Expressions = expressions
	return n
}

func (n *SetNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.set",
		DisplayName: "Set",
		Params: []types.ParamSpec{
			{Name: "fields", DisplayName: "Fields", Type: types.ParamObject, Required: false, Description: "Literal fields to assign"},
			{Name: "expressions", DisplayName: "Expressions", Type: types.ParamObject, Required: false, Description: "Fields computed from expressions"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *SetNode) NodeType() string { return "xflow.transform.set" }
func (n *SetNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *SetNode) RawParams() any {
	params := map[string]any{}
	if n.Fields != nil {
		params["fields"] = n.Fields
	}
	if n.Expressions != nil {
		params["expressions"] = n.Expressions
	}
	return params
}

func (n *SetNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	data := cloneData(input)
	if fields, ok := input.Params["fields"].(map[string]any); ok {
		for key, value := range fields {
			data[key] = value
		}
	}
	expressions, err := parseStringMap(input.Params["expressions"])
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.set: %w", err)
	}
	env := exprx.BuildExprEnv(input, nil)
	// Evaluate expressions in a deterministic (lexicographic) order. A Go map
	// iterates in random order, which made the result of cross-expression
	// references (B reads a key that A sets) non-deterministic. Sorting the
	// keys makes the evaluation order stable so the same input always yields
	// the same output; expressions that reference a not-yet-computed key will
	// resolve against the pre-existing value (or be absent), which is the
	// expected behavior for a declared evaluation order.
	keys := make([]string, 0, len(expressions))
	for key := range expressions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		expression := expressions[key]
		value, err := exprx.EvalExpr(expression, env, false)
		if err != nil {
			return nil, fmt.Errorf("xflow.transform.set: field %q: %w", key, err)
		}
		data[key] = value
		env[key] = value
	}
	return &types.Output{Data: data}, nil
}
