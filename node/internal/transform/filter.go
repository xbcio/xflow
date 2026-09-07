package transform

import (
	"context"
	"fmt"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/exprx"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

// FilterNode implements xflow.transform.filter — keeps items matching an
// expression. The expression can reference item, index, and item map fields.
type FilterNode struct {
	nodeinternal.BaseNode
	Items     string
	Condition string
}

func Filter(itemsExpr string, condition string) *FilterNode {
	return &FilterNode{Items: itemsExpr, Condition: condition}
}

func (n *FilterNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.filter",
		DisplayName: "Filter",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to filter"},
			{Name: "condition", DisplayName: "Condition", Type: types.ParamString, Required: true, Description: "Boolean expression evaluated per item"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *FilterNode) NodeType() string { return "xflow.transform.filter" }
func (n *FilterNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *FilterNode) RawParams() any {
	return map[string]any{"items": n.Items, "condition": n.Condition}
}

func (n *FilterNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	items, itemsKey, err := itemsFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.filter: %w", err)
	}
	condition := cast.ToString(input.Params["condition"])
	if condition == "" {
		return nil, fmt.Errorf("xflow.transform.filter: condition parameter is required")
	}
	filtered := make([]any, 0, len(items))
	baseEnv := exprx.BuildExprEnv(input, nil)
	for index, item := range items {
		matched, err := evalItemCondition(condition, baseEnv, item, index)
		if err != nil {
			return nil, fmt.Errorf("xflow.transform.filter: item %d: %w", index, err)
		}
		if matched {
			filtered = append(filtered, item)
		}
	}
	data := cloneData(input)
	data[itemsKey] = filtered
	data["total"] = len(filtered)
	return &types.Output{Data: data}, nil
}

func evalItemCondition(code string, baseEnv map[string]any, item any, index int) (bool, error) {
	env := make(map[string]any, len(baseEnv)+4)
	for key, value := range baseEnv {
		env[key] = value
	}
	// Expand item fields first, then reserve the loop control keys so an item
	// that happens to contain a field named "item" or "index" cannot shadow
	// them and corrupt the condition's iteration variables.
	if itemMap, ok := item.(map[string]any); ok {
		for key, value := range itemMap {
			env[key] = value
		}
	}
	env["item"] = item
	env["index"] = index
	result, err := exprx.EvalExpr(code, env, true)
	if err != nil {
		return false, err
	}
	matched, ok := result.(bool)
	if !ok {
		return false, fmt.Errorf("condition returned %T, want bool", result)
	}
	return matched, nil
}
