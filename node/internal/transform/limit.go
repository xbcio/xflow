package transform

import (
	"context"
	"fmt"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
	"github.com/spf13/cast"
)

// LimitNode implements xflow.transform.limit — keeps the first max items.
type LimitNode struct {
	BaseNode
	Items string
	Max   int
}

func Limit(itemsExpr string, max int) *LimitNode {
	return &LimitNode{Items: itemsExpr, Max: max}
}

func (n *LimitNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.limit",
		DisplayName: "Limit",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to limit"},
			{Name: "max", DisplayName: "Max", Type: types.ParamNumber, Required: true, Description: "Maximum number of items to keep"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *LimitNode) NodeType() string { return "xflow.transform.limit" }
func (n *LimitNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *LimitNode) RawParams() any { return map[string]any{"items": n.Items, "max": n.Max} }

func (n *LimitNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	items, itemsKey, err := itemsFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.limit: %w", err)
	}
	limit := cast.ToInt(input.Params["max"])
	if limit < 0 {
		return nil, fmt.Errorf("xflow.transform.limit: max must not be negative")
	}
	if limit > len(items) {
		limit = len(items)
	}
	limited := append([]any(nil), items[:limit]...)
	data := cloneData(input)
	data[itemsKey] = limited
	data["total"] = len(limited)
	return &types.Output{Data: data}, nil
}
