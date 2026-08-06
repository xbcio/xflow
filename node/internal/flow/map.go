package flow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	"slices"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/node/internal/utils/exprx"
	"github.com/xbcio/xflow/node/registry"
	"github.com/spf13/cast"
)

// MapNode implements xflow.map — runs a body sub-graph once per item.
type MapNode struct {
	nodeinternal.BaseNode
	Items     string
	BatchSize int
}

// Map creates a map node that runs a body sub-graph over a collection.
//
//	node.Map("items", 5)
func Map(itemsExpr string, batchSize int) *MapNode {
	if batchSize <= 0 {
		batchSize = 1
	}
	return &MapNode{Items: itemsExpr, BatchSize: batchSize}
}

func (n *MapNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.map",
		DisplayName: "Map",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to iterate"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: types.ParamNumber, Required: false, Default: 1, Description: "Number of items processed per batch"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: types.ParamBool, Required: false, Default: false, Description: "Continue iteration when a sub-graph execution fails"},
			{Name: "body", DisplayName: "Body", Type: types.ParamObject, Required: true, Description: "Sub-graph definition executed for each item"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *MapNode) NodeType() string { return "xflow.map" }
func (n *MapNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *MapNode) RawParams() any {
	return map[string]any{
		"items":      n.Items,
		"batch_size": n.BatchSize,
	}
}

func (n *MapNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	itemsExpr, _ := input.Params["items"].(string)
	if itemsExpr == "" {
		return nil, fmt.Errorf("xflow.map: items parameter is required")
	}

	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(itemsExpr, env, false)
	if err != nil {
		return nil, fmt.Errorf("xflow.map: %w", err)
	}

	items, err := conv.ToSlice(result)
	if err != nil {
		return nil, fmt.Errorf("xflow.map: items must evaluate to an array: %w", err)
	}

	batchSize := 1
	if bs, err := cast.ToIntE(input.Params["batch_size"]); err == nil && bs > 0 {
		batchSize = bs
	}

	batches := slices.Collect(slices.Chunk(items, batchSize))

	return &types.Output{
		Data: map[string]any{
			"_loop":       true,
			"items":       items,
			"batches":     batches,
			"batch_size":  batchSize,
			"total":       len(items),
			"batch_count": len(batches),
		},
	}, nil
}

func init() { registry.Register(&MapNode{}) }
