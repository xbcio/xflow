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

// SplitNode implements xflow.split — fans out one execution per item in a collection.
type SplitNode struct {
	nodeinternal.BaseNode
	Items     string
	BatchSize int
}

// Split creates a fan-out node that emits one execution per item.
//
//	node.Split("orders")
func Split(itemsExpr string) *SplitNode {
	return &SplitNode{Items: itemsExpr, BatchSize: 1}
}

func (n *SplitNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.split",
		DisplayName: "Split",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to split"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: types.ParamNumber, Required: false, Description: "Items per batch (omit for one-item-per-execution)"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: types.ParamBool, Required: false, Default: false, Description: "Continue splitting when a downstream branch fails"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *SplitNode) NodeType() string { return "xflow.split" }
func (n *SplitNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *SplitNode) RawParams() any {
	return map[string]any{"items": n.Items, "batch_size": n.BatchSize}
}

func (n *SplitNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	itemsExpr, _ := input.Params["items"].(string)
	if itemsExpr == "" {
		return nil, fmt.Errorf("xflow.split: items parameter is required")
	}

	env := exprx.BuildExprEnv(input, nil)
	result, err := exprx.EvalExpr(itemsExpr, env, false)
	if err != nil {
		return nil, fmt.Errorf("xflow.split: %w", err)
	}

	items, err := conv.ToSlice(result)
	if err != nil {
		return nil, fmt.Errorf("xflow.split: items must evaluate to an array: %w", err)
	}

	batchSize := 1
	if bs, err := cast.ToIntE(input.Params["batch_size"]); err == nil && bs > 0 {
		batchSize = bs
	}

	batches := slices.Collect(slices.Chunk(items, batchSize))

	return &types.Output{
		Data: map[string]any{
			"_split":      true,
			"items":       items,
			"batches":     batches,
			"batch_size":  batchSize,
			"total":       len(items),
			"batch_count": len(batches),
		},
	}, nil
}

func init() { registry.Register(&SplitNode{}) }
