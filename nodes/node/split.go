package node

import (
	"context"
	"fmt"

	"github.com/expr-lang/expr"
)

// SplitNode implements xflow.split — fans out one execution per item in a collection.
type SplitNode struct {
	BaseNode
	Items     string
	BatchSize int
}

// Split creates a fan-out node that emits one execution per item.
//
//	node.Split("orders")
func Split(itemsExpr string) *SplitNode {
	return &SplitNode{Items: itemsExpr, BatchSize: 1}
}

func (n *SplitNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.split",
		DisplayName: "Split",
		Params: []ParamSpec{
			{Name: "items", DisplayName: "Items", Type: ParamString, Required: true, Description: "Expression that evaluates to the array to split"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: ParamNumber, Required: false, Description: "Items per batch (omit for one-item-per-execution)"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: ParamBool, Required: false, Default: false, Description: "Continue splitting when a downstream branch fails"},
		},
		Inputs:       []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs:      []PortSpec{{Name: "main", DisplayName: "Main"}},
		Capabilities: []string{CapBodySubgraphRequired},
	}
}

func (n *SplitNode) NodeType() string { return "xflow.split" }
func (n *SplitNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *SplitNode) RawParams() any {
	return map[string]any{"items": n.Items, "batch_size": n.BatchSize}
}

func (n *SplitNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	itemsExpr, _ := input.Params["items"].(string)
	if itemsExpr == "" {
		return nil, fmt.Errorf("xflow.split: items parameter is required")
	}

	env := buildExprEnv(input)
	program, err := expr.Compile(itemsExpr, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("xflow.split: compile items expression: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("xflow.split: evaluate items expression: %w", err)
	}

	items, err := toSlice(result)
	if err != nil {
		return nil, fmt.Errorf("xflow.split: items must evaluate to an array: %w", err)
	}

	batchSize := 1
	if bs, ok := toInt(input.Params["batch_size"]); ok && bs > 0 {
		batchSize = bs
	}

	batches := makeBatches(items, batchSize)

	return &Output{
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

func init() { Register(&SplitNode{}) }
