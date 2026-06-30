package node

import (
	"context"
	"fmt"

	"github.com/expr-lang/expr"
)

// LoopNode implements xflow.loop — iterates over a collection with an embedded sub-graph.
type LoopNode struct {
	BaseNode
	Items     string
	BatchSize int
}

// Loop creates a loop node that iterates over a collection.
//
//	node.Loop("items", 5)
func Loop(itemsExpr string, batchSize int) *LoopNode {
	if batchSize <= 0 {
		batchSize = 1
	}
	return &LoopNode{Items: itemsExpr, BatchSize: batchSize}
}

func (n *LoopNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.loop",
		DisplayName: "Loop",
		Params: []ParamSpec{
			{Name: "items", DisplayName: "Items", Type: ParamString, Required: true, Description: "Expression that evaluates to the array to iterate"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: ParamNumber, Required: false, Default: 1, Description: "Number of items processed per batch"},
			{Name: "max_concurrency", DisplayName: "Max Concurrency", Type: ParamNumber, Required: false, Default: 1, Description: "Maximum concurrent executions of the sub-graph"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: ParamBool, Required: false, Default: false, Description: "Continue iteration when a sub-graph execution fails"},
			{Name: "body", DisplayName: "Body", Type: ParamObject, Required: true, Description: "Sub-graph definition executed for each item"},
		},
		Inputs:       []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs:      []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
		Capabilities: []string{CapBodySubgraphRequired},
	}
}

func (n *LoopNode) NodeType() string { return "xflow.loop" }
func (n *LoopNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *LoopNode) RawParams() any {
	return map[string]any{
		"items":      n.Items,
		"batch_size": n.BatchSize,
	}
}

func (n *LoopNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	itemsExpr, _ := input.Params["items"].(string)
	if itemsExpr == "" {
		return nil, fmt.Errorf("xflow.loop: items parameter is required")
	}

	env := buildExprEnv(input)
	program, err := expr.Compile(itemsExpr, expr.Env(env))
	if err != nil {
		return nil, fmt.Errorf("xflow.loop: compile items expression: %w", err)
	}

	result, err := expr.Run(program, env)
	if err != nil {
		return nil, fmt.Errorf("xflow.loop: evaluate items expression: %w", err)
	}

	items, err := toSlice(result)
	if err != nil {
		return nil, fmt.Errorf("xflow.loop: items must evaluate to an array: %w", err)
	}

	batchSize := 1
	if bs, ok := toInt(input.Params["batch_size"]); ok && bs > 0 {
		batchSize = bs
	}

	batches := makeBatches(items, batchSize)

	return &Output{
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

func toSlice(v any) ([]any, error) {
	switch items := v.(type) {
	case []any:
		return items, nil
	case []string:
		result := make([]any, len(items))
		for i, s := range items {
			result[i] = s
		}
		return result, nil
	case []int:
		result := make([]any, len(items))
		for i, n := range items {
			result[i] = n
		}
		return result, nil
	case []float64:
		result := make([]any, len(items))
		for i, f := range items {
			result[i] = f
		}
		return result, nil
	case []map[string]any:
		result := make([]any, len(items))
		for i, m := range items {
			result[i] = m
		}
		return result, nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func makeBatches(items []any, size int) [][]any {
	if size <= 0 {
		size = 1
	}
	batches := make([][]any, 0, (len(items)+size-1)/size)
	for i := 0; i < len(items); i += size {
		end := i + size
		if end > len(items) {
			end = len(items)
		}
		batches = append(batches, items[i:end])
	}
	return batches
}

func init() { Register(&LoopNode{}) }
