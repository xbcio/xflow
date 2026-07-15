package transform

import (
	"context"
	"fmt"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
	"github.com/spf13/cast"
)

type AggregateOperation struct {
	Kind  string `json:"kind,omitempty"`
	Field string `json:"field,omitempty"`
	As    string `json:"as,omitempty"`
}

// AggregateNode implements xflow.transform.aggregate — summarizes item arrays.
type AggregateNode struct {
	nodeinternal.BaseNode
	Items      string
	Operations []AggregateOperation
}

func Aggregate(itemsExpr string) *AggregateNode {
	return &AggregateNode{Items: itemsExpr}
}

func (n *AggregateNode) Count(as string) *AggregateNode {
	n.Operations = append(n.Operations, AggregateOperation{Kind: "count", As: as})
	return n
}

func (n *AggregateNode) Sum(field string, as string) *AggregateNode {
	n.Operations = append(n.Operations, AggregateOperation{Kind: "sum", Field: field, As: as})
	return n
}

func (n *AggregateNode) Average(field string, as string) *AggregateNode {
	n.Operations = append(n.Operations, AggregateOperation{Kind: "avg", Field: field, As: as})
	return n
}

func (n *AggregateNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.aggregate",
		DisplayName: "Aggregate",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to aggregate"},
			{Name: "operations", DisplayName: "Operations", Type: types.ParamArray, Required: true, Description: "Aggregate operations"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *AggregateNode) NodeType() string { return "xflow.transform.aggregate" }
func (n *AggregateNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *AggregateNode) RawParams() any {
	ops := make([]map[string]any, 0, len(n.Operations))
	for _, op := range n.Operations {
		ops = append(ops, map[string]any{"kind": op.Kind, "field": op.Field, "as": op.As})
	}
	return map[string]any{"items": n.Items, "operations": ops}
}

func (n *AggregateNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	items, _, err := itemsFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.aggregate: %w", err)
	}
	ops, err := parseAggregateOperations(input.Params["operations"])
	if err != nil || len(ops) == 0 {
		return nil, fmt.Errorf("xflow.transform.aggregate: operations parameter is required")
	}
	data := map[string]any{}
	for _, op := range ops {
		if op.As == "" {
			return nil, fmt.Errorf("xflow.transform.aggregate: output name is required")
		}
		switch op.Kind {
		case "count":
			data[op.As] = len(items)
		case "sum":
			data[op.As] = sumField(items, op.Field)
		case "avg", "average":
			if len(items) == 0 {
				data[op.As] = float64(0)
				continue
			}
			data[op.As] = sumField(items, op.Field) / float64(len(items))
		default:
			return nil, fmt.Errorf("xflow.transform.aggregate: unsupported operation %q", op.Kind)
		}
	}
	return &types.Output{Data: data}, nil
}

func parseAggregateOperations(value any) ([]AggregateOperation, error) {
	switch typed := value.(type) {
	case []AggregateOperation:
		return typed, nil
	case []map[string]any:
		ops := make([]AggregateOperation, 0, len(typed))
		for _, item := range typed {
			ops = append(ops, AggregateOperation{
				Kind:  cast.ToString(item["kind"]),
				Field: cast.ToString(item["field"]),
				As:    cast.ToString(item["as"]),
			})
		}
		return ops, nil
	case []any:
		ops := make([]AggregateOperation, 0, len(typed))
		for _, item := range typed {
			if m, ok := item.(map[string]any); ok {
				ops = append(ops, AggregateOperation{
					Kind:  cast.ToString(m["kind"]),
					Field: cast.ToString(m["field"]),
					As:    cast.ToString(m["as"]),
				})
			}
		}
		return ops, nil
	default:
		return nil, fmt.Errorf("expected operations array")
	}
}

func sumField(items []any, field string) float64 {
	var sum float64
	for _, item := range items {
		sum += cast.ToFloat64(itemFieldValue(item, field))
	}
	return sum
}
