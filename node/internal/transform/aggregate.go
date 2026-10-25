package transform

import (
	"context"
	"fmt"

	"github.com/spf13/cast"
	"github.com/xbcio/xflow/exprx"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
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
		Description: "对数组做计数、求和或平均值等聚合，输出结果字段。",
		Category:    "transform",
		Examples: []types.DescriptorExample{{
			Name: "订单金额求和",
			Params: map[string]any{
				"items":      "$input.orders",
				"operations": []any{map[string]any{"kind": "sum", "field": "amount", "as": "total"}},
			},
		}},
		CardSummary: []string{"items", "operations"},
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to aggregate",
				Widget: nodeinternal.WidgetExpression, Constraints: nodeinternal.Format(nodeinternal.FormatExpression)},
			{Name: "operations", DisplayName: "Operations", Type: types.ParamArray, Required: true, Description: "Aggregate operations",
				Item: &types.ParamSpec{Type: types.ParamObject, Fields: []types.ParamSpec{
					{Name: "kind", DisplayName: "Kind", Type: types.ParamString, Required: true,
						Enum: []types.EnumOption{
							{Value: "count", DisplayName: "Count"},
							{Value: "sum", DisplayName: "Sum"},
							{Value: "avg", DisplayName: "Average"},
							{Value: "average", DisplayName: "Average (alias of avg)"},
						}},
					{Name: "field", DisplayName: "Field", Type: types.ParamString, Description: "Item field to sum or average",
						VisibleWhen: nodeinternal.CondIn("kind", "sum", "avg", "average")},
					{Name: "as", DisplayName: "As", Type: types.ParamString, Required: true, Description: "Output field name"},
				}}},
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
	// Preserve upstream input (cloneData) and layer the aggregate results on top,
	// matching the behaviour of other transform nodes. Previously a fresh empty
	// map discarded all upstream context including the items array itself.
	data := cloneData(input)
	for _, op := range ops {
		if op.As == "" {
			return nil, fmt.Errorf("xflow.transform.aggregate: output name is required")
		}
		switch op.Kind {
		case "count":
			data[op.As] = len(items)
		case "sum":
			total := sumField(items, op.Field)
			if err := exprx.CheckFinite(total); err != nil {
				return nil, aggregateNonFinite(op.As, err)
			}
			data[op.As] = total
		case "avg", "average":
			if len(items) == 0 {
				data[op.As] = float64(0)
				continue
			}
			average := sumField(items, op.Field) / float64(len(items))
			if err := exprx.CheckFinite(average); err != nil {
				return nil, aggregateNonFinite(op.As, err)
			}
			data[op.As] = average
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

// aggregateNonFinite classifies an overflowed sum/avg as permanent: the items
// and the operation are both fixed, so a retry recomputes the same ±Inf and
// the result still cannot be JSON-encoded at report time.
func aggregateNonFinite(as string, cause error) error {
	return types.NewPermanentError("aggregate.non_finite",
		fmt.Sprintf("xflow.transform.aggregate: output %q: %v", as, cause))
}

func sumField(items []any, field string) float64 {
	var sum float64
	for _, item := range items {
		sum += cast.ToFloat64(itemFieldValue(item, field))
	}
	return sum
}
