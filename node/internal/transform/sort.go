package transform

import (
	"context"
	"fmt"
	"sort"

	"github.com/spf13/cast"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

// SortField describes one sort key for SortNode.
type SortField struct {
	Field string `json:"field,omitempty"`
	Desc  bool   `json:"desc,omitempty"`
}

func SortAsc(field string) SortField  { return SortField{Field: field} }
func SortDesc(field string) SortField { return SortField{Field: field, Desc: true} }

// SortNode implements xflow.transform.sort — sorts items by one or more fields.
type SortNode struct {
	nodeinternal.BaseNode
	Items  string
	Fields []SortField
}

func SortItems(itemsExpr string, fields ...SortField) *SortNode {
	return &SortNode{Items: itemsExpr, Fields: fields}
}

func (n *SortNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.sort",
		DisplayName: "Sort",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to sort"},
			{Name: "fields", DisplayName: "Fields", Type: types.ParamArray, Required: true, Description: "Sort fields in priority order"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *SortNode) NodeType() string { return "xflow.transform.sort" }
func (n *SortNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *SortNode) RawParams() any {
	fields := make([]map[string]any, 0, len(n.Fields))
	for _, field := range n.Fields {
		fields = append(fields, map[string]any{"field": field.Field, "desc": field.Desc})
	}
	return map[string]any{"items": n.Items, "fields": fields}
}

func (n *SortNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	items, itemsKey, err := itemsFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.sort: %w", err)
	}
	fields, err := parseSortFields(input.Params["fields"])
	if err != nil || len(fields) == 0 {
		return nil, fmt.Errorf("xflow.transform.sort: fields parameter is required")
	}
	sorted := append([]any(nil), items...)
	sort.SliceStable(sorted, func(i, j int) bool {
		for _, field := range fields {
			left := itemFieldValue(sorted[i], field.Field)
			right := itemFieldValue(sorted[j], field.Field)
			cmp := compareValues(left, right)
			if cmp == 0 {
				continue
			}
			if field.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	data := cloneData(input)
	data[itemsKey] = sorted
	data["total"] = len(sorted)
	return &types.Output{Data: data}, nil
}

func compareValues(left any, right any) int {
	lf, lerr := cast.ToFloat64E(left)
	rf, rerr := cast.ToFloat64E(right)
	if lerr == nil && rerr == nil {
		switch {
		case lf < rf:
			return -1
		case lf > rf:
			return 1
		default:
			return 0
		}
	}
	ls := cast.ToString(left)
	rs := cast.ToString(right)
	switch {
	case ls < rs:
		return -1
	case ls > rs:
		return 1
	default:
		return 0
	}
}

func parseSortFields(value any) ([]SortField, error) {
	switch typed := value.(type) {
	case []SortField:
		return typed, nil
	case []map[string]any:
		fields := make([]SortField, 0, len(typed))
		for _, item := range typed {
			fields = append(fields, SortField{Field: cast.ToString(item["field"]), Desc: cast.ToBool(item["desc"])})
		}
		return fields, nil
	case []any:
		fields := make([]SortField, 0, len(typed))
		for _, item := range typed {
			if m, ok := item.(map[string]any); ok {
				fields = append(fields, SortField{Field: cast.ToString(m["field"]), Desc: cast.ToBool(m["desc"])})
			}
		}
		return fields, nil
	default:
		return nil, fmt.Errorf("expected fields array")
	}
}
