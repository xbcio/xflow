package transform

import (
	"context"
	"encoding/json"
	"fmt"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
	"github.com/spf13/cast"
)

// RemoveDuplicatesNode implements xflow.transform.remove_duplicates — keeps
// the first item for each unique key.
type RemoveDuplicatesNode struct {
	nodeinternal.BaseNode
	Items  string
	Fields []string
}

func RemoveDuplicates(itemsExpr string, fields ...string) *RemoveDuplicatesNode {
	return &RemoveDuplicatesNode{Items: itemsExpr, Fields: fields}
}

func (n *RemoveDuplicatesNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.remove_duplicates",
		DisplayName: "Remove Duplicates",
		Params: []types.ParamSpec{
			{Name: "items", DisplayName: "Items", Type: types.ParamString, Required: true, Description: "Expression that evaluates to the array to deduplicate"},
			{Name: "fields", DisplayName: "Fields", Type: types.ParamArray, Required: false, Description: "Fields used as the unique key; whole item when omitted"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *RemoveDuplicatesNode) NodeType() string { return "xflow.transform.remove_duplicates" }
func (n *RemoveDuplicatesNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *RemoveDuplicatesNode) RawParams() any {
	return map[string]any{"items": n.Items, "fields": n.Fields}
}

func (n *RemoveDuplicatesNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	items, itemsKey, err := itemsFromInput(input)
	if err != nil {
		return nil, fmt.Errorf("xflow.transform.remove_duplicates: %w", err)
	}
	fields, _ := cast.ToStringSliceE(input.Params["fields"])
	seen := make(map[string]struct{}, len(items))
	unique := make([]any, 0, len(items))
	for _, item := range items {
		key, err := dedupKey(item, fields)
		if err != nil {
			return nil, fmt.Errorf("xflow.transform.remove_duplicates: %w", err)
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		unique = append(unique, item)
	}
	data := cloneData(input)
	data[itemsKey] = unique
	data["total"] = len(unique)
	return &types.Output{Data: data}, nil
}

func dedupKey(item any, fields []string) (string, error) {
	value := item
	if len(fields) > 0 {
		key := make(map[string]any, len(fields))
		for _, field := range fields {
			key[field] = itemFieldValue(item, field)
		}
		value = key
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}
