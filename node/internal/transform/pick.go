package transform

import (
	"context"
	"fmt"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/internal/utils/conv"
	"github.com/xbcio/xflow/types"
)

// PickNode implements xflow.transform.pick — keeps only the named fields from
// the input data's top-level map, removing all others.
type PickNode struct {
	BaseNode
	Fields []string
}

// Pick creates a pick node that retains only the named fields.
func Pick(fields ...string) *PickNode {
	return &PickNode{Fields: fields}
}

func (n *PickNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.pick",
		DisplayName: "Pick",
		Params: []types.ParamSpec{
			{Name: "fields", DisplayName: "Fields", Type: types.ParamArray, Required: true,
				Description: "Field names to keep; all others are removed"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *PickNode) NodeType() string { return "xflow.transform.pick" }

func (n *PickNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *PickNode) RawParams() any {
	return map[string]any{"fields": n.Fields}
}

func (n *PickNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	fields := conv.NonEmptyStringSlice(input.Params["fields"])
	if len(fields) == 0 {
		return nil, fmt.Errorf("xflow.transform.pick: fields parameter is required")
	}
	keep := make(map[string]struct{}, len(fields))
	for _, f := range fields {
		keep[f] = struct{}{}
	}
	data := cloneData(input)
	for k := range data {
		if _, ok := keep[k]; !ok {
			delete(data, k)
		}
	}
	return &types.Output{Data: data}, nil
}
