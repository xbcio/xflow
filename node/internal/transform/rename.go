package transform

import (
	"context"
	"fmt"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

// RenameNode implements xflow.transform.rename — renames fields according to
// an old-name to new-name mapping.
type RenameNode struct {
	BaseNode
	Mapping map[string]string
}

func Rename(mapping map[string]string) *RenameNode {
	return &RenameNode{Mapping: mapping}
}

func (n *RenameNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.rename",
		DisplayName: "Rename",
		Params: []types.ParamSpec{
			{Name: "mapping", DisplayName: "Mapping", Type: types.ParamObject, Required: true, Description: "Old field name to new field name mapping"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *RenameNode) NodeType() string { return "xflow.transform.rename" }
func (n *RenameNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *RenameNode) RawParams() any {
	return map[string]any{"mapping": n.Mapping}
}

func (n *RenameNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	mapping, err := parseStringMap(input.Params["mapping"])
	if err != nil || len(mapping) == 0 {
		return nil, fmt.Errorf("xflow.transform.rename: mapping parameter is required")
	}
	data := cloneData(input)
	for oldName, newName := range mapping {
		if oldName == "" || newName == "" {
			return nil, fmt.Errorf("xflow.transform.rename: mapping names must not be empty")
		}
		value, ok := data[oldName]
		if !ok {
			continue
		}
		delete(data, oldName)
		data[newName] = value
	}
	return &types.Output{Data: data}, nil
}
