package transform

import (
	"context"
	"fmt"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/types"
)

// RenameNode implements xflow.transform.rename — renames fields according to
// an old-name to new-name mapping.
type RenameNode struct {
	nodeinternal.BaseNode
	Mapping map[string]string
}

func Rename(mapping map[string]string) *RenameNode {
	return &RenameNode{Mapping: mapping}
}

func (n *RenameNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.transform.rename",
		DisplayName: "Rename",
		Description: "按映射表重命名字段，未列出的字段保持不变。",
		Category:    "transform",
		Examples: []types.DescriptorExample{{
			Name:   "重命名字段",
			Params: map[string]any{"mapping": map[string]any{"order_id": "id"}},
		}},
		CardSummary: []string{"mapping"},
		Params: []types.ParamSpec{
			{Name: "mapping", DisplayName: "Mapping", Type: types.ParamObject, Required: true, Description: "Old field name to new field name mapping",
				Widget: nodeinternal.WidgetKeyValue},
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
	for oldName, newName := range mapping {
		if oldName == "" || newName == "" {
			return nil, fmt.Errorf("xflow.transform.rename: mapping names must not be empty")
		}
	}
	// Build the output by reading each original field once and writing it to
	// its final name. In-place delete+set over a map made chained renames
	// (e.g. {"a":"b","b":"c"}) order-dependent: if a→b ran first it clobbered
	// the original b before b→c could read it. A separate destination map
	// decouples read from write, so every source field is captured before any
	// rename target is written.
	src := cloneData(input)
	dst := make(map[string]any, len(src))
	// Fields not referenced as a source name pass through unchanged.
	for k, v := range src {
		if _, isSource := mapping[k]; isSource {
			continue
		}
		dst[k] = v
	}
	for oldName, newName := range mapping {
		if value, ok := src[oldName]; ok {
			dst[newName] = value
		}
	}
	return &types.Output{Data: dst}, nil
}
