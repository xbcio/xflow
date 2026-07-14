package flow

import (
	"github.com/xbcio/xflow/types"

	"context"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
)

// EndNode implements xflow.end, an explicit workflow terminal node.
//
// The engine still treats any leaf node as a completion checkpoint; xflow.end
// exists to make workflow terminals explicit in DSLs and UI-authored graphs.
type EndNode struct {
	BaseNode
}

// End returns the built-in xflow.end node builder.
func End() *EndNode {
	return &EndNode{}
}

func (n *EndNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.end",
		DisplayName: "End",
		Kind:        "action",
		Inputs:      []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs:     []types.PortSpec{},
	}
}

func (n *EndNode) NodeType() string { return "xflow.end" }
func (n *EndNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}
func (n *EndNode) RawParams() any { return nil }

func (n *EndNode) Execute(_ context.Context, input *types.Input) (*types.Output, error) {
	if input == nil {
		return &types.Output{Data: map[string]any{}}, nil
	}
	return &types.Output{Data: input.Data}, nil
}

func init() { registry.Register(&EndNode{}) }
