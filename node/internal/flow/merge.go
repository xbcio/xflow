package flow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	"github.com/spf13/cast"
	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
)

// MergeNode implements xflow.merge — fan-in synchronisation node.
type MergeNode struct {
	nodeinternal.BaseNode
	Mode nodeinternal.MergeMode
}

// Merge creates a fan-in synchronisation node.
//
//	node.Merge(node.nodeinternal.MergeWaitAll)
func Merge(mode nodeinternal.MergeMode) *MergeNode {
	return &MergeNode{Mode: mode}
}

func (n *MergeNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        "xflow.merge",
		DisplayName: "Merge",
		Params: []types.ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: true, Description: "Merge strategy: \"wait_all\" or \"wait_any\""},
			{Name: "on_others", DisplayName: "On Others", Type: types.ParamString, Required: false, Default: "cancel", Description: "Action for remaining branches in wait_any mode (default: cancel)"},
		},
		Inputs:  []types.PortSpec{},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *MergeNode) NodeType() string { return "xflow.merge" }
func (n *MergeNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
	return n
}

func (n *MergeNode) RawParams() any {
	return map[string]any{"mode": string(n.Mode)}
}

func (n *MergeNode) Execute(ctx context.Context, input *types.Input) (*types.Output, error) {
	mode := cast.ToString(input.Params["mode"])
	if mode == "" {
		mode = "wait_all"
	}

	switch nodeinternal.MergeMode(mode) {
	case nodeinternal.MergeWaitAll:
		return n.mergeAll(input)
	case nodeinternal.MergeWaitAny:
		return n.mergeAny(input)
	default:
		return nil, fmt.Errorf("xflow.merge: unknown mode %q", mode)
	}
}

func (n *MergeNode) mergeAll(input *types.Input) (*types.Output, error) {
	merged := make(map[string]any)

	if input.Inputs != nil {
		for port, data := range input.Inputs {
			merged[port] = data
		}
	}

	if input.Data != nil {
		for k, v := range input.Data {
			merged[k] = v
		}
	}

	return &types.Output{Data: merged}, nil
}

func (n *MergeNode) mergeAny(input *types.Input) (*types.Output, error) {
	onOthers := cast.ToString(input.Params["on_others"])
	if onOthers == "" {
		onOthers = "cancel"
	}

	result := make(map[string]any)

	if input.Data != nil {
		for k, v := range input.Data {
			result[k] = v
		}
	}

	if input.Inputs != nil {
		for port, data := range input.Inputs {
			result[port] = data
		}
	}

	result["_on_others"] = onOthers

	return &types.Output{Data: result}, nil
}

func init() { registry.Register(&MergeNode{}) }
