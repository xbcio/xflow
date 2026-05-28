package node

import (
	"context"
	"fmt"
)

// MergeNode implements xflow.merge — fan-in synchronisation node.
type MergeNode struct {
	BaseNode
	Mode MergeMode
}

// Merge creates a fan-in synchronisation node.
//
//	node.Merge(node.MergeWaitAll)
func Merge(mode MergeMode) *MergeNode {
	return &MergeNode{Mode: mode}
}

func (n *MergeNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.merge",
		DisplayName: "Merge",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: true, Description: "Merge strategy: \"wait_all\" or \"wait_any\""},
			{Name: "on_others", DisplayName: "On Others", Type: ParamString, Required: false, Default: "cancel", Description: "Action for remaining branches in wait_any mode (default: cancel)"},
		},
		Inputs:  []PortSpec{},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (n *MergeNode) NodeType() string { return "xflow.merge" }
func (n *MergeNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *MergeNode) RawParams() any {
	return map[string]any{"mode": string(n.Mode)}
}

func (n *MergeNode) Execute(ctx context.Context, input *Input) (*Output, error) {
	mode, _ := input.Params["mode"].(string)
	if mode == "" {
		mode = "wait_all"
	}

	switch MergeMode(mode) {
	case MergeWaitAll:
		return n.mergeAll(input)
	case MergeWaitAny:
		return n.mergeAny(input)
	default:
		return nil, fmt.Errorf("xflow.merge: unknown mode %q", mode)
	}
}

func (n *MergeNode) mergeAll(input *Input) (*Output, error) {
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

	return &Output{Data: merged}, nil
}

func (n *MergeNode) mergeAny(input *Input) (*Output, error) {
	onOthers, _ := input.Params["on_others"].(string)
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

	return &Output{Data: result}, nil
}

func init() { Register(&MergeNode{}) }
