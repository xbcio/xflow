package node

import "context"

// MergeHandler implements xflow.merge — fan-in synchronisation node.
// Inputs are dynamic (any number of upstream ports converge here).
// Execute is a stub; the real implementation lives in the Worker layer.
type MergeHandler struct{}

func (h *MergeHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.merge",
		DisplayName: "Merge",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: true, Description: "Merge strategy: \"wait_all\" or \"wait_any\""},
			{Name: "on_others", DisplayName: "On Others", Type: ParamString, Required: false, Default: "cancel", Description: "Action for remaining branches in wait_any mode (default: cancel)"},
		},
		Inputs:  []PortSpec{}, // dynamic — connected at graph-build time
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (h *MergeHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.merge", "_stub": true}}, nil
}

func init() { Register(&MergeHandler{}) }
