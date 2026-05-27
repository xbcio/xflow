package node

import "context"

// SplitHandler implements xflow.split — fans out one execution per item in a collection.
// Unlike loop, split does not embed a sub-graph; each item is emitted as a separate execution
// through the main output port.
// Execute is a stub; the real implementation lives in the Worker layer.
type SplitHandler struct{}

func (h *SplitHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.split",
		DisplayName: "Split",
		Params: []ParamSpec{
			{Name: "items", DisplayName: "Items", Type: ParamString, Required: true, Description: "Expression that evaluates to the array to split"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: ParamNumber, Required: false, Description: "Items per batch (omit for one-item-per-execution)"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: ParamBool, Required: false, Default: false, Description: "Continue splitting when a downstream branch fails"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}},
	}
}

func (h *SplitHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.split", "_stub": true}}, nil
}

func init() { Register(&SplitHandler{}) }
