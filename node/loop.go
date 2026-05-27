package node

import "context"

// LoopHandler implements xflow.loop — iterates over a collection with an embedded sub-graph.
// Execute is a stub; the real implementation lives in the Worker layer.
type LoopHandler struct{}

func (h *LoopHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.loop",
		DisplayName: "Loop",
		Params: []ParamSpec{
			{Name: "items", DisplayName: "Items", Type: ParamString, Required: true, Description: "Expression that evaluates to the array to iterate"},
			{Name: "batch_size", DisplayName: "Batch Size", Type: ParamNumber, Required: false, Default: 1, Description: "Number of items processed per batch"},
			{Name: "max_concurrency", DisplayName: "Max Concurrency", Type: ParamNumber, Required: false, Default: 1, Description: "Maximum concurrent executions of the sub-graph"},
			{Name: "continue_on_error", DisplayName: "Continue On Error", Type: ParamBool, Required: false, Default: false, Description: "Continue iteration when a sub-graph execution fails"},
			{Name: "body", DisplayName: "Body", Type: ParamObject, Required: true, Description: "Sub-graph definition executed for each item"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *LoopHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.loop", "_stub": true}}, nil
}

func init() { Register(&LoopHandler{}) }
