package node

import "context"

// IfHandler implements xflow.if — conditional branch node.
// Execute is a stub; the real implementation lives in the Worker layer.
type IfHandler struct{}

func (h *IfHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.if",
		DisplayName: "IF Condition",
		Params: []ParamSpec{
			{Name: "condition", DisplayName: "Condition", Type: ParamString, Required: true, Description: "Boolean expression to evaluate"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "true", DisplayName: "True"}, {Name: "false", DisplayName: "False"}},
	}
}

func (h *IfHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.if", "_stub": true}}, nil
}

func init() { Register(&IfHandler{}) }
