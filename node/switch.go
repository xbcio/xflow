package node

import "context"

// SwitchHandler implements xflow.switch — multi-branch routing node.
// Outputs are dynamic and determined by the parameters.outputs field at runtime.
// Execute is a stub; the real implementation lives in the Worker layer.
type SwitchHandler struct{}

func (h *SwitchHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.switch",
		DisplayName: "Switch",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: true, Description: "Routing mode: \"rules\" or \"expression\""},
			{Name: "outputs", DisplayName: "Outputs", Type: ParamArray, Required: true, Description: "List of output port names (dynamic)"},
			{Name: "rules", DisplayName: "Rules", Type: ParamArray, Required: false, Description: "Rule list for rules mode"},
			{Name: "expression", DisplayName: "Expression", Type: ParamString, Required: false, Description: "Expression for expression mode"},
			{Name: "default_output", DisplayName: "Default Output", Type: ParamString, Required: false, Description: "Port name used when no rule matches"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{}, // dynamic — resolved at runtime from parameters.outputs
	}
}

func (h *SwitchHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.switch", "_stub": true}}, nil
}

func init() { Register(&SwitchHandler{}) }
