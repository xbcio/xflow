package node

import "context"

// FunctionHandler implements xflow.function — executes a named Go function or an inline Expr expression.
// Execute is a stub; the real implementation lives in the Worker layer.
type FunctionHandler struct{}

func (h *FunctionHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        "xflow.function",
		DisplayName: "Function",
		Params: []ParamSpec{
			{Name: "function_name", DisplayName: "Function Name", Type: ParamString, Required: false, Description: "Name of a pre-registered Go function to call"},
			{Name: "code", DisplayName: "Code", Type: ParamString, Required: false, Description: "Inline Expr expression to evaluate"},
			{Name: "params", DisplayName: "Params", Type: ParamObject, Required: false, Description: "Extra parameters passed to the function or expression"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *FunctionHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": "xflow.function", "_stub": true}}, nil
}

func init() { Register(&FunctionHandler{}) }
