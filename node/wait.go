package node

import "context"

// WaitNodeType is the canonical type identifier for the wait node.
const WaitNodeType = "xflow.wait"

// WaitMode represents the trigger mode of an xflow.wait node.
type WaitMode string

const (
	// WaitTimer fires after a fixed duration.
	WaitTimer WaitMode = "timer"
	// WaitSignal fires when an external signal arrives (default).
	WaitSignal WaitMode = "signal"
)

// WaitHandler implements xflow.wait — suspends execution until a signal or timer fires.
// Execute is a stub; the real implementation lives in the Worker layer.
type WaitHandler struct{}

func (h *WaitHandler) Descriptor() Descriptor {
	return Descriptor{
		Type:        WaitNodeType,
		DisplayName: "Wait",
		Params: []ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: false, Default: "signal", Description: "Trigger mode: \"signal\" (default) or \"timer\""},
			{Name: "signal_name", DisplayName: "Signal Name", Type: ParamString, Required: false, Description: "Name of the external signal to wait for (signal mode)"},
			{Name: "signals", DisplayName: "Signal Names", Type: ParamArray, Required: false, Description: "List of signal names to wait for (multi-signal mode)"},
			{Name: "quorum", DisplayName: "Quorum", Type: ParamNumber, Required: false, Description: "Number of signals required to proceed; 0 or unset means all (multi-signal mode)"},
			{Name: "timeout", DisplayName: "Timeout", Type: ParamString, Required: false, Description: "Maximum wait duration before routing to timeout port (e.g. \"48h\")"},
			{Name: "duration", DisplayName: "Duration", Type: ParamString, Required: false, Description: "Fixed wait duration (timer mode, e.g. \"5m\")"},
			{Name: "until", DisplayName: "Until", Type: ParamString, Required: false, Description: "Absolute time expression to wait until (timer mode)"},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "timeout", DisplayName: "Timeout"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (h *WaitHandler) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": WaitNodeType, "_stub": true}}, nil
}

func init() { Register(&WaitHandler{}) }
