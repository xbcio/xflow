package node

import (
	"context"
	"fmt"
	"time"
)

// WaitNodeType is the canonical type identifier for the wait node.
const WaitNodeType = "xflow.wait"

// WaitMode represents the trigger mode of an xflow.wait node.
type WaitMode string

const (
	WaitModeTimer  WaitMode = "timer"
	WaitModeSignal WaitMode = "signal"
)

// WaitNode implements xflow.wait — suspends execution until a signal or timer fires.
type WaitNode struct {
	BaseNode
	Mode       WaitMode
	SignalName string
	Signals    []string
	Quorum     int
	Duration   string
	Until      string
	TimeoutStr string
}

// Wait creates a wait node that suspends until a signal arrives.
//
//	node.Wait("order_paid")
func Wait(signalName string) *WaitNode {
	return &WaitNode{Mode: WaitModeSignal, SignalName: signalName}
}

// WaitDuration creates a wait node that suspends for a fixed duration.
//
//	node.WaitDuration("5m")
func WaitDuration(duration string) *WaitNode {
	return &WaitNode{Mode: WaitModeTimer, Duration: duration}
}

func (n *WaitNode) Descriptor() Descriptor {
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

func (n *WaitNode) NodeType() string { return WaitNodeType }
func (n *WaitNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *WaitNode) RawParams() any {
	params := map[string]any{"mode": string(n.Mode)}
	if n.SignalName != "" {
		params["signal_name"] = n.SignalName
	}
	if len(n.Signals) > 0 {
		params["signals"] = n.Signals
	}
	if n.Quorum > 0 {
		params["quorum"] = n.Quorum
	}
	if n.Duration != "" {
		params["duration"] = n.Duration
	}
	if n.Until != "" {
		params["until"] = n.Until
	}
	if n.TimeoutStr != "" {
		params["timeout"] = n.TimeoutStr
	}
	return params
}

func (n *WaitNode) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": WaitNodeType}}, nil
}

func (n *WaitNode) PrepareSuspend(_ context.Context, input *Input) (*SuspendSpec, error) {
	mode := WaitModeSignal
	if m, ok := input.Params["mode"].(string); ok && m != "" {
		mode = WaitMode(m)
	}

	timeout, _ := parseDurationParam(input.Params["timeout"])

	switch mode {
	case WaitModeTimer:
		return n.prepareTimer(input, timeout)
	case WaitModeSignal:
		return n.prepareSignal(input, timeout)
	default:
		return nil, fmt.Errorf("xflow.wait: unknown mode %q", mode)
	}
}

func (n *WaitNode) prepareTimer(input *Input, timeout time.Duration) (*SuspendSpec, error) {
	var timer time.Duration

	if d, ok := parseDurationParam(input.Params["duration"]); ok {
		timer = d
	} else if untilStr, ok := input.Params["until"].(string); ok && untilStr != "" {
		t, err := time.Parse(time.RFC3339, untilStr)
		if err != nil {
			return nil, fmt.Errorf("xflow.wait: invalid until time: %w", err)
		}
		timer = time.Until(t)
		if timer < 0 {
			timer = 0
		}
	} else {
		return nil, fmt.Errorf("xflow.wait: timer mode requires duration or until parameter")
	}

	return &SuspendSpec{
		Mode:    ModeTimer,
		Timer:   timer,
		Timeout: timeout,
	}, nil
}

func (n *WaitNode) prepareSignal(input *Input, timeout time.Duration) (*SuspendSpec, error) {
	if signals := parseStringSlice(input.Params["signals"]); len(signals) > 0 {
		quorum := 0
		if q, ok := toInt(input.Params["quorum"]); ok {
			quorum = q
		}
		return &SuspendSpec{
			Mode:    ModeMultiSignal,
			Signals: signals,
			Quorum:  quorum,
			Timeout: timeout,
		}, nil
	}

	signalName, _ := input.Params["signal_name"].(string)
	if signalName == "" {
		signalName = input.NodeName + "/signal"
	}

	return &SuspendSpec{
		Mode:    ModeSignal,
		Signals: []string{signalName},
		Timeout: timeout,
	}, nil
}

func (n *WaitNode) OnResume(_ context.Context, input *Input, signal *SignalPayload) (*Output, error) {
	switch signal.Triggered {
	case TimeoutFired:
		return &Output{Data: map[string]any{"reason": "timeout"}, Port: "timeout"}, nil
	case TimerFired:
		return &Output{Data: input.Data, Port: "main"}, nil
	case SignalReceived:
		data := make(map[string]any)
		if input.Data != nil {
			for k, v := range input.Data {
				data[k] = v
			}
		}
		data["signal_name"] = signal.Name
		data["signal_data"] = signal.Data
		if signal.All != nil {
			data["signals"] = signal.All
		}
		return &Output{Data: data, Port: "main"}, nil
	default:
		return nil, fmt.Errorf("xflow.wait: unknown trigger %d", signal.Triggered)
	}
}

func parseDurationParam(v any) (time.Duration, bool) {
	switch d := v.(type) {
	case string:
		if d == "" {
			return 0, false
		}
		parsed, err := time.ParseDuration(d)
		if err != nil {
			return 0, false
		}
		return parsed, true
	case float64:
		return time.Duration(d), true
	case time.Duration:
		return d, true
	}
	return 0, false
}

func parseStringSlice(v any) []string {
	switch s := v.(type) {
	case []string:
		return s
	case []any:
		result := make([]string, 0, len(s))
		for _, item := range s {
			if str, ok := item.(string); ok {
				result = append(result, str)
			}
		}
		return result
	}
	return nil
}

func init() { Register(&WaitNode{}) }
