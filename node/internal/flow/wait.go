package flow

import (
	"context"
	"fmt"

	"github.com/xbcio/xflow/types"

	"time"

	. "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
	"github.com/spf13/cast"
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

// WithTimeout configures a maximum wait duration before routing to the timeout
// output. It is valid for signal and timer waits.
func (n *WaitNode) WithTimeout(duration string) *WaitNode {
	n.TimeoutStr = duration
	return n
}

func (n *WaitNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        WaitNodeType,
		DisplayName: "Wait",
		Params: []types.ParamSpec{
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: false, Default: "signal", Description: "Trigger mode: \"signal\" (default) or \"timer\""},
			{Name: "signal_name", DisplayName: "Signal Name", Type: types.ParamString, Required: false, Description: "Name of the external signal to wait for (signal mode)"},
			{Name: "signals", DisplayName: "Signal Names", Type: types.ParamArray, Required: false, Description: "List of signal names to wait for (multi-signal mode)"},
			{Name: "quorum", DisplayName: "Quorum", Type: types.ParamNumber, Required: false, Description: "Number of signals required to proceed; 0 or unset means all (multi-signal mode)"},
			{Name: "timeout", DisplayName: "Timeout", Type: types.ParamString, Required: false, Description: "Maximum wait duration before routing to timeout port (e.g. \"48h\")"},
			{Name: "duration", DisplayName: "Duration", Type: types.ParamString, Required: false, Description: "Fixed wait duration (timer mode, e.g. \"5m\")"},
			{Name: "until", DisplayName: "Until", Type: types.ParamString, Required: false, Description: "Absolute time expression to wait until (timer mode)"},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "main", DisplayName: "Main"}, {Name: "timeout", DisplayName: "Timeout"}, {Name: "error", DisplayName: "Error"}},
	}
}

func (n *WaitNode) NodeType() string { return WaitNodeType }
func (n *WaitNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
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

func (n *WaitNode) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"_type": WaitNodeType}}, nil
}

func (n *WaitNode) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	mode := WaitModeSignal
	if m := cast.ToString(input.Params["mode"]); m != "" {
		mode = WaitMode(m)
	}

	timeout, _ := cast.ToDurationE(input.Params["timeout"])

	switch mode {
	case WaitModeTimer:
		return n.prepareTimer(input, timeout)
	case WaitModeSignal:
		return n.prepareSignal(input, timeout)
	default:
		return nil, fmt.Errorf("xflow.wait: unknown mode %q", mode)
	}
}

func (n *WaitNode) prepareTimer(input *types.Input, timeout time.Duration) (*types.SuspendSpec, error) {
	var timer time.Duration

	// duration takes precedence; fall back to until when it is missing or
	// unparseable. cast.ToDurationE returns (0, nil) for nil, so the nil/empty
	// check is what distinguishes "absent" from "zero".
	rawDuration := input.Params["duration"]
	durationOk := rawDuration != nil && rawDuration != ""
	if durationOk {
		if d, err := cast.ToDurationE(rawDuration); err == nil {
			timer = d
		} else {
			durationOk = false
		}
	}

	if !durationOk {
		if untilStr := cast.ToString(input.Params["until"]); untilStr != "" {
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
	}

	return &types.SuspendSpec{
		Mode:    types.ModeTimer,
		Timer:   timer,
		Timeout: timeout,
	}, nil
}

func (n *WaitNode) prepareSignal(input *types.Input, timeout time.Duration) (*types.SuspendSpec, error) {
	if signals, err := cast.ToStringSliceE(input.Params["signals"]); err == nil && len(signals) > 0 {
		quorum := 0
		if q, err := cast.ToIntE(input.Params["quorum"]); err == nil {
			quorum = q
		}
		return &types.SuspendSpec{
			Mode:    types.ModeMultiSignal,
			Signals: signals,
			Quorum:  quorum,
			Timeout: timeout,
		}, nil
	}

	signalName := cast.ToString(input.Params["signal_name"])
	if signalName == "" {
		signalName = input.NodeName + "/signal"
	}

	return &types.SuspendSpec{
		Mode:    types.ModeSignal,
		Signals: []string{signalName},
		Timeout: timeout,
	}, nil
}

func (n *WaitNode) OnResume(_ context.Context, input *types.Input, signal *types.SignalPayload) (*types.Output, error) {
	switch signal.Triggered {
	case types.TimeoutFired:
		return &types.Output{Data: map[string]any{"reason": "timeout"}, Port: "timeout"}, nil
	case types.TimerFired:
		return &types.Output{Data: input.Data, Port: "main"}, nil
	case types.SignalReceived:
		data := make(map[string]any)
		if input.Inputs != nil {
			for k, v := range input.Inputs {
				data[k] = v
			}
		}
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
		return &types.Output{Data: data, Port: "main"}, nil
	default:
		return nil, fmt.Errorf("xflow.wait: unknown trigger %d", signal.Triggered)
	}
}

func init() { registry.Register(&WaitNode{}) }
