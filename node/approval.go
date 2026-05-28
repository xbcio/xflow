package node

import (
	"context"
	"fmt"
	"time"
)

// ApprovalNodeType is the canonical type identifier for the approval node.
const ApprovalNodeType = "xflow.approval"

// ApprovalMode represents the approval strategy.
type ApprovalMode string

const (
	ApprovalAny        ApprovalMode = "any"
	ApprovalAll        ApprovalMode = "all"
	ApprovalSequential ApprovalMode = "sequential"
)

// ApprovalParams holds the configuration for an approval node.
type ApprovalParams struct {
	Approvers     []string      `json:"approvers"`
	Mode          ApprovalMode  `json:"mode"`
	Timeout       time.Duration `json:"timeout"`
	TimeoutAction string        `json:"timeout_action"`
}

// ApprovalNode implements xflow.approval — suspends execution until
// approvers deliver their decisions via signals.
type ApprovalNode struct {
	BaseNode
	Approvers     []string
	Mode          ApprovalMode
	TimeoutStr    string
	TimeoutAction string
}

// Approval creates an approval gate node.
//
//	node.Approval([]string{"manager@co.com"}, node.ApprovalAny)
func Approval(approvers []string, mode ApprovalMode) *ApprovalNode {
	return &ApprovalNode{Approvers: approvers, Mode: mode}
}

func (n *ApprovalNode) Descriptor() Descriptor {
	return Descriptor{
		Type:        ApprovalNodeType,
		DisplayName: "Approval",
		Params: []ParamSpec{
			{Name: "approvers", DisplayName: "Approvers", Type: ParamArray, Required: true, Description: "List of approver identifiers"},
			{Name: "mode", DisplayName: "Mode", Type: ParamString, Required: false, Default: "any", Description: "Approval mode: \"any\", \"all\", or \"sequential\""},
			{Name: "timeout", DisplayName: "Timeout", Type: ParamString, Required: false, Description: "Maximum wait duration before timeout routing (e.g. \"48h\")"},
			{Name: "timeout_action", DisplayName: "Timeout Action", Type: ParamString, Required: false, Default: "route", Description: "Action on timeout: \"reject\" or \"route\""},
		},
		Inputs:  []PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []PortSpec{{Name: "approved", DisplayName: "Approved"}, {Name: "rejected", DisplayName: "Rejected"}, {Name: "timeout", DisplayName: "Timeout"}},
	}
}

func (n *ApprovalNode) NodeType() string { return ApprovalNodeType }
func (n *ApprovalNode) OnError(s OnError) Builder {
	n.onError = s
	return n
}

func (n *ApprovalNode) RawParams() any {
	params := map[string]any{
		"approvers": n.Approvers,
		"mode":      string(n.Mode),
	}
	if n.TimeoutStr != "" {
		params["timeout"] = n.TimeoutStr
	}
	if n.TimeoutAction != "" {
		params["timeout_action"] = n.TimeoutAction
	}
	return params
}

func (n *ApprovalNode) Execute(_ context.Context, _ *Input) (*Output, error) {
	return &Output{Data: map[string]any{"_type": ApprovalNodeType}}, nil
}

func (n *ApprovalNode) PrepareSuspend(_ context.Context, input *Input) (*SuspendSpec, error) {
	params, err := parseApprovalParams(input.Params)
	if err != nil {
		return nil, err
	}

	switch params.Mode {
	case ApprovalAny:
		return &SuspendSpec{
			Mode:    ModeSignal,
			Signals: []string{approvalSignal(input.NodeName)},
			Timeout: params.Timeout,
		}, nil

	case ApprovalAll:
		return &SuspendSpec{
			Mode:    ModeSignal,
			Signals: []string{approvalSignal(input.NodeName)},
			Timeout: params.Timeout,
		}, nil

	case ApprovalSequential:
		idx := getApproverIndex(input.Data)
		if idx >= len(params.Approvers) {
			return nil, fmt.Errorf("approver index %d out of range (total %d)", idx, len(params.Approvers))
		}
		return &SuspendSpec{
			Mode:    ModeSignal,
			Signals: []string{approverSignal(input.NodeName, params.Approvers[idx])},
			Timeout: params.Timeout,
		}, nil
	}

	return nil, fmt.Errorf("unknown approval mode: %s", params.Mode)
}

func (n *ApprovalNode) OnResume(_ context.Context, input *Input, signal *SignalPayload) (*Output, error) {
	params, err := parseApprovalParams(input.Params)
	if err != nil {
		return nil, err
	}

	if signal.Triggered == TimeoutFired {
		switch params.TimeoutAction {
		case "reject":
			return &Output{Data: map[string]any{"approved": false, "reason": "timeout"}, Port: "rejected"}, nil
		default:
			return &Output{Data: map[string]any{"reason": "timeout"}, Port: "timeout"}, nil
		}
	}

	actionRaw, ok := signal.Data["action"]
	if !ok {
		return nil, fmt.Errorf("approval signal missing \"action\" field")
	}
	action, ok := actionRaw.(string)
	if !ok {
		return nil, fmt.Errorf("approval signal \"action\" field is not a string")
	}

	switch action {
	case "approve":
		return n.handleApprove(params, input, signal)
	case "reject":
		return &Output{
			Data: map[string]any{"approved": false, "approver": signal.Data["approver"], "comment": signal.Data["comment"]},
			Port: "rejected",
		}, nil
	case "return":
		return &Output{Resuspend: true}, nil
	}

	return nil, fmt.Errorf("unknown approval action: %s", action)
}

func (n *ApprovalNode) handleApprove(params *ApprovalParams, input *Input, signal *SignalPayload) (*Output, error) {
	switch params.Mode {
	case ApprovalAny:
		return &Output{
			Data: map[string]any{"approved": true, "approver": signal.Data["approver"], "comment": signal.Data["comment"]},
			Port: "approved",
		}, nil

	case ApprovalAll:
		decisions := getDecisions(input.Data)
		decisions = append(decisions, map[string]any{
			"approver": signal.Data["approver"],
			"action":   "approve",
			"comment":  signal.Data["comment"],
		})
		if len(decisions) < len(params.Approvers) {
			return &Output{
				Resuspend: true,
				Data:      map[string]any{"_decisions": decisions},
			}, nil
		}
		return &Output{
			Data: map[string]any{"approved": true, "decisions": decisions},
			Port: "approved",
		}, nil

	case ApprovalSequential:
		idx := getApproverIndex(input.Data) + 1
		if idx < len(params.Approvers) {
			return &Output{
				Resuspend: true,
				Data:      map[string]any{"_approver_idx": idx},
			}, nil
		}
		return &Output{
			Data: map[string]any{"approved": true},
			Port: "approved",
		}, nil
	}

	return nil, nil
}

func approvalSignal(nodeName string) string {
	return nodeName + "/approval"
}

func approverSignal(nodeName, approver string) string {
	return nodeName + "/approval/" + approver
}

func parseApprovalParams(params map[string]any) (*ApprovalParams, error) {
	p := &ApprovalParams{
		Mode:          ApprovalAny,
		TimeoutAction: "route",
	}

	if approvers, ok := params["approvers"]; ok {
		switch v := approvers.(type) {
		case []string:
			p.Approvers = v
		case []any:
			for _, a := range v {
				s, ok := a.(string)
				if !ok {
					return nil, fmt.Errorf("approvers must be a list of strings")
				}
				p.Approvers = append(p.Approvers, s)
			}
		default:
			return nil, fmt.Errorf("approvers must be a list of strings")
		}
	}
	if len(p.Approvers) == 0 {
		return nil, fmt.Errorf("approvers list must not be empty")
	}

	if mode, ok := params["mode"]; ok {
		if s, ok := mode.(string); ok {
			p.Mode = ApprovalMode(s)
		}
	}

	if timeout, ok := params["timeout"]; ok {
		switch v := timeout.(type) {
		case string:
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, fmt.Errorf("invalid timeout duration: %w", err)
			}
			p.Timeout = d
		case float64:
			p.Timeout = time.Duration(v)
		case time.Duration:
			p.Timeout = v
		}
	}

	if ta, ok := params["timeout_action"]; ok {
		if s, ok := ta.(string); ok {
			p.TimeoutAction = s
		}
	}

	return p, nil
}

func getApproverIndex(data map[string]any) int {
	if data == nil {
		return 0
	}
	v, ok := data["_approver_idx"]
	if !ok {
		return 0
	}
	switch idx := v.(type) {
	case int:
		return idx
	case float64:
		return int(idx)
	}
	return 0
}

func getDecisions(data map[string]any) []map[string]any {
	if data == nil {
		return nil
	}
	v, ok := data["_decisions"]
	if !ok {
		return nil
	}
	switch d := v.(type) {
	case []map[string]any:
		return d
	case []any:
		result := make([]map[string]any, 0, len(d))
		for _, item := range d {
			if m, ok := item.(map[string]any); ok {
				result = append(result, m)
			}
		}
		return result
	}
	return nil
}

func init() { Register(&ApprovalNode{}) }
