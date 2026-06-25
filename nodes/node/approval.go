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

// WithTimeout configures how long the approval node waits before routing to
// the timeout output or rejecting automatically.
func (n *ApprovalNode) WithTimeout(duration string, action string) *ApprovalNode {
	n.TimeoutStr = duration
	n.TimeoutAction = action
	return n
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
		if len(getInternalDecisions(input.Data)) > 0 {
			return &SuspendSpec{
				Mode:    ModeSignal,
				Signals: []string{approvalSignal(input.NodeName)},
				Timeout: params.Timeout,
			}, nil
		}
		return &SuspendSpec{
			Mode:    ModeMultiSignal,
			Signals: approverSignals(input.NodeName, params.Approvers),
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
	if params.Mode == ApprovalAll && len(signal.All) > 0 {
		return n.handleAllSignals(params, input, signal.All)
	}

	actionRaw, ok := signal.Data["action"]
	if !ok {
		return nil, fmt.Errorf("approval signal missing \"action\" field")
	}
	action, ok := actionRaw.(string)
	if !ok {
		return nil, fmt.Errorf("approval signal \"action\" field is not a string")
	}
	approver, err := validateApprovalApprover(params, signal.Data)
	if err != nil {
		return nil, err
	}
	if params.Mode == ApprovalSequential {
		if err := validateCurrentSequentialApprover(params, input.Data, approver); err != nil {
			return nil, err
		}
	}

	switch action {
	case "approve":
		return n.handleApprove(params, input, signal, approver)
	case "reject":
		if params.Mode == ApprovalAll && hasApproverDecision(getDecisions(input.Data), approver) {
			return nil, fmt.Errorf("approval already received from approver %q", approver)
		}
		decisions := appendDecision(getDecisions(input.Data), approver, "reject", signal.Data["comment"])
		return &Output{
			Data: approvalOutput(input.Data, map[string]any{
				"approved":  false,
				"approver":  approver,
				"comment":   signal.Data["comment"],
				"decisions": decisions,
			}),
			Port: "rejected",
		}, nil
	case "return":
		return &Output{Resuspend: true}, nil
	}

	return nil, fmt.Errorf("unknown approval action: %s", action)
}

func (n *ApprovalNode) handleApprove(params *ApprovalParams, input *Input, signal *SignalPayload, approver string) (*Output, error) {
	switch params.Mode {
	case ApprovalAny:
		return &Output{
			Data: approvalOutput(input.Data, map[string]any{"approved": true, "approver": approver, "comment": signal.Data["comment"]}),
			Port: "approved",
		}, nil

	case ApprovalAll:
		decisions := getDecisions(input.Data)
		if hasApproverDecision(decisions, approver) {
			return nil, fmt.Errorf("approval already received from approver %q", approver)
		}
		decisions = appendDecision(decisions, approver, "approve", signal.Data["comment"])
		if len(decisions) < len(params.Approvers) {
			return &Output{
				Resuspend: true,
				Data:      approvalOutput(input.Data, map[string]any{"_decisions": decisions, "decisions": decisions}),
			}, nil
		}
		return &Output{
			Data: approvalOutput(input.Data, map[string]any{"approved": true, "decisions": decisions}),
			Port: "approved",
		}, nil

	case ApprovalSequential:
		currentIdx := getApproverIndex(input.Data)
		decisions := appendDecision(getDecisions(input.Data), approver, "approve", signal.Data["comment"])
		nextIdx := currentIdx + 1
		if nextIdx < len(params.Approvers) {
			return &Output{
				Resuspend: true,
				Data:      approvalOutput(input.Data, map[string]any{"_approver_idx": nextIdx, "decisions": decisions}),
			}, nil
		}
		return &Output{
			Data: approvalOutput(input.Data, map[string]any{"approved": true, "decisions": decisions}),
			Port: "approved",
		}, nil
	}

	return nil, nil
}

func (n *ApprovalNode) handleAllSignals(params *ApprovalParams, input *Input, all map[string]map[string]any) (*Output, error) {
	decisions := make([]map[string]any, 0, len(params.Approvers))
	var rejectedApprover string
	var rejectedComment any

	for _, approver := range params.Approvers {
		signalName := approverSignal(input.NodeName, approver)
		payload, ok := all[signalName]
		if !ok {
			return nil, fmt.Errorf("approval signal missing payload for %q", signalName)
		}

		payloadApprover, err := validateApprovalApprover(params, payload)
		if err != nil {
			return nil, err
		}
		if payloadApprover != approver {
			return nil, fmt.Errorf("approval signal %q expected approver %q, got %q", signalName, approver, payloadApprover)
		}

		actionRaw, ok := payload["action"]
		if !ok {
			return nil, fmt.Errorf("approval signal missing \"action\" field")
		}
		action, ok := actionRaw.(string)
		if !ok {
			return nil, fmt.Errorf("approval signal \"action\" field is not a string")
		}
		if action != "approve" && action != "reject" {
			return nil, fmt.Errorf("unknown approval action: %s", action)
		}

		decisions = appendDecision(decisions, approver, action, payload["comment"])
		if action == "reject" {
			rejectedApprover = approver
			rejectedComment = payload["comment"]
			break
		}
	}

	if rejectedApprover != "" {
		return &Output{
			Data: approvalOutput(input.Data, map[string]any{
				"approved":  false,
				"approver":  rejectedApprover,
				"comment":   rejectedComment,
				"decisions": decisions,
			}),
			Port: "rejected",
		}, nil
	}

	return &Output{
		Data: approvalOutput(input.Data, map[string]any{
			"approved":  true,
			"decisions": decisions,
		}),
		Port: "approved",
	}, nil
}

func approvalOutput(base map[string]any, overlay map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(overlay))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overlay {
		out[k] = v
	}
	return out
}

func validateCurrentSequentialApprover(params *ApprovalParams, data map[string]any, approver string) error {
	currentIdx := getApproverIndex(data)
	if currentIdx >= len(params.Approvers) {
		return fmt.Errorf("approver index %d out of range (total %d)", currentIdx, len(params.Approvers))
	}
	currentApprover := params.Approvers[currentIdx]
	if approver != currentApprover {
		return fmt.Errorf("approval expected from approver %q, got %q", currentApprover, approver)
	}
	return nil
}

func validateApprovalApprover(params *ApprovalParams, data map[string]any) (string, error) {
	if data == nil {
		return "", fmt.Errorf("approval signal missing \"approver\" field")
	}
	approverRaw, ok := data["approver"]
	if !ok {
		return "", fmt.Errorf("approval signal missing \"approver\" field")
	}
	approver, ok := approverRaw.(string)
	if !ok {
		return "", fmt.Errorf("approval signal \"approver\" field is not a string")
	}
	for _, allowed := range params.Approvers {
		if approver == allowed {
			return approver, nil
		}
	}
	return "", fmt.Errorf("approval signal approver %q is not authorized", approver)
}

func appendDecision(decisions []map[string]any, approver string, action string, comment any) []map[string]any {
	return append(decisions, map[string]any{
		"approver": approver,
		"action":   action,
		"comment":  comment,
	})
}

func hasApproverDecision(decisions []map[string]any, approver string) bool {
	for _, decision := range decisions {
		if decision["approver"] == approver {
			return true
		}
	}
	return false
}

func approvalSignal(nodeName string) string {
	return nodeName + "/approval"
}

func approverSignals(nodeName string, approvers []string) []string {
	signals := make([]string, 0, len(approvers))
	for _, approver := range approvers {
		signals = append(signals, approverSignal(nodeName, approver))
	}
	return signals
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
	v, ok := data["decisions"]
	if !ok {
		v, ok = data["_decisions"]
	}
	if !ok {
		return nil
	}
	return parseDecisionList(v)
}

func getInternalDecisions(data map[string]any) []map[string]any {
	if data == nil {
		return nil
	}
	v, ok := data["_decisions"]
	if !ok {
		return nil
	}
	return parseDecisionList(v)
}

func parseDecisionList(v any) []map[string]any {
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
