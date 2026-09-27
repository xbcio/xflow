package group

import (
	"context"
	"fmt"
	"time"

	"github.com/xbcio/xflow/types"

	nodeinternal "github.com/xbcio/xflow/node/internal"
	"github.com/xbcio/xflow/node/registry"
)

// ApprovalNodeType is the canonical type identifier for the approval node.
const ApprovalNodeType = "xflow.approval"

// Approval actions. These are wire values: the action travels in the signal
// payload under the "action" field.
const (
	actionApprove = "approve"
	actionReject  = "reject"
	actionReturn  = "return"
)

// Keys the node keeps in its own output data across resumptions.
const (
	// ledgerKey holds this node's own record of who decided what. It is
	// underscore-prefixed because input.Data carries the merged output of every
	// upstream node: a node that happens to publish a "decisions" field of its
	// own would otherwise be read as approvers who had already voted. The
	// public "decisions" copy below is for downstream readers, never for this
	// node's state.
	ledgerKey = "_decisions"
	// decisionsKey is the public copy of the ledger, read by whatever follows
	// the approval node.
	decisionsKey = "decisions"
	// ignoredKey holds the bounded trail of signals that were delivered but not
	// counted, and ignoredCountKey the total number seen. Together they make a
	// refusal observable without letting a caller grow the stored output: the
	// trail is what a person reads, the count is what it cannot silently hide.
	ignoredKey      = "_ignored"
	ignoredCountKey = "_ignored_count"
	// ignoredTrailLimit bounds the recorded trail. Anything beyond it is
	// reflected only in ignoredCountKey.
	ignoredTrailLimit = 20
	// ignoredNameLimit bounds a recorded signal name, which a caller controls.
	ignoredNameLimit = 128
)

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
	nodeinternal.BaseNode
	Approvers     []string
	Mode          ApprovalMode
	TimeoutStr    string
	TimeoutAction string
}

// Approval creates an approval gate node.
//
//	node.Approval([]string{"manager@example.com"}, node.ApprovalAny)
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

func (n *ApprovalNode) Descriptor() types.Descriptor {
	return types.Descriptor{
		Type:        ApprovalNodeType,
		DisplayName: "Approval",
		Params: []types.ParamSpec{
			{Name: "approvers", DisplayName: "Approvers", Type: types.ParamArray, Required: true, Description: "List of approver identifiers"},
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: false, Default: "any", Description: "Approval mode: \"any\", \"all\", or \"sequential\""},
			{Name: "timeout", DisplayName: "Timeout", Type: types.ParamString, Required: false, Description: "Maximum wait duration before timeout routing (e.g. \"48h\")"},
			{Name: "timeout_action", DisplayName: "Timeout Action", Type: types.ParamString, Required: false, Default: "route", Description: "Action on timeout: \"reject\" or \"route\""},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "approved", DisplayName: "Approved"}, {Name: "rejected", DisplayName: "Rejected"}, {Name: "timeout", DisplayName: "Timeout"}},
	}
}

func (n *ApprovalNode) NodeType() string { return ApprovalNodeType }
func (n *ApprovalNode) OnError(s types.OnError) types.Builder {
	n.SetOnError(s)
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

func (n *ApprovalNode) Execute(_ context.Context, _ *types.Input) (*types.Output, error) {
	return &types.Output{Data: map[string]any{"_type": ApprovalNodeType}}, nil
}

// PrepareSuspend arms the wait for the next decision.
//
// Every mode except "any" waits on a signal per approver with a quorum of one,
// so the node resumes as soon as one approver acts instead of waiting for the
// whole set. That is what makes a rejection final without the remaining
// approvers having to answer first.
func (n *ApprovalNode) PrepareSuspend(_ context.Context, input *types.Input) (*types.SuspendSpec, error) {
	params, err := parseApprovalParams(input.Params)
	if err != nil {
		return nil, err
	}

	switch params.Mode {
	case ApprovalAny:
		return &types.SuspendSpec{
			Mode:    types.ModeSignal,
			Signals: []string{approvalSignal(input.NodeName)},
			Timeout: params.Timeout,
		}, nil

	case ApprovalAll:
		return &types.SuspendSpec{
			Mode:    types.ModeMultiSignal,
			Signals: approverSignals(input.NodeName, params.Approvers),
			Quorum:  1,
			Timeout: params.Timeout,
		}, nil

	case ApprovalSequential:
		// Every approver's signal is armed, not only the one whose turn it is.
		// A signal delivered ahead of its turn then resumes the node and is
		// refused by OnResume, which is what keeps the order a sequence is for:
		// if the later signals were not armed, an early one would stay parked in
		// the backend and be consumed later, counting a vote cast before its
		// approver could see the earlier decisions.
		return &types.SuspendSpec{
			Mode:    types.ModeMultiSignal,
			Signals: approverSignals(input.NodeName, params.Approvers),
			Quorum:  1,
			Timeout: params.Timeout,
		}, nil
	}

	return nil, fmt.Errorf("unknown approval mode: %s", params.Mode)
}

func (n *ApprovalNode) OnResume(_ context.Context, input *types.Input, signal *types.SignalPayload) (*types.Output, error) {
	params, err := parseApprovalParams(input.Params)
	if err != nil {
		return nil, err
	}

	if signal.Triggered == types.TimeoutFired {
		switch params.TimeoutAction {
		case "reject":
			return &types.Output{Data: map[string]any{"approved": false, "reason": "timeout"}, Port: "rejected"}, nil
		default:
			return &types.Output{Data: map[string]any{"reason": "timeout"}, Port: "timeout"}, nil
		}
	}

	// Everything below this point decides whether the signal may change the
	// outcome. A signal that cannot be attributed to an authorized approver, or
	// cannot be read as a decision, is recorded and the node keeps waiting. It
	// deliberately does not fail the execution: failing here would hand any
	// caller a way to kill an in-flight approval with one misattributed or
	// malformed signal.
	//
	// Identity is established before anything in the payload is interpreted: an
	// unauthenticated signal is not read at all, so a caller cannot get the node
	// to act on a field it supplied before we know who is speaking.
	actor, ok := resolveActor(signal.Data)
	if !ok {
		return ignoreApprovalSignal(input, signal, "unverified-actor")
	}
	if !isRegisteredApprover(params, actor) {
		return ignoreApprovalSignal(input, signal, "unauthorized-approver")
	}
	action, ok := signalAction(signal.Data)
	if !ok {
		return ignoreApprovalSignal(input, signal, "malformed-action")
	}
	if signal.Name != expectedApproverSignal(input.NodeName, params.Mode, actor) {
		return ignoreApprovalSignal(input, signal, "signal-name-mismatch")
	}
	if params.Mode == ApprovalSequential && actor != currentSequentialApprover(params, input.Data) {
		return ignoreApprovalSignal(input, signal, "not-current-approver")
	}
	if hasApproverDecision(getDecisions(input.Data), actor) {
		// Idempotent: a decision already on the ledger is not counted twice, and
		// a retried delivery is not an error. Critical operations must tolerate
		// a repeat without acting twice.
		return &types.Output{Resuspend: true}, nil
	}

	switch action {
	case actionApprove:
		return n.handleApprove(params, input, signal, actor)
	case actionReject:
		return n.handleReject(input, signal, actor)
	case actionReturn:
		return &types.Output{Resuspend: true}, nil
	}

	return ignoreApprovalSignal(input, signal, "unknown-action")
}

// handleApprove records one approval. In "any" mode a single approval decides;
// in "all" and "sequential" the gate opens once every approver has decided,
// which the ledger alone determines.
func (n *ApprovalNode) handleApprove(params *ApprovalParams, input *types.Input, signal *types.SignalPayload, approver string) (*types.Output, error) {
	if params.Mode == ApprovalAny {
		return &types.Output{
			Data: approvalOutput(input.Data, map[string]any{"approved": true, "approver": approver, "comment": signal.Data["comment"]}),
			Port: "approved",
		}, nil
	}

	decisions := appendDecision(getDecisions(input.Data), approver, actionApprove, signal.Data["comment"])
	if len(decisions) < len(params.Approvers) {
		return &types.Output{
			Resuspend: true,
			Data:      approvalOutput(input.Data, decisionLedger(decisions)),
		}, nil
	}
	return &types.Output{
		Data: approvalOutput(input.Data, decisionLedger(decisions), map[string]any{"approved": true}),
		Port: "approved",
	}, nil
}

// handleReject rejects the gate on the first rejection, whatever the mode. The
// ledger keeps the decisions recorded so far, so the trail shows who had
// answered before the gate closed.
func (n *ApprovalNode) handleReject(input *types.Input, signal *types.SignalPayload, approver string) (*types.Output, error) {
	decisions := appendDecision(getDecisions(input.Data), approver, actionReject, signal.Data["comment"])
	return &types.Output{
		Data: approvalOutput(input.Data,
			decisionLedger(decisions),
			map[string]any{
				"approved": false,
				"approver": approver,
				"comment":  signal.Data["comment"],
			}),
		Port: "rejected",
	}, nil
}

// decisionLedger is the overlay that records the ledger in both its private and
// public form.
func decisionLedger(decisions []map[string]any) map[string]any {
	return map[string]any{ledgerKey: decisions, decisionsKey: decisions}
}

// ignoreApprovalSignal re-suspends the node with its state unchanged, recording
// why the signal was not counted. Data is returned so the trail is persisted;
// the wait specification is recomputed from the same ledger either way.
func ignoreApprovalSignal(input *types.Input, signal *types.SignalPayload, reason string) (*types.Output, error) {
	return &types.Output{
		Resuspend: true,
		Data: approvalOutput(input.Data, map[string]any{
			ignoredKey:      appendIgnored(input.Data, signal.Name, reason),
			ignoredCountKey: getIgnoredCount(input.Data) + 1,
		}),
	}, nil
}

func appendIgnored(data map[string]any, signalName, reason string) []map[string]any {
	trail := getIgnored(data)
	trail = append(trail, map[string]any{
		"signal": truncateForLedger(signalName, ignoredNameLimit),
		"reason": reason,
	})
	if len(trail) > ignoredTrailLimit {
		trail = trail[len(trail)-ignoredTrailLimit:]
	}
	return trail
}

func getIgnored(data map[string]any) []map[string]any {
	if data == nil {
		return nil
	}
	return parseDecisionList(data[ignoredKey])
}

func getIgnoredCount(data map[string]any) int {
	if data == nil {
		return 0
	}
	switch n := data[ignoredCountKey].(type) {
	case int:
		return n
	case float64:
		return int(n)
	}
	return 0
}

// truncateForLedger bounds a caller-supplied string recorded in the node's
// output, so a single signal cannot inflate what the node persists.
func truncateForLedger(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}

// signalAction reads the decision the signal carries. It reports false for a
// missing or non-string value rather than an error: a payload the node cannot
// read is one it does not act on, not a reason to fail the execution.
func signalAction(data map[string]any) (string, bool) {
	raw, ok := data["action"]
	if !ok {
		return "", false
	}
	action, ok := raw.(string)
	return action, ok
}

// currentSequentialApprover is the first approver who has not decided yet. The
// ledger is the source of truth rather than a stored cursor, so the two cannot
// disagree.
func currentSequentialApprover(params *ApprovalParams, data map[string]any) string {
	decisions := getDecisions(data)
	for _, approver := range params.Approvers {
		if !hasApproverDecision(decisions, approver) {
			return approver
		}
	}
	return ""
}

// expectedApproverSignal is the signal name that carries a given approver's
// decision. In "any" mode every approver answers on the node's single signal;
// in the other modes each approver has their own name, which keeps one
// approver's delivery from displacing another's while both are in flight.
func expectedApproverSignal(nodeName string, mode ApprovalMode, approver string) string {
	if mode == ApprovalAny {
		return approvalSignal(nodeName)
	}
	return approverSignal(nodeName, approver)
}

// approvalOutput layers each overlay over the data carried into this
// resumption, so fields an earlier step set are preserved unless replaced.
func approvalOutput(base map[string]any, overlays ...map[string]any) map[string]any {
	size := len(base)
	for _, overlay := range overlays {
		size += len(overlay)
	}
	out := make(map[string]any, size)
	for k, v := range base {
		out[k] = v
	}
	for _, overlay := range overlays {
		for k, v := range overlay {
			out[k] = v
		}
	}
	return out
}

// resolveActor reads the identity the control plane established for the caller.
//
// The "approver" field is whatever the caller wrote in the payload, so it is a
// claim about who someone is, not evidence of it: an approval gate that acted on
// it would let anyone able to deliver a signal decide any approval by claiming
// to be a named approver. The subject the API layer verified from the caller's
// credentials is the only accepted identity, and its absence means the request
// cannot be attributed to anybody.
func resolveActor(data map[string]any) (string, bool) {
	if data == nil {
		return "", false
	}
	actor, ok := data[types.VerifiedActorKey].(string)
	if !ok || actor == "" {
		return "", false
	}
	return actor, true
}

func isRegisteredApprover(params *ApprovalParams, actor string) bool {
	for _, allowed := range params.Approvers {
		if actor == allowed {
			return true
		}
	}
	return false
}

func appendDecision(decisions []map[string]any, approver string, action string, comment any) []map[string]any {
	return append(decisions, map[string]any{
		"approver": approver,
		"action":   action,
		"comment":  comment,
		"at":       time.Now().UTC().Format(time.RFC3339),
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

func getDecisions(data map[string]any) []map[string]any {
	if data == nil {
		return nil
	}
	return parseDecisionList(data[ledgerKey])
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

func init() { registry.Register(&ApprovalNode{}) }
