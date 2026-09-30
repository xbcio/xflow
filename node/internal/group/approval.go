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
	actionApprove   = "approve"
	actionReject    = "reject"
	actionReturn    = "return"
	actionDelegate  = "delegate"
	actionAddSigner = "add_signer"
	actionForce     = "force"
)

// Positions an added signer can take relative to the approver who added them.
// The distinction only changes the outcome in "sequential" mode, where the chain
// is an order; in the other modes it only affects how the chain reads.
const (
	positionBefore = "before"
	positionAfter  = "after"
)

// Keys the node keeps in its own private state (Input.State / Output.State).
//
// They are read back only from this node's own record. The data channel
// (Input.Data) is the merged output of every upstream node, so a key kept there
// can also be written by a node upstream of this one -- a caller could open the
// gate by publishing a plausible ledger. State has no such writer, which is what
// makes these keys safe to trust as the gate's own memory.
const (
	// decisionsKey holds the ledger: who decided what, in order. The same name is
	// published in the node's output data, rebuilt from the state, so whoever
	// follows the gate can read the record without being able to write it.
	decisionsKey = "decisions"
	// ignoredKey holds the bounded trail of signals that were delivered but not
	// counted, and ignoredCountKey the total number seen. Together they make a
	// refusal observable without letting a caller grow the stored output: the
	// trail is what a person reads, the count is what it cannot silently hide.
	ignoredKey      = "ignored"
	ignoredCountKey = "ignored_count"
	// ignoredTrailLimit bounds the recorded trail. Anything beyond it is
	// reflected only in ignoredCountKey.
	ignoredTrailLimit = 20
	// ignoredNameLimit bounds a recorded signal name, which a caller controls.
	ignoredNameLimit = 128
	// maxApproverChain bounds how long the chain may become. Each added signer is
	// a slot the gate must wait for and an entry in the node's stored output, and
	// the approvers who may add one are themselves chain members, so without a
	// bound the chain would grow for as long as somebody kept asking for help.
	// The limit is far past any chain a group could actually operate.
	maxApproverChain = 32
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
	// ForceApprovers names the subjects who may close the gate without the
	// outstanding approvers, by declaring that the decision is made above them.
	// Empty -- the default -- means nobody can, and the force signal is not even
	// armed; see handleForce for what a force approval is and is not allowed to
	// skip.
	//
	// This param is evaluated like every other one, so a definition may template
	// it; a definition that resolves it from execution data is choosing to let
	// whoever supplies that data name the force approvers.
	ForceApprovers []string `json:"force_approvers"`
}

// ApprovalNode implements xflow.approval — suspends execution until
// approvers deliver their decisions via signals.
type ApprovalNode struct {
	nodeinternal.BaseNode
	Approvers      []string
	Mode           ApprovalMode
	TimeoutStr     string
	TimeoutAction  string
	ForceApprovers []string
}

// Approval creates an approval gate node.
//
//	node.Approval([]string{"manager@example.com"}, node.ApprovalAny)
func Approval(approvers []string, mode ApprovalMode) *ApprovalNode {
	return &ApprovalNode{Approvers: approvers, Mode: mode}
}

// WithForceApprovers names the subjects allowed to force this gate open. It is
// meant for a chain that reports to a decision maker who may not be made to wait
// for every slot, so the list is part of the workflow definition and changes by
// editing the definition, not by the people it names.
func (n *ApprovalNode) WithForceApprovers(forceApprovers []string) *ApprovalNode {
	n.ForceApprovers = forceApprovers
	return n
}

// WithTimeout configures how long the approval node waits before routing to
// the timeout output or rejecting automatically.
func (n *ApprovalNode) WithTimeout(duration string, action string) *ApprovalNode {
	n.TimeoutStr = duration
	n.TimeoutAction = action
	return n
}

func (n *ApprovalNode) Descriptor() types.Descriptor {
	// minApprovers mirrors parseApprovalParams, which refuses an empty list.
	minApprovers := 1
	return types.Descriptor{
		Type:        ApprovalNodeType,
		DisplayName: "Approval",
		Params: []types.ParamSpec{
			{Name: "approvers", DisplayName: "Approvers", Type: types.ParamArray, Required: true, Description: "List of approver identifiers",
				Item: nodeinternal.StringItem(), Constraints: &types.Constraints{MinItems: &minApprovers}},
			{Name: "mode", DisplayName: "Mode", Type: types.ParamString, Required: false, Default: "any", Description: "Approval mode: \"any\", \"all\", or \"sequential\"",
				Enum: []types.EnumOption{
					{Value: string(ApprovalAny), DisplayName: "Any", Description: "The first approver's decision closes the gate"},
					{Value: string(ApprovalAll), DisplayName: "All", Description: "Every approver must approve"},
					{Value: string(ApprovalSequential), DisplayName: "Sequential", Description: "Approvers decide one after another, in list order"},
				}},
			{Name: "timeout", DisplayName: "Timeout", Type: types.ParamString, Required: false, Description: "Maximum wait duration before timeout routing (e.g. \"48h\")",
				Widget: nodeinternal.WidgetDuration, Constraints: nodeinternal.Format(nodeinternal.FormatDuration)},
			{Name: "timeout_action", DisplayName: "Timeout Action", Type: types.ParamString, Required: false, Default: "route", Description: "Action on timeout: \"reject\" or \"route\"",
				VisibleWhen: nodeinternal.CondTruthy("timeout"),
				Enum: []types.EnumOption{
					{Value: "route", DisplayName: "Route", Description: "Leave through the timeout port"},
					{Value: actionReject, DisplayName: "Reject", Description: "Leave through the rejected port"},
				}},
			{Name: "force_approvers", DisplayName: "Force Approvers", Type: types.ParamArray, Required: false, Description: "Identifiers allowed to pass the gate without the outstanding approvers; unset means nobody can",
				Item: nodeinternal.StringItem()},
		},
		Inputs:  []types.PortSpec{{Name: "main", DisplayName: "Main"}},
		Outputs: []types.PortSpec{{Name: "approved", DisplayName: "Approved"}, {Name: "rejected", DisplayName: "Rejected"}, {Name: "returned", DisplayName: "Returned"}, {Name: "timeout", DisplayName: "Timeout"}},
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
	if len(n.ForceApprovers) > 0 {
		params["force_approvers"] = n.ForceApprovers
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
	// The chain is derived from the ledger, so a delegation made in an earlier
	// resumption is still waited on here: the wait is re-armed on every
	// resumption, and a chain that reverted to the configured approvers would
	// silently drop the people the gate now depends on.
	//
	// The ledger comes from the node's private state, never from input.Data: a
	// chain read out of the merged upstream data would let whoever published it
	// choose who the gate waits on.
	chain := approvedChain(params, getDecisions(input.State))

	// A force approval can close the gate from any mode, so its signal is armed
	// beside the approvers'. With no force_approvers the name is not armed at
	// all: a wait that does not listen for it cannot be resumed by it, which
	// makes an unconfigured gate unreachable by force rather than merely
	// unpersuaded by it.
	force := forceSignals(input.NodeName, params.ForceApprovers)

	switch params.Mode {
	case ApprovalAny:
		return &types.SuspendSpec{
			Mode:    types.ModeSignal,
			Signals: append([]string{approvalSignal(input.NodeName)}, force...),
			Timeout: params.Timeout,
		}, nil

	case ApprovalAll:
		return &types.SuspendSpec{
			Mode:    types.ModeMultiSignal,
			Signals: append(approverSignals(input.NodeName, chain), force...),
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
			Signals: append(approverSignals(input.NodeName, chain), force...),
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
	// outcome. A signal that cannot be attributed to somebody authorized for what
	// it asks, or cannot be read as a decision, is recorded and the node keeps
	// waiting. It deliberately does not fail the execution: failing here would
	// hand any caller a way to kill an in-flight approval with one misattributed
	// or malformed signal.
	//
	// Identity is established before anything in the payload is interpreted: an
	// unauthenticated signal is not read at all, so a caller cannot get the node
	// to act on a field it supplied before we know who is speaking. The action is
	// then read only to choose which authorization rule applies -- there are two,
	// the approver chain and the force list -- and that rule is applied before any
	// of the payload's other fields are looked at.
	actor, ok := resolveActor(signal.Data)
	if !ok {
		return ignoreApprovalSignal(input, signal, "unverified-actor")
	}
	record := recordOf(input)
	chain := approvedChain(params, record.decisions)
	action, ok := signalAction(signal.Data)
	if !ok {
		return ignoreApprovalSignal(input, signal, "malformed-action")
	}

	// A force approval is the one action authorized by the node's own force list
	// rather than by a slot in the chain: it is a statement that the decision is
	// made above the outstanding approvers, so the person making it need not be
	// one of them. It is also the one action that outranks the rollout of the
	// gate, so it is answered before the turn and repeat checks below, which
	// exist to sequence and to deduplicate slot decisions.
	if action == actionForce {
		if !containsApprover(params.ForceApprovers, actor) {
			return ignoreApprovalSignal(input, signal, "unauthorized-force-approver")
		}
		if signal.Name != forceSignal(input.NodeName) {
			return ignoreApprovalSignal(input, signal, "signal-name-mismatch")
		}
		return handleForce(input, record, signal, actor, chain)
	}

	if !containsApprover(chain, actor) {
		return ignoreApprovalSignal(input, signal, "unauthorized-approver")
	}
	if signal.Name != expectedApproverSignal(input.NodeName, params.Mode, actor) {
		return ignoreApprovalSignal(input, signal, "signal-name-mismatch")
	}
	if params.Mode == ApprovalSequential && actor != currentSequentialApprover(chain, record) {
		return ignoreApprovalSignal(input, signal, "not-current-approver")
	}
	if hasApproverDecision(record.decisions, actor) {
		// Idempotent: a decision already on the ledger is not counted twice, and
		// a retried delivery is not an error. Critical operations must tolerate
		// a repeat without acting twice. It also fences the amend-the-chain
		// action below: whoever has already decided cannot re-delegate a
		// decision that is on the record.
		//
		// Neither data nor state is returned, which is how the engine is told to
		// leave the stored output alone: a repeat has nothing new to say, and
		// rewriting the same record would be a store write per redelivery.
		return &types.Output{Resuspend: true}, nil
	}

	switch action {
	case actionApprove:
		return n.handleApprove(params, chain, input, signal, actor, record)
	case actionReject:
		return handleReject(input, record, signal, actor)
	case actionReturn:
		return handleReturn(input, record, signal, actor)
	case actionDelegate:
		return handleDelegate(input, record, signal, actor, chain)
	case actionAddSigner:
		return handleAddSigner(input, record, signal, actor, chain)
	}

	return ignoreApprovalSignal(input, signal, "unknown-action")
}

// handleApprove records one approval. In "any" mode a single approval decides;
// in "all" and "sequential" the gate opens once every member of the chain has
// decided. Completion is judged against the chain rather than against the length
// of the ledger: a delegated slot or an added signer changes who is being waited
// on, and the ledger also holds entries for the delegate and add_signer
// decisions themselves.
func (n *ApprovalNode) handleApprove(params *ApprovalParams, chain []string, input *types.Input, signal *types.SignalPayload, approver string, record approvalRecord) (*types.Output, error) {
	record.decisions = appendDecision(record.decisions, approver, actionApprove, signal.Data["comment"])
	if params.Mode == ApprovalAny {
		// "any" closes on the first approval, and names that approver beside
		// "approved" so a consumer need not read the ledger to know who decided.
		// The decision goes on the ledger all the same: it is the entry an
		// auditor looks for, and a gate whose record omitted the approval that
		// closed it would read like a gate nobody decided. Publishing through
		// decide() also keeps the rule the "all" path is pinned on -- the copy
		// downstream is rebuilt from this node's state, so an upstream map
		// under "decisions" cannot ride out as this node's record.
		return decide(input, record, "approved", map[string]any{
			"approved": true,
			"approver": approver,
			"comment":  signal.Data["comment"],
		}), nil
	}

	if !allChainMembersDecided(chain, record.decisions) {
		return resuspendWithRecord(input, record), nil
	}
	return decide(input, record, "approved", map[string]any{"approved": true}), nil
}

// handleForce closes the gate on the authority of a configured force approver,
// whatever the chain still has outstanding. The slots it passes over are
// recorded as bypassed rather than quietly left out: the point of the action is
// that the decision was taken above them, and a record that did not say whose
// decisions were skipped would read exactly like a gate that had collected them
// all.
//
// What it does not do is skip the identity check or the ledger: the caller is a
// subject the workflow itself named, verified by the control plane, and the
// decision is appended to the same record as every other one. Nor does it
// rewrite the chain -- the outstanding slots stay outstanding, because they were
// never decided; the gate simply does not wait for them.
func handleForce(input *types.Input, record approvalRecord, signal *types.SignalPayload, actor string, chain []string) (*types.Output, error) {
	bypassed := undecidedApprovers(chain, record.decisions)
	record.decisions = appendDecision(record.decisions, actor, actionForce, signal.Data["comment"])
	return decide(input, record, "approved", map[string]any{
		"approved": true,
		"approver": actor,
		"forced":   true,
		"bypassed": bypassed,
	}), nil
}

// undecidedApprovers lists the chain slots a force approval passes over, in
// chain order, so the record of a forced approval names them in the order the
// gate would have asked them.
func undecidedApprovers(chain []string, decisions []map[string]any) []string {
	var pending []string
	for _, approver := range chain {
		if !hasApproverDecision(decisions, approver) {
			pending = append(pending, approver)
		}
	}
	return pending
}

// handleDelegate moves the delegating approver's own slot in the chain to
// somebody else, so the person who now holds the decision is the person the gate
// waits on. The delegator's slot is replaced rather than duplicated: leaving them
// in the chain would keep a decision outstanding for someone who has already
// passed theirs on, and the gate would wait for a signature that is no longer
// theirs to give. The assignee may be anyone — a chain is not a role list, and
// the delegatee's identity is verified the same way as anyone else's when they
// sign.
func handleDelegate(input *types.Input, record approvalRecord, signal *types.SignalPayload, actor string, chain []string) (*types.Output, error) {
	assignee, ok := signalAssignee(signal.Data)
	if !ok || containsApprover(withoutApprover(chain, actor), assignee) {
		// The second condition covers both a payload that names nobody and one
		// that names somebody the chain already holds a slot for: either way the
		// chain would end up with a repeated approver, whose single decision
		// would stand for two outstanding ones.
		return ignoreApprovalSignal(input, signal, "malformed-assignee")
	}
	// The amended chain is not written out here: it is read back off the ledger
	// on the next resumption, so this decision is the only thing that has to be
	// stored.
	record.decisions = appendDecision(record.decisions, actor, actionDelegate, signal.Data["comment"],
		map[string]any{"assignee": assignee})
	return resuspendWithRecord(input, record), nil
}

// handleAddSigner inserts another slot into the chain next to the approver who
// asked for it. The signer then becomes a slot like any other, so the gate waits
// for their decision before it opens.
func handleAddSigner(input *types.Input, record approvalRecord, signal *types.SignalPayload, actor string, chain []string) (*types.Output, error) {
	assignee, ok := signalAssignee(signal.Data)
	if !ok || containsApprover(chain, assignee) {
		// A chain holds each name once: a repeated approver's single decision
		// would otherwise stand for two outstanding slots.
		return ignoreApprovalSignal(input, signal, "malformed-assignee")
	}
	position, ok := signalPosition(signal.Data)
	if !ok {
		return ignoreApprovalSignal(input, signal, "malformed-position")
	}
	if len(chain) >= maxApproverChain {
		return ignoreApprovalSignal(input, signal, "chain-limit-reached")
	}
	record.decisions = appendDecision(record.decisions, actor, actionAddSigner, signal.Data["comment"],
		map[string]any{"assignee": assignee, "position": position})
	return resuspendWithRecord(input, record), nil
}

// handleReject rejects the gate on the first rejection, whatever the mode. The
// ledger keeps the decisions recorded so far, so the trail shows who had
// answered before the gate closed.
func handleReject(input *types.Input, record approvalRecord, signal *types.SignalPayload, approver string) (*types.Output, error) {
	record.decisions = appendDecision(record.decisions, approver, actionReject, signal.Data["comment"])
	return decide(input, record, "rejected", map[string]any{
		"approved": false,
		"approver": approver,
		"comment":  signal.Data["comment"],
	}), nil
}

// handleReturn sends the work back to the caller on the "returned" port, which
// is what it means when an approver hands a request back for rework rather than
// declining it outright. It short-circuits the same way a reject does: the
// request has left the gate, so waiting for the remaining approvers could only
// delay a decision they no longer have anything to decide.
func handleReturn(input *types.Input, record approvalRecord, signal *types.SignalPayload, approver string) (*types.Output, error) {
	record.decisions = appendDecision(record.decisions, approver, actionReturn, signal.Data["comment"])
	return decide(input, record, "returned", map[string]any{
		"approved": false,
		"returned": true,
		"approver": approver,
		"comment":  signal.Data["comment"],
	}), nil
}

// approvalRecord is the node's own memory: what the gate has decided, and which
// signals it refused. It is read from Input.State and written to Output.State,
// and those are the only two places it travels -- see the key constants.
type approvalRecord struct {
	decisions []map[string]any
	ignored   []map[string]any
	ignoredN  int
}

func recordOf(input *types.Input) approvalRecord {
	return approvalRecord{
		decisions: getDecisions(input.State),
		ignored:   getIgnored(input.State),
		ignoredN:  getIgnoredCount(input.State),
	}
}

// state is the private half: what the engine persists and hands back to this
// node alone on its next resumption.
func (r approvalRecord) state() map[string]any {
	return map[string]any{
		decisionsKey:    r.decisions,
		ignoredKey:      r.ignored,
		ignoredCountKey: r.ignoredN,
	}
}

// resuspendWithRecord parks the gate again with an unchanged outcome and the
// record as it now stands.
func resuspendWithRecord(input *types.Input, record approvalRecord) *types.Output {
	return &types.Output{
		Resuspend: true,
		State:     record.state(),
		Data:      publicApprovalData(input, record, nil),
	}
}

// decide closes the gate on one of its decision ports.
func decide(input *types.Input, record approvalRecord, port string, extra map[string]any) *types.Output {
	return &types.Output{
		State: record.state(),
		Data:  publicApprovalData(input, record, extra),
		Port:  port,
	}
}

// publicApprovalData is what downstream sees: the data that arrived at the gate,
// with the record republished. The record is rebuilt from the node's own state
// on every write, so a value an upstream node published under one of these names
// is replaced rather than passed on.
func publicApprovalData(input *types.Input, record approvalRecord, extra map[string]any) map[string]any {
	return approvalOutput(input.Data, record.state(), extra)
}

// ignoreApprovalSignal re-suspends the node with its outcome unchanged,
// recording why the signal was not counted. The record is returned so the trail
// is persisted; the wait specification is recomputed from the same ledger either
// way.
func ignoreApprovalSignal(input *types.Input, signal *types.SignalPayload, reason string) (*types.Output, error) {
	record := recordOf(input)
	record.ignored = appendIgnored(record.ignored, signal.Name, reason)
	record.ignoredN++
	return resuspendWithRecord(input, record), nil
}

func appendIgnored(trail []map[string]any, signalName, reason string) []map[string]any {
	trail = append(trail, map[string]any{
		"signal": truncateForLedger(signalName, ignoredNameLimit),
		"reason": reason,
	})
	if len(trail) > ignoredTrailLimit {
		trail = trail[len(trail)-ignoredTrailLimit:]
	}
	return trail
}

func getIgnored(state map[string]any) []map[string]any {
	if state == nil {
		return nil
	}
	return parseDecisionList(state[ignoredKey])
}

func getIgnoredCount(state map[string]any) int {
	if state == nil {
		return 0
	}
	switch n := state[ignoredCountKey].(type) {
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

// currentSequentialApprover is the first member of the chain who has not decided
// yet. The chain and the ledger are both read from the node's own state rather
// than from a stored cursor, so the two cannot disagree.
func currentSequentialApprover(chain []string, record approvalRecord) string {
	for _, approver := range chain {
		if !hasApproverDecision(record.decisions, approver) {
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

// signalAssignee reads the subject an amend-the-chain action hands the decision
// to. It is an identity, so it is checked the same way as any approver's: it
// travels in the payload only as a name, and the person it names still has to
// authenticate as themselves before their decision counts.
func signalAssignee(data map[string]any) (string, bool) {
	raw, ok := data["assignee"]
	if !ok {
		return "", false
	}
	assignee, ok := raw.(string)
	if !ok || assignee == "" {
		return "", false
	}
	return assignee, true
}

// signalPosition reads where an added signer goes relative to the approver who
// added them. It reports false for anything but the two known positions rather
// than defaulting: a misspelled position that silently meant "after" would put
// the signer somewhere the request did not ask for.
func signalPosition(data map[string]any) (string, bool) {
	raw, ok := data["position"]
	if !ok {
		return positionAfter, true
	}
	position, ok := raw.(string)
	if !ok || (position != positionBefore && position != positionAfter) {
		return "", false
	}
	return position, true
}

// approvedChain is the chain the gate is actually waiting on: the configured
// approvers, as amended by the delegations and added signers on the ledger.
//
// It is recomputed from the ledger rather than stored beside it, so the chain
// and the record of who decided it cannot drift apart. Storing the amended chain
// as its own field would also mean a second piece of state a resumption has to
// carry, and the two would have to be kept in step by hand.
func approvedChain(params *ApprovalParams, decisions []map[string]any) []string {
	chain := append([]string(nil), params.Approvers...)
	for _, decision := range decisions {
		actor, actorOK := decision["approver"].(string)
		assignee, assigneeOK := decision["assignee"].(string)
		if !actorOK || !assigneeOK {
			continue
		}
		switch decision["action"] {
		case actionDelegate:
			chain = replaceApprover(chain, actor, assignee)
		case actionAddSigner:
			position, _ := decision["position"].(string)
			chain = insertApprover(chain, actor, assignee, position)
		}
	}
	return chain
}

// allChainMembersDecided reports whether every slot in the chain has a decision
// on the ledger.
func allChainMembersDecided(chain []string, decisions []map[string]any) bool {
	for _, approver := range chain {
		if !hasApproverDecision(decisions, approver) {
			return false
		}
	}
	return true
}

func containsApprover(chain []string, approver string) bool {
	for _, candidate := range chain {
		if candidate == approver {
			return true
		}
	}
	return false
}

// replaceApprover hands the first slot held by from over to to, keeping its
// position. Precondition: from holds a slot in chain.
func replaceApprover(chain []string, from string, to string) []string {
	out := append([]string(nil), chain...)
	for i, candidate := range out {
		if candidate == from {
			out[i] = to
			break
		}
	}
	return out
}

// insertApprover puts assignee next to actor's slot. Precondition: actor holds a
// slot in chain, which is what the caller established when it accepted the
// signal as this actor's own.
func insertApprover(chain []string, actor string, assignee string, position string) []string {
	out := make([]string, 0, len(chain)+1)
	for _, candidate := range chain {
		if candidate == actor && position == positionBefore {
			out = append(out, assignee)
		}
		out = append(out, candidate)
		if candidate == actor && position != positionBefore {
			out = append(out, assignee)
		}
	}
	return out
}

// withoutApprover drops the first slot held by approver.
func withoutApprover(chain []string, approver string) []string {
	out := make([]string, 0, len(chain))
	dropped := false
	for _, candidate := range chain {
		if candidate == approver && !dropped {
			dropped = true
			continue
		}
		out = append(out, candidate)
	}
	return out
}

// appendDecision adds one entry to the ledger. extra carries the fields an
// amend-the-chain decision needs beyond the common shape (the assignee).
func appendDecision(decisions []map[string]any, approver string, action string, comment any, extra ...map[string]any) []map[string]any {
	return append(decisions, approvalOutput(map[string]any{
		"approver": approver,
		"action":   action,
		"comment":  comment,
		"at":       time.Now().UTC().Format(time.RFC3339),
	}, extra...))
}

// hasApproverDecision reports whether this approver's own slot has been
// resolved. The ledger is an event log rather than a list of votes, so not every
// entry in it closes a slot, which is what closesSlot decides.
func hasApproverDecision(decisions []map[string]any, approver string) bool {
	for _, decision := range decisions {
		if decision["approver"] == approver && closesSlot(decision["action"]) {
			return true
		}
	}
	return false
}

func closesSlot(action any) bool {
	switch action {
	case actionApprove, actionReject, actionReturn, actionDelegate:
		return true
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

// forceSignal is the name a force approval travels under. It is a fixed name
// rather than one derived from the caller, because the force list is a property
// of the workflow and not a slot in the chain: nothing about the wait depends on
// which of the configured approvers sends it.
//
// An approver literally named "force" therefore shares this name with the force
// action. That is not a conflict: the action in the payload decides which rule
// applies, and each rule checks its own list, so such an approver can still
// decide their own slot with an ordinary action, and can force only if the
// workflow also names them a force approver.
func forceSignal(nodeName string) string {
	return nodeName + "/approval/force"
}

// forceSignals names the force signal, or nothing when the gate has no
// configured force approvers.
func forceSignals(nodeName string, forceApprovers []string) []string {
	if len(forceApprovers) == 0 {
		return nil
	}
	return []string{forceSignal(nodeName)}
}

func parseApprovalParams(params map[string]any) (*ApprovalParams, error) {
	p := &ApprovalParams{
		Mode:          ApprovalAny,
		TimeoutAction: "route",
	}

	if approvers, err := parseStringList(params["approvers"]); err != nil {
		return nil, fmt.Errorf("approvers must be a list of strings")
	} else {
		p.Approvers = approvers
	}
	if len(p.Approvers) == 0 {
		return nil, fmt.Errorf("approvers list must not be empty")
	}

	// A gate with no force approvers is the default, not an error: it is a gate
	// nobody may force.
	if forceApprovers, err := parseStringList(params["force_approvers"]); err != nil {
		return nil, fmt.Errorf("force_approvers must be a list of strings")
	} else {
		p.ForceApprovers = forceApprovers
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

// parseStringList reads a param that may be absent, a []string as a Go caller
// writes it, or a []any of strings as a decoded YAML or JSON document gives it.
// An absent param is an empty list, not an error: the caller decides whether
// empty is allowed, which for approvers it is not and for force_approvers it is.
func parseStringList(v any) ([]string, error) {
	switch list := v.(type) {
	case nil:
		return nil, nil
	case []string:
		return list, nil
	case []any:
		out := make([]string, 0, len(list))
		for _, item := range list {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element is %T, want a string", item)
			}
			out = append(out, s)
		}
		return out, nil
	}
	return nil, fmt.Errorf("value is %T, want a list of strings", v)
}

// getDecisions reads the ledger out of the node's private state. A nil state --
// a first resumption -- reads as an empty ledger.
func getDecisions(state map[string]any) []map[string]any {
	if state == nil {
		return nil
	}
	return parseDecisionList(state[decisionsKey])
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
