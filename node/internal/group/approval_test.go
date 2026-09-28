package group_test

import (
	"context"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
)

// Reason codes the node records for a signal it did not count. They are spelled
// out here rather than shared with the node's own constants: this is an
// external test package, and renaming a code should break these tests loudly
// instead of passing against a stale expectation.
const (
	reasonUnverifiedActor      = "unverified-actor"
	reasonUnauthorizedApprover = "unauthorized-approver"
	reasonSignalNameMismatch   = "signal-name-mismatch"
	reasonNotCurrentApprover   = "not-current-approver"
	reasonMalformedAction      = "malformed-action"
	reasonUnknownAction        = "unknown-action"
	reasonMalformedAssignee    = "malformed-assignee"
)

func TestApproval_IgnoresAnApproverOutsideTheApproverList(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		sharedApprovalSignal("mallory", "approve", ""))
	if err != nil {
		t.Fatalf("OnResume() error = %v: an approver the node does not know is a "+
			"signal it declines to act on, not a reason to fail the execution", err)
	}
	if !out.Resuspend {
		t.Fatalf("an unauthorized approver resolved the gate (port %q); anyone "+
			"able to deliver a signal could then decide the approval", out.Port)
	}
	if _, ok := out.Data["approved"]; ok {
		t.Fatalf("approved = %v was recorded for an unauthorized approver", out.Data["approved"])
	}
	assertIgnored(t, out.Data, 0, reasonUnauthorizedApprover)
}

func TestApproval_IgnoresASignalWithNoActor(t *testing.T) {
	// No verified subject means the request cannot be attributed to anybody,
	// whatever the payload says about who is asking.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      "approval_1/approval",
			Data:      map[string]any{"approver": "alice", "action": "approve"},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal naming no verified actor resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonUnverifiedActor)
}

func TestApproval_IgnoresANonStringActor(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      "approval_1/approval",
			Data:      map[string]any{"action": "approve", types.VerifiedActorKey: 42},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal whose actor is not a string resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonUnverifiedActor)
}

// TestApproval_DecidesOnTheVerifiedActorNotTheClaimedApprover is the
// impersonation case the actor key exists to close: the payload claims to be
// bob, the verified subject is alice, and alice is who gets counted.
func TestApproval_DecidesOnTheVerifiedActorNotTheClaimedApprover(t *testing.T) {
	sh := approvalHandler(t)
	signal := sharedApprovalSignal("alice", "approve", "claiming to be bob")
	signal.Data["approver"] = "bob"

	out, err := sh.OnResume(context.Background(), approvalInput(node.ApprovalAny, nil), signal)
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("Port = %q, want approved: the verified actor is a registered "+
			"approver, so the decision counts", out.Port)
	}
	if got := out.Data["approver"]; got != "alice" {
		t.Fatalf("approver = %v, want alice; the payload's claim to be bob was "+
			"recorded as fact", got)
	}
}

func TestApproval_IgnoresAnUnknownAction(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		sharedApprovalSignal("alice", "cancel", ""))
	if err != nil {
		t.Fatalf("OnResume() error = %v: an unrecognized action must not fail the "+
			"execution, or a single typo'd payload would kill an in-flight approval", err)
	}
	if !out.Resuspend {
		t.Fatalf("an unrecognized action resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonUnknownAction)
}

func TestApproval_IgnoresASignalWithNoAction(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      "approval_1/approval",
			Data:      map[string]any{types.VerifiedActorKey: "alice"},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal carrying no action resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonMalformedAction)
}

func TestApproval_IgnoresANonStringAction(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      "approval_1/approval",
			Data:      map[string]any{types.VerifiedActorKey: "alice", "action": 7},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal whose action is not a string resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonMalformedAction)
}

func TestApproval_IgnoresAnApproverRepeatingTheirDecision(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"_decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
	})
	out, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "approve", ""))
	if err != nil {
		t.Fatalf("OnResume() error = %v: a retried delivery must be idempotent", err)
	}
	if !out.Resuspend {
		t.Fatalf("a repeated approval completed the node (port %q)", out.Port)
	}
	// No data is returned, which is how a resumption says "state unchanged":
	// the runner replays the previously stored output, so the ledger keeps its
	// single entry rather than being rewritten with a second one.
	if out.Data != nil {
		t.Fatalf("a repeated decision rewrote the node's output (%v); the ledger "+
			"must keep exactly what the first decision recorded", out.Data)
	}
}

func TestApproval_IgnoresAnApproverContradictingTheirDecision(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"_decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
	})
	out, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "reject", "changed mind"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a contradicting repeat resolved the node (port %q); the ledger "+
			"already records what alice decided", out.Port)
	}
	// The ledger is left exactly as it was, so the recorded decision is still
	// alice's original approval. A reject that overwrote it would let an
	// approver reverse a decision already relied upon by the nodes after them.
	if out.Data != nil {
		t.Fatalf("a contradicting repeat rewrote the node's output (%v)", out.Data)
	}
	if got := decisionsOf(t, input.Data, "_decisions"); len(got) != 1 || got[0]["action"] != "approve" {
		t.Fatalf("the input ledger was mutated in place: %#v", got)
	}
}

func TestApproval_IgnoresASignalDeliveredOnAnotherApproversName(t *testing.T) {
	sh := approvalHandler(t)
	// The verified actor is bob, but the signal arrived on alice's name. The name
	// is the slot the backend matched, so delivering on someone else's slot is a
	// mismatch rather than a vote for either: without this, a verified alice
	// could answer on bob's behalf, which is the same impersonation reached
	// through the signal name instead of the payload.
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval/alice",
		Data:      map[string]any{types.VerifiedActorKey: "bob", "action": "approve"},
	}
	out, err := sh.OnResume(context.Background(), approvalInput(node.ApprovalAll, nil), signal)
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal delivered on another approver's name resolved the node (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonSignalNameMismatch)
}

func TestApprovalAll_PrepareSuspendUsesPerApproverMultiSignal(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Mode != types.ModeMultiSignal {
		t.Fatalf("Mode = %v, want types.ModeMultiSignal", spec.Mode)
	}
	wantSignals := []string{"SecurityApproval/approval/alice", "SecurityApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
	}
	// A quorum of one is the whole point: the node resumes on the first
	// decision, so a rejection is not held open waiting for the rest.
	if spec.Quorum != 1 {
		t.Fatalf("Quorum = %d, want 1: with the default (all approvers) a reject "+
			"waits for every approver who has not answered yet", spec.Quorum)
	}
}

func TestApprovalAll_PrepareSuspendKeepsArmingEveryApproverSignal(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
		Data: map[string]any{
			"_decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	wantSignals := []string{"SecurityApproval/approval/alice", "SecurityApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v: the remaining wait must still list every "+
			"approver, including the one who already decided, so a retry is "+
			"recognized as a repeat rather than parked forever", spec.Signals, wantSignals)
	}
	if spec.Quorum != 1 {
		t.Fatalf("Quorum = %d, want 1", spec.Quorum)
	}
}

func TestApprovalAll_PrepareSuspendIgnoresUpstreamDecisionHistory(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "ChangeApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
		Data: map[string]any{
			// A field named "decisions" published by some upstream node. It is
			// not this node's ledger, and reading it as one would let whatever
			// ran before the gate approve on an approver's behalf.
			"decisions": []map[string]any{{"approver": "sec-owner", "action": "approve"}},
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Mode != types.ModeMultiSignal {
		t.Fatalf("Mode = %v, want types.ModeMultiSignal", spec.Mode)
	}
	wantSignals := []string{"ChangeApproval/approval/alice", "ChangeApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
	}
}

func TestApproval_IgnoresAnUpstreamFieldNamedLikeTheLedger(t *testing.T) {
	// The same collision, reached through a decision instead of the suspend
	// spec: an upstream "decisions" field must not count as a vote.
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
	})
	out, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("alice's approval completed a two-approver gate (port %q); an "+
			"upstream field named \"decisions\" was read as this node's ledger", out.Port)
	}
}

func TestApprovalAny_PrepareSuspendKeepsSharedSignal(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAny)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "any",
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Mode != types.ModeSignal {
		t.Fatalf("Mode = %v, want types.ModeSignal", spec.Mode)
	}
	wantSignals := []string{"SecurityApproval/approval"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
	}
}

func TestApprovalSequential_PrepareSuspendArmsEveryApproverSignal(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalSequential)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "sequential",
		},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Mode != types.ModeMultiSignal {
		t.Fatalf("Mode = %v, want types.ModeMultiSignal", spec.Mode)
	}
	// bob's signal is armed even though it is not bob's turn: if it were not, a
	// signal bob sent early would sit in the backend and be consumed when his
	// turn came, counting a vote cast before he could see alice's decision.
	wantSignals := []string{"SecurityApproval/approval/alice", "SecurityApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
	}
	if spec.Quorum != 1 {
		t.Fatalf("Quorum = %d, want 1", spec.Quorum)
	}
}

func TestApprovalAll_OnResumeRejectIsFinalWithoutTheRemainingApprovers(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob", "carol"}, node.ApprovalAll)
	// The node name has to agree with the signal name the helper builds
	// ("approval_1/approval/<approver>"): a signal is accepted only under the
	// name its own node armed, so a mismatch would be discarded before the
	// reject under test ever ran.
	input := &types.Input{
		NodeName: "approval_1",
		Params: map[string]any{
			"approvers": []any{"alice", "bob", "carol"},
			"mode":      "all",
		},
		Data: map[string]any{
			"_decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
		},
	}
	out, err := sh.OnResume(context.Background(), input, approvalSignal("bob", "reject", "needs reassessment"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "rejected" {
		t.Fatalf("Port = %q, want rejected: bob declined and carol had not answered "+
			"yet, so the gate must close now rather than wait on a vote that can no "+
			"longer change the outcome", out.Port)
	}
	if out.Data["approved"] != false {
		t.Fatalf("approved = %v, want false", out.Data["approved"])
	}
	if out.Data["approver"] != "bob" {
		t.Fatalf("approver = %v, want bob", out.Data["approver"])
	}
	decisions := decisionsOf(t, out.Data, "decisions")
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want 2 (alice's approval and bob's rejection)", len(decisions))
	}
	if decisions[0]["approver"] != "alice" || decisions[1]["approver"] != "bob" {
		t.Fatalf("decisions = %#v, want alice then bob", decisions)
	}
}

func TestApprovalSequential_IgnoresAnApproverWhoseTurnHasNotCome(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, nil)

	out, err := sh.OnResume(context.Background(), input, approvalSignal("bob", "approve", "skipping ahead"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("bob's approval resolved a sequential gate whose first approver "+
			"had not decided (port %q)", out.Port)
	}
	if _, ok := out.Data["approved"]; ok {
		t.Fatalf("approved = %v was recorded before the first approver decided", out.Data["approved"])
	}
	assertIgnored(t, out.Data, 0, reasonNotCurrentApprover)
}

func TestApprovalSequential_IgnoresAReturnFromAnApproverWhoseTurnHasNotCome(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, nil),
		approvalSignal("bob", "return", "needs changes"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a return from an out-of-turn approver stopped the wait (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonNotCurrentApprover)
}

func TestApprovalSequential_OnResumeCarriesDecisionsThroughCompletion(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, nil)

	first, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("unexpected first approval error: %v", err)
	}
	if !first.Resuspend {
		t.Fatal("expected first approval to resuspend")
	}
	assertDecision(t, first.Data, "_decisions", 0, "alice", "approve", "ok")

	nextInput := approvalInput(node.ApprovalSequential, first.Data)
	second, err := sh.OnResume(context.Background(), nextInput, approvalSignal("bob", "approve", "ship"))
	if err != nil {
		t.Fatalf("unexpected second approval error: %v", err)
	}
	if second.Resuspend {
		t.Fatal("expected second approval to complete")
	}
	if second.Port != "approved" {
		t.Fatalf("expected approved port, got %q", second.Port)
	}
	assertDecision(t, second.Data, "decisions", 0, "alice", "approve", "ok")
	assertDecision(t, second.Data, "decisions", 1, "bob", "approve", "ship")
}

func TestApproval_WithTimeoutAddsTimeoutParams(t *testing.T) {
	b := node.Approval([]string{"alice"}, node.ApprovalAny).WithTimeout("48h", "reject")
	params := b.RawParams().(map[string]any)

	if params["timeout"] != "48h" {
		t.Fatalf("timeout = %v, want 48h", params["timeout"])
	}
	if params["timeout_action"] != "reject" {
		t.Fatalf("timeout_action = %v, want reject", params["timeout_action"])
	}
}

func approvalHandler(t *testing.T) types.SuspendingHandler {
	t.Helper()
	h, ok := registry.Lookup(node.ApprovalNodeType)
	if !ok {
		t.Fatal("approval node is not registered")
	}
	sh, ok := h.(types.SuspendingHandler)
	if !ok {
		t.Fatal("approval node does not implement SuspendingHandler")
	}
	return sh
}

func approvalInput(mode node.ApprovalMode, data map[string]any) *types.Input {
	return &types.Input{
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      string(mode),
		},
		Data:     data,
		NodeName: "approval_1",
	}
}

// approvalSignal builds the signal for one approver in "all"/"sequential" mode,
// where each approver has a name of their own. The actor is the server-set key:
// identity travels there, never in a caller-supplied field.
func approvalSignal(approver string, action string, comment string) *types.SignalPayload {
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval/" + approver,
		Data: map[string]any{
			types.VerifiedActorKey: approver,
			"action":               action,
			"comment":              comment,
		},
	}
}

// sharedApprovalSignal builds the node's single signal, which is the one every
// approver answers on in "any" mode.
func sharedApprovalSignal(approver string, action string, comment string) *types.SignalPayload {
	signal := approvalSignal(approver, action, comment)
	signal.Name = "approval_1/approval"
	return signal
}

func assertDecision(t *testing.T, data map[string]any, key string, idx int, approver string, action string, comment string) {
	t.Helper()
	decisions := decisionsOf(t, data, key)
	if len(decisions) <= idx {
		t.Fatalf("expected decision index %d in %v", idx, decisions)
	}
	if decisions[idx]["approver"] != approver {
		t.Fatalf("expected approver %q at index %d, got %v", approver, idx, decisions[idx]["approver"])
	}
	if decisions[idx]["action"] != action {
		t.Fatalf("expected action %q at index %d, got %v", action, idx, decisions[idx]["action"])
	}
	if decisions[idx]["comment"] != comment {
		t.Fatalf("expected comment %q at index %d, got %v", comment, idx, decisions[idx]["comment"])
	}
}

func decisionsOf(t *testing.T, data map[string]any, key string) []map[string]any {
	t.Helper()
	decisions, ok := data[key].([]map[string]any)
	if !ok {
		t.Fatalf("expected %q decisions, got %T: %v", key, data[key], data[key])
	}
	return decisions
}

// assertIgnored pins that a declined signal left a trail entry, so a refusal is
// visible rather than indistinguishable from nothing having been delivered.
func assertIgnored(t *testing.T, data map[string]any, idx int, reason string) {
	t.Helper()
	trail, ok := data["_ignored"].([]map[string]any)
	if !ok {
		t.Fatalf("no ignored trail in %v; a declined signal left no record of why", data)
	}
	if len(trail) <= idx {
		t.Fatalf("ignored trail = %v, want an entry at %d", trail, idx)
	}
	if trail[idx]["reason"] != reason {
		t.Fatalf("ignored reason = %v, want %q", trail[idx]["reason"], reason)
	}
}
