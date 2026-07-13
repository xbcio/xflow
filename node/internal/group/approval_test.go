package group_test

import (
	"context"
	"github.com/xbcio/xflow/internal/noderuntime"
	"github.com/xbcio/xflow/types"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
)

func TestApproval_OnResumeRejectsUnauthorizedApprover(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAny, nil)
	signal := approvalSignal("mallory", "approve", "")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for unauthorized approver")
	}
}

func TestApprovalAll_OnResumeRejectsDuplicateApprover(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"_decisions": []map[string]any{
			{"approver": "alice", "action": "approve"},
		},
	})
	signal := approvalSignal("alice", "approve", "")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for duplicate approver")
	}
}

func TestApprovalAll_OnResumeRejectsDuplicateRejectApprover(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"decisions": []map[string]any{
			{"approver": "alice", "action": "approve"},
		},
	})
	signal := approvalSignal("alice", "reject", "changed mind")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for duplicate reject approver")
	}
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
	if spec.Quorum != 0 {
		t.Fatalf("Quorum = %d, want 0 for all approvers", spec.Quorum)
	}
}

func TestApprovalAll_PrepareSuspendKeepsLegacySharedSignalAfterPartialDecision(t *testing.T) {
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
	if spec.Mode != types.ModeSignal {
		t.Fatalf("Mode = %v, want types.ModeSignal", spec.Mode)
	}
	wantSignals := []string{"SecurityApproval/approval"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
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

func TestApprovalSequential_PrepareSuspendKeepsCurrentApproverSignal(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalSequential)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "sequential",
		},
		Data: map[string]any{"_approver_idx": 1},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	if spec.Mode != types.ModeSignal {
		t.Fatalf("Mode = %v, want types.ModeSignal", spec.Mode)
	}
	wantSignals := []string{"SecurityApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v", spec.Signals, wantSignals)
	}
}

func TestApprovalAll_OnResumeConsumesMultiSignalPayloadInApproverOrder(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	out, err := sh.OnResume(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
	}, &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "SecurityApproval/approval/bob",
		Data:      map[string]any{"approver": "bob", "action": "approve", "comment": "ok-bob"},
		All: map[string]map[string]any{
			"SecurityApproval/approval/bob":   {"approver": "bob", "action": "approve", "comment": "ok-bob"},
			"SecurityApproval/approval/alice": {"approver": "alice", "action": "approve", "comment": "ok-alice"},
		},
	})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("Port = %q, want approved", out.Port)
	}
	decisions, ok := out.Data["decisions"].([]map[string]any)
	if !ok {
		t.Fatalf("decisions type = %T, want []map[string]any", out.Data["decisions"])
	}
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want 2", len(decisions))
	}
	if decisions[0]["approver"] != "alice" || decisions[1]["approver"] != "bob" {
		t.Fatalf("decisions = %#v, want approver order alice,bob", decisions)
	}
}

func TestApprovalAll_OnResumeRoutesMultiSignalReject(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob", "carol"}, node.ApprovalAll)
	out, err := sh.OnResume(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob", "carol"},
			"mode":      "all",
		},
	}, &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "SecurityApproval/approval/bob",
		Data:      map[string]any{"approver": "bob", "action": "reject", "comment": "needs reassessment"},
		All: map[string]map[string]any{
			"SecurityApproval/approval/alice": {"approver": "alice", "action": "approve", "comment": "ok-alice"},
			"SecurityApproval/approval/bob":   {"approver": "bob", "action": "reject", "comment": "needs reassessment"},
			"SecurityApproval/approval/carol": {"approver": "carol", "action": "approve", "comment": "ok-carol"},
		},
	})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "rejected" {
		t.Fatalf("Port = %q, want rejected", out.Port)
	}
	if out.Data["approved"] != false {
		t.Fatalf("approved = %v, want false", out.Data["approved"])
	}
	if out.Data["approver"] != "bob" {
		t.Fatalf("approver = %v, want bob", out.Data["approver"])
	}
	decisions, ok := out.Data["decisions"].([]map[string]any)
	if !ok {
		t.Fatalf("decisions type = %T, want []map[string]any", out.Data["decisions"])
	}
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want only through rejecting approver", len(decisions))
	}
	if decisions[0]["approver"] != "alice" || decisions[1]["approver"] != "bob" {
		t.Fatalf("decisions = %#v, want alice then bob only", decisions)
	}
}

func TestApprovalSequential_OnResumeRejectsNonCurrentApprover(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_approver_idx": 1,
		"decisions": []map[string]any{
			{"approver": "alice", "action": "approve"},
		},
	})
	signal := approvalSignal("alice", "approve", "")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for non-current approver")
	}
}

func TestApprovalSequential_OnResumeRejectsNonCurrentReturnApprover(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_approver_idx": 1,
		"decisions": []map[string]any{
			{"approver": "alice", "action": "approve"},
		},
	})
	signal := approvalSignal("alice", "return", "needs changes")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for non-current return approver")
	}
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
	if first.Data["_approver_idx"] != 1 {
		t.Fatalf("expected next approver index 1, got %v", first.Data["_approver_idx"])
	}
	assertDecision(t, first.Data, "decisions", 0, "alice", "approve", "ok")

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
	h, ok := noderuntime.Lookup(node.ApprovalNodeType)
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

func approvalSignal(approver string, action string, comment string) *types.SignalPayload {
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval",
		Data: map[string]any{
			"approver": approver,
			"action":   action,
			"comment":  comment,
		},
	}
}

func assertDecision(t *testing.T, data map[string]any, key string, idx int, approver string, action string, comment string) {
	t.Helper()
	decisions, ok := data[key].([]map[string]any)
	if !ok {
		t.Fatalf("expected %q decisions, got %T: %v", key, data[key], data[key])
	}
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
