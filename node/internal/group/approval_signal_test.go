package group_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// validateApprovalApprover (approval.go) is reached on every OnResume call
// through approvalSignal(), but that helper always supplies a well-formed
// string "approver" field. The two branches below — the field missing
// entirely, and the field present but not a string — are otherwise never
// exercised on the single-signal OnResume path.

func TestApproval_OnResumeRejectsSignalMissingApproverField(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAny, nil)
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval",
		Data:      map[string]any{"action": "approve"},
	}
	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error when the signal payload has no \"approver\" field")
	}
	if !strings.Contains(err.Error(), "missing \"approver\" field") {
		t.Fatalf("error = %q, want substring about a missing approver field", err.Error())
	}
}

func TestApproval_OnResumeRejectsNonStringApproverField(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAny, nil)
	signal := &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval",
		Data:      map[string]any{"action": "approve", "approver": 42},
	}
	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error when the approver field is not a string")
	}
	if !strings.Contains(err.Error(), "not a string") {
		t.Fatalf("error = %q, want substring about the approver field not being a string", err.Error())
	}
}

// Every existing OnResume test drives one of "approve", "reject", "return",
// or a timeout signal. Nothing pins the terminal `default` error for an
// action the node does not recognize on the single-signal path (as opposed
// to the ApprovalAll multi-signal path, which validates it separately).
func TestApproval_OnResumeRejectsUnknownAction(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		approvalSignal("alice", "cancel", ""))
	if err == nil {
		t.Fatalf("expected error for unrecognized action %q, got output %#v", "cancel", out)
	}
	if !strings.Contains(err.Error(), "unknown approval action") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "unknown approval action")
	}
}
