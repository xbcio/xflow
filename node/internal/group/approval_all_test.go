package group_test

import (
	"context"
	"strings"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// handleAllSignals (approval.go) is the ApprovalAll multi-signal path.
// approval_test.go and approval_decision_test.go both drive it, but only ever
// with a fully-formed, correctly-keyed signal.All map where every payload's
// own "approver" field matches the signal name it arrived under and every
// action is "approve" or "reject". The three malformed-input branches below
// are never reached by any existing test.

func TestApprovalAll_OnResumeRejectsMissingApproverPayload(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	_, err := sh.OnResume(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
	}, &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "SecurityApproval/approval/alice",
		Data:      map[string]any{"approver": "alice", "action": "approve"},
		All: map[string]map[string]any{
			"SecurityApproval/approval/alice": {"approver": "alice", "action": "approve"},
			// bob's payload is missing entirely even though bob is a
			// required approver.
		},
	})
	if err == nil {
		t.Fatal("expected error when a required approver's payload is absent from signal.All")
	}
	if !strings.Contains(err.Error(), "missing payload") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "missing payload")
	}
}

func TestApprovalAll_OnResumeRejectsMismatchedApproverIdentity(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	_, err := sh.OnResume(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
	}, &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "SecurityApproval/approval/alice",
		Data:      map[string]any{"approver": "alice", "action": "approve"},
		All: map[string]map[string]any{
			// The payloads are keyed by the right signal names, but each
			// payload's own "approver" field names the other person.
			"SecurityApproval/approval/alice": {"approver": "bob", "action": "approve"},
			"SecurityApproval/approval/bob":   {"approver": "alice", "action": "approve"},
		},
	})
	if err == nil {
		t.Fatal("expected error when a payload's own approver identity does not match the signal slot it was found under")
	}
	if !strings.Contains(err.Error(), "expected approver") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "expected approver")
	}
}

func TestApprovalAll_OnResumeRejectsUnknownActionInMultiSignalPath(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalAll)
	_, err := sh.OnResume(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "all",
		},
	}, &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "SecurityApproval/approval/alice",
		Data:      map[string]any{"approver": "alice", "action": "approve"},
		All: map[string]map[string]any{
			"SecurityApproval/approval/alice": {"approver": "alice", "action": "cancel"},
			"SecurityApproval/approval/bob":   {"approver": "bob", "action": "approve"},
		},
	})
	if err == nil {
		t.Fatal("expected error for an unrecognized action inside the multi-signal payload")
	}
	if !strings.Contains(err.Error(), "unknown approval action") {
		t.Fatalf("error = %q, want substring %q", err.Error(), "unknown approval action")
	}
}
