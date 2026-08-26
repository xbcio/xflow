package group_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// getApproverIndex and parseDecisionList (approval.go) both accept two
// input shapes: the native Go type produced in-process (int, []map[string]any)
// and the shape a JSON round trip through persisted state produces (float64,
// []any of map[string]any). Every existing sequential test only ever
// constructs Data with the native Go shape by hand, so the JSON-shaped
// branches are unexercised.

func TestApprovalSequential_PrepareSuspendHandlesFloat64ApproverIndex(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalSequential)
	spec, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "sequential",
		},
		// A previous round trip through persisted state decodes the saved
		// index as float64, not int.
		Data: map[string]any{"_approver_idx": float64(1)},
	})
	if err != nil {
		t.Fatalf("PrepareSuspend() error = %v", err)
	}
	wantSignals := []string{"SecurityApproval/approval/bob"}
	if !reflect.DeepEqual(spec.Signals, wantSignals) {
		t.Fatalf("Signals = %#v, want %#v: a float64 _approver_idx of 1 must resolve "+
			"to the second approver, not silently fall back to the first", spec.Signals, wantSignals)
	}
}

func TestApprovalSequential_OnResumeCarriesJSONShapedDecisionHistory(t *testing.T) {
	sh := approvalHandler(t)
	// Decision history as it would come back from a JSON-encoded store:
	// []any of map[string]any, not the native []map[string]any that
	// appendDecision produces in-process.
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_approver_idx": 1,
		"decisions": []any{
			map[string]any{"approver": "alice", "action": "approve", "comment": "ok"},
		},
	})
	out, err := sh.OnResume(context.Background(), input, approvalSignal("bob", "approve", "ship"))
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
		t.Fatalf("len(decisions) = %d, want 2: alice's earlier decision, stored in "+
			"the []any shape a JSON round trip produces, must not be dropped", len(decisions))
	}
	if decisions[0]["approver"] != "alice" || decisions[1]["approver"] != "bob" {
		t.Fatalf("decisions = %#v, want approver order alice,bob", decisions)
	}
}
