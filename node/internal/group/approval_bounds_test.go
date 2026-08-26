package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// Both PrepareSuspend and validateCurrentSequentialApprover guard sequential
// mode with `idx >= len(params.Approvers)` before indexing
// params.Approvers[idx]. Every existing sequential test only ever supplies an
// index strictly inside range (0 or 1, for a 2-approver list), so the exact
// boundary where idx == len(Approvers) is never exercised. If either `>=` is
// weakened to `>`, idx == len(Approvers) slips past the guard and the very
// next line indexes one past the end of the slice: a real, reachable
// out-of-range panic, not merely a missing error.

func TestApprovalSequential_PrepareSuspendRejectsApproverIndexAtApproversLength(t *testing.T) {
	sh := node.Approval([]string{"alice", "bob"}, node.ApprovalSequential)
	_, err := sh.PrepareSuspend(context.Background(), &types.Input{
		NodeName: "SecurityApproval",
		Params: map[string]any{
			"approvers": []any{"alice", "bob"},
			"mode":      "sequential",
		},
		// _approver_idx == len(approvers): every approver has already signed
		// off, so there is no next approver to address a signal to.
		Data: map[string]any{"_approver_idx": 2},
	})
	if err == nil {
		t.Fatal("expected error for approver index == len(approvers); if the " +
			"guard is weakened from >= to >, this index is used to read " +
			"params.Approvers[idx] one element past the end of the slice and " +
			"panics instead of returning a clean error")
	}
}

func TestApprovalSequential_OnResumeRejectsApproverIndexAtApproversLength(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{"_approver_idx": 2})
	// approvalInput() configures exactly two approvers (alice, bob), so index
	// 2 is one past the end — the same boundary as above, but reached through
	// OnResume's validateCurrentSequentialApprover instead of PrepareSuspend.
	signal := approvalSignal("alice", "approve", "")

	_, err := sh.OnResume(context.Background(), input, signal)
	if err == nil {
		t.Fatal("expected error for approver index == len(approvers) in " +
			"validateCurrentSequentialApprover; weakening >= to > lets " +
			"currentIdx == len(params.Approvers) through to " +
			"params.Approvers[currentIdx], which panics instead of returning a " +
			"clean error")
	}
}
