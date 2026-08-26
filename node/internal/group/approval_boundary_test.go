package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// TestApprovalAll_OnResumeLastSignatureCompletesTheNode is the other half of
// TestApprovalAll_OnResumeOneSignatureDoesNotCompleteTheNode.
//
// That test drives the first of two approvers through handleApprove's
// ApprovalAll arm and pins that the node resuspends. It only exercises the
// `len(decisions) < len(params.Approvers)` side of that branch, so widening the
// comparison to `<=` — which sends the completing round back to resuspend
// instead — leaves it green. Verified: with `<` changed to `<=`, all 14
// packages that import node/internal/group pass.
//
// The branch is not reachable through the multi-signal path the rest of the
// ApprovalAll tests use. handleAllSignals consumes a types.ModeMultiSignal
// payload and has its own completion logic; this branch belongs to the shared
// single-signal fallback PrepareSuspend switches to once a partial decision
// exists (the behavior TestApprovalAll_PrepareSuspendKeepsLegacySharedSignalAfterPartialDecision
// pins the suspend spec of, but never resumes). Signatures arriving one at a
// time is the ordinary case for that fallback, so the failure mode is not
// exotic: the final approver signs, the node resuspends anyway, and the
// approval waits forever on a signal every approver has already sent.
//
// The second round is fed the first round's own output rather than a
// hand-built decision list, so the two rounds are pinned as composing — the
// keys production writes are the keys production then reads back.
func TestApprovalAll_OnResumeLastSignatureCompletesTheNode(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()

	first, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, nil),
		approvalSignal("alice", "approve", "one of two"))
	if err != nil {
		t.Fatalf("first OnResume() error = %v", err)
	}
	if !first.Resuspend {
		t.Fatalf("setup: the first of two approvers already completed the node")
	}

	second, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, first.Data),
		approvalSignal("bob", "approve", "two of two"))
	if err != nil {
		t.Fatalf("second OnResume() error = %v", err)
	}

	if second.Resuspend {
		t.Fatalf("the last outstanding approver signed and the node resuspended anyway; "+
			"it now waits on a signal that every approver has already sent, so the "+
			"approval never completes (data %v)", second.Data)
	}
	if second.Port != "approved" {
		t.Fatalf("port = %q, want approved: every approver signed off", second.Port)
	}
	if second.Data["approved"] != true {
		t.Fatalf("approved = %v, want true", second.Data["approved"])
	}
	// Both signatures must survive into the completing round's payload: the
	// decision list is what an auditor reads to see who actually signed, and a
	// completion carrying only the last signature is indistinguishable from a
	// single approver having decided for everyone.
	assertDecision(t, second.Data, "decisions", 0, "alice", "approve", "one of two")
	assertDecision(t, second.Data, "decisions", 1, "bob", "approve", "two of two")
}

// TestApproval_PrepareSuspendRejectsUnknownMode pins the fall-through error at
// the bottom of PrepareSuspend's switch.
//
// ApprovalMode is an exported string type, so node.Approval(approvers,
// node.ApprovalMode("aproved")) compiles and passes review as readily as the
// correct spelling. No test supplies anything but "any"/"all"/"sequential", so
// replacing the error with `return nil, nil` leaves all 14 importing packages
// green.
//
// What that costs is not a nil dereference. Measured end to end on a local
// engine, with a workflow whose single approval node has a typo'd mode:
//
//	with the error:    status="failed"  error="unknown approval mode: aproved"
//	returning nil,nil: status="success" output={Ap:{} Start:{x:1}}
//
// The runner turns (nil, nil) into engine.TaskResult{Suspend: nil}, which
// commit.go:85 routes down the ordinary success path. So a single typo'd
// character converts an approval gate into a no-op that reports success: no
// approver is ever asked, nothing suspends, and everything downstream of the
// gate runs. A gate that fails closed is an incident; a gate that silently
// opens is the thing the gate existed to prevent.
func TestApproval_PrepareSuspendRejectsUnknownMode(t *testing.T) {
	sh := approvalHandler(t)

	spec, err := sh.PrepareSuspend(context.Background(), approvalInput(node.ApprovalMode("aproved"), nil))
	if err == nil {
		t.Fatalf("PrepareSuspend accepted an unrecognized mode and returned spec %v; a "+
			"nil spec with a nil error commits as an ordinary success, so the approval "+
			"gate is skipped and the workflow proceeds with nobody having approved", spec)
	}
	// A nil spec alongside the error matters on its own: the caller reads spec
	// before it reads err on the non-error path, and a non-nil spec here would
	// be a suspend on signals nobody derived.
	if spec != nil {
		t.Fatalf("PrepareSuspend returned both an error and spec %v", spec)
	}
	var _ *types.SuspendSpec = spec
}
