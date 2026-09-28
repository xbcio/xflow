package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// Countersign ("all") mode is decided one signal at a time now, so what matters
// is how successive rounds compose: the ledger a round writes is the ledger the
// next round reads, and the gate opens exactly when the last outstanding
// approver decides — not before, and not later.

func TestApprovalAll_CompletesOnlyAfterEveryApproverHasDecided(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()

	first, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, nil),
		approvalSignal("alice", "approve", "one of two"))
	if err != nil {
		t.Fatalf("first OnResume() error = %v", err)
	}
	if !first.Resuspend {
		t.Fatalf("one of two approvers completed the gate (port %q); the second "+
			"approver is never asked", first.Port)
	}

	second, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, first.State),
		approvalSignal("bob", "approve", "two of two"))
	if err != nil {
		t.Fatalf("second OnResume() error = %v", err)
	}
	if second.Resuspend {
		t.Fatalf("the last outstanding approver signed and the node resuspended "+
			"anyway, so the approval can never complete (data %v)", second.Data)
	}
	if second.Port != "approved" {
		t.Fatalf("port = %q, want approved: every approver signed off", second.Port)
	}
	// Both signatures must survive into the completing round: a completion
	// carrying only the last one is indistinguishable from a single approver
	// having decided for everyone.
	assertDecision(t, second.Data, "decisions", 0, "alice", "approve", "one of two")
	assertDecision(t, second.Data, "decisions", 1, "bob", "approve", "two of two")
}

func TestApprovalAll_RejectOnTheLastSignatureStillRejects(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()

	first, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, nil),
		approvalSignal("alice", "approve", "one of two"))
	if err != nil {
		t.Fatalf("first OnResume() error = %v", err)
	}

	second, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, first.State),
		approvalSignal("bob", "reject", "not this time"))
	if err != nil {
		t.Fatalf("second OnResume() error = %v", err)
	}
	// The count of decisions now equals the number of approvers, so completion
	// and rejection are decided by the same round. A completion check that runs
	// before the rejection check would open the gate on a declined request.
	if second.Port != "rejected" {
		t.Fatalf("port = %q, want rejected: the last signature was a rejection and "+
			"the ledger is what an auditor reads", second.Port)
	}
	if second.Data["approved"] != false {
		t.Fatalf("approved = %v, want false", second.Data["approved"])
	}
	assertDecision(t, second.Data, "decisions", 1, "bob", "reject", "not this time")
}

func TestApprovalAll_KeepsWaitingAfterASignalItCannotCount(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()

	// A blocker and a stranger deliver first. Neither may move the gate, and
	// neither may stop it: the approvers who are entitled to decide still can.
	afterBlocker, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, nil),
		&types.SignalPayload{
			Triggered: types.SignalReceived,
			Name:      "approval_1/approval/alice",
			Data:      map[string]any{types.VerifiedActorKey: "alice"},
		})
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	afterStranger, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, afterBlocker.State),
		approvalSignal("mallory", "approve", "let me in"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !afterStranger.Resuspend {
		t.Fatalf("a stranger's approval moved the gate (port %q)", afterStranger.Port)
	}
	assertIgnored(t, afterStranger.State, 0, reasonMalformedAction)
	assertIgnored(t, afterStranger.State, 1, reasonUnauthorizedApprover)

	// Retrying alice's decision, properly formed this time, is still accepted.
	final, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalAll, afterStranger.State),
		approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	assertDecision(t, final.Data, "decisions", 0, "alice", "approve", "ok")
}
