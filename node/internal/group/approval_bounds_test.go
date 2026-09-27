package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

// Sequential mode's current approver is derived from the ledger rather than
// from a stored cursor, so the two cannot drift apart. The boundary that needs
// pinning is the end of the chain, where there is no current approver left:
// the derivation returns the empty string there, and nothing may treat that
// sentinel as a person — an approver named "" is not in the list, so a signal
// claiming it must not be counted.

func TestApprovalSequential_CurrentApproverFollowsTheLedger(t *testing.T) {
	sh := approvalHandler(t)
	ctx := context.Background()

	afterAlice, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalSequential, nil),
		approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("alice's OnResume() error = %v", err)
	}

	// alice's turn has passed. Repeating her decision is not a second vote.
	repeat, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalSequential, afterAlice.Data),
		approvalSignal("alice", "approve", "ok again"))
	if err != nil {
		t.Fatalf("repeat OnResume() error = %v", err)
	}
	if !repeat.Resuspend {
		t.Fatalf("alice's repeated approval completed the gate (port %q)", repeat.Port)
	}
	if decisions := decisionsOf(t, repeat.Data, "_decisions"); len(decisions) != 1 {
		t.Fatalf("len(decisions) = %d, want 1: alice was counted twice", len(decisions))
	}

	// bob is now the current approver, and his decision completes the chain.
	final, err := sh.OnResume(ctx,
		approvalInput(node.ApprovalSequential, repeat.Data),
		approvalSignal("bob", "approve", "ship it"))
	if err != nil {
		t.Fatalf("bob's OnResume() error = %v", err)
	}
	if final.Port != "approved" {
		t.Fatalf("port = %q, want approved after the last approver in the chain "+
			"decided", final.Port)
	}
}

func TestApprovalSequential_IgnoresASignalWhenTheWholeChainHasDecided(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_decisions": []map[string]any{
			{"approver": "alice", "action": "approve"},
			{"approver": "bob", "action": "approve"},
		},
	})

	// Every approver in the chain has decided, so there is no current approver
	// to match. The node must decline the signal rather than index the empty
	// sentinel or panic on a chain that has run out.
	out, err := sh.OnResume(context.Background(), input, approvalSignal("carol", "approve", "me too"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal from outside the finished chain resolved the gate (port %q)", out.Port)
	}
	if got := out.Data["approved"]; got != nil {
		t.Fatalf("approved = %v was set by an approver who is not in the chain", got)
	}
}

func TestApprovalSequential_IgnoresAnUnknownApproverBeforeAnyoneDecides(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, nil),
		approvalSignal("mallory", "approve", "first"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("an unknown approver resolved the gate (port %q)", out.Port)
	}
	assertIgnored(t, out.Data, 0, reasonUnauthorizedApprover)
}
