package group_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// The gate's approver chain is not fixed: an approver may hand their own slot to
// somebody else (delegate) or call in an extra signer (add_signer). Both are
// amendments to who the gate is waiting on, so what these tests pin is that the
// amended chain is what the node actually waits for — a delegation that the wait
// spec ignored would let the gate open on the delegator's slot while the person
// they handed it to was still unreached.
//
// The amended chain is asserted through what the gate does — who it accepts, who
// it refuses, and when it opens — rather than by reading a stored copy of it,
// because the chain is derived from the ledger rather than carried beside it.

// maxChainInTest restates the node's chain limit. It is spelled out rather than
// shared so that changing the limit breaks this test loudly instead of the
// expectation silently following the implementation.
const maxChainInTest = 32

func delegateSignal(actor, assignee, comment string) *types.SignalPayload {
	signal := approvalSignal(actor, "delegate", comment)
	if assignee != "" {
		signal.Data["assignee"] = assignee
	}
	return signal
}

func addSignerSignal(actor, assignee, position, comment string) *types.SignalPayload {
	signal := approvalSignal(actor, "add_signer", comment)
	if assignee != "" {
		signal.Data["assignee"] = assignee
	}
	if position != "" {
		signal.Data["position"] = position
	}
	return signal
}

// ledgerOf reads the node's own decision record, treating an absent one as
// empty: a signal the node refuses stores no ledger at all, so a reader that
// insisted on the key would report "nothing was recorded" as a test failure
// rather than as the result being asserted.
func ledgerOf(t *testing.T, data map[string]any) []map[string]any {
	t.Helper()
	switch raw := data["_decisions"].(type) {
	case nil:
		return nil
	case []map[string]any:
		return raw
	default:
		t.Fatalf("_decisions = %#v (%T), want a decision list", raw, raw)
		return nil
	}
}

func TestApprovalDelegate_HandsTheSlotToSomebodyOutsideTheApproverList(t *testing.T) {
	// The delegatee need not be one of the configured approvers: a chain is a
	// list of people who can decide this request, not a role membership. What
	// has to hold is that the person the gate now waits on is the one who signs.
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, nil)

	delegated, err := sh.OnResume(context.Background(), input, delegateSignal("alice", "dave", "on leave"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !delegated.Resuspend {
		t.Fatalf("a delegation resolved the gate (port %q)", delegated.Port)
	}
	assertDecision(t, delegated.Data, "decisions", 0, "alice", "delegate", "on leave")
	if got := decisionsOf(t, delegated.Data, "decisions")[0]["assignee"]; got != "dave" {
		t.Fatalf("assignee = %v, want dave: the trail does not name who received the slot", got)
	}

	// dave signs on his own slot name.
	byDave, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, delegated.Data),
		approvalSignal("dave", "approve", "taking this one"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !byDave.Resuspend {
		t.Fatalf("dave's approval completed the gate (port %q) with bob still "+
			"undecided", byDave.Port)
	}
	assertDecision(t, byDave.Data, "decisions", 1, "dave", "approve", "taking this one")

	// alice is no longer a slot holder, so her own token no longer decides it.
	byAlice, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, byDave.Data),
		approvalSignal("alice", "approve", "changed my mind"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !byAlice.Resuspend {
		t.Fatalf("the delegator's approval still counted (port %q); the slot was "+
			"handed away and only the person who received it may decide it", byAlice.Port)
	}
	assertIgnored(t, byAlice.Data, 0, reasonUnauthorizedApprover)

	done, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, byAlice.Data),
		approvalSignal("bob", "approve", "fine by me"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if done.Port != "approved" {
		t.Fatalf("Port = %q, want approved once every slot holder had decided", done.Port)
	}
}

func TestApprovalDelegate_RequiresAnUndecidedSlot(t *testing.T) {
	// Whoever has already decided cannot re-delegate: the ledger is the record
	// of what they decided, and amending the chain afterwards would move a slot
	// that a decision on the record already refers to.
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalAll, map[string]any{
		"_decisions": []map[string]any{{"approver": "alice", "action": "approve"}},
	})

	out, err := sh.OnResume(context.Background(), input, delegateSignal("alice", "dave", "changed my mind"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("an approver who had already decided delegated afterwards (port %q)", out.Port)
	}
	if out.Data != nil {
		t.Fatalf("a repeated delivery rewrote the node's output (%v)", out.Data)
	}
}

func TestApprovalDelegate_IgnoresAnUnusableAssignee(t *testing.T) {
	tests := map[string]func() *types.SignalPayload{
		"no assignee":      func() *types.SignalPayload { return delegateSignal("alice", "", "for dave really") },
		"already in chain": func() *types.SignalPayload { return delegateSignal("alice", "bob", "to bob") },
	}

	for name, build := range tests {
		t.Run(name, func(t *testing.T) {
			sh := approvalHandler(t)
			out, err := sh.OnResume(context.Background(), approvalInput(node.ApprovalAll, nil), build())
			if err != nil {
				t.Fatalf("OnResume() error = %v: a delegation the node cannot apply is "+
					"a signal it declines, not a reason to fail the execution", err)
			}
			if !out.Resuspend {
				t.Fatalf("an unusable delegation resolved the gate (port %q)", out.Port)
			}
			if got := ledgerOf(t, out.Data); len(got) != 0 {
				t.Fatalf("ledger = %v, want none: the delegation was not applied, so "+
					"it must not be on the record either", got)
			}
			assertIgnored(t, out.Data, 0, reasonMalformedAssignee)
		})
	}
}

func TestApprovalDelegate_ReplacesRatherThanGrowsTheChain(t *testing.T) {
	// Delegation hands over an existing slot rather than adding one, so the gate
	// opens on the same number of decisions it always needed. A chain that grew
	// instead would leave the delegator as a slot nobody owes an answer for, and
	// the gate would wait for them forever.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		delegateSignal("alice", "newcomer", "on leave"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	for _, approver := range []string{"newcomer", "bob"} {
		next, err := sh.OnResume(context.Background(),
			approvalInput(node.ApprovalAll, out.Data),
			approvalSignal(approver, "approve", "ok"))
		if err != nil {
			t.Fatalf("OnResume(%s) error = %v", approver, err)
		}
		out = next
		if approver == "newcomer" && !out.Resuspend {
			t.Fatalf("the gate approved (port %q) after one of the two slots had "+
				"decided: delegating grew the chain", out.Port)
		}
	}
	if out.Port != "approved" {
		t.Fatalf("Port = %q, want approved after two decisions, the number of slots "+
			"the configured chain has", out.Port)
	}
}

func TestApprovalDelegate_IsRefusedOutOfTurnInASequence(t *testing.T) {
	// A sequence admits only the current slot holder's decisions, and handing a
	// slot on is a decision about that slot like any other.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, nil),
		delegateSignal("bob", "dave", "let dave handle it"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("an out-of-turn delegation resolved the gate (port %q)", out.Port)
	}
	if got := ledgerOf(t, out.Data); len(got) != 0 {
		t.Fatalf("ledger = %v, want none: a delegation refused for being out of "+
			"turn must not be on the record", got)
	}
	assertIgnored(t, out.Data, 0, reasonNotCurrentApprover)
}

func TestApprovalAddSigner_AddsASlotTheGateThenWaitsFor(t *testing.T) {
	sh := approvalHandler(t)
	added, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		addSignerSignal("alice", "erin", "after", "needs a second pair of eyes"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !added.Resuspend {
		t.Fatalf("adding a signer resolved the gate (port %q)", added.Port)
	}
	assertDecision(t, added.Data, "decisions", 0, "alice", "add_signer", "needs a second pair of eyes")

	// Both original approvers decide; the gate stays open, because the signer
	// who was added is part of what it is waiting on now.
	state := added.Data
	for _, approver := range []string{"alice", "bob"} {
		out, err := sh.OnResume(context.Background(),
			approvalInput(node.ApprovalAll, state),
			approvalSignal(approver, "approve", "ok"))
		if err != nil {
			t.Fatalf("OnResume(%s) error = %v", approver, err)
		}
		if !out.Resuspend {
			t.Fatalf("the gate approved (port %q) after %s, with the added signer "+
				"still unreached: adding a signer changed nothing", out.Port, approver)
		}
		state = out.Data
	}

	done, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, state),
		approvalSignal("erin", "approve", "reviewed"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if done.Port != "approved" {
		t.Fatalf("Port = %q, want approved once the added signer had decided too", done.Port)
	}
}

func TestApprovalAddSigner_IgnoresAnUnusablePosition(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		addSignerSignal("alice", "erin", "sideways", "put her somewhere"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("an unusable position resolved the gate (port %q)", out.Port)
	}
	if got := ledgerOf(t, out.Data); len(got) != 0 {
		t.Fatalf("ledger = %v, want none: a position the node does not recognize "+
			"must not be guessed at", got)
	}
	assertIgnored(t, out.Data, 0, reasonMalformedPosition)
}

func TestApprovalAddSigner_DefaultsToSigningAfterTheRequester(t *testing.T) {
	// A payload with no position puts the signer after the requester, which is
	// only observable in a sequence: the requester keeps their turn. A default
	// of "before" would quietly hand the turn to the signer instead.
	sh := approvalHandler(t)
	added, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, nil),
		addSignerSignal("alice", "erin", "", "no position given"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if got := decisionsOf(t, added.Data, "decisions"); got[0]["position"] != "after" {
		t.Fatalf("position = %v, want after: the default is recorded, so the trail "+
			"says where the signer actually went", got[0]["position"])
	}

	byAlice, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, added.Data),
		approvalSignal("alice", "approve", "signing"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !byAlice.Resuspend {
		t.Fatalf("the gate resolved on alice's approval (port %q); she still holds "+
			"the first turn", byAlice.Port)
	}
	assertDecision(t, byAlice.Data, "decisions", 1, "alice", "approve", "signing")
}

func TestApprovalSequential_AddSignerBeforeYieldsTheTurn(t *testing.T) {
	// In a sequence the chain is an order, so this is where "before" means
	// something: alice asks erin to sign first, and alice's own turn moves behind
	// hers. Without that, alice would hold a turn she has just given away.
	sh := approvalHandler(t)
	added, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, nil),
		addSignerSignal("alice", "erin", "before", "she signs first"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	aliceTooSoon, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, added.Data),
		approvalSignal("alice", "approve", "signing anyway"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !aliceTooSoon.Resuspend {
		t.Fatalf("alice's approval resolved the gate (port %q) while erin, whose turn "+
			"had been moved ahead of her, had not decided", aliceTooSoon.Port)
	}
	assertIgnored(t, aliceTooSoon.Data, 0, reasonNotCurrentApprover)

	byErin, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalSequential, aliceTooSoon.Data),
		approvalSignal("erin", "approve", "reviewed first"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !byErin.Resuspend {
		t.Fatalf("erin's approval resolved the gate (port %q) with alice and bob "+
			"still to decide", byErin.Port)
	}
}

func TestApprovalAddSigner_RefusesToGrowTheChainPastItsLimit(t *testing.T) {
	// Every added signer is a slot to wait for and an entry in the node's stored
	// output, and a newly added signer can ask for one of their own, so the chain
	// would otherwise grow for as long as somebody kept asking.
	sh := approvalHandler(t)

	// The chain is built out of the ledger, so a chain at its limit is seeded the
	// way a real one would be: one add_signer decision per extra slot.
	seeded := make([]map[string]any, 0, maxChainInTest-1)
	for i := 0; len(seeded) < maxChainInTest-1; i++ {
		seeded = append(seeded, map[string]any{
			"approver": "alice",
			"action":   "add_signer",
			"assignee": fmt.Sprintf("signer-%d", i),
			"position": "after",
		})
	}
	input := approvalInput(node.ApprovalAll, map[string]any{"_decisions": seeded})

	out, err := sh.OnResume(context.Background(), input,
		addSignerSignal("alice", "newcomer", "after", "one more"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signer was added to a chain already at its limit (port %q)", out.Port)
	}
	if got := ledgerOf(t, out.Data); len(got) != len(seeded) {
		t.Fatalf("ledger grew to %d entries, want it unchanged at %d: the signer was "+
			"added despite the limit", len(got), len(seeded))
	}
	assertIgnored(t, out.Data, 0, reasonChainLimitReached)
}
