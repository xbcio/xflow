package group_test

import (
	"context"
	"testing"
	"time"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// approval_test.go is thorough about which approver is ALLOWED to decide — it
// pins unauthorized approvers, repeated decisions, and the out-of-turn
// approver in sequential mode. What it never asserts is what the node then DOES
// with a decision it accepted. Every one of those tests ends at `err == nil`.
//
// That leaves the single most consequential output in this node unpinned: the
// port and the approved flag on a reject. Swapping `Port: "rejected"` for
// `Port: "approved"` and `"approved": false` for true in the reject arm leaves
// the whole node package green, so an authorized approver who explicitly
// declined would route the workflow down the approved branch. There is no error
// and no log line on that path — the decision is simply inverted, and the
// decisions list still faithfully records "reject" while the routing says
// otherwise, which makes the audit trail actively misleading rather than absent.
//
// The same hole covers the timeout arm (default action "route" vs "reject"), the
// ApprovalAny approve path, and all mode's rule that a single signature must NOT
// complete the node.

func TestApproval_OnResumeRejectDoesNotRouteToApproved(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		sharedApprovalSignal("alice", "reject", "budget not available"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	if out.Port != "rejected" {
		t.Fatalf("an explicit reject routed to port %q, want rejected: the workflow "+
			"continues down the approved branch after somebody declined", out.Port)
	}
	if approved, ok := out.Data["approved"].(bool); !ok || approved {
		t.Fatalf("approved = %v (%T) after a reject, want false: downstream nodes "+
			"read this flag rather than the port", out.Data["approved"], out.Data["approved"])
	}
	if out.Resuspend {
		t.Fatal("a reject resuspended the node; the decision is final and the " +
			"workflow would wait for a second signal that never comes")
	}
	if got := out.Data["approver"]; got != "alice" {
		t.Fatalf("approver = %v, want alice: the audit trail does not name who declined", got)
	}
	if got := out.Data["comment"]; got != "budget not available" {
		t.Fatalf("comment = %v, want the reason the approver gave", got)
	}
	// The decisions list is what an auditor reads back. It must agree with the
	// port, not merely exist.
	assertDecision(t, out.Data, "decisions", 0, "alice", "reject", "budget not available")
}

func TestApprovalAny_OnResumeApproveRoutesToApprovedPort(t *testing.T) {
	// The mirror of the above. Without it, "rejected" could be hard-coded on both
	// arms and only one of the two tests would notice.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, map[string]any{"order_id": "ord-1"}),
		sharedApprovalSignal("bob", "approve", "looks good"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	if out.Port != "approved" {
		t.Fatalf("an approve routed to port %q, want approved", out.Port)
	}
	if approved, ok := out.Data["approved"].(bool); !ok || !approved {
		t.Fatalf("approved = %v (%T), want true", out.Data["approved"], out.Data["approved"])
	}
	if got := out.Data["approver"]; got != "bob" {
		t.Fatalf("approver = %v, want bob", got)
	}
	// Upstream data has to survive the decision: the node sits mid-workflow and
	// everything downstream of it reads the payload it was approving.
	if got := out.Data["order_id"]; got != "ord-1" {
		t.Fatalf("order_id = %v, want ord-1: the payload under approval was dropped "+
			"and every downstream node now sees an empty envelope", got)
	}
}

func TestApproval_DecisionRecordsWhenItWasMade(t *testing.T) {
	// An approval is a business decision somebody is accountable for, and the
	// ledger is the record of it. A record that says who decided but not when
	// cannot establish the order decisions were made in, or whether one landed
	// before a deadline — so the moment is part of the decision, not decoration.
	sh := approvalHandler(t)
	// Truncated to the second the ledger records in: RFC3339 carries no
	// sub-second precision, so a finer bound would read as the stamp being
	// early by less than a second.
	before := time.Now().UTC().Truncate(time.Second)

	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	decisions := decisionsOf(t, out.Data, "decisions")
	raw, ok := decisions[0]["at"].(string)
	if !ok {
		t.Fatalf("at = %v (%T), want an RFC3339 timestamp", decisions[0]["at"], decisions[0]["at"])
	}
	at, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		t.Fatalf("at = %q, want a parseable RFC3339 timestamp: %v", raw, err)
	}
	if at.Before(before) || at.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("at = %v, want the moment the decision was made (between %v and now)", at, before)
	}
}

func TestApprovalAll_OnResumeOneSignatureDoesNotCompleteTheNode(t *testing.T) {
	// The rule that makes countersign countersign: the gate opens only once
	// every approver has decided. Dropping the len(decisions) < len(approvers)
	// branch lets the first approver's signature complete it on behalf of
	// everyone.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAll, nil),
		approvalSignal("alice", "approve", "one of two"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	if !out.Resuspend {
		t.Fatalf("one of two approvers completed the node (port %q, data %v); the "+
			"second approver is never asked", out.Port, out.Data)
	}
	if out.Port != "" {
		t.Fatalf("a partially approved node emitted on port %q; downstream runs "+
			"before the approval finished", out.Port)
	}
	if _, ok := out.Data["approved"]; ok {
		t.Fatalf("approved = %v is already set with one signature outstanding", out.Data["approved"])
	}
	// _decisions is this node's own ledger and the only key its state is read
	// from; decisions is the public copy downstream reads. They are written
	// together but are not interchangeable — reading the public one back would
	// let an upstream node's output pass for votes.
	assertDecision(t, out.Data, "_decisions", 0, "alice", "approve", "one of two")
	assertDecision(t, out.Data, "decisions", 0, "alice", "approve", "one of two")
}

func TestApproval_OnResumeTimeoutRoutesByConfiguredAction(t *testing.T) {
	// "route" is the DEFAULT timeout_action, so this arm is what every approval
	// node with a timeout and no explicit action does. Confusing the two arms
	// turns an unanswered request into a recorded rejection, or a configured
	// auto-reject into a silent pass down the timeout branch.
	sh := approvalHandler(t)

	for _, tc := range []struct {
		name       string
		action     string
		wantPort   string
		wantDecide bool // whether "approved" must be present at all
	}{
		{name: "default is route", action: "", wantPort: "timeout", wantDecide: false},
		{name: "explicit reject", action: "reject", wantPort: "rejected", wantDecide: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := approvalInput(node.ApprovalAny, nil)
			input.Params["timeout"] = "48h"
			if tc.action != "" {
				input.Params["timeout_action"] = tc.action
			}
			signal := sharedApprovalSignal("", "", "")
			signal.Triggered = types.TimeoutFired

			out, err := sh.OnResume(context.Background(), input, signal)
			if err != nil {
				t.Fatalf("OnResume() error = %v", err)
			}
			if out.Port != tc.wantPort {
				t.Fatalf("timeout with action %q routed to %q, want %q",
					tc.action, out.Port, tc.wantPort)
			}
			if got := out.Data["reason"]; got != "timeout" {
				t.Fatalf("reason = %v, want timeout: nothing downstream can tell this "+
					"apart from a real decision", got)
			}
			approved, present := out.Data["approved"]
			if present != tc.wantDecide {
				t.Fatalf("approved present = %v (value %v), want present = %v: routing "+
					"on timeout must not claim a decision nobody made",
					present, approved, tc.wantDecide)
			}
			if tc.wantDecide {
				if b, ok := approved.(bool); !ok || b {
					t.Fatalf("approved = %v (%T) on an auto-reject, want false", approved, approved)
				}
			}
		})
	}
}

func TestApproval_OnResumeReturnRoutesToTheReturnedPort(t *testing.T) {
	// "return" means the approver handed the request back for revision. That is
	// a decision about the request — it must leave the gate — and it is neither
	// an approval nor a rejection, so it needs a port of its own. The alternative
	// the node previously took, silently resuspending, left the request parked
	// waiting for a decision that had already been made: the approver saw their
	// return accepted and nothing happened.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		sharedApprovalSignal("alice", "return", "needs more detail"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	if out.Port != "returned" {
		t.Fatalf("a returned request emitted on port %q, want returned", out.Port)
	}
	if out.Resuspend {
		t.Fatal("a returned request kept waiting; the gate is still holding a " +
			"decision that was already handed back")
	}
	if returned, ok := out.Data["returned"].(bool); !ok || !returned {
		t.Fatalf("returned = %v (%T), want true", out.Data["returned"], out.Data["returned"])
	}
	if approved, ok := out.Data["approved"].(bool); !ok || approved {
		t.Fatalf("approved = %v (%T) on a returned request, want false", out.Data["approved"], out.Data["approved"])
	}
	if got := out.Data["approver"]; got != "alice" {
		t.Fatalf("approver = %v, want alice", got)
	}
	if got := out.Data["comment"]; got != "needs more detail" {
		t.Fatalf("comment = %v, want the reason the approver gave", got)
	}
	assertDecision(t, out.Data, "decisions", 0, "alice", "return", "needs more detail")
}

func TestApprovalAll_OnResumeReturnIsFinalWithoutTheRemainingApprovers(t *testing.T) {
	// The same short-circuit as a reject, for the same reason: once the request
	// is on its way back for revision, a vote from an approver who has not
	// answered yet cannot change where it went.
	sh := node.Approval([]string{"alice", "bob", "carol"}, node.ApprovalAll)
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
	out, err := sh.OnResume(context.Background(), input, approvalSignal("bob", "return", "resubmit the docs"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "returned" {
		t.Fatalf("Port = %q, want returned: carol had not answered yet, so the gate "+
			"must close now rather than wait on a vote that can no longer change "+
			"where the request is going", out.Port)
	}
	decisions := decisionsOf(t, out.Data, "decisions")
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want 2 (alice's approval and bob's return)", len(decisions))
	}
	if decisions[1]["approver"] != "bob" || decisions[1]["action"] != "return" {
		t.Fatalf("decisions[1] = %#v, want bob's return", decisions[1])
	}
}
