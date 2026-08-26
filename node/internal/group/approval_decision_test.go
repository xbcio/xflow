package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// approval_test.go is thorough about which approver is ALLOWED to decide — it
// pins unauthorized approvers, duplicate approvers, and the non-current approver
// in sequential mode. What it never asserts is what the node then DOES with a
// decision it accepted, on any single-signal path. Every one of those tests ends
// at `err == nil`.
//
// That leaves the single most consequential output in this node unpinned: the
// port and the approved flag on a reject. Swapping `Port: "rejected"` for
// `Port: "approved"` and `"approved": false` for true in OnResume's reject arm
// leaves the whole node package green, so an authorized approver who explicitly
// declined would route the workflow down the approved branch. There is no error
// and no log line on that path — the decision is simply inverted, and the
// decisions list still faithfully records "reject" while the routing says
// otherwise, which makes the audit trail actively misleading rather than absent.
//
// The same hole covers the timeout arm (default action "route" vs "reject"), the
// ApprovalAny approve path, the "return" action, and ApprovalAll's rule that a
// single signature must NOT complete the node.

func TestApproval_OnResumeRejectDoesNotRouteToApproved(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		approvalSignal("alice", "reject", "budget not available"))
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
		approvalSignal("bob", "approve", "looks good"))
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

func TestApprovalAll_OnResumeOneSignatureDoesNotCompleteTheNode(t *testing.T) {
	// ApprovalAll's entire contract is "everybody signs". approval_test.go pins
	// that rule on the multi-signal path (handleAllSignals) but not on this one,
	// which is the path taken whenever the signals arrive one at a time. Dropping
	// the len(decisions) < len(approvers) branch lets the first approver's
	// signature complete the node on behalf of everyone.
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
	// _decisions is the key PrepareSuspend reads to decide it must fall back to a
	// shared signal on the next round; decisions is the one an auditor reads.
	// Both are written here and the two are not interchangeable.
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
			signal := approvalSignal("", "", "")
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

func TestApproval_OnResumeReturnResuspendsWithoutDeciding(t *testing.T) {
	// "return" means the approver handed the request back for revision. It is
	// neither an approval nor a rejection, and emitting on either port would end
	// the wait — the node must keep waiting instead.
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		approvalSignal("alice", "return", "needs more detail"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}

	if !out.Resuspend {
		t.Fatal("a returned request stopped waiting; the approval is resolved " +
			"without anybody having approved or rejected it")
	}
	if out.Port != "" {
		t.Fatalf("a returned request emitted on port %q", out.Port)
	}
	if _, ok := out.Data["approved"]; ok {
		t.Fatalf("approved = %v was recorded for a request that was handed back", out.Data["approved"])
	}
}
