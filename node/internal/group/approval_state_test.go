package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
)

// State that has been through persisted storage comes back in the shapes a JSON
// round trip produces, not the native Go shapes an in-process call builds: a
// ledger is []any of map[string]any, and a count is float64. Every other test
// constructs these by hand in their native form, so the decoding branches would
// otherwise be unexercised — and a node that silently drops a stored ledger
// starts the approval over with nobody having voted.

func TestApprovalSequential_OnResumeCarriesJSONShapedDecisionHistory(t *testing.T) {
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_decisions": []any{
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
	decisions := decisionsOf(t, out.Data, "decisions")
	if len(decisions) != 2 {
		t.Fatalf("len(decisions) = %d, want 2: alice's decision, stored in the "+
			"[]any shape a JSON round trip produces, was dropped", len(decisions))
	}
	if decisions[0]["approver"] != "alice" || decisions[1]["approver"] != "bob" {
		t.Fatalf("decisions = %#v, want approver order alice,bob", decisions)
	}
}

func TestApprovalSequential_OrderCheckReadsAJSONShapedLedger(t *testing.T) {
	// The same stored shape, but for the decision the order check depends on:
	// if alice's stored approval is not recognized, bob becomes the current
	// approver and a chain that should wait on alice advances past her.
	sh := approvalHandler(t)
	input := approvalInput(node.ApprovalSequential, map[string]any{
		"_decisions": []any{
			map[string]any{"approver": "alice", "action": "approve"},
		},
	})

	out, err := sh.OnResume(context.Background(), input, approvalSignal("alice", "approve", "again"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("alice was accepted a second time (port %q): her stored approval "+
			"was not read, so the ledger no longer showed her as having decided", out.Port)
	}
}

func TestApproval_IgnoredCountSurvivesAJSONRoundTrip(t *testing.T) {
	sh := approvalHandler(t)
	// A count that came back from storage is a float64, as JSON decoding
	// produces. Read as an int it would look like zero, and every further
	// refusal would overwrite the running total with 1.
	input := approvalInput(node.ApprovalAny, map[string]any{
		"_ignored_count": float64(3),
	})

	out, err := sh.OnResume(context.Background(),
		input, sharedApprovalSignal("mallory", "approve", ""))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if got := out.Data["_ignored_count"]; got != 4 {
		t.Fatalf("_ignored_count = %v (%T), want 4", got, got)
	}
}
