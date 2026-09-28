package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/types"
)

// "any" mode addresses every approver on one signal name, so the name an
// approver is entitled to answer on differs by mode. A node that accepted the
// per-approver name in "any" mode would resolve the gate on a signal delivered
// under a name the specification for that mode never declares, and the reverse
// mistake — requiring the per-approver name in "any" mode — makes the gate
// unanswerable, since it arms only the shared name.

func TestApprovalAny_IgnoresAPerApproverSignalName(t *testing.T) {
	sh := approvalHandler(t)
	// approvalSignal builds the per-approver form, which is not the name armed
	// in "any" mode; sharedApprovalSignal is.
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		approvalSignal("alice", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("a signal on an undeclared name resolved an any-mode gate (port %q)", out.Port)
	}
	assertIgnored(t, out.State, 0, reasonSignalNameMismatch)
}

func TestApprovalAny_AcceptsTheArmedSharedSignalName(t *testing.T) {
	sh := approvalHandler(t)
	out, err := sh.OnResume(context.Background(),
		approvalInput(node.ApprovalAny, nil),
		sharedApprovalSignal("bob", "approve", "ok"))
	if err != nil {
		t.Fatalf("OnResume() error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("port = %q, want approved: any one approver decides in any mode", out.Port)
	}
}

// TestApproval_PrepareSuspendRejectsUnknownMode pins the fall-through error at
// the bottom of PrepareSuspend's switch.
//
// ApprovalMode is an exported string type, so node.Approval(approvers,
// node.ApprovalMode("aproved")) compiles and passes review as readily as the
// correct spelling. No test supplies anything but "any"/"all"/"sequential", so
// replacing the error with `return nil, nil` leaves every importing package
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
