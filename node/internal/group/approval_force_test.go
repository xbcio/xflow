package group_test

import (
	"context"
	"testing"

	"github.com/xbcio/xflow/node"
	"github.com/xbcio/xflow/node/registry"
	"github.com/xbcio/xflow/types"
)

// forceInput builds a resumption input for a gate that names force approvers.
// A nil list leaves the param out entirely, which is how a definition that does
// not configure the action looks.
func forceInput(mode node.ApprovalMode, forceApprovers []any, state map[string]any) *types.Input {
	input := approvalInput(mode, state)
	if forceApprovers != nil {
		input.Params["force_approvers"] = forceApprovers
	}
	return input
}

// forcePayload builds the signal a force approver sends. Like every other
// signal, the identity is the server-set actor key and never a field the caller
// chose to write.
func forcePayload(actor string, comment string) *types.SignalPayload {
	return &types.SignalPayload{
		Triggered: types.SignalReceived,
		Name:      "approval_1/approval/force",
		Data: map[string]any{
			types.VerifiedActorKey: actor,
			"action":               "force",
			"comment":              comment,
		},
	}
}

// approvedBy is the node's private state after one approver has decided, which
// is what a resumption of a partly-decided gate reads back.
func approvedBy(approver string) map[string]any {
	return map[string]any{
		"decisions": []map[string]any{
			{"approver": approver, "action": "approve", "comment": "ok"},
		},
	}
}

func TestApprovalForce_PassesTheGateAndRecordsWhoWasBypassed(t *testing.T) {
	h := approvalHandler(t)
	// alice has answered; bob has not. The force approver is not an approver of
	// this gate at all -- that is the point of the action.
	out, err := h.OnResume(context.Background(),
		forceInput(node.ApprovalAll, []any{"vp"}, approvedBy("alice")),
		forcePayload("vp", "decided at the review"))
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("port = %q, want approved", out.Port)
	}
	if out.Data["approved"] != true || out.Data["forced"] != true {
		t.Fatalf("data = %v, want approved and forced", out.Data)
	}
	if out.Data["approver"] != "vp" {
		t.Fatalf("approver = %v, want vp", out.Data["approver"])
	}
	bypassed, ok := out.Data["bypassed"].([]string)
	if !ok {
		t.Fatalf("bypassed = %#v (%T), want a list of the passed-over approvers", out.Data["bypassed"], out.Data["bypassed"])
	}
	if len(bypassed) != 1 || bypassed[0] != "bob" {
		t.Fatalf("bypassed = %v, want [bob]: alice had already decided", bypassed)
	}
	// The force is on the same ledger as every other decision, so the record
	// says who opened the gate rather than only that it opened.
	assertDecision(t, out.State, "decisions", 1, "vp", "force", "decided at the review")
}

func TestApprovalForce_AForceApproverWhoAlreadyDecidedCanStillForce(t *testing.T) {
	h := approvalHandler(t)
	// The force list and the chain overlap here. A force is a statement about
	// the gate, not a second slot decision, so the repeat rule that ignores a
	// repeated approval does not apply to it.
	out, err := h.OnResume(context.Background(),
		forceInput(node.ApprovalAll, []any{"alice"}, approvedBy("alice")),
		forcePayload("alice", "closing this out"))
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("port = %q, want approved", out.Port)
	}
	bypassed, _ := out.Data["bypassed"].([]string)
	if len(bypassed) != 1 || bypassed[0] != "bob" {
		t.Fatalf("bypassed = %v, want [bob]", bypassed)
	}
}

func TestApprovalForce_IgnoresAnActorOutsideTheForceList(t *testing.T) {
	h := approvalHandler(t)
	// bob holds a slot in the chain but is not a force approver: being an
	// approver of this gate must not be enough to override the other slots.
	out, err := h.OnResume(context.Background(),
		forceInput(node.ApprovalAll, []any{"vp"}, nil),
		forcePayload("bob", "let me through"))
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("output = %+v, want the gate left waiting", out)
	}
	if out.Port != "" {
		t.Fatalf("port = %q, want no port: the gate must not have moved", out.Port)
	}
	assertIgnored(t, out.State, 0, "unauthorized-force-approver")
	if decisions, ok := out.State["decisions"].([]map[string]any); ok && len(decisions) != 0 {
		t.Fatalf("decisions = %v, want the refused force not on the record", decisions)
	}
}

func TestApprovalForce_IsUnreachableWhenNoForceApproversAreConfigured(t *testing.T) {
	h := approvalHandler(t)
	t.Run("the signal is never armed", func(t *testing.T) {
		spec, err := h.PrepareSuspend(context.Background(), approvalInput(node.ApprovalAll, nil))
		if err != nil {
			t.Fatalf("PrepareSuspend() error = %v", err)
		}
		for _, name := range spec.Signals {
			if name == "approval_1/approval/force" {
				t.Fatalf("signals = %v, want no force name on a gate nobody may force", spec.Signals)
			}
		}
	})

	t.Run("the signal is refused even from a force name", func(t *testing.T) {
		// No force list, so there is no actor the action could be authorized
		// for -- not the chain's own approvers either.
		out, err := h.OnResume(context.Background(), approvalInput(node.ApprovalAll, nil), forcePayload("alice", ""))
		if err != nil {
			t.Fatalf("OnResume(force) error = %v", err)
		}
		if !out.Resuspend {
			t.Fatalf("output = %+v, want the gate left waiting", out)
		}
		assertIgnored(t, out.State, 0, "unauthorized-force-approver")
	})
}

func TestApprovalForce_IgnoresASignalWithNoVerifiedActor(t *testing.T) {
	h := approvalHandler(t)
	signal := forcePayload("vp", "trust me")
	delete(signal.Data, types.VerifiedActorKey)

	out, err := h.OnResume(context.Background(), forceInput(node.ApprovalAny, []any{"vp"}, nil), signal)
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if !out.Resuspend || out.Port != "" {
		t.Fatalf("output = %+v, want the gate left waiting", out)
	}
	assertIgnored(t, out.State, 0, "unverified-actor")
}

func TestApprovalForce_IgnoresTheForceActionOnAnApproversOwnName(t *testing.T) {
	h := approvalHandler(t)
	// A force approver still has to send it to the force name. Accepting it on
	// an approver's name would make the name in the payload interchangeable with
	// the authority behind it.
	signal := forcePayload("vp", "here")
	signal.Name = "approval_1/approval/bob"

	out, err := h.OnResume(context.Background(), forceInput(node.ApprovalAll, []any{"vp"}, nil), signal)
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("output = %+v, want the gate left waiting", out)
	}
	assertIgnored(t, out.State, 0, "signal-name-mismatch")
}

func TestApprovalForce_PassesTheGateWithoutTheCurrentSequentialApprover(t *testing.T) {
	h := approvalHandler(t)
	// The gate is waiting on alice's turn and nobody has decided. A force
	// approval is the one action that does not queue behind the turn order.
	out, err := h.OnResume(context.Background(),
		forceInput(node.ApprovalSequential, []any{"vp"}, nil),
		forcePayload("vp", "urgent"))
	if err != nil {
		t.Fatalf("OnResume(force) error = %v", err)
	}
	if out.Port != "approved" {
		t.Fatalf("port = %q, want approved", out.Port)
	}
	bypassed, _ := out.Data["bypassed"].([]string)
	if len(bypassed) != 2 || bypassed[0] != "alice" || bypassed[1] != "bob" {
		t.Fatalf("bypassed = %v, want [alice bob] in chain order", bypassed)
	}
}

func TestApprovalForce_ArmsTheSignalInEveryMode(t *testing.T) {
	h := approvalHandler(t)
	for _, mode := range []node.ApprovalMode{node.ApprovalAny, node.ApprovalAll, node.ApprovalSequential} {
		t.Run(string(mode), func(t *testing.T) {
			spec, err := h.PrepareSuspend(context.Background(), forceInput(mode, []any{"vp"}, nil))
			if err != nil {
				t.Fatalf("PrepareSuspend() error = %v", err)
			}
			var armed bool
			for _, name := range spec.Signals {
				if name == "approval_1/approval/force" {
					armed = true
				}
			}
			if !armed {
				t.Fatalf("signals = %v, want the force name among them", spec.Signals)
			}
		})
	}
}

func TestApprovalForce_AnApproverNamedForceStillDecidesTheirOwnSlot(t *testing.T) {
	h := approvalHandler(t)
	// The force signal name collides with the per-approver name of an approver
	// called "force". The action decides which rule applies, so this approver's
	// ordinary decision is still read as their own slot and not as a force.
	input := approvalInput(node.ApprovalAll, nil)
	input.Params["approvers"] = []any{"alice", "force"}

	out, err := h.OnResume(context.Background(), input,
		approvalSignal("force", "approve", "mine"))
	if err != nil {
		t.Fatalf("OnResume(approve) error = %v", err)
	}
	if !out.Resuspend {
		t.Fatalf("output = %+v, want the gate still waiting on alice", out)
	}
	if out.Port != "" {
		t.Fatalf("port = %q, want no port while alice is outstanding", out.Port)
	}
	if out.Data["forced"] != nil {
		t.Fatalf("data = %v, want no forced marker on an ordinary approval", out.Data)
	}
	assertDecision(t, out.State, "decisions", 0, "force", "approve", "mine")
}

func TestApprovalForce_RejectsAMalformedForceList(t *testing.T) {
	h := approvalHandler(t)
	input := approvalInput(node.ApprovalAny, nil)
	input.Params["force_approvers"] = "vp"

	if _, err := h.PrepareSuspend(context.Background(), input); err == nil {
		t.Fatal("PrepareSuspend(force_approvers as a string) error = nil, want a rejection")
	}
}

func TestApprovalForce_BuilderExposesTheForceList(t *testing.T) {
	b := node.Approval([]string{"alice"}, node.ApprovalAny).WithForceApprovers([]string{"vp"})
	params := b.RawParams().(map[string]any)
	got, ok := params["force_approvers"].([]string)
	if !ok || len(got) != 1 || got[0] != "vp" {
		t.Fatalf("force_approvers = %#v, want [vp]", params["force_approvers"])
	}

	handler, ok := registry.Lookup(node.ApprovalNodeType)
	if !ok {
		t.Fatal("approval node is not registered")
	}
	var declared bool
	for _, spec := range handler.Descriptor().Params {
		if spec.Name == "force_approvers" {
			declared = true
		}
	}
	if !declared {
		t.Fatalf("descriptor params = %v, want force_approvers declared", handler.Descriptor().Params)
	}
}
